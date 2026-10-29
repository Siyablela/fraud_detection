import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from app.database import (
    MessageProvenance,
    RecordOutcome,
    TransactionFilters,
    create_rule_set_version,
    get_latest_rule_set,
    get_velocity_aggregates,
    list_rule_set_versions,
    list_transactions,
    record_decision,
)
from app.errors import ConflictingDuplicateError, RuleSetVersionConflictError
from app.rules import DEFAULT_RULE_SET_DEFINITION, content_hash, parse_rule_set_definition
from tests.conftest import make_decision

pytestmark = [pytest.mark.integration, pytest.mark.anyio]

T0 = datetime(2026, 10, 29, 12, tzinfo=UTC)


def provenance(offset: int, partition: int = 0) -> MessageProvenance:
    return MessageProvenance(topic="transactions", partition=partition, offset=offset, timestamp_ms=1)


async def history_rows(session_factory, transaction_id):
    async with session_factory() as session:
        result = await session.execute(
            sa.text("SELECT outcome, source_offset FROM transaction_history WHERE transaction_id = :t ORDER BY id"),
            {"t": transaction_id},
        )
        return [tuple(row) for row in result]


class TestRecordDecision:
    async def test_first_write_is_evaluated_with_audit_row(self, session_factory):
        decision = make_decision(amount=Decimal("10000.00"), is_fraud=True, triggered_rules=["HIGH_VALUE_THRESHOLD"])
        result = await record_decision(session_factory, decision, provenance(1))
        assert result.outcome is RecordOutcome.EVALUATED
        assert await history_rows(session_factory, decision.transaction_id) == [("evaluated", 1)]

    async def test_redelivery_of_same_message_is_a_no_op(self, session_factory):
        decision = make_decision()
        await record_decision(session_factory, decision, provenance(1))
        again = await record_decision(session_factory, decision, provenance(1))
        assert again.outcome is RecordOutcome.DUPLICATE
        assert await history_rows(session_factory, decision.transaction_id) == [("evaluated", 1)]

    async def test_republished_event_keeps_first_decision_and_audits_duplicate(self, session_factory):
        first = make_decision(is_fraud=False, triggered_rules=[])
        await record_decision(session_factory, first, provenance(1))
        # Same transaction, re-evaluated later with different context.
        second = make_decision(
            transaction_id=first.transaction_id,
            user_id=first.user_id,
            is_fraud=True,
            triggered_rules=["VELOCITY_COUNT_EXCEEDED"],
            rules_version="fraud-default@v2",
        )
        result = await record_decision(session_factory, second, provenance(9))
        assert result.outcome is RecordOutcome.DUPLICATE
        assert result.decision.is_fraud is False and result.decision.rules_version == "fraud-default@v1"
        assert await history_rows(session_factory, first.transaction_id) == [("evaluated", 1), ("duplicate", 9)]

    async def test_same_id_with_different_data_is_rejected_atomically(self, session_factory):
        first = make_decision()
        await record_decision(session_factory, first, provenance(1))
        conflicting = make_decision(transaction_id=first.transaction_id, user_id=first.user_id, amount=Decimal("1.00"))
        with pytest.raises(ConflictingDuplicateError):
            await record_decision(session_factory, conflicting, provenance(2))
        assert await history_rows(session_factory, first.transaction_id) == [("evaluated", 1)]

    async def test_database_enforces_positive_amount(self, session_factory):
        with pytest.raises(IntegrityError):
            await record_decision(session_factory, make_decision(amount=Decimal("-1.00")), provenance(1))


class TestVelocityAggregates:
    async def test_window_is_inclusive_and_excludes_current_and_other_users(self, session_factory):
        user_id = uuid4()
        offsets = iter(range(100))
        for seconds, amount in ((-61, "1.00"), (-60, "2.00"), (-30, "3.00"), (0, "4.00"), (1, "5.00")):
            await record_decision(
                session_factory,
                make_decision(user_id=user_id, amount=Decimal(amount), occurred_at=T0 + timedelta(seconds=seconds)),
                provenance(next(offsets)),
            )
        await record_decision(session_factory, make_decision(occurred_at=T0), provenance(next(offsets)))
        current = make_decision(user_id=user_id, occurred_at=T0)
        await record_decision(session_factory, current, provenance(next(offsets)))

        count, total = await get_velocity_aggregates(
            session_factory, user_id, T0 - timedelta(seconds=60), T0, exclude_transaction_id=current.transaction_id
        )
        assert (count, total) == (3, Decimal("9.00"))  # -60, -30 and 0; not -61, +1, other user, or itself

    async def test_no_history_returns_zero(self, session_factory):
        assert await get_velocity_aggregates(session_factory, uuid4(), T0, T0, uuid4()) == (0, Decimal("0"))


class TestListTransactions:
    async def test_keyset_pages_are_complete_ordered_and_disjoint(self, session_factory):
        user_id = uuid4()
        expected = []
        for index in range(7):
            decision = make_decision(user_id=user_id, occurred_at=T0 + timedelta(minutes=index // 2))
            await record_decision(session_factory, decision, provenance(index))
            expected.append(decision)
        await record_decision(session_factory, make_decision(occurred_at=T0), provenance(99))  # another user

        seen, cursor, pages = [], None, 0
        while True:
            rows, cursor = await list_transactions(session_factory, TransactionFilters(user_id=user_id), 3, cursor)
            seen.extend(rows)
            pages += 1
            if cursor is None:
                break
        assert pages == 3
        assert {row.transaction_id for row in seen} == {d.transaction_id for d in expected}
        keys = [(row.occurred_at, row.transaction_id) for row in seen]
        assert keys == sorted(keys, reverse=True) and len(set(keys)) == 7

    async def test_filters_combine(self, session_factory):
        await record_decision(session_factory, make_decision(category="CRYPTO", is_fraud=True), provenance(1))
        await record_decision(session_factory, make_decision(category="CRYPTO", is_fraud=False), provenance(2))
        await record_decision(
            session_factory,
            make_decision(category="GROCERIES", is_fraud=True, occurred_at=T0 + timedelta(days=1)),
            provenance(3),
        )
        rows, _ = await list_transactions(
            session_factory, TransactionFilters(category="CRYPTO", is_fraud=True), 10, None
        )
        assert len(rows) == 1
        rows, _ = await list_transactions(
            session_factory, TransactionFilters(occurred_from=T0 + timedelta(hours=1)), 10, None
        )
        assert [row.category for row in rows] == ["GROCERIES"]

    async def test_category_queries_use_the_index(self, session_factory):
        async with session_factory() as session:
            await session.execute(sa.text("SET enable_seqscan = off"))
            plan = "\n".join(
                row[0]
                for row in await session.execute(
                    sa.text(
                        "EXPLAIN SELECT * FROM transactions WHERE category = 'CRYPTO' "
                        "ORDER BY occurred_at DESC, transaction_id DESC LIMIT 51"
                    )
                )
            )
        assert "ix_transactions_category_occurred" in plan


class TestRuleSetVersions:
    async def test_migration_seeds_default_rule_set(self, session_factory):
        rule_set = await get_latest_rule_set(session_factory, "fraud-default")
        assert rule_set.version == 1
        assert rule_set.content_hash == content_hash(parse_rule_set_definition(DEFAULT_RULE_SET_DEFINITION))

    async def test_append_and_optimistic_concurrency(self, session_factory):
        definition = parse_rule_set_definition(DEFAULT_RULE_SET_DEFINITION)
        args = dict(
            key="fraud-default",
            definition=definition,
            definition_hash=content_hash(definition),
            description="v2",
            created_by="admin",
        )
        created = await create_rule_set_version(session_factory, expected_current_version=1, **args)
        assert created.version == 2 and created.created_by == "admin"
        with pytest.raises(RuleSetVersionConflictError, match="is 2, expected 1"):
            await create_rule_set_version(session_factory, expected_current_version=1, **args)
        new_key = await create_rule_set_version(
            session_factory, **{**args, "key": "brand-new"}, expected_current_version=0
        )
        assert new_key.version == 1
        versions = await list_rule_set_versions(session_factory, "fraud-default", limit=10, before_version=None)
        assert [version.version for version in versions] == [2, 1]

    async def test_concurrent_unconditional_writes_never_share_a_version(self, session_factory):
        definition = parse_rule_set_definition(DEFAULT_RULE_SET_DEFINITION)

        async def write():
            try:
                return await create_rule_set_version(
                    session_factory,
                    key="fraud-default",
                    definition=definition,
                    definition_hash=content_hash(definition),
                    description="race",
                    created_by="x",
                    expected_current_version=None,
                )
            except RuleSetVersionConflictError as exc:
                return exc

        results = await asyncio.gather(*(write() for _ in range(5)))
        versions = [result.version for result in results if not isinstance(result, Exception)]
        assert len(versions) == len(set(versions)) >= 1
        assert all(
            isinstance(result, RuleSetVersionConflictError) for result in results if not hasattr(result, "version")
        )
