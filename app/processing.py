"""Transport-independent scoring: event in, stored decision out.

Kept apart from the Kafka loop so it can be unit-tested with fakes and reused by
the API's dry-run endpoint.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol
from uuid import UUID

from app.database import (
    MessageProvenance,
    RecordResult,
    SessionFactory,
    TransactionDecision,
    get_velocity_aggregates,
    record_decision,
)
from app.events import parse_transaction_event
from app.observability import bind_correlation_id, get_logger
from app.rules import Assessment, EvaluationContext, RuleSet, evaluate_rule_set

logger = get_logger(__name__)


class ActiveRuleSetSource(Protocol):
    async def get_active(self) -> RuleSet: ...


@dataclass(frozen=True)
class Score:
    assessment: Assessment
    prior_count: int
    prior_amount: Decimal


async def score_transaction(
    session_factory: SessionFactory,
    rule_set: RuleSet,
    *,
    transaction_id: UUID | None,
    user_id: UUID,
    occurred_at: datetime,
    amount: Decimal,
    currency: str,
    category: str,
    channel: str | None = None,
    country: str | None = None,
    enrichment: dict[str, Any] | None = None,
) -> Score:
    """Evaluate ``rule_set`` for one transaction, including its velocity context.

    The velocity window is measured in *event* time, ``[occurred_at - window,
    occurred_at]``, so replays and consumer lag do not change the result.
    """
    window = timedelta(seconds=rule_set.definition.velocity_window_seconds)
    prior_count, prior_amount = await get_velocity_aggregates(
        session_factory,
        user_id=user_id,
        window_start=occurred_at - window,
        window_end=occurred_at,
        # A sentinel nil UUID never matches a real row.
        exclude_transaction_id=transaction_id or UUID(int=0),
        currency=currency,
    )
    context = EvaluationContext(
        amount=amount,
        currency=currency,
        category=category,
        prior_count=prior_count,
        prior_amount=prior_amount,
        channel=channel,
        country=country,
        enrichment=enrichment or {},
    )
    return Score(
        assessment=evaluate_rule_set(rule_set.definition, context),
        prior_count=prior_count,
        prior_amount=prior_amount,
    )


class TransactionProcessor:
    """Parse, score and persist one Kafka message value."""

    def __init__(self, session_factory: SessionFactory, rule_sets: ActiveRuleSetSource):
        self._session_factory = session_factory
        self._rule_sets = rule_sets

    async def process(self, value: bytes | None, provenance: MessageProvenance) -> RecordResult:
        """Raises the typed errors from :mod:`app.errors`; the caller decides retry vs DLQ."""
        event = parse_transaction_event(value)
        bind_correlation_id(event.correlation_id or str(event.transaction_id))
        enrichment = event.enrichment.model_dump(exclude_none=True) if event.enrichment else None

        rule_set = await self._rule_sets.get_active()
        score = await score_transaction(
            self._session_factory,
            rule_set,
            transaction_id=event.transaction_id,
            user_id=event.user_id,
            occurred_at=event.occurred_at,
            amount=event.amount,
            currency=event.currency,
            category=event.category,
            channel=event.channel,
            country=event.country,
            enrichment=enrichment,
        )
        assessment = score.assessment
        decision = TransactionDecision(
            transaction_id=event.transaction_id,
            user_id=event.user_id,
            amount=event.amount,
            currency=event.currency,
            category=event.category,
            occurred_at=event.occurred_at,
            risk_score=assessment.risk_score,
            verdict=assessment.verdict.value,
            is_fraud=assessment.is_fraud,
            triggered_rules=assessment.triggered_rules,
            rules_version=rule_set.label,
            rules_hash=rule_set.content_hash,
            correlation_id=event.correlation_id,
            channel=event.channel,
            country=event.country,
            # Stored as JSON: Decimal features become exact strings.
            enrichment=event.enrichment.model_dump(mode="json", exclude_none=True) if event.enrichment else None,
        )
        result = await record_decision(self._session_factory, decision, provenance)
        logger.info(
            "transaction_processed",
            transaction_id=str(event.transaction_id),
            outcome=result.outcome.value,
            verdict=result.decision.verdict,
            risk_score=result.decision.risk_score,
            triggered_rules=result.decision.triggered_rules,
            rules_version=result.decision.rules_version,
            velocity_prior_count=score.prior_count,
            source_partition=provenance.partition,
            source_offset=provenance.offset,
        )
        return result
