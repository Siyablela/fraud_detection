"""initial schema: transactions, processing audit, versioned rule sets

* ``transactions``      - one fraud decision per transaction id (first decision wins).
* ``transaction_history`` - append-only processing audit; the unique Kafka source
  key makes a redelivered message a no-op.
* ``rule_set_versions`` - append-only rule-set versions, seeded with the default
  rules as ``fraud-default`` v1.

Revision ID: 20261029_0001
Revises:
Create Date: 2026-10-29 00:00:00
"""

import hashlib
import json
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20261029_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# A frozen copy: migrations must not import application code, which keeps changing.
DEFAULT_RULE_SET = {
    "velocity_window_seconds": 60,
    "rules": [
        {
            "code": "HIGH_VALUE_THRESHOLD",
            "description": "A single transaction of 10,000.00 or more.",
            "condition": {">=": [{"var": "amount"}, 10000]},
        },
        {
            "code": "RISKY_CATEGORY_LIMIT",
            "description": "More than 5,000.00 in a high-risk category (GAMBLING, CRYPTO).",
            "condition": {
                "and": [
                    {"in": [{"var": "category"}, ["GAMBLING", "CRYPTO"]]},
                    {">": [{"var": "amount"}, 5000]},
                ]
            },
        },
        {
            "code": "VELOCITY_COUNT_EXCEEDED",
            "description": "More than 5 transactions by the same user within the velocity window.",
            "condition": {">": [{"+": [{"var": "velocity.prior_count"}, 1]}, 5]},
        },
        {
            "code": "VELOCITY_AMOUNT",
            "description": "More than 50,000.00 spent by the same user within the velocity window.",
            "condition": {">": [{"+": [{"var": "velocity.prior_amount"}, {"var": "amount"}]}, 50000]},
        },
    ],
}

_UTC_NOW = sa.text("timezone('UTC', now())")


def _content_hash(definition: dict) -> str:
    canonical = json.dumps(definition, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _decision_columns() -> list[sa.Column]:
    return [
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("category", sa.String(64), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("is_fraud", sa.Boolean(), nullable=False),
        sa.Column("triggered_rules", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("rules_version", sa.String(128), nullable=False),
        sa.Column("rules_hash", sa.String(64), nullable=False),
        sa.Column("correlation_id", sa.String(128), nullable=True),
    ]


def upgrade() -> None:
    op.create_table(
        "transactions",
        sa.Column("transaction_id", sa.Uuid(), primary_key=True),
        *_decision_columns(),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=_UTC_NOW),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=_UTC_NOW),
        sa.CheckConstraint("amount > 0", name="ck_transactions_amount_positive"),
    )
    op.create_index("ix_transactions_user_occurred", "transactions", ["user_id", "occurred_at", "transaction_id"])
    op.create_index("ix_transactions_category_occurred", "transactions", ["category", "occurred_at", "transaction_id"])
    op.create_index("ix_transactions_occurred", "transactions", ["occurred_at", "transaction_id"])
    op.create_index(
        "ix_transactions_fraud_occurred",
        "transactions",
        ["occurred_at", "transaction_id"],
        postgresql_where=sa.text("is_fraud"),
    )

    op.create_table(
        "transaction_history",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "transaction_id",
            sa.Uuid(),
            sa.ForeignKey("transactions.transaction_id", ondelete="CASCADE", name="fk_transaction_history_transaction"),
            nullable=False,
        ),
        sa.Column("outcome", sa.String(16), nullable=False),
        *_decision_columns(),
        sa.Column("source_topic", sa.String(255), nullable=False),
        sa.Column("source_partition", sa.Integer(), nullable=False),
        sa.Column("source_offset", sa.BigInteger(), nullable=False),
        sa.Column("source_timestamp", sa.BigInteger(), nullable=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=False, server_default=_UTC_NOW),
        sa.CheckConstraint("outcome IN ('evaluated', 'duplicate')", name="ck_transaction_history_outcome"),
        sa.UniqueConstraint("source_topic", "source_partition", "source_offset", name="uq_transaction_history_source"),
    )
    op.create_index(
        "ix_transaction_history_transaction_processed_at", "transaction_history", ["transaction_id", "processed_at"]
    )

    rule_set_versions = op.create_table(
        "rule_set_versions",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("rule_set_key", sa.String(64), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("definition", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("description", sa.String(500), nullable=False),
        sa.Column("created_by", sa.String(255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=_UTC_NOW),
        sa.CheckConstraint("version >= 1", name="ck_rule_set_versions_version_positive"),
        sa.UniqueConstraint("rule_set_key", "version", name="uq_rule_set_versions_key_version"),
    )
    op.bulk_insert(
        rule_set_versions,
        [
            {
                "rule_set_key": "fraud-default",
                "version": 1,
                "definition": DEFAULT_RULE_SET,
                "content_hash": _content_hash(DEFAULT_RULE_SET),
                "description": "Initial rule set: high value, risky category, velocity count and amount.",
                "created_by": f"migration:{revision}",
            }
        ],
    )


def downgrade() -> None:
    op.drop_table("rule_set_versions")
    op.drop_table("transaction_history")
    op.drop_table("transactions")
