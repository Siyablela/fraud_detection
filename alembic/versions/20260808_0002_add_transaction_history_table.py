"""add transaction history table

Revision ID: 20260808_0002
Revises: 20260731_0001
Create Date: 2026-08-08 00:00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "20260808_0002"
down_revision: Union[str, None] = "20260731_0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    table_exists = inspector.has_table("transaction_history")

    if not table_exists:
        op.create_table(
            "transaction_history",
            sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True, nullable=False),
            sa.Column("transaction_id", sa.String(), nullable=False),
            sa.Column("user_id", sa.String(), nullable=False),
            sa.Column("amount", sa.Float(), nullable=False),
            sa.Column("category", sa.String(), nullable=False),
            sa.Column("timestamp", sa.Float(), nullable=False),
            sa.Column("is_fraud", sa.Boolean(), nullable=False),
            sa.Column("triggered_rules", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
            sa.Column("source_topic", sa.String(), nullable=True),
            sa.Column("source_partition", sa.Integer(), nullable=True),
            sa.Column("source_offset", sa.BigInteger(), nullable=True),
            sa.Column("source_timestamp", sa.BigInteger(), nullable=True),
            sa.Column(
                "processed_at",
                sa.DateTime(timezone=True),
                server_default=sa.text("NOW()"),
                nullable=False,
            ),
        )

    index_names = {index["name"] for index in inspector.get_indexes("transaction_history")}
    if "ix_transaction_history_transaction_processed_at" not in index_names:
        op.create_index(
            "ix_transaction_history_transaction_processed_at",
            "transaction_history",
            ["transaction_id", "processed_at"],
            unique=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if inspector.has_table("transaction_history"):
        index_names = {index["name"] for index in inspector.get_indexes("transaction_history")}
        if "ix_transaction_history_transaction_processed_at" in index_names:
            op.drop_index("ix_transaction_history_transaction_processed_at", table_name="transaction_history")
        op.drop_table("transaction_history")
