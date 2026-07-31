"""create transactions table

Revision ID: 20260731_0001
Revises: 
Create Date: 2026-07-31 00:00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "20260731_0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    table_exists = inspector.has_table("transactions")

    if not table_exists:
        op.create_table(
            "transactions",
            sa.Column("transaction_id", sa.String(), nullable=False),
            sa.Column("user_id", sa.String(), nullable=False),
            sa.Column("amount", sa.Float(), nullable=False),
            sa.Column("category", sa.String(), nullable=False),
            sa.Column("timestamp", sa.Float(), nullable=False),
            sa.Column("is_fraud", sa.Boolean(), nullable=False),
            sa.Column("triggered_rules", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.text("NOW()"),
                nullable=False,
            ),
            sa.PrimaryKeyConstraint("transaction_id"),
        )

    index_names = {index["name"] for index in inspector.get_indexes("transactions")}
    if "ix_transactions_category_created_at" not in index_names:
        op.create_index(
            "ix_transactions_category_created_at",
            "transactions",
            ["category", "created_at"],
            unique=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if inspector.has_table("transactions"):
        index_names = {index["name"] for index in inspector.get_indexes("transactions")}
        if "ix_transactions_category_created_at" in index_names:
            op.drop_index("ix_transactions_category_created_at", table_name="transactions")
