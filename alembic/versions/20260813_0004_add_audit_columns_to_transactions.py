"""add audit columns to transactions

Revision ID: 20260813_0004
Revises: 20260811_0003
Create Date: 2026-08-13 00:00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "20260813_0004"
down_revision: Union[str, None] = "20260811_0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if inspector.has_table("transactions"):
        columns = {column["name"] for column in inspector.get_columns("transactions")}
        if "correlation_id" not in columns:
            op.add_column(
                "transactions",
                sa.Column("correlation_id", sa.String(), nullable=True),
            )
        if "updated_at" not in columns:
            op.add_column(
                "transactions",
                sa.Column(
                    "updated_at",
                    sa.DateTime(timezone=True),
                    server_default=sa.text("timezone('UTC', now())"),
                    nullable=False,
                ),
            )

        op.execute(
            sa.text(
                "UPDATE transactions SET updated_at = timezone('UTC', created_at) WHERE updated_at IS NULL"
            )
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if inspector.has_table("transactions"):
        columns = {column["name"] for column in inspector.get_columns("transactions")}
        if "updated_at" in columns:
            op.drop_column("transactions", "updated_at")
        if "correlation_id" in columns:
            op.drop_column("transactions", "correlation_id")
