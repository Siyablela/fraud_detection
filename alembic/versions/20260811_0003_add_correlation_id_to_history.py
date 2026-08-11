"""add correlation id to transaction history

Revision ID: 20260811_0003
Revises: 20260808_0002
Create Date: 2026-08-11 00:00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "20260811_0003"
down_revision: Union[str, None] = "20260808_0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if inspector.has_table("transaction_history"):
        columns = {column["name"] for column in inspector.get_columns("transaction_history")}
        if "correlation_id" not in columns:
            op.add_column("transaction_history", sa.Column("correlation_id", sa.String(), nullable=True))


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if inspector.has_table("transaction_history"):
        columns = {column["name"] for column in inspector.get_columns("transaction_history")}
        if "correlation_id" in columns:
            op.drop_column("transaction_history", "correlation_id")
