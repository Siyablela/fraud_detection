"""add rules traceability columns

Revision ID: 20261024_0005
Revises: 20260813_0004
Create Date: 2026-10-24 00:00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "20261024_0005"
down_revision: Union[str, None] = "20260813_0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _ensure_column(
    table_name: str,
    column_name: str,
    column: sa.Column,
) -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table(table_name):
        return

    columns = {item["name"] for item in inspector.get_columns(table_name)}

    if column_name not in columns:
        op.add_column(table_name, column)


def upgrade() -> None:
    _ensure_column(
        "transactions",
        "rules_version",
        sa.Column(
            "rules_version",
            sa.String(),
            nullable=False,
            server_default="unknown",
        ),
    )

    _ensure_column(
        "transactions",
        "rules_hash",
        sa.Column(
            "rules_hash",
            sa.String(),
            nullable=False,
            server_default="unknown",
        ),
    )

    _ensure_column(
        "transaction_history",
        "rules_version",
        sa.Column(
            "rules_version",
            sa.String(),
            nullable=False,
            server_default="unknown",
        ),
    )

    _ensure_column(
        "transaction_history",
        "rules_hash",
        sa.Column(
            "rules_hash",
            sa.String(),
            nullable=False,
            server_default="unknown",
        ),
    )


def _drop_column_if_exists(
    table_name: str,
    column_name: str,
) -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not inspector.has_table(table_name):
        return

    columns = {item["name"] for item in inspector.get_columns(table_name)}

    if column_name in columns:
        op.drop_column(table_name, column_name)


def downgrade() -> None:
    _drop_column_if_exists("transaction_history", "rules_hash")
    _drop_column_if_exists("transaction_history", "rules_version")
    _drop_column_if_exists("transactions", "rules_hash")
    _drop_column_if_exists("transactions", "rules_version")