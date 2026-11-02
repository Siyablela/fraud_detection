"""index decisions by processing time; drop the unused updated_at column

* ``ix_transactions_created`` serves the daily batch summary, which selects by
  ``created_at`` (when the decision was made) and would otherwise scan the table.
* ``transactions.updated_at`` is dropped: decisions are immutable (first write
  wins), so it only ever repeated ``created_at``.

Revision ID: 20261102_0002
Revises: 20261029_0001
Create Date: 2026-11-02 00:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20261102_0002"
down_revision: str | None = "20261029_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index("ix_transactions_created", "transactions", ["created_at"])
    op.drop_column("transactions", "updated_at")


def downgrade() -> None:
    op.add_column(
        "transactions",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("timezone('UTC', now())"),
        ),
    )
    op.drop_index("ix_transactions_created", table_name="transactions")
