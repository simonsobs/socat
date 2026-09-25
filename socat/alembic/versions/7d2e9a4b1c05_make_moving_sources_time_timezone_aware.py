"""Make moving_sources.time timezone-aware

Revision ID: 7d2e9a4b1c05
Revises: 1bfc5561403e
Create Date: 2026-09-23 00:00:00.000000

socat now binds moving_sources.time as timezone-aware UTC datetimes (see
socat.database.sources.utc_datetime), and sqlmodel >= 0.0.45 declares
`datetime` fields as DateTime(timezone=True) and rejects naive values.
35a6a33e0a34 created the column as a naive sa.DateTime, which on PostgreSQL
is TIMESTAMP WITHOUT TIME ZONE and refuses aware values, so convert it to
TIMESTAMP WITH TIME ZONE, interpreting existing values as UTC. SQLite has no
timezone-aware column type and stores the same text either way, so this is
a no-op there.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7d2e9a4b1c05"
down_revision: str | None = "1bfc5561403e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    if op.get_context().dialect.name != "postgresql":
        return
    op.alter_column(
        "moving_sources",
        "time",
        type_=sa.DateTime(timezone=True),
        existing_type=sa.DateTime(),
        existing_nullable=False,
        postgresql_using="time AT TIME ZONE 'UTC'",
    )


def downgrade() -> None:
    if op.get_context().dialect.name != "postgresql":
        return
    op.alter_column(
        "moving_sources",
        "time",
        type_=sa.DateTime(),
        existing_type=sa.DateTime(timezone=True),
        existing_nullable=False,
        postgresql_using="time AT TIME ZONE 'UTC'",
    )
