"""Add monitored and pointing boolean flags to fixed_sources and solarsystem_objects

Revision ID: c3d4e5f6a7b8
Revises: 35a6a33e0a34
Create Date: 2026-06-05 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c3d4e5f6a7b8"
down_revision: str | None = "35a6a33e0a34"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "fixed_sources",
        sa.Column(
            "monitored",
            sa.Boolean,
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "fixed_sources",
        sa.Column(
            "pointing",
            sa.Boolean,
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "solarsystem_objects",
        sa.Column(
            "monitored",
            sa.Boolean,
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.add_column(
        "solarsystem_objects",
        sa.Column(
            "pointing",
            sa.Boolean,
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade() -> None:
    op.drop_column("fixed_sources", "monitored")
    op.drop_column("fixed_sources", "pointing")
    op.drop_column("solarsystem_objects", "monitored")
    op.drop_column("solarsystem_objects", "pointing")
