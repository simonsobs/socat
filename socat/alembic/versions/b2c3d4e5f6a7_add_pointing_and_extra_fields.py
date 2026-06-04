"""Add monitored, pointing, and extra fields to fixed_sources and solarsystem_objects

Revision ID: b2c3d4e5f6a7
Revises: 35a6a33e0a34
Create Date: 2026-06-04 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b2c3d4e5f6a7"
down_revision: str | None = "35a6a33e0a34"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    for table in ("fixed_sources", "solarsystem_objects"):
        op.add_column(
            table,
            sa.Column(
                "monitored", sa.Boolean, nullable=False, server_default=sa.false()
            ),
        )
        op.add_column(
            table,
            sa.Column(
                "pointing", sa.Boolean, nullable=False, server_default=sa.false()
            ),
        )
        op.add_column(
            table,
            sa.Column("extra", sa.JSON, nullable=True),
        )


def downgrade() -> None:
    for table in ("fixed_sources", "solarsystem_objects"):
        op.drop_column(table, "extra")
        op.drop_column(table, "pointing")
        op.drop_column(table, "monitored")
