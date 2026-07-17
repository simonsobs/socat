"""Add sso_id/time composite index to moving_sources

Revision ID: 1bfc5561403e
Revises: c3d4e5f6a7b8
Create Date: 2026-07-17 00:00:00.000000

get_ephem_points()/get_source_generator() filter moving_sources by a known
sso_id first, then narrow by time. moving_sources.time already has its own
index (added by 35a6a33e0a34's initial op.create_table), but sso_id was
never indexed -- at real ephemeris-table scale (tens of millions of rows,
see simonsobs/sotrplib's scripts/solar_system/build_socat_db.py), that made
every per-object ephemeris lookup a full table scan. This mirrors the
RegisteredMovingSourceTable.__table_args__ index added to the ORM model in
the same change, so a database provisioned via `alembic upgrade head` ends
up with the same indexes as one provisioned via SQLModel.metadata.create_all().
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "1bfc5561403e"
down_revision: str | None = "c3d4e5f6a7b8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "idx_moving_sources_sso_time",
        "moving_sources",
        ["sso_id", "time"],
    )


def downgrade() -> None:
    op.drop_index("idx_moving_sources_sso_time", table_name="moving_sources")
