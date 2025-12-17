"""Initial database

Revision ID: 35a6a33e0a34
Revises:
Create Date: 2024-10-23 09:53:07.953912

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "35a6a33e0a34"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "extragalactic_sources",
        sa.Column("source_id", sa.Integer, primary_key=True),
        sa.Column("ra_deg", sa.Float, nullable=False),
        sa.Column("dec_deg", sa.Float, nullable=False),
        sa.Column("flux_mJy", sa.Float, nullable=True),
        sa.Column("name", sa.String, index=True, nullable=True),
    )

    op.create_table(
        "astroquery_services",
        sa.Column("service_id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String, nullable=False),
        sa.Column("config", sa.JSON, nullable=False),
    )

    op.create_table(
        "solarsystem_sources",
        sa.Column("sso_id", sa.Integer, primary_key=True),
        sa.Column("MPC_id", sa.Integer, index=True, nullable=True, unique=True),
        sa.Column("name", sa.String, index=True, nullable=False, unique=True),
    )

    op.create_table(
        "solarsystem_ephem",
        sa.Column("ephem_id", sa.Integer, primary_key=True),
        sa.Column(
            "sso_id",
            sa.Integer,
            sa.ForeignKey(
                "solarsystem_sources.sso_id", ondelete="CASCADE", onupdate="CASCADE"
            ),
            nullable=False,
        ),
        sa.Column(
            "MPC_id",
            sa.Integer,
            sa.ForeignKey(
                "solarsystem_sources.MPC_id", ondelete="CASCADE", onupdate="CASCADE"
            ),
            nullable=False,
        ),
        sa.Column(
            "name",
            sa.Integer,
            sa.ForeignKey(
                "solarsystem_sources.name", ondelete="CASCADE", onupdate="CASCADE"
            ),
            nullable=False,
        ),
        sa.Column("time", sa.Integer, index=True, nullable=False),
        sa.Column("ra_deg", sa.Float, nullable=False),
        sa.Column("dec_deg", sa.Float, nullable=False),
        sa.Column("flux_mJy", sa.Float, nullable=True),
        postgresql_partition_by="LIST (sso_id)",
    )


def downgrade() -> None:
    op.drop_table("extragalactic_sources")
    op.drop_table("astroquery_services")
    op.drop_table("astroquery_sources")
