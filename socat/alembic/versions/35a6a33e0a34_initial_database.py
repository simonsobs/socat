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
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("ra_deg", sa.Float, nullable=False),
        sa.Column("dec_deg", sa.Float, nullable=False),
        sa.Column("flux_mJy", sa.Float, nullable=True),
        sa.Column("name", sa.String, nullable=True),
    )

    op.create_table(
        "astroquery_services",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String, nullable=False),
        sa.Column("config", sa.JSON, nullable=False),
    )

    op.create_table(
        "astroquery_sources",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String, index=True, nullable=False),
        sa.Column("config", sa.JSON, nullable=False),
    )


def downgrade() -> None:
    op.drop_table("extragalactic_sources")
    op.drop_table("astroquery_services")
    op.drop_table("astroquery_sources")
