import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


def run_migration(database_path: str):
    """
    Run the migration on the database.
    """
    from alembic import command
    from alembic.config import Config

    alembic_cfg = Config("alembic.ini")
    database_url = f"sqlite:///{database_path}"
    alembic_cfg.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(alembic_cfg, "heads")

    return


@pytest_asyncio.fixture(scope="session", autouse=True)
async def database_async_sesionmaker(tmp_path_factory):
    """
    Create a temporary SQLite database for testing. This is a
    somewhat tricky scenario as we must create the database using
    the synchronous engine, but access it using the asynchronous
    engine.
    """

    tmp_path = tmp_path_factory.mktemp("socat")
    # Create a temporary SQLite database for testing.
    database_path = tmp_path / "test.db"

    # Run the migration on the database. This is blocking.
    run_migration(database_path)

    database_url = f"sqlite+aiosqlite:///{database_path}"

    async_engine = create_async_engine(database_url, echo=True, future=True)

    yield async_sessionmaker(bind=async_engine, expire_on_commit=False)

    # Clean up the database (don't do this in case we want to inspect)
    # database_path.unlink()
