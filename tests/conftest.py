import os

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


def run_migration(database_path: str):
    """
    Run the migration on the database.
    """
    from alembic import command
    from alembic.config import Config

    alembic_cfg = Config("socat/alembic.ini")
    database_url = f"sqlite:///{database_path}"
    alembic_cfg.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(alembic_cfg, "head")


@pytest.fixture(scope="session", autouse=True)
def database(tmp_path_factory):
    """
    Create a temporary SQLite database for testing.
    """

    tmp_path = tmp_path_factory.mktemp("socat")
    # Create a temporary SQLite database for testing.
    database_path = tmp_path / "test.db"

    os.environ["socat_model_database_name"] = str(database_path)

    run_migration(database_path)

    yield str(database_path)

    # Clean up the database (don't do this in case we want to inspect)
    # database_path.unlink()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def database_async_sessionmaker(database):
    database_url = f"sqlite+aiosqlite:///{database}"

    async_engine = create_async_engine(database_url, echo=True, future=True)

    yield async_sessionmaker(bind=async_engine, expire_on_commit=False)

    # Clean up the database (don't do this in case we want to inspect)
    # database_path.unlink()


@pytest.fixture(scope="session")
def client(database):
    """
    Create a test client for the FastAPI app.
    """
    os.environ["socat_model_database_name"] = str(database)

    from sqlalchemy.ext.asyncio import create_async_engine

    import socat.database as db
    from socat.api import app

    db.async_engine = create_async_engine(
        f"sqlite+aiosqlite:///{database}", echo=True, future=True
    )

    test_client = TestClient(app)

    yield test_client


@pytest.fixture(scope="session")
def mock_client(tmp_path_factory):
    """
    Create a test mock database client for mock test.
    """
    from socat.client import mock

    yield mock.Client()


@pytest.fixture(scope="session")
def mock_client_astroquery(tmp_path_factory):
    """
    Create a test mock database client for mock test.
    """
    from socat.client import mock

    yield mock.AstorqueryClient()
