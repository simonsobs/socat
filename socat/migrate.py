"""
Module for running alembic integration migrations.
"""


def migrate():
    """
    CLI script to run 'alembic upgrade head' with the correct location.
    """
    import subprocess

    location = __file__.replace("migrate.py", "alembic.ini")

    subprocess.call(["alembic", "-c", location, "upgrade", "head"])
