"""
Client connection settings for socat.
"""

import pickle
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class SOCatClientSettings(BaseSettings):
    client_type: Literal["http", "pickle", "db"] = "pickle"

    # Pickle
    pickle_path: Path | None = None
    "The serialized mock client for socat."

    # HTTP
    hostname: str | None = None
    token_tag: str | None = "socat"
    identity_server: str | None = None

    model_config: SettingsConfigDict = {"env_prefix": "socat_client_"}

    def _pickle_client(self):
        with open(self.pickle_path, "rb") as handle:
            return pickle.load(handle)

    def _http_client(self):
        raise NotImplementedError

    def _db_client(self):
        from .db import Client

        return Client()

    @property
    def client(self):
        if self.client_type == "pickle":
            return self._pickle_client()
        if self.client_type == "db":
            return self._db_client()
        else:
            raise NotImplementedError(
                "Supported client settings backends are pickle and db"
            )
