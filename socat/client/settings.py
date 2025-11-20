"""
Client connection settings for socat.
"""

import pickle
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class SOCatClientSettings(BaseSettings):
    client_type: Literal["http", "pickle"] = "pickle"

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
            client = pickle.load(handle)
        return client

    def _http_client(self):
        raise NotImplementedError

    @property
    def client(self):
        if self.client_type == "pickle":
            return self._pickle_client()
        else:
            raise NotImplementedError(
                "Only the pickle client works with the settings functionality"
            )
