from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "sqlite+aiosqlite:///socat.db"

    class Config:
        env_prefix = "socat_"


settings = Settings()
