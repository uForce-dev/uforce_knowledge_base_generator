from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"


class DatabaseSettings(BaseSettings):
    name: str
    user: str
    password: str
    host: str
    port: int

    model_config = SettingsConfigDict(env_file=ENV_FILE, env_prefix="DB_")


class Settings(BaseSettings):
    base_dir: Path = BASE_DIR
    env_file: Path = ENV_FILE

    db: DatabaseSettings = DatabaseSettings()  # noqa

    model_config = SettingsConfigDict(env_file=ENV_FILE)


settings = Settings()
