from pathlib import Path

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent
TEMP_DIR = BASE_DIR / "temp"
LOGS_DIR = BASE_DIR / "logs"
ENV_FILE = BASE_DIR / ".env"


class DatabaseSettings(BaseSettings):
    name: str
    user: str
    password: str
    host: str
    port: int

    model_config = SettingsConfigDict(env_file=ENV_FILE, env_prefix="DB_", extra="ignore")

    @computed_field
    def mysql_connection_str(self) -> str:
        return (
            f"mysql+pymysql://"
            f"{settings.db.user}:"
            f"{settings.db.password}@"
            f"{settings.db.host}:"
            f"{settings.db.port}/"
            f"{settings.db.name}"
        )


class Settings(BaseSettings):
    base_dir: Path = BASE_DIR
    temp_dir: Path = TEMP_DIR
    logs_dir: Path = LOGS_DIR
    env_file: Path = ENV_FILE

    google_account_file_name: str

    db: DatabaseSettings = DatabaseSettings()  # noqa

    model_config = SettingsConfigDict(env_file=ENV_FILE, extra="ignore")

    @computed_field
    def google_account_file(self) -> Path:
        return self.base_dir / self.google_account_file_name


settings = Settings()  # noqa
