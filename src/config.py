from pathlib import Path

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent
TEMP_DIR = BASE_DIR / "temp"
MATTERMOST_TEMP_DIR = TEMP_DIR / "mattermost"
TEAMLY_TEMP_DIR = TEMP_DIR / "teamly"
SECRETS_DIR = BASE_DIR / "secrets"
LOGS_DIR = BASE_DIR / "logs"
ENV_FILE = BASE_DIR / ".env"


class DatabaseSettings(BaseSettings):
    name: str
    user: str
    password: str
    host: str
    port: int

    model_config = SettingsConfigDict(
        env_file=ENV_FILE, env_prefix="DB_", extra="ignore"
    )

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
    secrets_dir: Path = SECRETS_DIR
    temp_dir: Path = TEMP_DIR
    mattermost_temp_dir: Path = MATTERMOST_TEMP_DIR
    teamly_temp_dir: Path = TEAMLY_TEMP_DIR
    logs_dir: Path = LOGS_DIR
    env_file: Path = ENV_FILE

    # Google
    google_account_file_name: str
    google_drive_teamly_source_dir_id: str
    google_drive_teamly_processed_dir_id: str
    google_drive_mattermost_processed_dir_id: str
    google_sheets_hr_spreadsheet_id: str
    google_sheets_hr_sheet_name: str
    google_sheets_hr_sheet_gid: int
    google_sheets_hr_range: str
    google_drive_hr_processed_dir_id: str

    teamly_space_id: str
    teamly_api_slug: str
    teamly_api_access_token: str
    teamly_api_refresh_token: str
    teamly_api_client_id: str
    teamly_api_client_secret: str

    db: DatabaseSettings = DatabaseSettings()  # noqa

    model_config = SettingsConfigDict(env_file=ENV_FILE, extra="ignore")

    @computed_field
    def google_account_file(self) -> Path:
        return self.secrets_dir / self.google_account_file_name


settings = Settings()  # noqa
