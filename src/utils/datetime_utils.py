import datetime
from zoneinfo import ZoneInfo


MOSCOW_TZ = ZoneInfo("Europe/Moscow")


def epoch_ms_to_moscow_dt(epoch_ms: int) -> datetime.datetime:
    """
    Convert epoch milliseconds to a timezone-aware datetime in Europe/Moscow.
    """
    utc_dt = datetime.datetime.fromtimestamp(epoch_ms / 1000, tz=datetime.timezone.utc)
    return utc_dt.astimezone(MOSCOW_TZ)


def format_dt_human_msk(dt: datetime.datetime) -> str:
    """
    Format a timezone-aware datetime as 'YYYY-MM-DD HH:MM:SS MSK'.
    Assumes dt is already in Europe/Moscow.
    """
    return dt.strftime("%Y-%m-%d %H:%M MSK")


def format_date_ymd_msk(dt: datetime.datetime) -> str:
    """
    Format a timezone-aware datetime (assumed Moscow) as 'YYYY-MM-DD'.
    """
    return dt.strftime("%Y-%m-%d")
