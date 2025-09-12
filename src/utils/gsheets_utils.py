import logging
from typing import List, Optional

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError

from src.config import settings

logger = logging.getLogger(__name__)

SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]


def get_gsheets_service() -> Optional[Resource]:
    try:
        creds = Credentials.from_service_account_file(
            settings.google_account_file, scopes=SHEETS_SCOPES
        )
        service = build("sheets", "v4", credentials=creds)
        return service
    except FileNotFoundError:
        logger.error(
            f"Google service account file not found at: {settings.google_account_file}"
        )
    except Exception as e:
        logger.error(f"Failed to authenticate with Google Sheets: {e}")
    return None


def read_sheet_values(
    service: Resource,
    spreadsheet_id: str,
    range_a1: str,
) -> Optional[List[List[str]]]:
    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_a1)
            .execute()
        )
        values = result.get("values", [])
        return values
    except HttpError as error:
        logger.error(f"Error reading sheet values: {error}")
    except Exception as e:
        logger.error(f"Unexpected error reading sheet values: {e}")
    return None
