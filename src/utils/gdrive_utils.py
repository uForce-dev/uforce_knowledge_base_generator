import logging
from pathlib import Path

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

from src.config import settings

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive"]


def get_gdrive_service():
    try:
        creds = Credentials.from_service_account_file(
            settings.google_account_file, scopes=SCOPES
        )
        service = build("drive", "v3", credentials=creds)
        return service
    except FileNotFoundError:
        logger.error(
            f"Google service account file not found at: {settings.google_account_file}"
        )
    except Exception as e:
        logger.error(f"Failed to authenticate with Google Drive: {e}")
    return None


def upload_file_to_gdrive(service, file_path: Path, folder_id: str):
    try:
        file_metadata = {"name": file_path.name, "parents": [folder_id]}
        media = MediaFileUpload(str(file_path), mimetype="text/plain")
        file = (
            service.files()
            .create(
                body=file_metadata,
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            )
            .execute()
        )
        logger.info(
            f"File '{file_path.name}' uploaded to Google Drive with ID: {file.get('id')}"
        )
    except HttpError as error:
        logger.error(f"An error occurred while uploading {file_path.name}: {error}")
    except Exception as e:
        logger.error(
            f"An unexpected error occurred during upload of {file_path.name}: {e}"
        )


def delete_files_in_folder(service, folder_id: str):
    try:
        query = f"'{folder_id}' in parents and trashed=false"
        results = (
            service.files()
            .list(
                q=query,
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        items = results.get("files", [])
        if not items:
            logger.info(f"No files found in folder ID: {folder_id} to delete.")
            return

        logger.info(f"Found {len(items)} files to delete in folder ID: {folder_id}.")
        for item in items:
            try:
                service.files().delete(
                    fileId=item["id"], supportsAllDrives=True,
                ).execute()
                logger.info(f"Deleted file: {item['name']} (ID: {item['id']})")
            except HttpError as error:
                logger.error(
                    f"Failed to delete file {item['name']} (ID: {item['id']}): {error}"
                )
    except HttpError as error:
        logger.error(
            f"An error occurred while listing files for deletion in folder {folder_id}: {error}"
        )
    except Exception as e:
        logger.error(
            f"An unexpected error occurred during file deletion in folder {folder_id}: {e}"
        )
