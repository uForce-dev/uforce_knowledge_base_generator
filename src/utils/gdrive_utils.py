import logging
from pathlib import Path

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
import time

from src.config import settings

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive"]


def get_gdrive_service() -> Resource | None:
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


def _detect_mimetype(file_path: Path) -> str:
    suffix = file_path.suffix.lower()
    if suffix == ".docx":
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    if suffix in {".txt", ".md"}:
        return "text/plain"
    if suffix == ".html":
        return "text/html"
    return "application/octet-stream"


def upload_file_to_gdrive(
    service, file_path: Path, folder_id: str, as_gdoc: bool = False
):
    """Uploads a file to a specific folder in Google Drive, optionally as a Google Doc.

    - When as_gdoc is True, file is imported as Google Doc by setting target mimeType
      to application/vnd.google-apps.document while media mimetype reflects the source.
    - Includes simple retries for transient 5xx errors.
    """
    file_name_display = file_path.stem if as_gdoc else file_path.name
    if as_gdoc:
        file_metadata = {
            "name": file_path.stem,
            "parents": [folder_id],
            "mimeType": "application/vnd.google-apps.document",
        }
    else:
        file_metadata = {"name": file_path.name, "parents": [folder_id]}

    media_mime = _detect_mimetype(file_path)
    media = MediaFileUpload(str(file_path), mimetype=media_mime, resumable=True)

    max_attempts = 3
    delay = 2.0
    attempt = 1
    tried_binary_fallback = False
    while True:
        try:
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
                "File '%s' uploaded to Google Drive with ID: %s",
                file_name_display,
                file.get("id"),
            )
            return
        except HttpError as error:
            status = getattr(error, "status_code", None) or getattr(
                error.resp, "status", None
            )
            # If import (as Google Doc) is too large, fall back to uploading as a regular file
            if status and int(status) == 413 and as_gdoc and not tried_binary_fallback:
                logger.warning(
                    "Drive import too large for %s. Falling back to binary upload without conversion.",
                    file_path.name,
                )
                as_gdoc = False
                file_metadata = {"name": file_path.name, "parents": [folder_id]}
                file_name_display = file_path.name
                tried_binary_fallback = True
                # retry immediately without backoff
                continue
            if status and 500 <= int(status) < 600 and attempt < max_attempts:
                logger.warning(
                    "Transient Drive error (%s) uploading %s. Retrying in %.1fs (attempt %d/%d)",
                    status,
                    file_path.name,
                    delay,
                    attempt,
                    max_attempts,
                )
                time.sleep(delay)
                attempt += 1
                delay *= 2
                continue
            logger.error(
                "An error occurred while uploading %s: %s", file_path.name, error
            )
            return
        except Exception as e:
            logger.error(
                "An unexpected error occurred during upload of %s: %s",
                file_path.name,
                e,
            )
            return


def delete_files_in_folder(service, folder_id: str):
    try:
        query = f"'{folder_id}' in parents and trashed=false"
        results = (
            service.files()
            .list(
                q=query,
                fields=(
                    "files("
                    "id, name, mimeType, driveId, "
                    "owners(displayName,emailAddress), "
                    "capabilities(canTrash,canDelete)"
                    ")"
                ),
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
            file_id = item.get("id")
            file_name = item.get("name")
            capabilities = item.get("capabilities", {})
            can_trash = capabilities.get("canTrash", False)
            can_delete = capabilities.get("canDelete", False)

            owners = item.get("owners", [])
            owners_str = (
                ", ".join(
                    f"{o.get('displayName')}<{o.get('emailAddress')}>" for o in owners
                )
                or "unknown"
            )

            # Prefer moving to trash to avoid hard-delete permission issues
            if can_trash:
                try:
                    service.files().update(
                        fileId=file_id,
                        body={"trashed": True},
                        supportsAllDrives=True,
                    ).execute()
                    logger.info(
                        f"Trashed file: {file_name} (ID: {file_id}) owned by {owners_str}"
                    )
                    continue
                except HttpError as error:
                    # Fall through to try hard delete if permitted
                    logger.warning(
                        f"Failed to trash file {file_name} (ID: {file_id}): {error}"
                    )

            if can_delete:
                try:
                    service.files().delete(
                        fileId=file_id,
                        supportsAllDrives=True,
                    ).execute()
                    logger.info(
                        f"Permanently deleted file: {file_name} (ID: {file_id}) owned by {owners_str}"
                    )
                    continue
                except HttpError as error:
                    logger.error(
                        f"Failed to hard-delete file {file_name} (ID: {file_id}): {error}"
                    )
                    continue

            # If neither action is permitted, provide a clear diagnostic
            logger.warning(
                "Insufficient permissions to remove file %s (ID: %s). Owners: %s. "
                "Required: canTrash or canDelete on this item or higher role on its drive (Content manager/Manager).",
                file_name,
                file_id,
                owners_str,
            )
    except HttpError as error:
        logger.error(
            f"An error occurred while listing files for deletion in folder {folder_id}: {error}"
        )
    except Exception as e:
        logger.error(
            f"An unexpected error occurred during file deletion in folder {folder_id}: {e}"
        )
