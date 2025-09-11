import io
import logging
import os
from pathlib import Path

import docx
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from src.config import settings

logger = logging.getLogger(__name__)

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']


def get_gdrive_service():
    """Authenticates and returns a Google Drive service object."""
    try:
        creds = Credentials.from_service_account_file(
            settings.google_account_file, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)
        return service
    except FileNotFoundError:
        logger.error(f"Google service account file not found at: {settings.google_account_file}")
    except Exception as e:
        logger.error(f"Failed to authenticate with Google Drive: {e}")
    return None


def list_files_recursive(service, folder_id: str, parent_path: str = "") -> list[dict]:
    """Recursively lists all files in a Google Drive folder."""
    files_list = []
    try:
        query = f"'{folder_id}' in parents and trashed=false"
        results = service.files().list(
            q=query,
            pageSize=1000,
            fields="nextPageToken, files(id, name, mimeType, parents)"
        ).execute()

        items = results.get('files', [])
        for item in items:
            current_path = os.path.join(parent_path, item['name'])
            if item['mimeType'] == 'application/vnd.google-apps.folder':
                files_list.extend(list_files_recursive(service, item['id'], current_path))
            else:
                item['path'] = current_path
                files_list.append(item)

    except HttpError as error:
        logger.error(f"An error occurred while listing files: {error}")

    return files_list


def download_file(service, file_id: str) -> io.BytesIO | None:
    """Downloads a file from Google Drive and returns it as a BytesIO object."""
    try:
        request = service.files().get_media(fileId=file_id)
        file_stream = io.BytesIO()
        downloader = MediaIoBaseDownload(file_stream, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            logger.info(f"Download {int(status.progress() * 100)}%.")
        file_stream.seek(0)
        return file_stream
    except HttpError as error:
        logger.error(f"An error occurred while downloading file {file_id}: {error}")
    return None


def extract_text_from_docx(file_stream: io.BytesIO) -> str:
    """Extracts text from a .docx file stream."""
    try:
        document = docx.Document(file_stream)
        return "\n".join(para.text for para in document.paragraphs if para.text)
    except Exception as e:
        logger.error(f"Error extracting text from docx stream: {e}")
    return ""


def process_teamly_documents() -> None:
    """
    Processes .docx files from a specified Google Drive directory,
    extracts their text content, and saves them as .txt files with metadata.
    """
    logger.info("Starting Teamly documents processing...")
    service = get_gdrive_service()
    if not service:
        logger.error("Could not get Google Drive service. Aborting.")
        return

    source_folder_id = settings.google_drive_source_dir_id
    logger.info(f"Fetching files from Google Drive folder ID: {source_folder_id}")

    all_files = list_files_recursive(service, source_folder_id)
    docx_files = [f for f in all_files if f['name'].endswith('.docx')]

    if not docx_files:
        logger.info("No .docx files found in the specified Google Drive directory.")
        return

    logger.info(f"Found {len(docx_files)} .docx files to process.")

    for file_info in docx_files:
        logger.info(f"Processing file: {file_info['path']}")

        file_stream = download_file(service, file_info['id'])
        if not file_stream:
            logger.warning(f"Skipping file {file_info['path']} due to download error.")
            continue

        text_content = extract_text_from_docx(file_stream)
        if not text_content:
            logger.warning(f"Skipping file {file_info['path']} as no text could be extracted.")
            continue

        relative_path = Path(file_info['path'])
        flat_file_name = str(relative_path.with_suffix('.txt')).replace(os.path.sep, '_')
        output_file_path = settings.teamly_temp_dir / flat_file_name

        metadata = (
            f"---\n"
            f"source: Teamly Google Drive\n"
            f"category: {relative_path.parent}\n"
            f"data_format: 'plain_text'\n"
            f"---\n\n"
        )

        try:
            with open(output_file_path, 'w', encoding='utf-8') as f:
                f.write(metadata)
                f.write(text_content)
            logger.info(f"Generated file: {output_file_path}")
        except IOError as e:
            logger.error(f"Error writing to file {output_file_path}: {e}")

    logger.info("Finished processing Teamly documents.")
