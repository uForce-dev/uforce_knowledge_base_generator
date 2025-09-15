import io
import logging
import os
import re
from pathlib import Path

import docx
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from src.config import settings
from src.utils.gdrive_utils import (
    get_gdrive_service,
    upload_file_to_gdrive,
    delete_files_in_folder,
)

logger = logging.getLogger(__name__)


def clean_text(text: str) -> str:
    if not text:
        return ""

    emoji_pattern = re.compile(
        "["
        "\U0001f600-\U0001f64f"
        "\U0001f300-\U0001f5ff"
        "\U0001f680-\U0001f6ff"
        "\U0001f1e0-\U0001f1ff"
        "\U00002500-\U00002bef"
        "\U00002702-\U000027b0"
        "\U00002702-\U000027b0"
        "\U000024c2-\U0001f251"
        "\U0001f926-\U0001f937"
        "\U00010000-\U0010ffff"
        "\u2640-\u2642"
        "\u2600-\u2b55"
        "\u200d"
        "\u23cf"
        "\u23e9"
        "\u231a"
        "\ufe0f"
        "\u3030"
        "]+",
        flags=re.UNICODE,
    )
    text = emoji_pattern.sub(r"", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text


def list_files_recursive(service, folder_id: str, parent_path: str = "") -> list[dict]:
    files_list = []
    try:
        query = f"'{folder_id}' in parents and trashed=false"
        results = (
            service.files()
            .list(
                q=query,
                pageSize=1000,
                fields="nextPageToken, files(id, name, mimeType, parents)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )

        items = results.get("files", [])
        for item in items:
            current_path = os.path.join(parent_path, item["name"])
            if item["mimeType"] == "application/vnd.google-apps.folder":
                files_list.extend(
                    list_files_recursive(service, item["id"], current_path)
                )
            else:
                item["path"] = current_path
                files_list.append(item)

    except HttpError as error:
        logger.error(f"An error occurred while listing files: {error}")

    return files_list


def download_file(service, file_id: str) -> io.BytesIO | None:
    try:
        request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
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
    try:
        document = docx.Document(file_stream)
        lines: list[str] = []
        for para in document.paragraphs:
            lines.append(para.text)
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"Error extracting text from docx stream: {e}")
    return ""


def process_teamly_documents() -> None:
    logger.info("Starting Teamly documents processing...")
    service = get_gdrive_service()
    if not service:
        logger.error("Could not get Google Drive service. Aborting.")
        return

    source_folder_id = settings.google_drive_teamly_source_dir_id
    processed_folder_id = settings.google_drive_teamly_processed_dir_id
    logger.info(f"Fetching files from Google Drive folder ID: {source_folder_id}")

    logger.info(f"Clearing processed folder ID: {processed_folder_id}...")
    delete_files_in_folder(service, processed_folder_id)
    logger.info("Processed folder cleared.")

    all_files = list_files_recursive(service, source_folder_id)
    docx_files = [f for f in all_files if f["name"].endswith(".docx")]

    if not docx_files:
        logger.info("No .docx files found in the specified Google Drive directory.")
        return

    logger.info(f"Found {len(docx_files)} .docx files to process.")

    if settings.teamly_combine_files:
        # Group by top-level folder under the source root
        groups: dict[str, list[dict]] = {}
        for file_info in docx_files:
            path_parts = file_info["path"].split(os.path.sep)
            top_level_folder = path_parts[0] if path_parts else "root"
            groups.setdefault(top_level_folder, []).append(file_info)

        if not groups:
            logger.info("No groups detected in combine mode. Nothing to upload.")
            return

        for folder_name, files_in_group in groups.items():
            combined_chunks: list[str] = []
            for file_info in files_in_group:
                logger.info(
                    f"Processing file (combine mode: {folder_name}): {file_info['path']}"
                )
                file_stream = download_file(service, file_info["id"])
                if not file_stream:
                    logger.warning(
                        f"Skipping file {file_info['path']} due to download error."
                    )
                    continue
                raw_text = extract_text_from_docx(file_stream)
                # Clean without collapsing newlines: clean only emojis and excessive spaces within lines
                # Keep original newlines to preserve document structure
                cleaned = clean_text(raw_text)
                # Re-expand single spaces where multiple spaces were collapsed inside clean_text by splitting per line
                text_content = "\n".join(part.strip() for part in cleaned.split("\n"))
                if not text_content:
                    logger.warning(
                        f"Skipping file {file_info['path']} as no text could be extracted or was empty after cleaning."
                    )
                    continue
                combined_chunks.append((file_info["path"], text_content))

            if not combined_chunks:
                logger.info(
                    f"No content generated for group '{folder_name}'. Skipping upload."
                )
                continue

            combined_metadata = (
                "---\n"
                "category: Teamly\n"
                f"folder: {folder_name}\n"
                "body_format: documents concatenated; each begins with '# <path>' line, followed by original paragraphs with newlines preserved\n"
                "---\n\n"
            )

            safe_folder_name = re.sub(r"[^\w\-_. ]+", "_", folder_name)
            combined_name = f"{safe_folder_name}.txt"
            combined_path = settings.teamly_temp_dir / combined_name
            try:
                with open(combined_path, "w", encoding="utf-8") as f:
                    f.write(combined_metadata)
                    for path_str, content_str in combined_chunks:
                        f.write(f"# {path_str}\n")
                        f.write(content_str.rstrip("\n") + "\n\n")
                logger.info(
                    f"Generated combined file for '{folder_name}': {combined_path}"
                )

                logger.info(
                    f"Uploading {combined_path.stem} to Google Drive as a Google Doc..."
                )
                upload_file_to_gdrive(
                    service, combined_path, processed_folder_id, as_gdoc=True
                )
            except IOError as e:
                logger.error(
                    f"Error writing combined file {combined_path} for '{folder_name}': {e}"
                )
        return

    for file_info in docx_files:
        logger.info(f"Processing file: {file_info['path']}")

        file_stream = download_file(service, file_info["id"])
        if not file_stream:
            logger.warning(f"Skipping file {file_info['path']} due to download error.")
            continue

        raw_text = extract_text_from_docx(file_stream)
        cleaned = clean_text(raw_text)
        text_content = "\n".join(part.strip() for part in cleaned.split("\n"))

        if not text_content:
            logger.warning(
                f"Skipping file {file_info['path']} as no text could be extracted or was empty after cleaning."
            )
            continue

        relative_path = Path(file_info["path"])
        flat_file_name = str(relative_path.with_suffix(".txt")).replace(
            os.path.sep, "_"
        )
        output_file_path = settings.teamly_temp_dir / flat_file_name

        metadata = (
            "---\n"
            "category: Teamly\n"
            f"folder_path: {relative_path.parent}\n"
            "schema:\n"
            "  type: document\n"
            "  content: plain_text\n"
            "body_format: free-form paragraphs with newlines preserved\n"
            "---\n\n"
        )

        try:
            with open(output_file_path, "w", encoding="utf-8") as f:
                f.write(metadata)
                f.write(text_content)
            logger.info(f"Generated file: {output_file_path}")

            logger.info(
                f"Uploading {output_file_path.name} to Google Drive as a Google Doc..."
            )
            upload_file_to_gdrive(
                service, output_file_path, processed_folder_id, as_gdoc=True
            )

        except IOError as e:
            logger.error(f"Error writing to file {output_file_path}: {e}")

    logger.info("Finished processing Teamly documents.")
