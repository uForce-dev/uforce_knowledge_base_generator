import logging

logger = logging.getLogger(__name__)


def process_teamly_documents() -> None:
    """
    Processes .docx files from a specified Google Drive directory,
    extracts their text content, and saves them as .txt files with metadata.
    """
    logger.info("Processing of Teamly documents is not yet implemented.")
    # TODO: Implement the logic to:
    # 1. Connect to Google Drive.
    # 2. Find and download .docx files from settings.google_drive_source_dir_id.
    # 3. Extract text from .docx files.
    # 4. Preserve the folder structure from Google Drive inside the temp/teamly directory.
    # 5. Create metadata similar to the Mattermost processor.
    # 6. Save .txt files to settings.teamly_temp_dir.
    pass
