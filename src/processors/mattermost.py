import datetime
import logging
import re

import markdown
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session

from src.config import settings
from src.constants import (
    MATTERMOST_CHANNEL_IDS,
    TOTAL_SEARCH_PERIOD_DAYS,
    PROCESSING_CHUNK_DAYS,
)
from src.database import get_db
from src.logging_config import setup_logging
from src.models import Post
from src.processors.base import BaseProcessor
from src.repository import PostRepository
from src.utils.datetime_utils import (
    epoch_ms_to_moscow_dt,
    format_dt_human_msk,
    format_date_ymd_msk,
)
from src.utils.gdrive_utils import (
    get_gdrive_service,
    upload_file_to_gdrive,
    delete_files_in_folder,
)

logger = logging.getLogger(__name__)


def clean_text(text: str) -> str:
    """Cleans the input text by removing markdown, mentions, emojis, and extra whitespace."""
    if not text:
        return ""

    html = markdown.markdown(text)
    soup = BeautifulSoup(html, "html.parser")
    plain_text = soup.get_text(separator=" ")
    plain_text = re.sub(r"@[a-zA-Z0-9_.]+", "", plain_text)
    plain_text = re.sub(r"[😀-🙏🌀-🗿🚀-🛿🇠-🇿🤀-🧿☀-➿]+", "", plain_text)
    plain_text = re.sub(r"\s+", " ", plain_text).strip()

    return plain_text


def process_mattermost_posts(db: Session) -> None:
    """Entrypoint wrapper for class-based Mattermost processing."""
    setup_logging()
    processor = MattermostProcessor(db, logger=logging.getLogger(__name__))
    processor.run()


class MattermostProcessor(BaseProcessor):
    """Process Mattermost posts to knowledge base chunks."""

    def __init__(self, db: Session, logger: logging.Logger | None = None) -> None:
        super().__init__(logger)
        self.db = db

    def run(self) -> None:
        repo = PostRepository(self.db)

        gdrive_service = get_gdrive_service()
        if not gdrive_service:
            self.logger.error(
                "Could not get Google Drive service. Aborting Mattermost processing."
            )
            return

        gdrive_folder_id = settings.google_drive_mattermost_processed_dir_id
        if not gdrive_folder_id:
            self.logger.error(
                "Mattermost processed folder ID is not set in settings. Aborting."
            )
            return

        self.logger.info(
            f"Clearing Google Drive folder ID: {gdrive_folder_id} for Mattermost files..."
        )
        delete_files_in_folder(gdrive_service, gdrive_folder_id)

        self.logger.info(f"Ensured temp directory exists: {settings.temp_dir}")

        channel_ids_to_process = MATTERMOST_CHANNEL_IDS
        if not channel_ids_to_process:
            self.logger.warning(
                "No Mattermost channel IDs specified in settings. Skipping post processing."
            )
            return

        channels = repo.get_channels_by_ids(channel_ids_to_process)
        channel_map = {channel.Id: channel.Name for channel in channels}
        self.logger.info(f"Processing posts for channels: {list(channel_map.values())}")

        start_ts, max_ts = repo.get_posts_date_range(days_ago=TOTAL_SEARCH_PERIOD_DAYS)
        if not start_ts or not max_ts:
            self.logger.info(
                f"No posts found in the last {TOTAL_SEARCH_PERIOD_DAYS} days."
            )
            return

        start_date = epoch_ms_to_moscow_dt(start_ts)
        end_date = epoch_ms_to_moscow_dt(max_ts)
        self.logger.info(
            f"Processing posts from {format_date_ymd_msk(start_date)} to {format_date_ymd_msk(end_date)}"
        )

        current_date = start_date
        while current_date <= end_date:
            period_start_dt = current_date
            period_end_dt = period_start_dt + datetime.timedelta(
                days=PROCESSING_CHUNK_DAYS
            )
            period_start_ts = int(
                period_start_dt.astimezone(datetime.timezone.utc).timestamp() * 1000
            )
            period_end_ts = int(
                period_end_dt.astimezone(datetime.timezone.utc).timestamp() * 1000
            )

            self.logger.info(
                f"Processing period: {period_start_dt.strftime('%Y-%m-%d')} - {period_end_dt.strftime('%Y-%m-%d')}"
            )

            for channel_id, channel_name in channel_map.items():
                self.logger.info(f"Processing channel: {channel_name}")

                root_posts_in_period: list[Post] = repo.get_root_posts_in_date_range(
                    period_start_ts, period_end_ts, channel_id
                )

                if not root_posts_in_period:
                    self.logger.info(
                        f"No root posts found for channel {channel_name} in this period. Skipping."
                    )
                    continue

                root_post_ids = [post.Id for post in root_posts_in_period]
                all_relevant_posts: list[Post] = repo.get_posts_by_ids_or_root_ids(
                    root_post_ids
                )

                user_ids = {post.UserId for post in all_relevant_posts}
                users = repo.get_users_by_ids(list(user_ids))
                user_map = {user.Id: user.Username for user in users}

                threads: dict[str, list[Post]] = {
                    root_id: [] for root_id in root_post_ids
                }
                for post in all_relevant_posts:
                    if post.RootId and post.RootId in threads:
                        threads[post.RootId].append(post)
                    elif post.Id in threads:
                        threads[post.Id].insert(0, post)

                chunk_content: list[str] = []
                processed_threads_ids: set[str] = set()

                for root_post in root_posts_in_period:
                    if root_post.Id in processed_threads_ids:
                        continue

                    thread_posts_list = threads.get(root_post.Id, [])
                    thread_posts_list.sort(key=lambda p: p.CreateAt)

                    for post in thread_posts_list:
                        if post.Message:
                            cleaned_message = clean_text(post.Message)
                            username = user_map.get(post.UserId, f"user_{post.UserId}")
                            if cleaned_message:
                                ts_msk = format_dt_human_msk(
                                    epoch_ms_to_moscow_dt(post.CreateAt)
                                )
                                chunk_content.append(
                                    f"datetime: {ts_msk}, user: {username}, message: {cleaned_message}"
                                )

                    processed_threads_ids.add(root_post.Id)

                if not chunk_content:
                    self.logger.info(
                        f"No content generated for channel {channel_name} in this period. Skipping."
                    )
                    continue

                metadata = (
                    "---\n"
                    "source: Mattermost\n"
                    f"channel: {channel_name}\n"
                    "tz: Europe/Moscow\n"
                    "date_range:\n"
                    f"  start: {format_date_ymd_msk(period_start_dt)}\n"
                    f"  end: {format_date_ymd_msk(period_end_dt)}\n"
                    "body_format: kv-lines\n"
                    "body_format_fields:\n"
                    "  datetime: Message date and time (MSK)\n"
                    "  user: Mattermost username\n"
                    "  message: Message text (cleaned)\n"
                    "---\n\n"
                )

                file_name = f"mattermost_posts_{channel_name}_{format_date_ymd_msk(period_start_dt)}_to_{format_date_ymd_msk(period_end_dt)}.txt"
                file_path = settings.mattermost_temp_dir / file_name

                try:
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(metadata)
                        f.write("\n".join(chunk_content))
                    self.logger.info(f"Generated file: {file_path}")

                    self.logger.info(
                        f"Uploading {file_path.name} to Google Drive as a Google Doc..."
                    )
                    upload_file_to_gdrive(
                        gdrive_service, file_path, gdrive_folder_id, as_gdoc=True
                    )

                except IOError as e:
                    self.logger.error(f"Error writing to file {file_path}: {e}")

            current_date += datetime.timedelta(days=PROCESSING_CHUNK_DAYS)


if __name__ == "__main__":
    process_mattermost_posts(next(get_db()))
