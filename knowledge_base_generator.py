import datetime
import logging
import re

import markdown
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session

from config import settings
from models import Post
from repository import PostRepository

logger = logging.getLogger(__name__)


def clean_text(text: str) -> str:
    """Cleans the input text by removing markdown, mentions, emojis, and extra whitespace."""
    if not text:
        return ""

    html = markdown.markdown(text)
    soup = BeautifulSoup(html, "html.parser")
    plain_text = soup.get_text(separator=' ')
    plain_text = re.sub(r'@[a-zA-Z0-9_.]+', '', plain_text)
    plain_text = re.sub(r'[😀-🙏🌀-🗿🚀-🛿🇠-🇿🤀-🧿☀-➿]+', '',
                        plain_text)
    plain_text = re.sub(r'\s+', ' ', plain_text).strip()

    return plain_text


def process_mattermost_posts(db: Session) -> None:
    """
    Fetches Mattermost posts from specified channels for a configurable period,
    processes them, and saves them into chunk files per channel per configured period.
    """
    repo = PostRepository(db)
    logger.info(f"Ensured temp directory exists: {settings.temp_dir}")

    channel_ids_to_process = settings.mattermost_channel_ids_list
    if not channel_ids_to_process:
        logger.warning("No Mattermost channel IDs specified in settings. Skipping post processing.")
        return

    channels = repo.get_channels_by_ids(channel_ids_to_process)
    channel_map = {channel.Id: channel.Name for channel in channels}
    logger.info(f"Processing posts for channels: {list(channel_map.values())}")

    start_ts, max_ts = repo.get_posts_date_range(days_ago=settings.total_search_period_days)
    if not start_ts or not max_ts:
        logger.info(f"No posts found in the last {settings.total_search_period_days} days.")
        return

    start_date = datetime.datetime.fromtimestamp(start_ts / 1000)
    end_date = datetime.datetime.fromtimestamp(max_ts / 1000)
    logger.info(f"Processing posts from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    current_date = start_date
    while current_date <= end_date:
        period_start_dt = current_date
        period_end_dt = period_start_dt + datetime.timedelta(days=settings.processing_chunk_days)
        period_start_ts = int(period_start_dt.timestamp() * 1000)
        period_end_ts = int(period_end_dt.timestamp() * 1000)

        logger.info(f"Processing period: {period_start_dt.strftime('%Y-%m-%d')} - {period_end_dt.strftime('%Y-%m-%d')}")

        for channel_id, channel_name in channel_map.items():
            logger.info(f"Processing channel: {channel_name}")

            root_posts_in_period: list[Post] = repo.get_root_posts_in_date_range(
                period_start_ts, period_end_ts, channel_id
            )

            if not root_posts_in_period:
                logger.info(f"No root posts found for channel {channel_name} in this period. Skipping.")
                continue

            root_post_ids = [post.Id for post in root_posts_in_period]
            all_relevant_posts: list[Post] = repo.get_posts_by_ids_or_root_ids(root_post_ids)

            user_ids = {post.UserId for post in all_relevant_posts}
            users = repo.get_users_by_ids(list(user_ids))
            user_map = {user.Id: user.Username for user in users}

            threads: dict[str, list[Post]] = {root_id: [] for root_id in root_post_ids}
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
                            chunk_content.append(
                                f"timestamp: {post.CreateAt}, user: {username}, message: {cleaned_message}")

                processed_threads_ids.add(root_post.Id)

            if not chunk_content:
                logger.info(f"No content generated for channel {channel_name} in this period. Skipping.")
                continue

            metadata = (
                f"---\n"
                f"source: Mattermost\n"
                f"category: Posts\n"
                f"channel: {channel_name}\n"
                f"date_range_start: {period_start_dt.strftime('%Y-%m-%d')}\n"
                f"date_range_end: {period_end_dt.strftime('%Y-%m-%d')}\n"
                f"data_format: 'timestamp: <unix_timestamp_ms>, user: <username>, message: <cleaned_message>'.\n"
                f"---\n\n"
            )

            file_name = f"mattermost_posts_{channel_name}_{period_start_dt.strftime('%Y-%m-%d')}_to_{period_end_dt.strftime('%Y-%m-%d')}.txt"
            file_path = settings.temp_dir / file_name

            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(metadata)
                    f.write('\n'.join(chunk_content))
                logger.info(f"Generated file: {file_path}")
            except IOError as e:
                logger.error(f"Error writing to file {file_path}: {e}")

        current_date += datetime.timedelta(days=settings.processing_chunk_days)


if __name__ == "__main__":
    from database import get_db

    logger.info("Starting Mattermost knowledge base generation.")
    db_session = next(get_db())
    try:
        process_mattermost_posts(db_session)
    except Exception as e:
        logger.critical(f"An unhandled error occurred: {e}", exc_info=True)
    finally:
        db_session.close()
        logger.info("Mattermost knowledge base generation finished. Database session closed.")
