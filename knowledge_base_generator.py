import datetime
import logging
import re
from typing import Set, List, Dict

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

    # Convert markdown to HTML
    html = markdown.markdown(text)

    # Strip HTML tags to get plain text
    soup = BeautifulSoup(html, "html.parser")
    plain_text = soup.get_text(separator=' ')

    # Remove mentions (which might survive the markdown conversion)
    plain_text = re.sub(r'@[a-zA-Z0-9_.]+', '', plain_text)

    # Remove emojis
    plain_text = re.sub(r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF]+', '',
                        plain_text)

    # Normalize whitespace
    plain_text = re.sub(r'\s+', ' ', plain_text).strip()

    return plain_text


def process_mattermost_posts(db: Session) -> None:
    """
    Fetches Mattermost posts from the last 3 months, processes them,
    and saves them into weekly chunk files.
    """
    repo = PostRepository(db)
    logger.info(f"Ensured temp directory exists: {settings.temp_dir}")

    start_ts, max_ts = repo.get_posts_date_range_last_three_months()
    if not start_ts or not max_ts:
        logger.info("No posts found in the last 3 months.")
        return

    start_date = datetime.datetime.fromtimestamp(start_ts / 1000)
    end_date = datetime.datetime.fromtimestamp(max_ts / 1000)
    logger.info(f"Processing posts from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    current_date = start_date
    while current_date <= end_date:
        week_start_ts = int(current_date.timestamp() * 1000)
        week_end_dt = current_date + datetime.timedelta(days=7)
        week_end_ts = int(week_end_dt.timestamp() * 1000)

        logger.info(f"Processing week: {current_date.strftime('%Y-%m-%d')} - {week_end_dt.strftime('%Y-%m-%d')}")

        # Fetch root posts for the current week
        root_posts_in_week: List[Post] = repo.get_root_posts_in_date_range(week_start_ts, week_end_ts)

        if not root_posts_in_week:
            logger.info(f"No root posts found for week starting {current_date.strftime('%Y-%m-%d')}. Skipping.")
            current_date += datetime.timedelta(days=7)
            continue

        # Collect all root post IDs for batch fetching
        root_post_ids = [post.Id for post in root_posts_in_week]

        # Fetch all posts (root and replies) related to these root_post_ids in one go
        all_relevant_posts: List[Post] = repo.get_posts_by_ids_or_root_ids(root_post_ids)

        # Reconstruct threads in memory
        threads: Dict[str, List[Post]] = {root_id: [] for root_id in root_post_ids}
        for post in all_relevant_posts:
            if post.RootId and post.RootId in threads:  # It's a reply to a root post we care about
                threads[post.RootId].append(post)
            elif post.Id in threads:  # It's a root post itself
                threads[post.Id].insert(0, post)  # Insert at the beginning to keep root first

        chunk_content: List[str] = []
        processed_threads_ids: Set[str] = set()

        for root_post in root_posts_in_week:
            if root_post.Id in processed_threads_ids:
                continue

            # Get the posts for this specific thread from our in-memory reconstructed threads
            thread_posts_list = threads.get(root_post.Id, [])

            # Sort thread posts by creation time to maintain chronological order
            thread_posts_list.sort(key=lambda p: p.CreateAt)

            for post in thread_posts_list:
                if post.Message:
                    cleaned_message = clean_text(post.Message)
                    if cleaned_message:
                        chunk_content.append(f"user: {post.UserId}, message: {cleaned_message}")

            processed_threads_ids.add(root_post.Id)

        if not chunk_content:
            logger.info(f"No content generated for week starting {current_date.strftime('%Y-%m-%d')}. Skipping.")
            current_date += datetime.timedelta(days=7)
            continue

        metadata = (
            f"---\n"
            f"source: Mattermost\n"
            f"category: Posts\n"
            f"date_range_start: {current_date.strftime('%Y-%m-%d')}\n"
            f"date_range_end: {week_end_dt.strftime('%Y-%m-%d')}\n"
            f"data_format: plain_text\n"
            f"---\n\n"
        )

        file_name = f"mattermost_posts_{current_date.strftime('%Y-%U')}.txt"
        file_path = settings.temp_dir / file_name

        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(metadata)
                f.write('\n'.join(chunk_content))
            logger.info(f"Generated file: {file_path}")
        except IOError as e:
            logger.error(f"Error writing to file {file_path}: {e}")

        current_date += datetime.timedelta(days=7)


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
