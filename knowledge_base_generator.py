import datetime
import os
import re

import markdown
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session

from config import settings
from models import Post
from repository import PostRepository


def clean_text(text: str) -> str:
    """Cleans the input text by removing markdown, mentions, emojis, and extra whitespace."""
    if not text:
        return ""

    # Convert markdown to HTML
    html = markdown.markdown(text)

    # Strip HTML tags to get plain text
    soup = BeautifulSoup(html, "html.parser")
    plain_text = soup.get_text(separator=" ")

    # Remove mentions (which might survive the markdown conversion)
    plain_text = re.sub(r'@[a-zA-Z0-9_.]+', '', plain_text)

    # Remove emojis
    plain_text = re.sub(r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF]+', '', plain_text)

    # Normalize whitespace
    plain_text = re.sub(r'\s+', ' ', plain_text).strip()

    return plain_text


def process_mattermost_posts(db: Session) -> None:
    """
    Fetches Mattermost posts from the last 3 months, processes them,
    and saves them into weekly chunk files.
    """
    repo = PostRepository(db)
    os.makedirs(settings.temp_dir, exist_ok=True)

    start_ts, max_ts = repo.get_posts_date_range_last_three_months()
    if not start_ts or not max_ts:
        print("No posts found in the last 3 months.")
        return

    start_date = datetime.datetime.fromtimestamp(start_ts / 1000)
    end_date = datetime.datetime.fromtimestamp(max_ts / 1000)

    current_date = start_date
    while current_date <= end_date:
        week_start_ts = int(current_date.timestamp() * 1000)
        week_end_dt = current_date + datetime.timedelta(days=7)
        week_end_ts = int(week_end_dt.timestamp() * 1000)

        root_posts: list[type[Post]] = repo.get_root_posts_in_date_range(week_start_ts, week_end_ts)

        if not root_posts:
            current_date += datetime.timedelta(days=7)
            continue

        chunk_content: list[str] = []
        processed_threads: set[str] = set()

        for root_post in root_posts:
            if root_post.Id in processed_threads:
                continue

            thread_posts: list[type[Post]] = repo.get_thread_posts(str(root_post.Id))
            # Ensure the root post is the first in the thread list and combine
            all_thread_posts: list[type[Post]] = [root_post] + [p for p in thread_posts if p.Id != root_post.Id]

            for post in all_thread_posts:
                if post.Message:
                    cleaned_message = clean_text(str(post.Message))
                    if cleaned_message:
                        chunk_content.append(f"user: {post.UserId}, message: {cleaned_message}")

            processed_threads.add(str(root_post.Id))

        if not chunk_content:
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
            print(f"Generated file: {file_path}")
        except IOError as e:
            print(f"Error writing to file {file_path}: {e}")

        current_date += datetime.timedelta(days=7)
