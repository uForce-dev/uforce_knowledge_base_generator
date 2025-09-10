from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Type

from sqlalchemy import func
from sqlalchemy.orm import Session

from models import Post


class PostRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_posts_date_range_last_three_months(self) -> Tuple[Optional[int], Optional[int]]:
        """Returns the start timestamp (3 months ago) and the maximum CreateAt timestamp from the Posts table."""
        max_ts = self.db.query(func.max(Post.CreateAt)).scalar()
        if not max_ts:
            return None, None

        three_months_ago_dt = datetime.now() - timedelta(days=90)
        start_ts = int(three_months_ago_dt.timestamp() * 1000)

        return start_ts, max_ts

    def get_root_posts_in_date_range(self, start_ts: int, end_ts: int) -> List[Type[Post]]:
        """Returns root posts within a given date range, ordered by creation time."""
        return (
            self.db.query(Post)
            .filter(Post.CreateAt >= start_ts, Post.CreateAt < end_ts, Post.RootId == "")
            .order_by(Post.CreateAt)
            .all()
        )

    def get_thread_posts(self, thread_id: str) -> List[Type[Post]]:
        """Returns all posts belonging to a specific thread, ordered by creation time."""
        return self.db.query(Post).filter(Post.RootId == thread_id).order_by(Post.CreateAt).all()
