from datetime import datetime, timedelta

from sqlalchemy import func
from sqlalchemy.orm import Session

from models import Post, User


class PostRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_posts_date_range_last_three_months(self) -> tuple[int | None, int | None]:
        """Returns the start timestamp (3 months ago) and the maximum CreateAt timestamp from the Posts table."""
        max_ts = self.db.query(func.max(Post.CreateAt)).scalar()
        if not max_ts:
            return None, None

        three_months_ago_dt = datetime.now() - timedelta(days=90)
        start_ts = int(three_months_ago_dt.timestamp() * 1000)

        return start_ts, max_ts

    def get_root_posts_in_date_range(self, start_ts: int, end_ts: int) -> list[Post]:
        """Returns root posts within a given date range, ordered by creation time."""
        return (
            self.db.query(Post)
            .filter(Post.CreateAt >= start_ts, Post.CreateAt < end_ts, Post.RootId == "")
            .order_by(Post.CreateAt)
            .all()
        )

    def get_posts_by_ids_or_root_ids(self, post_ids: list[str]) -> list[Post]:
        """
        Returns posts whose Id is in post_ids, or whose RootId is in post_ids.
        This effectively fetches all posts belonging to the threads identified by post_ids.
        """
        if not post_ids:
            return []
        return self.db.query(Post).filter(
            (Post.Id.in_(post_ids)) | (Post.RootId.in_(post_ids))
        ).order_by(Post.CreateAt).all()

    def get_user_by_id(self, user_id: str) -> User | None:
        """Returns a user by their ID."""
        return self.db.query(User).filter(User.Id == user_id).first()

    def get_users_by_ids(self, user_ids: list[str]) -> list[User]:
        """Returns a list of users by their IDs."""
        if not user_ids:
            return []
        return self.db.query(User).filter(User.Id.in_(user_ids)).all()
