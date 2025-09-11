from datetime import datetime, timedelta

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.models import Post, Channel, User


class PostRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_posts_date_range(self, days_ago: int) -> tuple[int | None, int | None]:
        """Returns the start timestamp (days ago) and the maximum CreateAt timestamp from the Posts table."""
        max_ts = self.db.query(func.max(Post.CreateAt)).scalar()
        if not max_ts:
            return None, None

        start_dt = datetime.now() - timedelta(days=days_ago)
        start_ts = int(start_dt.timestamp() * 1000)

        return start_ts, max_ts

    def get_root_posts_in_date_range(
        self, start_ts: int, end_ts: int, channel_id: str
    ) -> list[Post]:
        """Returns root posts within a given date range for a specific channel, ordered by creation time."""
        return (
            self.db.query(Post)
            .join(Channel, Post.ChannelId == Channel.Id)
            .filter(
                Post.CreateAt >= start_ts,
                Post.CreateAt < end_ts,
                Post.RootId == "",
                Post.ChannelId == channel_id,
                Channel.Type.in_(["O", "P"]),
            )
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
        return (
            self.db.query(Post)
            .filter((Post.Id.in_(post_ids)) | (Post.RootId.in_(post_ids)))
            .order_by(Post.CreateAt)
            .all()
        )

    def get_user_by_id(self, user_id: str) -> User | None:
        """Returns a user by their ID."""
        return self.db.query(User).filter(User.Id == user_id).first()

    def get_users_by_ids(self, user_ids: list[str]) -> list[User]:
        """Returns a list of users by their IDs."""
        if not user_ids:
            return []
        return self.db.query(User).filter(User.Id.in_(user_ids)).all()

    def get_channels_by_ids(self, channel_ids: list[str]) -> list[Channel]:
        """Returns a list of channels by their IDs."""
        if not channel_ids:
            return []
        return self.db.query(Channel).filter(Channel.Id.in_(channel_ids)).all()

    def get_channel_name_by_id(self, channel_id: str) -> str | None:
        """Returns a channel's name by its ID."""
        channel = self.db.query(Channel).filter(Channel.Id == channel_id).first()
        return channel.Name if channel else None
