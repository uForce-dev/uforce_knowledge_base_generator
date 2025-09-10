from sqlalchemy.orm import Session

from models import Post


class PostRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_posts(self, skip: int = 0, limit: int = 100) -> list[type[Post]]:
        return self.db.query(Post).offset(skip).limit(limit).all()
