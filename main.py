from database import get_db
from knowledge_base_generator import process_mattermost_posts


def main() -> None:
    db_session = next(get_db())
    try:
        process_mattermost_posts(db_session)
    finally:
        db_session.close()


if __name__ == "__main__":
    main()
