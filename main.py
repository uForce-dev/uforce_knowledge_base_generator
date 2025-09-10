import logging

from database import get_db
from knowledge_base_generator import process_mattermost_posts

logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("Starting Mattermost knowledge base generation.")
    db_session = next(get_db())
    try:
        process_mattermost_posts(db_session)
    except Exception as e:
        logger.critical(f"An unhandled error occurred: {e}", exc_info=True)
    finally:
        db_session.close()
        logger.info("Mattermost knowledge base generation finished. Database session closed.")


if __name__ == "__main__":
    main()
