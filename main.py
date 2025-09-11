import logging
import shutil

from config import settings
from database import get_db
from knowledge_base_generator import process_mattermost_posts
from logging_config import setup_logging

logger = logging.getLogger(__name__)


def initialize() -> None:
    setup_logging()

    settings.logs_dir.mkdir(exist_ok=True)
    settings.secrets_dir.mkdir(exist_ok=True)

    if settings.temp_dir.exists():
        shutil.rmtree(settings.temp_dir)

    settings.temp_dir.mkdir(exist_ok=True)
    settings.mattermost_temp_dir.mkdir(exist_ok=True)


def main() -> None:
    initialize()
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
