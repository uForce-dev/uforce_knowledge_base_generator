import argparse
import logging
import shutil

from src.config import settings
from src.database import get_db
from src.logging_config import setup_logging
from src.processors.mattermost import process_mattermost_posts
from src.processors.teamly import process_teamly_documents

logger = logging.getLogger(__name__)


def initialize() -> None:
    setup_logging()

    settings.logs_dir.mkdir(exist_ok=True)
    settings.secrets_dir.mkdir(exist_ok=True)

    if settings.temp_dir.exists():
        shutil.rmtree(settings.temp_dir)

    settings.temp_dir.mkdir(exist_ok=True)
    settings.mattermost_temp_dir.mkdir(exist_ok=True)
    settings.teamly_temp_dir.mkdir(exist_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Knowledge Base Generator.")
    parser.add_argument(
        "processors",
        nargs="*",
        help="Specify processors to run: 'mattermost', 'teamly'. Run all if not specified.",
    )

    args = parser.parse_args()
    processors_to_run = args.processors if args.processors else ["mattermost", "teamly"]

    initialize()
    logger.info("Starting knowledge base generation.")

    run_mattermost = "mattermost" in processors_to_run
    run_teamly = "teamly" in processors_to_run

    db_session = next(get_db())
    try:
        if run_mattermost:
            logger.info("Starting Mattermost knowledge base generation.")
            process_mattermost_posts(db_session)
            logger.info("Mattermost knowledge base generation finished.")

        if run_teamly:
            logger.info("Starting Teamly knowledge base generation.")
            process_teamly_documents()
            logger.info("Teamly knowledge base generation finished.")

    except Exception as e:
        logger.critical(f"An unhandled error occurred: {e}", exc_info=True)
    finally:
        db_session.close()
        logger.info("Knowledge base generation finished. Database session closed.")
