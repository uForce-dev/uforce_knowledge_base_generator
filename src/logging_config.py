import logging

from src.config import settings


_LOGGING_CONFIGURED = False


def setup_logging() -> None:
    """Configure logging once. Safe to call multiple times without duplicating handlers."""
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    settings.logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = settings.logs_dir / "app.log"

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # If any handlers already exist, assume logging was configured elsewhere.
    if root_logger.handlers:
        _LOGGING_CONFIGURED = True
        return

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    _LOGGING_CONFIGURED = True
    logging.info(f"Logging configured. Logs will be saved to {log_file}")
