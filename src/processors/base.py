import logging


class BaseProcessor:
    """Base class for all processors."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def run(self) -> None:
        """Execute the processor."""
        raise NotImplementedError
