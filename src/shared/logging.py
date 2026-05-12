import logging
import sys

LOGGER_NAME = "wedscraper"


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure and return the project logger."""
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(levelname)-8s %(message)s"))
        logger.addHandler(handler)

    return logger


def get_logger() -> logging.Logger:
    return logging.getLogger(LOGGER_NAME)
