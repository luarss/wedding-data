import logging
import sys

LOGGER_NAME = "wedextractor"

_configured = False


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure and return the project logger."""
    global _configured
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(levelname)-8s %(message)s"))
        logger.addHandler(handler)

    _configured = True
    return logger


def get_logger() -> logging.Logger:
    if not _configured:
        setup_logging(verbose=False)
    return logging.getLogger(LOGGER_NAME)
