from os import environ
from sys import stdout

from loguru import logger

DEFAULT_LOG_LEVEL = "INFO"
LOG_LEVELS = {"TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"}

log_level = environ.get("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()

if log_level not in LOG_LEVELS:
    log_level = DEFAULT_LOG_LEVEL

log_format = (
    "[<green>{time:YYYY-MM-DD HH:mm:ss}</green>]"
    "[<level>{level.name[0]}</level>]"
    "[<blue>{file}</blue>:<cyan>{line}</cyan>] "
    "<level>{message}</level>"
)


def setup_logging() -> None:
    """Set up logging configuration for the application."""
    logger.remove()
    logger.add(sink=stdout, level=log_level, format=log_format, colorize=True)
