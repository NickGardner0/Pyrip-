"""
This is the Pyrip package.

This package provides a Python SDK for interacting with the Pyrip API.
It includes methods to scrape URLs, perform searches, initiate and monitor crawl jobs,
and check the status of these jobs.

For more information visit https://github.com/pyrip/
"""

import logging
import os

from .pyrip import PyripApp # noqa

__version__ = "1.5.0"

# Define the logger for the Pyrip project
logger: logging.Logger = logging.getLogger("pyrip")


def _configure_logger() -> None:
    """
    Configure the pyrip logger for console output.

    The function attaches a handler for console output with a specific format and date
    format to the pyrip logger.
    """
    try:
        # Create the formatter
        formatter = logging.Formatter(
            "[%(asctime)s - %(name)s:%(lineno)d - %(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Create the console handler and set the formatter
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        # Add the console handler to the pyrip logger
        logger.addHandler(console_handler)
    except Exception as e:
        logger.error("Failed to configure logging: %s", e)


def setup_logging() -> None:
    """Set up logging based on the PYRIP_LOGGING_LEVEL environment variable."""
    # Check if the pyrip logger already has a handler
    if logger.hasHandlers():
        return # To prevent duplicate logging

    # Check if the PYRIP_LOGGING_LEVEL environment variable is set
    if not (env := os.getenv("PYRIP_LOGGING_LEVEL", "").upper()):
        # Attach a no-op handler to prevent warnings about no handlers
        logger.addHandler(logging.NullHandler()) 
        return

    # Attach the console handler to the pyrip logger
    _configure_logger()

    # Set the logging level based on the PYRIP_LOGGING_LEVEL environment variable
    if env == "DEBUG":
        logger.setLevel(logging.DEBUG)
    elif env == "INFO":
        logger.setLevel(logging.INFO)
    elif env == "WARNING":
        logger.setLevel(logging.WARNING)
    elif env == "ERROR":
        logger.setLevel(logging.ERROR)
    elif env == "CRITICAL":
        logger.setLevel(logging.CRITICAL)
    else:
        logger.setLevel(logging.INFO)
        logger.warning("Unknown logging level: %s, defaulting to INFO", env)


# Initialize logging configuration when the module is imported
setup_logging()
logger.debug("Debugging logger setup")
