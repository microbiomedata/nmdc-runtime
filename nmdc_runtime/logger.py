import logging.config
from logging import getLogger
import sys

from asgi_correlation_id import CorrelationIdFilter

from nmdc_runtime.config import environ


def configure_logging() -> None:
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "filters": {  # correlation ID filter must be added here to make the %(correlation_id)s formatter work
                "correlation_id": {
                    "()": CorrelationIdFilter,
                    "uuid_length": 8,
                    "default_value": "-",
                },
            },
            "formatters": {
                # A formatter decides how our console logs look, and what info is included.
                # Adding %(correlation_id)s to this format is what make correlation IDs appear in our logs.
                # The `rich.logging.RichHandler` has its own level/time formatting, so we don't need to specify it here.
                "message_only": {
                    "format": "[%(correlation_id)s] %(message)s",
                },
                "verbose": {
                    "()": "nmdc_runtime.logger.ColoredFormatter",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                    "format": (
                        "%(asctime)s %(levelname)s [%(name)s:%(funcName)s:%(lineno)d] "
                        "[%(correlation_id)s] %(message)s"
                    ),
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": environ.get("LOG_LEVEL", "WARNING"),
                    "formatter": "verbose",
                    "filters": ["correlation_id"],
                },
            },
            # Loggers can be specified to set the log-level to log, and which handlers to use
            "loggers": {
                # project logger
                "nmdc_runtime": {
                    "handlers": ["console"],
                    "level": environ.get("LOG_LEVEL", "WARNING"),
                    "propagate": False,
                },
                # third-party package loggers
                # "databases": {"handlers": ["rich"], "level": "WARNING"},
                # "asgi_correlation_id": {"handlers": ["rich"], "level": "WARNING"},
            },
        }
    )


class ColoredFormatter(logging.Formatter):
    # ANSI color codes
    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"  # Reset to default color

    def format(self, record):
        # Get the original formatted message
        log_message = super().format(record)

        # Add color if outputting to terminal
        if hasattr(sys.stderr, "isatty") and sys.stderr.isatty():
            color = self.COLORS.get(record.levelname, "")
            return f"{color}{log_message}{self.RESET}"

        return log_message


logger = getLogger("nmdc_runtime")
