"""
Logging utilities for consistent logging across the application.
"""

import logging
import os
import sys
import json
from datetime import datetime
from typing import Any


def setup_logger(name: str, log_level: int = logging.INFO) -> logging.Logger:
    """
    Set up a logger with a specific configuration.

    Args:
        name: Name of the logger
        log_level: Logging level (default: INFO)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    # Avoid adding handlers if they already exist
    if not logger.handlers:
        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        # Create file handler if LOG_PATH env var is set
        log_path = os.environ.get("LOG_PATH")
        if log_path:
            os.makedirs(log_path, exist_ok=True)
            file_path = os.path.join(log_path, f"{name}.log")
            file_handler = logging.FileHandler(file_path)
            file_handler.setLevel(log_level)
            file_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)

    return logger


class JsonLogger:
    """
    JSON format logger for structured logging.
    Useful for log aggregation systems like ELK stack.
    """

    def __init__(self, name: str, log_level: int = logging.INFO):
        """
        Initialize JSON logger.

        Args:
            name: Name of the logger
            log_level: Logging level (default: INFO)
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)
        self.name = name

        # Avoid adding handlers if they already exist
        if not self.logger.handlers:
            # Create console handler
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(log_level)
            self.logger.addHandler(console_handler)

            # Create file handler if LOG_PATH env var is set
            log_path = os.environ.get("LOG_PATH")
            if log_path:
                os.makedirs(log_path, exist_ok=True)
                file_path = os.path.join(log_path, f"{name}_json.log")
                file_handler = logging.FileHandler(file_path)
                file_handler.setLevel(log_level)
                self.logger.addHandler(file_handler)

    def _log(self, level: int, message: str, **kwargs: Any) -> None:
        """
        Log a message with additional data in JSON format.

        Args:
            level: Logging level
            message: Log message
            **kwargs: Additional data to include in the log
        """
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": logging.getLevelName(level),
            "logger": self.name,
            "message": message,
        }
        log_data.update(kwargs)

        self.logger.log(level, json.dumps(log_data))

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log a debug message."""
        self._log(logging.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        """Log an info message."""
        self._log(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log a warning message."""
        self._log(logging.WARNING, message, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        """Log an error message."""
        self._log(logging.ERROR, message, **kwargs)

    def critical(self, message: str, **kwargs: Any) -> None:
        """Log a critical message."""
        self._log(logging.CRITICAL, message, **kwargs)


def get_logger(name: str, use_json: bool = False, log_level: int = logging.INFO) -> Any:
    """
    Get an appropriate logger based on configuration.

    Args:
        name: Name of the logger
        use_json: Whether to use JSON formatting
        log_level: Logging level

    Returns:
        Logger instance (either standard Logger or JsonLogger)
    """
    if use_json:
        return JsonLogger(name, log_level)
    return setup_logger(name, log_level)
