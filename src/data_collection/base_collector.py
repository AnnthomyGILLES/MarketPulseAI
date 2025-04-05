# src/data_collection/base_collector.py

import abc
from loguru import logger


class BaseCollector(abc.ABC):
    """
    Abstract base class for data collectors using Loguru.

    Provides common logging setup and defines the core interface (`collect`, `stop`).
    Subclasses must implement the abstract methods. Uses Loguru for logging.
    """

    def __init__(
        self,
        collector_name: str,
    ):
        """
        Initializes the collector and assigns a logger context.

        Args:
            collector_name: Identifier for the collector instance (used in logs).
        """
        self.collector_name = collector_name
        self.logger = logger.bind(collector=self.collector_name)
        self.logger.info(f"Collector '{self.collector_name}' initialized.")

    @abc.abstractmethod
    def collect(self) -> None:
        """
        Core data collection logic. Must be implemented by subclasses.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def stop(self) -> None:
        """
        Initiates graceful shutdown. Must be implemented by subclasses.

        Implementations should handle stopping loops, releasing resources,
        and ensuring clean termination.
        """
        self.logger.info(f"Stop signal received for '{self.collector_name}'.")
        # Subclass implementation should follow

    def cleanup(self) -> None:
        """
        Optional resource cleanup hook for subclasses.

        Override this method to perform specific cleanup tasks not handled in stop().
        """
        self.logger.debug(f"Performing base cleanup for '{self.collector_name}'...")
        # Subclasses can override to add specific cleanup actions.
        self.logger.debug(f"Base cleanup finished for '{self.collector_name}'.")
