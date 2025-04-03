# src/data_collection/base_collector.py

import abc  # Used for defining abstract base classes
import logging
from pathlib import Path
from typing import Dict, Any, Optional

import yaml

# Assuming these imports are correct based on the project structure
from src.common.messaging.kafka_producer import KafkaProducerWrapper  #
from src.utils.config import load_config  #
from src.utils.logging import get_logger  # Use the central logging utility


class BaseCollector(abc.ABC):
    """
    Abstract Base Class for data collectors that send data to Kafka.

    Handles common functionalities:
    - Loading Kafka configuration.
    - Setting up a dedicated logger.
    - Managing Kafka producer instances per topic.
    - Providing a method to send messages to Kafka.
    - Defining required methods (`collect`, `stop`) for subclasses.
    - Cleaning up resources (Kafka producers).
    """

    # Define a default path relative to this file's location if needed
    DEFAULT_KAFKA_CONFIG_PATH = (
        Path(__file__).resolve().parents[2] / "config" / "kafka" / "kafka_config.yaml"
    )

    def __init__(
        self,
        collector_name: str,
        kafka_config_path: Optional[str] = None,
        log_level: int = logging.INFO,
        use_json_logging: bool = False,
    ):
        """
        Initialize the base collector.

        Args:
            collector_name: Name of the collector (used for logging).
            kafka_config_path: Optional path to the Kafka configuration file.
                               Defaults to DEFAULT_KAFKA_CONFIG_PATH if None.
            log_level: Logging level (e.g., logging.INFO, logging.DEBUG).
            use_json_logging: Whether to use JSON format for logging.
        """
        self.collector_name = collector_name
        # Use the central get_logger utility
        self.logger = get_logger(
            f"collector.{self.collector_name}",
            use_json=use_json_logging,
            log_level=log_level,
        )

        self.kafka_config_path = Path(
            kafka_config_path or self.DEFAULT_KAFKA_CONFIG_PATH
        )
        self.kafka_config: Dict[str, Any] = self._load_kafka_config()

        # Dictionary to store Kafka producers, keyed by topic name
        self._kafka_producers: Dict[str, KafkaProducerWrapper] = {}
        self.logger.info(f"BaseCollector '{self.collector_name}' initialized.")

    def _load_kafka_config(self) -> Dict[str, Any]:
        """Loads Kafka configuration, ensuring necessary keys exist."""
        self.logger.info(f"Loading Kafka configuration from: {self.kafka_config_path}")
        try:
            config = load_config(str(self.kafka_config_path))  #
            # Validate essential config sections/keys
            if not config:
                raise ValueError("Kafka configuration file is empty.")
            if "kafka" not in config:
                raise ValueError("Kafka configuration missing 'kafka' section.")
            if (
                "bootstrap_servers_dev" not in config["kafka"]
            ):  # Or use a configurable key like 'bootstrap_servers'
                raise ValueError(
                    "Kafka configuration missing 'kafka.bootstrap_servers_dev' key."
                )
            if "topics" not in config["kafka"]:
                self.logger.warning(
                    "Kafka configuration missing 'kafka.topics' section. Topic names must be provided directly."
                )
                # Allow continuation but topic names will need to be explicit

            self.logger.info("Kafka configuration loaded successfully.")
            return config["kafka"]  # Return only the 'kafka' subsection
        except (FileNotFoundError, yaml.YAMLError, ValueError) as e:
            self.logger.exception(
                f"Failed to load or validate Kafka configuration from {self.kafka_config_path}: {e}"
            )
            raise  # Re-raise as config is critical

    def _get_producer(self, topic: str) -> KafkaProducerWrapper:
        """
        Retrieves or creates a Kafka producer for the specified topic.

        Manages a dictionary of producers, ensuring only one instance per topic.

        Args:
            topic: The Kafka topic name.

        Returns:
            An instance of KafkaProducerWrapper for the topic.

        Raises:
            ValueError: If bootstrap servers are not configured.
            Exception: If producer creation fails.
        """
        if topic not in self._kafka_producers:
            self.logger.info(f"Creating Kafka producer for topic: {topic}...")
            try:
                bootstrap_servers = self.kafka_config.get(
                    "bootstrap_servers_dev"
                )  # Or use a configurable key
                if not bootstrap_servers:
                    # This should have been caught in _load_kafka_config, but double-check
                    raise ValueError(
                        "Bootstrap servers are not defined in Kafka configuration."
                    )

                # Pass relevant producer settings from config if needed
                producer_kwargs = self.kafka_config.get("producer_defaults", {})

                self._kafka_producers[topic] = KafkaProducerWrapper(
                    bootstrap_servers=bootstrap_servers,
                    **producer_kwargs,  # Add other default producer settings
                )
                self.logger.info(
                    f"Kafka producer for topic '{topic}' created successfully."
                )
            except Exception as e:
                self.logger.exception(
                    f"Failed to create Kafka producer for topic '{topic}': {e}"
                )
                raise  # Re-raise the exception

        return self._kafka_producers[topic]

    def send_to_kafka(
        self, topic: str, message: Dict[str, Any], key: Optional[str] = None
    ) -> bool:
        """
        Sends a message dictionary to the specified Kafka topic.

        Uses the managed KafkaProducerWrapper instance for the topic.

        Args:
            topic: The target Kafka topic name.
            message: The message dictionary to send (must be serializable).
            key: Optional Kafka message key (usually a string, will be encoded).

        Returns:
            True if the message was accepted by the producer for sending, False otherwise.
            Note: Actual delivery confirmation might require producer callbacks.
        """
        try:
            producer = self._get_producer(topic)
            self.logger.debug(f"Sending message to Kafka topic '{topic}' (Key: {key})")
            # KafkaProducerWrapper's send_message handles serialization and retries
            # Pass the key if provided (producer wrapper should handle encoding if necessary)
            # Note: Original KafkaProducerWrapper didn't explicitly handle keys, might need adjustment
            # Assuming send_message in the wrapper is updated or ignores the key for now.
            # A more robust implementation might require the wrapper to accept the key.
            success = producer.send_message(
                topic, message
            )  # Key handling might need wrapper update
            if not success:
                self.logger.warning(
                    f"Producer reported failure sending message to topic '{topic}' (Key: {key}). Check producer logs."
                )
            return success
        except Exception as e:
            # Catch errors during producer retrieval or sending attempt
            self.logger.error(
                f"Error sending message to Kafka topic '{topic}': {e}", exc_info=True
            )
            return False

    @abc.abstractmethod
    def collect(self) -> None:
        """
        Main data collection logic.

        Subclasses MUST implement this method to perform their specific data gathering tasks.
        """
        raise NotImplementedError("Subclasses must implement the 'collect' method.")

    @abc.abstractmethod
    def stop(self) -> None:
        """
        Signal the collector to stop its operation gracefully.

        Subclasses MUST implement this method to handle shutdown signals,
        stop loops, and potentially wait for ongoing tasks to complete.
        """
        self.logger.info(f"Stop requested for collector '{self.collector_name}'.")
        # Base implementation can log, subclasses add specific stop logic

    def cleanup(self) -> None:
        """
        Cleans up resources, primarily closing Kafka producers.

        Should be called after the collector has stopped.
        """
        self.logger.info(
            f"Cleaning up resources for collector '{self.collector_name}'..."
        )
        closed_topics = []
        for topic, producer in self._kafka_producers.items():
            self.logger.info(f"Closing Kafka producer for topic: {topic}...")
            try:
                # Ensure flush is called before close, similar to wrapper's __exit__
                producer.producer.flush(timeout=10)  # Allow time to send buffer
                producer.close()  # Call the wrapper's close method
                closed_topics.append(topic)
                self.logger.info(
                    f"Successfully closed Kafka producer for topic: {topic}."
                )
            except Exception as e:
                self.logger.error(
                    f"Error closing Kafka producer for topic '{topic}': {e}",
                    exc_info=True,
                )

        # Remove closed producers from the dictionary
        for topic in closed_topics:
            del self._kafka_producers[topic]

        self.logger.info(f"Cleanup finished for collector '{self.collector_name}'.")
