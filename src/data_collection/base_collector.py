"""
Base class for data collectors that send data to Kafka.
"""

import logging
from typing import Dict, Any, Optional

from src.common.messaging.kafka_producer import KafkaProducerWrapper
from src.utils.config import load_config


class BaseCollector:
    """
    Base collector class that provides common functionality for data collectors.

    This class handles configuration loading, logging setup, and Kafka integration
    to be inherited by specific collector implementations.
    """

    def __init__(self, config_path: str, collector_name: str):
        """
        Initialize the base collector.

        Args:
            config_path: Path to the Kafka configuration file
            collector_name: Name of the collector (used for logging and topics)
        """
        self.config = load_config(config_path)
        self.collector_name = collector_name
        self.logger = self._setup_logger()
        self.kafka_producers = {}  # Dictionary to store Kafka producers by topic

    def _setup_logger(self) -> logging.Logger:
        """
        Set up logging for the collector.

        Returns:
            Configured logger
        """
        logger = logging.getLogger(f"{self.collector_name}")

        # Configure logging if it hasn't been configured already
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)

        return logger

    def _get_producer(self, topic: str) -> KafkaProducerWrapper:
        """
        Get or create a Kafka producer for a specific topic.

        Args:
            topic: Kafka topic to produce to

        Returns:
            KafkaProducerWrapper instance
        """
        if topic not in self.kafka_producers:
            bootstrap_servers = self.config["kafka"]["bootstrap_servers_dev"]
            self.kafka_producers[topic] = KafkaProducerWrapper(
                bootstrap_servers=bootstrap_servers, topic=topic
            )
            self.logger.info(f"Created Kafka producer for topic: {topic}")

        return self.kafka_producers[topic]

    def send_to_kafka(
        self, topic: str, message: Dict[str, Any], key: Optional[str] = None
    ) -> bool:
        """
        Send a message to a Kafka topic.

        Args:
            topic: Kafka topic to send to
            message: Message to send
            key: Optional message key

        Returns:
            True if message was sent successfully, False otherwise
        """
        try:
            producer = self._get_producer(topic)
            success = producer.send_message(message)
            return success
        except Exception as e:
            self.logger.error(f"Error sending message to Kafka: {str(e)}")
            return False

    def collect(self) -> None:
        """
        Start the data collection process.

        This method should be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement collect()")

    def stop(self) -> None:
        """
        Stop the data collection process.

        This method should be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement stop()")

    def cleanup(self) -> None:
        """
        Clean up resources, including Kafka producers.
        """
        for topic, producer in self.kafka_producers.items():
            try:
                producer.close()
                self.logger.info(f"Closed Kafka producer for topic: {topic}")
            except Exception as e:
                self.logger.error(
                    f"Error closing Kafka producer for topic {topic}: {str(e)}"
                )
