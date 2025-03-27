"""
Base collector module providing the foundation for all data collectors.
"""

import json
import time
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

from kafka import KafkaProducer

from src.utils.config import load_config
from src.utils.logging import setup_logger


class BaseCollector(ABC):
    """
    Abstract base class for all data collectors.

    This class provides common functionality for data collection,
    including connection to Kafka, configuration loading, and error handling.
    """

    def __init__(self, config_path: str, logger_name: str):
        """
        Initialize the base collector.

        Args:
            config_path: Path to the configuration file
            logger_name: Name for the logger instance
        """
        self.config = load_config(config_path)
        self.logger = setup_logger(logger_name)
        self.producer = self._create_kafka_producer()

    def _create_kafka_producer(self) -> KafkaProducer:
        """
        Create and configure a Kafka producer.

        Returns:
            KafkaProducer: Configured Kafka producer instance
        """
        try:
            kafka_config = self.config["kafka"]["producer"]
            bootstrap_servers = self.config["kafka"]["bootstrap_servers"]

            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                acks=kafka_config["acks"],
                retries=kafka_config["retries"],
                batch_size=kafka_config["batch_size"],
                linger_ms=kafka_config["linger_ms"],
                buffer_memory=kafka_config["buffer_memory"],
                value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            )
        except Exception as e:
            self.logger.error(f"Failed to create Kafka producer: {str(e)}")
            raise

    def send_to_kafka(
        self, topic: str, data: Dict[str, Any], key: Optional[str] = None
    ) -> None:
        """
        Send data to a Kafka topic.

        Args:
            topic: Kafka topic to send data to
            data: Data to be sent
            key: Optional key for the Kafka message
        """
        try:
            if key:
                self.producer.send(topic, key=key.encode("utf-8"), value=data)
            else:
                self.producer.send(topic, value=data)
            self.logger.debug(f"Data sent to topic {topic}")
        except Exception as e:
            self.logger.error(f"Failed to send data to Kafka topic {topic}: {str(e)}")
            self._handle_error(data, str(e))

    def _handle_error(self, data: Dict[str, Any], error_message: str) -> None:
        """
        Handle errors during data collection or sending.

        Args:
            data: The data that failed to process
            error_message: The error message
        """
        error_data = {
            "original_data": data,
            "error_message": error_message,
            "timestamp": time.time(),
        }
        try:
            self.producer.send(self._get_error_topic(), value=error_data)
        except Exception as e:
            self.logger.critical(f"Failed to send error data to error topic: {str(e)}")

    @abstractmethod
    def _get_error_topic(self) -> str:
        """
        Get the appropriate error topic for this collector.

        Returns:
            str: The name of the error topic
        """
        pass

    @abstractmethod
    def collect(self) -> None:
        """
        Collect data from the source.
        This method must be implemented by subclasses.
        """
        pass

    def close(self) -> None:
        """Close the Kafka producer and perform cleanup."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
        self.logger.info("Collector closed")
