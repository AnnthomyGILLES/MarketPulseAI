"""
Kafka-based validation service for news data.
"""

import json
import os
import signal
import time
from typing import Any
from pathlib import Path

from loguru import logger

from src.common.messaging.kafka_producer import KafkaProducerWrapper
from src.common.messaging.kafka_consumer import KafkaConsumerWrapper
from src.data_collection.news.validation.news_validator import NewsDataValidator
from src.utils.config import load_config


class NewsValidationService:
    """
    Service that consumes raw news data from Kafka, validates it,
    and produces validated and invalid records to separate topics.
    """

    def __init__(
        self,
        config_path: str,
        kafka_config_path: str,
        service_name: str = "NewsValidationService",
    ):
        """
        Initialize the validation service.

        Args:
            config_path: Path to the news config file
            kafka_config_path: Path to the Kafka config file
            service_name: Name identifier for the service
        """
        self.service_name = service_name
        self.config_path = config_path
        self.kafka_config_path = kafka_config_path

        # Load configurations
        self.config = load_config(config_path)
        self.kafka_config = load_config(kafka_config_path)

        # Set up validator
        self.validator = NewsDataValidator()

        # Set up Kafka clients
        self._setup_kafka_clients()

        # Runtime control
        self.running = False

        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        logger.info(f"{self.service_name} initialized")

    def _setup_kafka_clients(self):
        """Initialize Kafka consumer and producers."""
        try:
            # Determine bootstrap servers based on environment
            if os.getenv("CONTAINER_ENV") == "true":
                bootstrap_servers = self.kafka_config["bootstrap_servers_container"]
            else:
                bootstrap_servers = self.kafka_config["bootstrap_servers"]

            # Get topics
            topics_config = self.kafka_config.get("topics", {})

            # Input topic
            self.input_topic = topics_config.get("news_data_raw", "news-data-raw")

            # Output topics
            self.valid_topic = topics_config.get(
                "news_data_validated", "news-data-validated"
            )
            self.invalid_topic = topics_config.get(
                "news_data_invalid", "news-data-invalid"
            )
            self.error_topic = topics_config.get("news_data_error", "news-data-error")

            # Consumer setup
            consumer_group = self.kafka_config.get("consumer_groups", {}).get(
                "news_validation", "news-validation-group"
            )

            self.consumer = KafkaConsumerWrapper(
                bootstrap_servers=bootstrap_servers,
                topics=[self.input_topic],
                group_id=consumer_group,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            logger.info(f"Kafka consumer initialized for topic: {self.input_topic}")

            # Producers setup
            producer_config = {
                "bootstrap_servers": bootstrap_servers,
                "client_id": f"news-validation-{os.getpid()}",
                "acks": "all",
                "retries": 3,
            }

            self.valid_producer = KafkaProducerWrapper(**producer_config)
            self.invalid_producer = KafkaProducerWrapper(**producer_config)
            self.error_producer = KafkaProducerWrapper(**producer_config)

            logger.info("Kafka producers initialized")

        except Exception as e:
            logger.exception(f"Failed to set up Kafka clients: {e}")
            raise

    def run(self):
        """
        Run the validation service continuously.
        """
        self.running = True
        logger.info(f"Starting {self.service_name}")

        processed_count = 0
        valid_count = 0
        invalid_count = 0
        error_count = 0

        try:
            while self.running:
                messages = self.consumer.consume()

                if not messages:
                    continue

                for message in messages:
                    try:
                        # Parse the message
                        if not message.value:
                            continue

                        processed_count += 1
                        if processed_count % 100 == 0:
                            logger.info(
                                f"Progress: {processed_count} processed, {valid_count} valid, "
                                f"{invalid_count} invalid, {error_count} errors"
                            )

                        # Extract the message value and key
                        try:
                            data = message.value
                            if isinstance(data, bytes):
                                data = json.loads(data.decode("utf-8"))
                            elif isinstance(data, str):
                                data = json.loads(data)

                            key = message.key
                            if isinstance(key, bytes):
                                key = key.decode("utf-8")
                        except (json.JSONDecodeError, UnicodeDecodeError) as e:
                            logger.error(f"Failed to decode message: {e}")
                            self._send_error_message(
                                message.value, f"Message decoding error: {e}"
                            )
                            error_count += 1
                            continue

                        # Validate the data
                        is_valid, validated_data, validation_errors = (
                            self.validator.validate(data)
                        )

                        if is_valid and validated_data:
                            # Send to valid topic
                            self.valid_producer.send_message(
                                topic=self.valid_topic,
                                value=validated_data.model_dump(),
                                key=key,
                            )
                            valid_count += 1
                        else:
                            # Send to invalid topic with errors
                            invalid_data = {
                                "original_data": data,
                                "validation_errors": validation_errors,
                                "timestamp": time.time(),
                            }
                            self.invalid_producer.send_message(
                                topic=self.invalid_topic,
                                value=invalid_data,
                                key=key,
                            )
                            invalid_count += 1

                    except Exception as e:
                        logger.exception(f"Error processing message: {e}")
                        try:
                            self._send_error_message(message.value, str(e))
                        except Exception:
                            logger.exception("Failed to send error message")
                        error_count += 1

        except KeyboardInterrupt:
            logger.info("Validation service interrupted by user")
        except Exception as e:
            logger.exception(f"Unexpected error in validation service: {e}")
        finally:
            self.stop()

    def _send_error_message(self, original_data: Any, error_message: str):
        """Send a message to the error topic."""
        error_data = {
            "original_data": original_data,
            "error_message": error_message,
            "timestamp": time.time(),
            "service": self.service_name,
        }
        self.error_producer.send_message(
            topic=self.error_topic,
            value=error_data,
            key=None,
        )

    def _signal_handler(self, sig, frame):
        """Handle termination signals."""
        logger.info(f"Received signal {sig}, shutting down...")
        self.stop()

    def stop(self):
        """Stop the service and clean up resources."""
        logger.info(f"Stopping {self.service_name}")
        self.running = False

        # Close Kafka connections
        if hasattr(self, "consumer") and self.consumer:
            self.consumer.close()

        if hasattr(self, "valid_producer") and self.valid_producer:
            self.valid_producer.close()

        if hasattr(self, "invalid_producer") and self.invalid_producer:
            self.invalid_producer.close()

        if hasattr(self, "error_producer") and self.error_producer:
            self.error_producer.close()

        logger.info(f"{self.service_name} stopped")


if __name__ == "__main__":
    # Set up logger
    logger.add(
        "logs/validation_service_{time}.log",
        rotation="500 MB",
        retention="10 days",
        level="INFO",
    )

    # Resolve paths for configuration files using Pathlib
    base_path = (
        Path(__file__).resolve().parents[4]
    )  # Adjusting to get the base directory
    config_path = (
        base_path / "config/news_api_config.yaml"
    )  # Update with your actual config file
    kafka_config_path = (
        base_path / "config/kafka/kafka_config.yaml"
    )  # Optional, can be set to None if not needed

    # Initialize the validation service
    validation_service = NewsValidationService(config_path=str(config_path), kafka_config_path=str(kafka_config_path))
    
    try:
        validation_service.run()
    except KeyboardInterrupt:
        logger.info("Validation service stopped by user") 