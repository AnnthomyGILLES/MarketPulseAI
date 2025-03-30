# src/data_collection/market_data/validation/validation_service.py

import time
import traceback
from pathlib import Path
from typing import Dict, Any

from loguru import logger

from src.common.messaging.kafka_consumer import KafkaConsumerWrapper
from src.common.messaging.kafka_producer import KafkaProducerWrapper
from src.data_collection.market_data.validation.market_data_validator import (
    MarketDataValidator,
)
from src.utils.config import load_config


class MarketDataValidationService:
    """
    Service that consumes raw market data from Kafka, validates it,
    and produces validated data to another Kafka topic.

    Invalid data is sent to an error topic for monitoring and analysis.
    """

    def __init__(self, config_path: str = None):
        """
        Initialize the validation service.

        Args:
            config_path: Path to Kafka configuration file
        """
        # Set default config path if not provided
        if config_path is None:
            base_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
            config_path = str(base_dir / "config" / "kafka" / "kafka_config.yaml")

        # Configure loguru logger
        logger.add(
            "logs/market_data_validation_{time}.log",
            rotation="100 MB",
            retention="30 days",
            level="INFO",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
        )

        # Load configuration
        self.config = load_config(config_path)

        # Initialize validator
        self.validator = MarketDataValidator()

        # Initialize Kafka consumer and producers
        bootstrap_servers = self.config["kafka"]["bootstrap_servers_dev"]
        input_topic = self.config["kafka"]["topics"]["market_data_raw"]

        logger.info(f"Creating Kafka consumer for topic: {input_topic}")
        self.consumer = KafkaConsumerWrapper(
            bootstrap_servers=bootstrap_servers,
            topic=input_topic,
            group_id="market_data_validation_group",
        )

        self.valid_producer = KafkaProducerWrapper(
            bootstrap_servers=bootstrap_servers,
            topic=self.config["kafka"]["topics"]["market_data_validated"],
        )

        self.error_producer = KafkaProducerWrapper(
            bootstrap_servers=bootstrap_servers,
            topic=self.config["kafka"]["topics"]["market_data_error"],
        )

        # Keep track of processed records for statistics
        self.stats = {
            "processed": 0,
            "valid": 0,
            "invalid": 0,
            "last_report_time": time.time(),
        }

        self.running = False

    def _update_and_report_stats(self, is_valid: bool) -> None:
        """
        Update processing statistics and periodically log them.

        Args:
            is_valid: Whether the processed record was valid
        """
        self.stats["processed"] += 1
        if is_valid:
            self.stats["valid"] += 1
        else:
            self.stats["invalid"] += 1

        # Report stats every 1000 records or 60 seconds
        current_time = time.time()
        if (
            self.stats["processed"] % 1000 == 0
            or current_time - self.stats["last_report_time"] > 60
        ):
            logger.info(
                f"Processing stats: Processed={self.stats['processed']}, "
                f"Valid={self.stats['valid']} "
                f"({self.stats['valid'] / self.stats['processed'] * 100:.1f}%), "
                f"Invalid={self.stats['invalid']}"
            )
            self.stats["last_report_time"] = current_time

    def process_message(self, message: Dict[str, Any]) -> None:
        """
        Process and validate a single message from Kafka.

        Args:
            message: Dictionary containing market data
        """
        is_valid, validated_data, errors = self.validator.validate(message)

        if is_valid:
            # Send valid data to the validated topic
            self.valid_producer.send_message(validated_data)
        else:
            # Add error information to the data
            error_data = message.copy()
            error_data["validation_errors"] = errors
            error_data["validation_timestamp"] = time.time()

            # Send invalid data to the error topic
            self.error_producer.send_message(error_data)

        # Update statistics
        self._update_and_report_stats(is_valid)

    def run(self) -> None:
        """
        Run the validation service, continuously consuming and validating market data.
        """
        self.running = True
        logger.info("Starting market data validation service")

        try:
            for message in self.consumer.consume():
                if not self.running:
                    break

                try:
                    # Process the message from the value field
                    self.process_message(message["value"])
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    logger.error(traceback.format_exc())

        except KeyboardInterrupt:
            logger.info("Validation service stopped by user")
        except Exception as e:
            logger.error(f"Validation service failed: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop the validation service and clean up resources."""
        self.running = False
        logger.info("Stopping market data validation service")

        # Close Kafka connections
        try:
            self.consumer.close()
            self.valid_producer.close()
            self.error_producer.close()
            logger.info("Closed Kafka connections")
        except Exception as e:
            logger.error(f"Error closing Kafka connections: {str(e)}")


if __name__ == "__main__":
    # Create and run the validation service
    validation_service = MarketDataValidationService()

    try:
        validation_service.run()
    except KeyboardInterrupt:
        print("Service interrupted. Shutting down...")
    finally:
        validation_service.stop()
