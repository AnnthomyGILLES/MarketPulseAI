"""
Kafka consumer for validating and processing Reddit data.

This module consumes Reddit data from Kafka topics, validates it using
the Reddit validation module, and produces validated and enriched data
to downstream Kafka topics.
"""

import time
from pathlib import Path
from typing import Dict, Any

from loguru import logger

from src.common.messaging.kafka_consumer import KafkaConsumerWrapper
from src.common.messaging.kafka_producer import KafkaProducerWrapper
from src.data_collection.social_media.reddit.validation.reddit_validation import (
    RedditDataValidator,
    RedditDataEnricher,
)
from src.utils.config import load_config


class RedditValidationConsumer:
    """
    Kafka consumer for validating and processing Reddit data.

    This consumer:
    1. Consumes Reddit data from raw Kafka topics
    2. Validates the data using Pydantic models
    3. Enriches valid data with additional metadata
    4. Produces validated data to processed Kafka topics
    5. Logs validation errors and statistics
    """

    def __init__(self, config_path: str = None):
        """
        Initialize the Reddit validation consumer.

        Args:
            config_path: Path to the Kafka configuration file
        """
        if config_path is None:
            base_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
            config_path = str(base_dir / "config" / "kafka" / "kafka_config.yaml")

        self.config = load_config(config_path)
        self.validator = RedditDataValidator()
        self.enricher = RedditDataEnricher()
        self.running = False

        # Initialize consumers and producers
        self.consumers = {}
        self.producers = {}

        # Statistics
        self.processed_count = 0
        self.last_stats_time = time.time()
        self.stats_interval = 60  # Report stats every 60 seconds

    def _initialize_consumers(self):
        """Initialize Kafka consumers for Reddit data topics."""
        bootstrap_servers = self.config["kafka"]["bootstrap_servers_dev"]
        topics = [
            self.config["kafka"]["topics"]["social_media_reddit_raw"],
            self.config["kafka"]["topics"]["social_media_reddit_comments"],
            self.config["kafka"]["topics"]["social_media_reddit_symbols"],
        ]

        for topic in topics:
            consumer_group = f"reddit_validator_{topic.replace('.', '_')}"

            self.consumers[topic] = KafkaConsumerWrapper(
                bootstrap_servers=bootstrap_servers,
                topic=topic,
                group_id=consumer_group,
                auto_offset_reset="earliest",
            )

            logger.info(f"Initialized consumer for topic: {topic}")

    def _initialize_producers(self):
        """Initialize Kafka producers for validated Reddit data topics."""
        bootstrap_servers = self.config["kafka"]["bootstrap_servers_dev"]

        # Topics for validated data
        validated_topics = {
            "posts": self.config["kafka"]["topics"]["social_media_reddit_validated"],
            "comments": self.config["kafka"]["topics"][
                "social_media_reddit_comments_validated"
            ],
            "symbols": self.config["kafka"]["topics"][
                "social_media_reddit_symbols_validated"
            ],
            "invalid": self.config["kafka"]["topics"]["social_media_reddit_invalid"],
        }

        for topic_key, topic_name in validated_topics.items():
            self.producers[topic_key] = KafkaProducerWrapper(
                bootstrap_servers=bootstrap_servers, topic=topic_name
            )

            logger.info(f"Initialized producer for topic: {topic_name}")

    def process_message(self, message: Dict[str, Any]) -> None:
        """
        Process a message from Kafka.

        Args:
            message: Message from Kafka consumer
        """
        try:
            # Extract the actual data from the message
            data = message["value"]

            # Validate the data
            validated_data = self.validator.validate_reddit_data(data)

            if validated_data:
                # Determine if this is a post or comment
                is_post = "content_type" in data and data["content_type"] == "post"
                is_symbol_specific = "symbol" in data

                # Enrich the data
                if is_post:
                    enriched_data = self.enricher.enrich_post(validated_data)
                else:
                    enriched_data = self.enricher.enrich_comment(validated_data)

                # Send to the appropriate validated topic
                if is_symbol_specific:
                    success = self.producers["symbols"].send_message(
                        enriched_data, key=f"validated_{data.get('id', 'unknown')}"
                    )
                elif is_post:
                    success = self.producers["posts"].send_message(
                        enriched_data, key=f"validated_{data.get('id', 'unknown')}"
                    )
                else:
                    success = self.producers["comments"].send_message(
                        enriched_data, key=f"validated_{data.get('id', 'unknown')}"
                    )

                if not success:
                    logger.error(
                        f"Failed to send validated data to Kafka: {data.get('id', 'unknown')}"
                    )
            else:
                # Send invalid data to the invalid topic for investigation
                self.producers["invalid"].send_message(
                    data, key=f"invalid_{data.get('id', 'unknown')}"
                )

            self.processed_count += 1

            # Log statistics periodically
            current_time = time.time()
            if current_time - self.last_stats_time > self.stats_interval:
                stats = self.validator.get_validation_stats()
                logger.info(
                    f"Validation stats: "
                    f"Processed: {self.processed_count}, "
                    f"Valid: {stats['valid_count']}, "
                    f"Invalid: {stats['invalid_count']}, "
                    f"With warnings: {stats['warning_count']}"
                )
                self.last_stats_time = current_time

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")

    def start(self):
        """Start consuming and processing messages."""
        try:
            self._initialize_consumers()
            self._initialize_producers()

            self.running = True
            logger.info("Starting Reddit validation consumer")

            # Process messages from all topics
            while self.running:
                for topic, consumer in self.consumers.items():
                    for message in consumer.consume():
                        if not self.running:
                            break

                        self.process_message(message)

                # Small sleep to prevent CPU spinning
                time.sleep(0.1)

        except KeyboardInterrupt:
            logger.info("Reddit validation consumer stopped by user")
        except Exception as e:
            logger.error(f"Reddit validation consumer failed: {str(e)}")
        finally:
            self.stop()

    def stop(self):
        """Stop the consumer and close resources."""
        self.running = False
        logger.info("Stopping Reddit validation consumer")

        # Close consumers
        for topic, consumer in self.consumers.items():
            try:
                consumer.close()
                logger.info(f"Closed consumer for topic: {topic}")
            except Exception as e:
                logger.error(f"Error closing consumer for topic {topic}: {str(e)}")

        # Close producers
        for topic_key, producer in self.producers.items():
            try:
                producer.close()
                logger.info(f"Closed producer for topic: {topic_key}")
            except Exception as e:
                logger.error(f"Error closing producer for topic {topic_key}: {str(e)}")


if __name__ == "__main__":
    validator = RedditValidationConsumer()
    try:
        validator.start()
    except KeyboardInterrupt:
        print("Validation process interrupted. Shutting down...")
    finally:
        validator.stop()
