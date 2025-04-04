# src/data_collection/social_media/reddit/validation/reddit_validation_consumer.py

import logging
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

from confluent_kafka import (
    KafkaException,
)  # Using confluent_kafka types directly if wrappers use it

# Assuming wrappers are compatible or adjusting imports as needed
from src.common.messaging.kafka_consumer import KafkaConsumerWrapper
from src.common.messaging.kafka_producer import KafkaProducerWrapper
from src.data_collection.social_media.reddit.validation.reddit_validation import (
    validate_reddit_data,
    RedditPost,  # Import models for type hinting
    RedditComment,
)
from src.utils.config import load_config
from src.utils.logging import setup_logger

# Setup logger for this module
setup_logger("my_log")
logger = logging.getLogger(__name__)


class RedditValidationConsumer:
    """
    Kafka consumer service for validating and enriching Reddit data.

    Consumes from raw Reddit topics, validates using Pydantic schemas,
    enriches valid data, and produces results to downstream topics (validated, invalid, error).
    """

    DEFAULT_CONFIG_PATH = (
        Path(__file__).resolve().parents[5] / "config" / "kafka" / "kafka_config.yaml"
    )

    def __init__(self, config_path: Optional[str] = None):
        """Initialize the consumer, loading configuration and setting up components."""
        self.config_path = Path(config_path or self.DEFAULT_CONFIG_PATH)
        logger.info(f"Loading Kafka configuration from: {self.config_path}")
        try:
            self.config = load_config(
                self.config_path
            )  # Assuming load_config handles errors
            # Basic check for essential keys
            if "kafka" not in self.config or "topics" not in self.config["kafka"]:
                raise ValueError(
                    "Kafka configuration missing 'kafka' or 'kafka.topics' section."
                )
        except Exception as e:
            logger.exception(
                f"Failed to load Kafka configuration from {self.config_path}: {e}"
            )
            raise

        self.validator = validate_reddit_data  # Use the validation function directly
        self.running = False

        self._setup_kafka_clients()

        # Statistics
        self.processed_count = 0
        self.valid_count = 0
        self.invalid_count = 0
        self.error_count = 0
        self.last_stats_time = time.time()
        self.stats_interval = self.config.get("kafka", {}).get(
            "consumer_stats_interval_seconds", 60
        )

        # Signal handling for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _setup_kafka_clients(self):
        """Initializes Kafka consumers and producers based on config."""
        self.consumers: Dict[str, KafkaConsumerWrapper] = {}
        self.producers: Dict[str, KafkaProducerWrapper] = {}

        try:
            bootstrap_servers = self.config["kafka"][
                "bootstrap_servers_dev"
            ]  # Use appropriate server list

            # Define input topics and map to consumer groups
            topic_group_map = {
                self.config["kafka"]["topics"]["social_media_reddit_raw"]: self.config[
                    "kafka"
                ]["consumer_groups"]["reddit_validation"],
                self.config["kafka"]["topics"][
                    "social_media_reddit_comments"
                ]: self.config["kafka"]["consumer_groups"][
                    "reddit_comments_validation"
                ],
                self.config["kafka"]["topics"][
                    "social_media_reddit_symbols"
                ]: self.config["kafka"]["consumer_groups"]["reddit_symbols_validation"],
            }

            logger.info("Initializing Kafka consumers...")
            for topic, group_id in topic_group_map.items():
                self.consumers[topic] = KafkaConsumerWrapper(
                    topics=[topic],  # Pass the topic as a list to the 'topics' argument
                    bootstrap_servers=bootstrap_servers,
                    group_id=group_id,
                )
                logger.info(
                    f"Initialized consumer for topic '{topic}' with group '{group_id}'"
                )

            # Define output topics
            output_topics = {
                "validated_posts": self.config["kafka"]["topics"][
                    "social_media_reddit_validated"
                ],
                "validated_comments": self.config["kafka"]["topics"][
                    "social_media_reddit_comments_validated"
                ],
                "validated_symbols": self.config["kafka"]["topics"][
                    "social_media_reddit_symbols_validated"
                ],
                "invalid": self.config["kafka"]["topics"][
                    "social_media_reddit_invalid"
                ],
                "error": self.config["kafka"]["topics"]["social_media_reddit_error"],
            }

            logger.info("Initializing Kafka producers...")
            for key, topic_name in output_topics.items():
                # Add error callback and configure delivery reports if needed
                self.producers[key] = KafkaProducerWrapper(
                    bootstrap_servers=bootstrap_servers,
                )
                logger.info(
                    f"Initialized producer for topic key '{key}' ({topic_name})"
                )

        except KeyError as e:
            logger.exception(
                f"Configuration key error during Kafka client setup: Missing key {e}"
            )
            raise ValueError(f"Missing required Kafka configuration key: {e}")
        except Exception as e:
            logger.exception(f"Failed to initialize Kafka clients: {e}")
            raise

    def _kafka_error_callback(self, err: KafkaException):
        """Callback for Kafka client errors (consumers/producers)."""
        if err.code() == KafkaException._PARTITION_EOF:
            # Not really an error, normal event
            logger.debug(f"Reached end of partition: {err}")
        elif err.fatal():
            logger.error(f"FATAL Kafka Error: {err}. Stopping consumer.")
            # A fatal error often requires manual intervention or restart
            self.stop()  # Trigger shutdown on fatal errors
        else:
            logger.warning(f"Non-fatal Kafka Error: {err}")

    def process_message(self, raw_message: Dict[str, Any]) -> None:
        """Processes a single message consumed from Kafka."""
        self.processed_count += 1
        message_value = raw_message.get("value")
        source_topic = raw_message.get("topic", "unknown")
        message_key = raw_message.get("key", None)  # Assuming key might be present
        item_id = "UNKNOWN_ID"  # Default

        if not isinstance(message_value, dict):
            logger.error(
                f"Received non-dictionary message value from topic '{source_topic}': {type(message_value)}"
            )
            self._send_to_error_topic(
                raw_message,
                "invalid_message_format",
                "Message value is not a dictionary",
            )
            self.error_count += 1
            return

        item_id = message_value.get("id", item_id)
        logger.debug(f"Processing message ID {item_id} from topic '{source_topic}'")

        try:
            # --- 1. Validation ---
            validated_model, validation_errors = self.validator(message_value)

            if validated_model:
                self.valid_count += 1
                logger.debug(
                    f"Validation successful for {validated_model.content_type} {item_id}"
                )

                # Convert validated model back to dict for sending
                validated_data_dict = validated_model.model_dump()

                # --- 3. Produce Validated Data to Appropriate Topic ---
                target_producer_key = None
                # First check the source topic to determine the appropriate destination
                if (
                    source_topic
                    == self.config["kafka"]["topics"]["social_media_reddit_raw"]
                ):
                    target_producer_key = "validated_posts"
                elif (
                    source_topic
                    == self.config["kafka"]["topics"]["social_media_reddit_comments"]
                ):
                    target_producer_key = "validated_comments"
                elif (
                    source_topic
                    == self.config["kafka"]["topics"]["social_media_reddit_symbols"]
                    or validated_model.detected_symbols
                ):
                    target_producer_key = "validated_symbols"
                # Fallback to content type if source topic doesn't match expected patterns
                elif isinstance(validated_model, RedditPost):
                    target_producer_key = "validated_posts"
                elif isinstance(validated_model, RedditComment):
                    target_producer_key = "validated_comments"

                if target_producer_key and target_producer_key in self.producers:
                    # Map producer keys to their corresponding topic names
                    topic_mapping = {
                        "validated_posts": self.config["kafka"]["topics"][
                            "social_media_reddit_validated"
                        ],
                        "validated_comments": self.config["kafka"]["topics"][
                            "social_media_reddit_comments_validated"
                        ],
                        "validated_symbols": self.config["kafka"]["topics"][
                            "social_media_reddit_symbols_validated"
                        ],
                    }

                    # Get the appropriate topic for this producer
                    topic = topic_mapping.get(target_producer_key)

                    # Send message with correct parameter order
                    success = self.producers[target_producer_key].send_message(
                        topic=topic,  # Add required topic parameter
                        value=validated_data_dict,
                        key=f"validated_{item_id}",
                    )
                    if not success:
                        logger.error(
                            f"Failed to produce validated message ID {item_id} to {target_producer_key}"
                        )
                        # Send original message_value to error topic on producer failure
                        self._send_to_error_topic(
                            message_value,  # Send original message on failure
                            "producer_failure",
                            f"Failed to send validated data to {target_producer_key}",
                            source_topic,
                        )
                        self.error_count += 1
                else:
                    logger.error(
                        f"No valid producer found for validated item ID {item_id}. Target key: {target_producer_key}"
                    )
                    # Send original message_value to error topic if producer not found
                    self._send_to_error_topic(
                        message_value,  # Send original message on failure
                        "producer_not_found",
                        f"No producer for key {target_producer_key}",
                        source_topic,
                    )
                    self.error_count += 1

            else:
                # --- Handle Validation Failure ---
                self.invalid_count += 1
                logger.warning(
                    f"Validation failed for item ID {item_id} from topic '{source_topic}'. Errors: {validation_errors}"
                )
                invalid_data = {
                    "original_message": message_value,
                    "validation_errors": validation_errors,
                    "source_topic": source_topic,
                    "processing_timestamp": datetime.now(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z"),
                }
                # Send to invalid topic
                if "invalid" in self.producers:
                    invalid_topic = self.config["kafka"]["topics"][
                        "social_media_reddit_invalid"
                    ]
                    self.producers["invalid"].send_message(
                        topic=invalid_topic,
                        value=invalid_data,
                        key=f"invalid_{item_id}",
                    )
                else:
                    logger.error(
                        "Producer for 'invalid' topic not found. Cannot send invalid message."
                    )

        except Exception as e:
            # --- Handle Unexpected Processing Error ---
            self.error_count += 1
            logger.exception(
                f"Unexpected error processing message ID {item_id} from topic '{source_topic}': {e}"
            )
            self._send_to_error_topic(
                message_value, type(e).__name__, str(e), source_topic
            )

    def _send_to_error_topic(
        self,
        data_payload: Any,
        error_type: str,
        error_message: str,
        source_topic: str = "unknown",
    ):
        """Sends problematic data and error context to the designated error topic."""
        if "error" in self.producers:
            error_data = {
                "original_payload": data_payload,  # Include the data that caused the error
                "error_type": error_type,
                "error_message": error_message,
                "source_topic": source_topic,
                "service_name": "RedditValidationConsumer",
                "error_timestamp": datetime.now(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
            }
            error_topic = self.config["kafka"]["topics"]["social_media_reddit_error"]
            success = self.producers["error"].send_message(
                topic=error_topic,
                value=error_data,
                key=f"error_{source_topic}_{time.time_ns()}",
            )
            if not success:
                logger.error("CRITICAL: Failed to send message to error topic!")
        else:
            logger.error(
                "Producer for 'error' topic not found. Cannot send error message."
            )

    def run(self):
        """Starts the main consumer loop."""
        if self.running:
            logger.warning("Consumer is already running.")
            return

        self.running = True
        logger.info("Starting Reddit validation consumer...")

        # Subscription happens during KafkaConsumerWrapper initialization

        try:
            while self.running:
                message_processed_in_cycle = False  # Renamed for clarity
                # Poll each consumer round-robin
                for topic, consumer in self.consumers.items():
                    if not self.running:
                        break  # Check flag before potentially blocking call

                    logger.debug(f"Polling consumer for topic: {topic}")
                    message_generator = consumer.consume()

                    logger.debug(f"Got message generator for topic: {topic}")

                    try:
                        for msg in message_generator:
                            logger.debug(
                                f"Received from generator (topic: {topic}): {'Message' if msg else 'None/Timeout'}"
                            )
                            if not self.running:
                                break

                            if msg is not None:  # Process valid messages
                                self.process_message(msg)
                                message_processed_in_cycle = True

                        # Check running flag after iterating the generator for this topic
                        if not self.running:
                            logger.info(
                                f"Running flag false after processing generator for topic: {topic}"
                            )
                            break  # Exit the outer loop over consumers

                    except StopIteration:
                        # This might occur if the consumer is closed externally or if consumer_timeout_ms is set and reached
                        logger.info(
                            f"Message generator for topic {topic} stopped (StopIteration)."
                        )
                        if not self.running:  # Check if stop was requested
                            break
                        # Otherwise, continue to the next consumer or next cycle
                    except Exception as e:
                        logger.exception(
                            f"Error iterating message generator for topic {topic}: {e}"
                        )
                        # Decide if we should stop or continue polling other topics
                        self.stop()  # Example: stop on generator error
                        break  # Exit the outer loop over consumers

                # If no messages were processed in a full loop across ALL consumers, sleep briefly
                if not message_processed_in_cycle and self.running:
                    logger.debug("No messages processed in this cycle, sleeping.")
                    time.sleep(0.5)  # Prevent tight loop when idle

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt detected. Initiating shutdown.")
            # self.stop() will be called in the finally block
        except Exception as e:
            logger.exception(f"Critical error in consumer run loop: {e}")
            # self.stop() will be called in the finally block
        finally:
            logger.info("Consumer run loop exiting. Initiating cleanup...")
            self.stop()  # Ensure stop is called on any exit path

    def stop(self):
        """Stops the consumer and closes Kafka clients."""
        if not self.running:
            logger.info(
                "Stop called, but consumer was not running or already stopping."
            )
            return

        logger.info("Shutting down Reddit validation consumer...")
        self.running = False  # Signal loops to stop FIRST

        # Close consumers - this will interrupt the blocking iteration in message_generator
        logger.info("Closing Kafka consumers...")
        for topic, consumer in self.consumers.items():
            try:
                consumer.close()  # Calling close() on the wrapper
                logger.info(f"Closed consumer for topic: {topic}")
            except Exception as e:
                logger.error(
                    f"Error closing consumer for topic {topic}: {e}", exc_info=True
                )

        # Close producers
        logger.info("Closing Kafka producers...")
        for key, producer in self.producers.items():
            try:
                # Flush any buffered messages before closing
                producer.flush(timeout=10)  # Allow time for messages to send
                producer.close()  # Assuming wrapper has close()
                logger.info(f"Closed producer for key: {key}")
            except Exception as e:
                logger.error(
                    f"Error closing producer for key {key}: {e}", exc_info=True
                )

        logger.info("Reddit validation consumer shut down complete.")

    def _handle_signal(self, signum, frame):
        """Handles termination signals for graceful shutdown."""
        logger.warning(f"Received signal {signum}. Initiating graceful shutdown...")
        self.stop()


# Example Usage
if __name__ == "__main__":
    # Configure logging for standalone run
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    consumer_service = None
    try:
        consumer_service = RedditValidationConsumer()
        consumer_service.run()
    except Exception:
        logger.exception("Failed to initialize or run the consumer service.")
    # Shutdown is handled by signal handlers or exceptions within run()
