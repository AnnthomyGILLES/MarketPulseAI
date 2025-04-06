import signal
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List

from confluent_kafka import KafkaException
from loguru import logger

from src.common.messaging.kafka_consumer import KafkaConsumerWrapper
from src.common.messaging.kafka_producer import KafkaProducerWrapper
from src.data_collection.social_media.reddit.validation.schema import (
    RedditPost,
    RedditComment,
    ValidatedRedditItem,
)
from src.data_collection.social_media.reddit.validation.validator import (
    RedditDataValidator,
)  # Updated import
from src.utils.config import load_config
from src.common.validation import BaseValidationService


# Inherit from BaseValidationService
class RedditValidationService(BaseValidationService):
    """
    Kafka consumer service for validating and enriching Reddit data.

    Consumes from raw Reddit topics, validates using the RedditDataValidator,
    and produces results to downstream topics (validated, invalid, error).
    """

    # Define default topic keys (can be overridden by kafka_config.yaml)
    # Assuming these are the keys used in the config file
    DEFAULT_INPUT_TOPICS = ["social_media_reddit_posts", "social_media_reddit_comments"]
    DEFAULT_CONSUMER_GROUP = "reddit_validation" # Default group name if multiple sources feed validation
    DEFAULT_VALID_TOPIC = "social_media_reddit_validated" # Combined validated topic? Or separate? Assuming combined for now.
    DEFAULT_VALID_POST_TOPIC = "social_media_reddit_validated" # Let's keep the specific ones for flexibility in base class logic if needed later
    DEFAULT_VALID_COMMENT_TOPIC = "social_media_reddit_comments_validated"
    DEFAULT_VALID_SYMBOL_TOPIC = "social_media_reddit_symbols_validated"
    DEFAULT_INVALID_TOPIC = "social_media_reddit_invalid"
    DEFAULT_ERROR_TOPIC = "social_media_reddit_error"

    DEFAULT_CONFIG_PATH = (
        Path(__file__).resolve().parents[5] / "config" / "kafka" / "kafka_config.yaml"
    )

    def __init__(self, config_path: Optional[str] = None):
        """Initialize the service, loading configuration and setting up components."""
        self.config_path = Path(config_path or self.DEFAULT_CONFIG_PATH)
        logger.info(f"Loading Kafka configuration from: {self.config_path}")
        try:
            self.config = load_config(str(self.config_path))
            if "topics" not in self.config:
                raise ValueError("Kafka configuration missing 'kafka.topics' section.")
            self.kafka_config = self.config
        except Exception as e:
            logger.exception(
                f"Failed to load or parse Kafka configuration from {self.config_path}: {e}"
            )
            raise

        # Use the new validator class
        self.validator = RedditDataValidator()
        self.running = False
        self._setup_kafka_clients()

        # Statistics
        self.processed_count = 0
        self.valid_count = 0
        self.invalid_count = 0
        self.error_count = 0
        self.last_stats_time = time.time()
        self.stats_interval = self.kafka_config.get("stats_interval_seconds", 60)

        # Signal handling for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        logger.info("RedditValidationService initialized successfully.")

        # Define Kafka topic keys for the base class constructor
        # These keys should exist in the kafka_config.yaml file under 'topics' and 'consumer_groups'
        input_topics = self.DEFAULT_INPUT_TOPICS # List of keys
        consumer_group = self.DEFAULT_CONSUMER_GROUP # Key for the group name

        # For Reddit, the "valid" destination depends on the content type AND if symbols are detected.
        # The base class currently assumes a single 'valid' topic key.
        # We need to override the process_message or add logic to handle multiple valid topics.
        # Let's start by providing the main validated topic key, and override message processing.
        # Option 1: Provide one key, override process_message (Chosen for now)
        valid_topic = self.DEFAULT_VALID_TOPIC
        # Option 2: Pass multiple valid topic keys and modify base class (More complex)

        invalid_topic = self.DEFAULT_INVALID_TOPIC
        error_topic = self.DEFAULT_ERROR_TOPIC

        # Call the base class __init__
        super().__init__(
            service_name="RedditValidationService",
            validator=self.validator,
            input_topics_config_keys=input_topics,
            consumer_group_config_key=consumer_group,
            valid_topic_config_key=valid_topic, # Will need custom routing logic
            invalid_topic_config_key=invalid_topic,
            error_topic_config_key=error_topic,
            config_path=self.config_path,
        )

        # Store the specific topic keys needed for custom routing
        # These are fetched from the loaded kafka_config by the base class
        self.post_topic = self.kafka_config["topics"].get(self.DEFAULT_VALID_POST_TOPIC, self.valid_topic)
        self.comment_topic = self.kafka_config["topics"].get(self.DEFAULT_VALID_COMMENT_TOPIC, self.valid_topic)
        self.symbol_topic = self.kafka_config["topics"].get(self.DEFAULT_VALID_SYMBOL_TOPIC, self.valid_topic)

    def _setup_kafka_clients(self):
        """Initializes Kafka consumers and producers based on config."""
        self.consumers: Dict[str, KafkaConsumerWrapper] = {}
        self.producers: Dict[str, KafkaProducerWrapper] = {}

        try:
            bootstrap_servers = self.kafka_config["bootstrap_servers"]
            topics_config = self.kafka_config["topics"]
            consumer_groups = self.kafka_config.get("consumer_groups", {})

            # --- Consumer Setup ---
            # Get defaults from the 'consumer' section of the config
            consumer_defaults = self.kafka_config.get("consumer", {})
            logger.debug(f"Loaded consumer defaults: {consumer_defaults}")

            # Define input topics and map to consumer groups
            topic_group_map = {
                topics_config["social_media_reddit_posts"]: consumer_groups.get(
                    "reddit_validation",
                    "reddit-validation-group",  # Default group if not in config
                ),
                topics_config["social_media_reddit_comments"]: consumer_groups.get(
                    "reddit_comments_validation", "reddit-comments-validation-group"
                ),
                # Add other consumer topics here if needed
            }

            logger.info("Initializing Kafka consumers...")
            for topic, group_id in topic_group_map.items():
                # Prepare kwargs for KafkaConsumerWrapper constructor
                consumer_kwargs = {
                    "bootstrap_servers": bootstrap_servers,
                    "group_id": group_id,
                    # Map keys from YAML (or defaults) to KafkaConsumerWrapper args
                    "auto_offset_reset": consumer_defaults.get(
                        "auto_offset_reset", "earliest"
                    ),
                    "enable_auto_commit": consumer_defaults.get(
                        "enable_auto_commit", True
                    ),
                    "auto_commit_interval_ms": consumer_defaults.get(  # Passed via **kwargs
                        "auto_commit_interval_ms", 5000
                    ),
                    "fetch_max_wait_ms": consumer_defaults.get(
                        "fetch_max_wait_ms", 500
                    ),
                    "max_poll_interval_ms": consumer_defaults.get(
                        "max_poll_interval_ms", 300000
                    ),
                    "session_timeout_ms": consumer_defaults.get(
                        "session_timeout_ms", 10000
                    ),
                    # Add other relevant consumer settings from config if needed
                    # e.g., 'client_id', 'api_version', security settings
                    # "security_protocol": consumer_defaults.get("security_protocol"),
                    # ...
                    "consumer_timeout_ms": 1000,  # Keep the explicit timeout for the consume loop
                }
                # Remove None values to avoid overriding kafka-python defaults unintentionally
                consumer_kwargs = {
                    k: v for k, v in consumer_kwargs.items() if v is not None
                }

                logger.debug(f"Consumer args for topic '{topic}': {consumer_kwargs}")

                try:
                    self.consumers[topic] = KafkaConsumerWrapper(
                        topics=[topic],
                        **consumer_kwargs,  # Unpack the prepared arguments
                    )
                    logger.info(
                        f"Initialized consumer for topic '{topic}' with group '{group_id}'"
                    )
                except Exception:
                    logger.exception(
                        f"Failed to initialize consumer for topic '{topic}'"
                    )
                    # Decide if failure is critical: raise e or continue?
                    raise  # Re-raise to stop service initialization on consumer failure

            # --- Producer Setup ---
            # Get defaults from the 'producer' section of the config
            producer_defaults = self.kafka_config.get("producer", {})
            logger.debug(f"Loaded producer defaults: {producer_defaults}")

            # Define output topics mapping (remains the same)
            self.output_topic_map = {
                "validated_posts": topics_config["social_media_reddit_validated"],
                "validated_comments": topics_config[
                    "social_media_reddit_comments_validated"
                ],
                "validated_symbols": topics_config[
                    "social_media_reddit_symbols_validated"
                ],
                "invalid": topics_config["social_media_reddit_invalid"],
                "error": topics_config["social_media_reddit_error"],
            }

            # Prepare base producer kwargs
            # Map keys from YAML (or defaults) to KafkaProducerWrapper args
            base_producer_kwargs = {
                "bootstrap_servers": bootstrap_servers,
                "acks": producer_defaults.get(
                    "acks", 1
                ),  # Default to 1 if not specified
                "retries": producer_defaults.get("retries", 3),
                # Pass other settings via kwargs to KafkaProducerWrapper
                "linger_ms": producer_defaults.get("linger_ms", 5),
                "batch_size": producer_defaults.get("batch_size", 16384),
                "buffer_memory": producer_defaults.get("buffer_memory", 33554432),
                # Add other relevant producer settings from config if needed
                # e.g., 'client_id', 'compression_type', security settings
                "compression_type": producer_defaults.get("compression_type"),
                # "security_protocol": producer_defaults.get("security_protocol"),
                # ...
            }
            # Remove None values
            base_producer_kwargs = {
                k: v for k, v in base_producer_kwargs.items() if v is not None
            }

            logger.info("Initializing Kafka producers...")
            # All producers share the same config in this setup
            # If different configs per output topic were needed, this logic would change.
            shared_producer_kwargs = base_producer_kwargs.copy()
            logger.debug(f"Shared producer args: {shared_producer_kwargs}")

            try:
                # Create one producer instance shared by all outputs (typical use case)
                # We map logical keys ('validated_posts', etc.) to this single producer instance.
                shared_producer = KafkaProducerWrapper(**shared_producer_kwargs)
                logger.info("Initialized shared Kafka producer.")

                for key in self.output_topic_map.keys():
                    self.producers[key] = (
                        shared_producer  # All keys point to the same producer
                    )
                    logger.info(
                        f"Mapped producer key '{key}' (Topic: '{self.output_topic_map[key]}') to shared producer instance."
                    )

            except Exception:
                logger.exception("Failed to initialize shared Kafka producer.")
                raise  # Re-raise to stop service initialization

        except KeyError as e:
            logger.exception(
                f"Configuration key error during Kafka client setup: Missing key {e} in config: {self.kafka_config}"
            )
            raise ValueError(f"Missing required Kafka configuration key: {e}")
        except Exception as e:
            logger.exception(f"Failed to initialize Kafka clients: {e}")
            raise

    def _kafka_error_callback(self, err: KafkaException):
        """Callback for Kafka client errors (consumers/producers). NOTE: This is likely unused with kafka-python."""
        # This method was likely for confluent-kafka's error_cb.
        # kafka-python typically raises exceptions or logs internally.
        # Keep the method signature for now, but be aware it might not be called.
        logger.warning(
            f"Kafka Error Callback Invoked (may be unused with kafka-python): {err}"
        )
        if (
            hasattr(err, "code") and err.code() == KafkaException._PARTITION_EOF
        ):  # Check if it looks like confluent error obj
            logger.info(f"Reached end of partition: {err}")
        elif hasattr(err, "fatal") and err.fatal():
            logger.error(
                f"FATAL Kafka Error reported to callback: {err}. Stopping service."
            )
            self.stop()
        else:
            logger.warning(f"Non-fatal Kafka Error reported to callback: {err}")

    def _determine_target_producer(
        self, validated_model: ValidatedRedditItem, source_topic: str
    ) -> Optional[str]:
        """Determines the appropriate producer key based on the validated data and source."""
        if validated_model.detected_symbols:
            return "validated_symbols"
        elif isinstance(validated_model, RedditPost):
            # Check if it came from the posts topic initially, otherwise could be symbol search result
            if source_topic == self.kafka_config["topics"]["social_media_reddit_posts"]:
                return "validated_posts"
            else:
                # If not from posts topic but is a Post and has symbols, it goes to symbols topic
                # If it has no symbols and isn't from posts topic, where should it go? Error/Invalid?
                # For now, assume posts from non-post topics might be symbol related if they don't have symbols explicitly
                # This logic might need refinement based on data flow specifics.
                logger.warning(
                    f"Post {validated_model.id} received from non-post topic '{source_topic}' without detected symbols. Routing to 'validated_posts'."
                )
                return "validated_posts"  # Or potentially 'invalid' or log an error?
        elif isinstance(validated_model, RedditComment):
            if (
                source_topic
                == self.kafka_config["topics"]["social_media_reddit_comments"]
            ):
                return "validated_comments"
            else:
                logger.warning(
                    f"Comment {validated_model.id} received from non-comment topic '{source_topic}' without detected symbols. Routing to 'validated_comments'."
                )
                return "validated_comments"  # Or potentially 'invalid'?
        else:
            # Should not happen if validation succeeded
            logger.error(
                f"Cannot determine target producer for unknown validated model type: {type(validated_model)}"
            )
            return None

    def process_message(self, raw_message: Dict[str, Any]) -> None:
        """Processes a single message consumed from Kafka using the validator."""
        self.processed_count += 1
        message_value = raw_message.get("value")
        source_topic = raw_message.get("topic", "unknown")
        message_key = raw_message.get("key")  # Can be None
        message_offset = raw_message.get("offset", -1)  # Default if not present

        log_context = {
            "topic": source_topic,
            "offset": message_offset,
            "key": message_key,
        }

        logger.info(f"Received message | Context: {log_context}")
        logger.debug(f"Raw message value: {message_value} | Context: {log_context}")

        if not isinstance(message_value, dict):
            logger.error(
                f"Received non-dictionary message value | Type: {type(message_value)} | Context: {log_context}"
            )
            self._send_to_producer(
                self.error_producer, self.error_topic,
                {"original_payload": message_value, "error": "Message value is not a dictionary", "context": log_context},
                f"error_{message_key or 'unknown'}_{time.time_ns()}", log_context, "error"
            )
            self.error_count += 1
            return

        item_id = message_value.get("id", "UNKNOWN_ID")
        log_context["item_id"] = item_id  # Add item_id to context

        logger.info(f"Processing message | Context: {log_context}")

        try:
            logger.debug(f"Attempting validation | Context: {log_context}")
            # Use the new validator instance
            is_valid, validated_model, validation_errors = self.validator.validate(
                message_value
            )

            if is_valid and validated_model:
                self.valid_count += 1
                logger.info(
                    f"Validation SUCCEEDED | Type: {validated_model.content_type} | Context: {log_context}"
                )

                # Determine the target producer based on validated data type and potentially source topic
                target_producer_key = self._determine_target_producer(
                    validated_model, source_topic
                )

                if target_producer_key and target_producer_key in self.producers:
                    target_topic = self.output_topic_map.get(target_producer_key)
                    if not target_topic:
                        logger.error(
                            f"Configuration error: No topic found for producer key '{target_producer_key}' | Context: {log_context}"
                        )
                        self._send_to_producer(
                            self.error_producer, self.error_topic,
                            {"original_payload": validated_model, "error": f"No output topic for producer key {target_producer_key}", "context": log_context},
                            f"error_{item_id}", log_context, "error"
                        )
                        self.error_count += 1
                        return  # Stop processing this message

                    validated_data_dict = validated_model.model_dump(
                        mode="json"
                    )  # Use Pydantic's recommended way

                    log_context["target_topic"] = target_topic
                    log_context["producer_key"] = target_producer_key

                    logger.info(
                        f"Attempting to send validated message | Context: {log_context}"
                    )
                    logger.debug(
                        f"Sending data: {validated_data_dict} | Context: {log_context}"
                    )

                    send_success = self._send_to_producer(
                        self.valid_producer, target_topic,
                        validated_data_dict, f"validated_{item_id}", log_context, f"validated_{target_producer_key}"
                    )

                    if send_success:
                        logger.info(
                            f"Successfully queued validated message | Context: {log_context}"
                        )
                    else:
                        # The producer wrapper might log errors, but we add context here
                        logger.error(
                            f"Failed to queue validated message via send_message | Context: {log_context}"
                        )
                        # If send_message returns False, it usually means an immediate error (e.g., buffer full, serialization issue)
                        # Consider if sending to error topic is appropriate or if relying on producer's error_cb is enough
                        self._send_to_producer(
                            self.error_producer, self.error_topic,
                            {"original_payload": validated_data_dict, "error": "send_message returned false", "context": log_context},
                            f"error_{item_id}", log_context, "error"
                        )
                        self.error_count += 1

                else:
                    logger.error(
                        f"No valid producer found or configured for key '{target_producer_key}' | Context: {log_context}"
                    )
                    self._send_to_producer(
                        self.error_producer, self.error_topic,
                        {"original_payload": validated_model, "error": f"No producer for key '{target_producer_key}'", "context": log_context},
                        f"error_{item_id}", log_context, "error"
                    )
                    self.error_count += 1

            else:
                # Validation failed (either schema or rules)
                self.invalid_count += 1
                logger.warning(
                    f"Validation FAILED | Errors: {validation_errors} | Context: {log_context}"
                )

                invalid_data = {
                    "original_message": message_value,
                    "validation_errors": validation_errors,
                    "processing_timestamp": datetime.now(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z"),
                    "log_context": log_context,  # Include context for easier debugging
                }

                if "invalid" in self.producers:
                    invalid_topic = self.output_topic_map["invalid"]
                    log_context["target_topic"] = invalid_topic
                    log_context["producer_key"] = "invalid"
                    logger.info(
                        f"Attempting to send invalid message | Context: {log_context}"
                    )
                    self._send_to_producer(
                        self.invalid_producer, invalid_topic,
                        invalid_data, f"invalid_{item_id}", log_context, "invalid"
                    )
                else:
                    logger.error(
                        "Producer for 'invalid' topic not found. Cannot send invalid message."
                    )
                    # Potentially send to error topic if invalid topic is essential?
                    self._send_to_producer(
                        self.error_producer, self.error_topic,
                        {"original_payload": invalid_data, "error": "Producer for invalid topic not configured", "context": log_context},
                        f"error_{item_id}", log_context, "error"
                    )
                    self.error_count += 1  # Count as error if invalid topic missing

        except Exception as e:
            self.error_count += 1
            logger.exception(
                f"CRITICAL UNEXPECTED error during message processing | Error: {e} | Context: {log_context}"
            )
            # Send the original raw value if possible
            self._send_to_producer(
                self.error_producer, self.error_topic,
                {"original_message": message_value, "error": str(e), "traceback": traceback.format_exc(), "context": log_context},
                f"error_{item_id}", log_context, "error"
            )
        finally:
            self._report_stats()  # Report stats periodically

    def _send_to_error_topic(
        self,
        data_payload: Any,
        error_type: str,
        error_message: str,
        context: Dict[str, Any],  # Pass context for richer error messages
    ):
        """Sends problematic data and error context to the designated error topic."""
        if "error" in self.producers:
            error_topic = self.output_topic_map["error"]
            context["target_topic"] = error_topic
            context["producer_key"] = "error"

            error_data = {
                "original_payload": data_payload,  # Might fail serialization if not dict/list/primitive
                "error_type": error_type,
                "error_message": error_message,
                "service_name": self.__class__.__name__,
                "error_timestamp": datetime.now(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "log_context": context,
            }

            logger.info(f"Attempting to send error message | Context: {context}")
            success = self._send_to_producer(
                self.error_producer, error_topic,
                error_data, f"error_{context.get('topic', 'unknown')}_{time.time_ns()}", context, "error"
            )
            if not success:
                # This is critical - we failed to report an error
                logger.error(
                    f"CRITICAL: Failed to send message to error topic '{error_topic}' via send_message | Context: {context}"
                )
                # Potentially add fallback logging (e.g., to a file)
        else:
            logger.error(
                f"Producer for 'error' topic not found. Cannot send error message | Error Type: {error_type} | Context: {context}"
            )

    def _report_stats(self, force: bool = False):
        """Logs processing statistics periodically or if forced."""
        current_time = time.time()
        if force or (current_time - self.last_stats_time >= self.stats_interval):
            if self.processed_count > 0:
                valid_pct = (self.valid_count / self.processed_count) * 100
                invalid_pct = (self.invalid_count / self.processed_count) * 100
                error_pct = (self.error_count / self.processed_count) * 100
                logger.info(
                    f"Processing Stats ({int(current_time - self.last_stats_time)}s interval): "
                    f"Processed={self.processed_count}, "
                    f"Valid={self.valid_count} ({valid_pct:.1f}%), "
                    f"Invalid={self.invalid_count} ({invalid_pct:.1f}%), "
                    f"Errors={self.error_count} ({error_pct:.1f}%)"
                )
            else:
                logger.info(
                    f"Processing Stats ({int(current_time - self.last_stats_time)}s interval): No messages processed yet."
                )

            # Reset counters for next interval? Or keep cumulative? Cumulative is usually better.
            self.last_stats_time = current_time
            # If resetting:
            # self.processed_count = 0
            # self.valid_count = 0
            # self.invalid_count = 0
            # self.error_count = 0

    def run(self):
        """Starts the main consumer loop."""
        if self.running:
            logger.warning("Service is already running.")
            return

        self.running = True
        logger.info("Starting Reddit validation service...")

        try:
            while self.running:
                message_processed_in_cycle = False
                # Iterate through configured consumers
                for topic, consumer in self.consumers.items():
                    if not self.running:
                        break  # Check running flag frequently

                    logger.debug(f"Polling consumer for topic: {topic}")
                    # The consume() method in the wrapper should yield messages or None
                    message_generator = consumer.consume()

                    try:
                        for msg in message_generator:
                            if not self.running:
                                break

                            if msg is not None:
                                # Check for Kafka errors embedded in the message wrapper
                                if "error" in msg and msg["error"] is not None:
                                    # The wrapper might log, but we ensure it's handled
                                    logger.error(
                                        f"Kafka consume error for topic {topic}: {msg['error']}"
                                    )
                                    # Depending on error type, might need specific handling
                                    # self._kafka_error_callback(msg['error']) # Call if wrapper doesn't
                                    self.error_count += 1  # Count consumer errors
                                elif "value" in msg:
                                    self.process_message(msg)
                                    message_processed_in_cycle = True
                                else:
                                    # Should not happen with the current wrapper structure
                                    logger.warning(
                                        f"Received unexpected message structure from consumer wrapper: {msg}"
                                    )

                            # Add a small sleep even if processing to prevent busy-waiting if queue drains fast
                            time.sleep(0.01)

                    except StopIteration:
                        # Generator exhausted (may happen with timeout) - log as debug
                        logger.debug(
                            f"Consumer generator for topic {topic} finished cycle or timed out."
                        )
                    except KafkaException as e:
                        # Handle Kafka-specific exceptions during consumption
                        logger.error(
                            f"KafkaException during consumption from topic {topic}: {e}"
                        )
                        self._kafka_error_callback(
                            e
                        )  # Use the callback for consistent handling
                    except Exception as e:
                        # Catch-all for unexpected errors during the consumption loop for a topic
                        logger.exception(
                            f"Unexpected error consuming from topic {topic}: {e}"
                        )
                        self.error_count += 1
                        # Consider if this should stop the service or just log and continue
                        # self.stop() # Uncomment to stop service on unexpected consumer loop error

                    if not self.running:
                        break  # Check again after inner loop

                # If no messages were processed across all consumers in a cycle, sleep a bit longer
                if not message_processed_in_cycle and self.running:
                    logger.debug(
                        f"No messages consumed in this cycle across {len(self.consumers)} topics. Sleeping..."
                    )
                    time.sleep(0.5)  # Main loop sleep

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt detected. Initiating shutdown.")
        except Exception as e:
            # Catch exceptions in the main `while self.running` loop
            logger.exception(f"Critical error in service run loop: {e}")
        finally:
            logger.info("Service run loop exiting. Initiating final cleanup...")
            self.stop()  # Ensure stop is called

    def stop(self):
        """Stops the consumer service and closes Kafka clients gracefully."""
        # Prevent multiple shutdowns
        if not self.running:
            logger.info("Stop called, but service was not running or already stopping.")
            return

        logger.info("Shutting down Reddit validation service...")
        self.running = False  # Signal loops to stop

        # Report final stats
        logger.info("Reporting final statistics...")
        self._report_stats(force=True)

        # Wait a moment for loops to finish processing current item
        time.sleep(1)

        logger.info("Closing Kafka consumers...")
        for topic, consumer in self.consumers.items():
            try:
                consumer.close()
                logger.info(f"Closed consumer for topic: {topic}")
            except Exception as e:
                logger.exception(f"Error closing consumer for topic {topic}: {e}")

        logger.info("Flushing and closing Kafka producers...")
        for key, producer in self.producers.items():
            try:
                producer.flush(timeout=10)  # Flush remaining messages
                # Close is often implicit in modern librdkafka wrappers upon deletion/gc,
                # but explicit close might be needed depending on the wrapper implementation.
                # If KafkaProducerWrapper has an explicit close, call it here.
                # producer.close()
                logger.info(f"Flushed producer for key: {key}")
            except Exception as e:
                logger.exception(f"Error flushing/closing producer for key {key}: {e}")

        logger.info("Reddit validation service shut down complete.")

    def _handle_signal(self, signum, frame):
        """Handles termination signals for graceful shutdown."""
        if self.running:
            logger.warning(
                f"Received signal {signal.Signals(signum).name}. Initiating graceful shutdown..."
            )
            self.stop()
        else:
            logger.warning(
                f"Received signal {signal.Signals(signum).name}, but service already stopping."
            )

    def _get_message_key(self, data: Optional[Dict[str, Any]]) -> Optional[str]:
        """Extracts the 'id' field from the raw message."""
        if isinstance(data, dict):
            return data.get("id")
        return None

    def _get_validated_message_key(self, validated_data: ValidatedRedditItem) -> Optional[str]:
        """Extracts the 'id' field from the validated Pydantic model."""
        # validated_data is the Pydantic model (RedditPost or RedditComment)
        return validated_data.id


if __name__ == "__main__":
    # Basic configuration for running standalone
    log_path = Path("logs") / "reddit_validation_service_{time}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)  # Ensure logs directory exists
    logger.add(
        str(log_path),
        rotation="100 MB",
        retention="10 days",  # Keep logs for shorter period for standalone runs?
        level="DEBUG",  # More verbose for standalone testing
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
    )

    service_instance = None
    try:
        # Consider adding argument parsing for config path if needed
        # config_arg = ...
        service_instance = RedditValidationService()  # Uses default config path
        service_instance.run()
    except Exception as e:
        logger.exception(
            f"Failed to initialize or run the RedditValidationService: {e}"
        )
        if service_instance and service_instance.running:
            service_instance.stop()  # Attempt cleanup even on init/run failure
    finally:
        logger.info("RedditValidationService standalone execution finished.")
