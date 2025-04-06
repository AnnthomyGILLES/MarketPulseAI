import abc
import signal
import time
import traceback
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

from confluent_kafka import KafkaException
from loguru import logger

from src.common.messaging.kafka_consumer import KafkaConsumerWrapper
from src.common.messaging.kafka_producer import KafkaProducerWrapper
from src.common.validation.base_validator import BaseValidator
from src.utils.config import load_config


class BaseValidationService(abc.ABC):
    """
    Abstract base class for Kafka-based validation services.

    Handles common tasks like:
    - Loading configuration.
    - Setting up Kafka consumers and producers (valid, invalid, error topics).
    - Managing the run loop and graceful shutdown.
    - Tracking and reporting processing statistics.
    - Providing a framework for message processing using a specific validator.
    """

    DEFAULT_CONFIG_DIR = Path(__file__).resolve().parents[3] / "config" / "kafka"
    DEFAULT_CONFIG_FILE = "kafka_config.yaml"

    def __init__(
        self,
        service_name: str,
        validator: BaseValidator,
        input_topics_config_keys: List[str],
        consumer_group_config_key: str,
        valid_topic_config_key: str,
        invalid_topic_config_key: str,
        error_topic_config_key: str,
        config_path: Optional[str] = None,
    ):
        """
        Initialize the base validation service.

        Args:
            service_name: Name of the service (e.g., "MarketDataValidation", "RedditValidation").
            validator: An instance of a BaseValidator subclass.
            input_topics_config_keys: List of keys in kafka_config['topics'] for input topics.
            consumer_group_config_key: Key in kafka_config['consumer_groups'] for the group ID.
            valid_topic_config_key: Key in kafka_config['topics'] for the valid output topic.
            invalid_topic_config_key: Key in kafka_config['topics'] for the invalid output topic.
            error_topic_config_key: Key in kafka_config['topics'] for the error output topic.
            config_path: Optional path to the Kafka configuration file.
        """
        self.service_name = service_name
        self.validator = validator
        self.input_topics_config_keys = input_topics_config_keys
        self.consumer_group_config_key = consumer_group_config_key
        self.valid_topic_config_key = valid_topic_config_key
        self.invalid_topic_config_key = invalid_topic_config_key
        self.error_topic_config_key = error_topic_config_key

        self.config_path = Path(config_path or (self.DEFAULT_CONFIG_DIR / self.DEFAULT_CONFIG_FILE))
        logger.info(f"[{self.service_name}] Loading Kafka configuration from: {self.config_path}")
        try:
            self.config = load_config(str(self.config_path))
            # Basic validation of required keys directly in the loaded config
            if "topics" not in self.config: raise KeyError("Missing 'topics' key in config.")
            if "consumer_groups" not in self.config: raise KeyError("Missing 'consumer_groups' key in config.")
            if "bootstrap_servers" not in self.config: raise KeyError("Missing 'bootstrap_servers' key in config.")
            # No longer expect a top-level 'kafka' key, self.config holds the direct YAML structure
            # self.kafka_config = self.config["kafka"] # Removed this line

        except (FileNotFoundError, KeyError, ValueError) as e:
            logger.exception(
                f"[{self.service_name}] Failed to load or parse required keys from Kafka configuration {self.config_path}: {e}"
            )
            raise

        self.running = False
        self._setup_logging() # Setup service-specific logging
        self._setup_kafka_clients()

        # Statistics
        self.stats = {
            "processed": 0,
            "valid": 0,
            "invalid": 0,
            "errors": 0, # Renamed from error_count for clarity
            "last_report_time": time.time(),
        }
        # Get stats interval from general config section or provide a default
        self.stats_interval = self.config.get("config", {}).get("stats_interval_seconds", 60)

        # Signal handling for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        logger.info(f"[{self.service_name}] Service initialized successfully.")


    @abc.abstractmethod
    def _get_message_key(self, data: Dict[str, Any]) -> Optional[str]:
        """Subclasses must implement how to extract a unique identifier from the raw message."""
        raise NotImplementedError

    @abc.abstractmethod
    def _get_validated_message_key(self, validated_data: Any) -> Optional[str]:
        """Subclasses must implement how to extract a key for the validated message."""
        raise NotImplementedError

    def _setup_logging(self):
        """Sets up Loguru sinks for the service."""
        log_dir = Path("logs") / self.service_name.lower()
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "{time}.log"

        logger.add(
            str(log_file),
            rotation="100 MB",
            retention="10 days",
            level=self.config.get("logging", {}).get("level", "INFO"), # Configurable level
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
            enqueue=True, # Make logging asynchronous
            backtrace=True,
            diagnose=True,
        )
        logger.info(f"[{self.service_name}] Logging configured. Log files in: {log_dir}")


    def _setup_kafka_clients(self):
        """Initializes Kafka consumers and producers based on config."""
        self.consumer: Optional[KafkaConsumerWrapper] = None # Assuming one consumer per service instance for now
        self.valid_producer: Optional[KafkaProducerWrapper] = None
        self.invalid_producer: Optional[KafkaProducerWrapper] = None
        self.error_producer: Optional[KafkaProducerWrapper] = None

        try:
            # Access config keys directly from self.config
            bootstrap_servers = self.config["bootstrap_servers"]
            topics_config = self.config["topics"]
            consumer_groups = self.config["consumer_groups"]

            # --- Consumer Setup ---
            input_topics = [topics_config[key] for key in self.input_topics_config_keys]
            group_id = consumer_groups[self.consumer_group_config_key]

            # Get consumer defaults from the 'consumer' section or provide empty dict
            consumer_defaults = self.config.get("consumer", {})
            consumer_conf = {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                # Use defaults from the 'consumer' section in the config file
                "auto.offset.reset": consumer_defaults.get("auto.offset.reset", "earliest"),
                "enable.auto.commit": consumer_defaults.get("enable.auto.commit", True),
                "auto.commit.interval.ms": consumer_defaults.get("auto.commit.interval.ms", 5000),
                "error_cb": self._kafka_error_callback,
                # Add any other default or specific consumer configs here
            }
            # Allow overriding defaults from config if needed (e.g., a service-specific section)
            consumer_conf.update(self.config.get(f"{self.service_name.lower()}_consumer_config", {}))

            logger.info(f"[{self.service_name}] Initializing Kafka consumer for topics: {input_topics}, group: {group_id}")
            self.consumer = KafkaConsumerWrapper(
                topics=input_topics,
                config=consumer_conf,
                consumer_timeout_ms=1000, # For non-blocking consume loop
            )
            logger.info(f"[{self.service_name}] Consumer initialized.")

            # --- Producer Setup ---
            # Get producer defaults from the 'producer' section or provide empty dict
            producer_defaults = self.config.get("producer", {})
            base_producer_conf = {
                "bootstrap.servers": bootstrap_servers,
                 # Use defaults from the 'producer' section in the config file
                "acks": producer_defaults.get("acks", "all"),
                "retries": producer_defaults.get("retries", 3),
                "error_cb": self._kafka_error_callback,
                 # Add other producer defaults like batch.size, linger.ms etc. if desired
                 "batch.size": producer_defaults.get("batch.size", 16384),
                 "linger.ms": producer_defaults.get("linger.ms", 5),
            }

            # Valid Producer
            self.valid_topic = topics_config[self.valid_topic_config_key]
            valid_producer_conf = base_producer_conf.copy()
            # Allow overriding defaults from config if needed (e.g., a service-specific section)
            valid_producer_conf.update(self.config.get(f"{self.service_name.lower()}_valid_producer_config", {}))
            self.valid_producer = KafkaProducerWrapper(config=valid_producer_conf)
            logger.info(f"[{self.service_name}] Initialized 'valid' producer for topic: {self.valid_topic}")

            # Invalid Producer
            self.invalid_topic = topics_config[self.invalid_topic_config_key]
            invalid_producer_conf = base_producer_conf.copy()
            # Allow overriding defaults from config if needed (e.g., a service-specific section)
            invalid_producer_conf.update(self.config.get(f"{self.service_name.lower()}_invalid_producer_config", {}))
            self.invalid_producer = KafkaProducerWrapper(config=invalid_producer_conf)
            logger.info(f"[{self.service_name}] Initialized 'invalid' producer for topic: {self.invalid_topic}")

            # Error Producer
            self.error_topic = topics_config[self.error_topic_config_key]
            error_producer_conf = base_producer_conf.copy()
            # Allow overriding defaults from config if needed (e.g., a service-specific section)
            error_producer_conf.update(self.config.get(f"{self.service_name.lower()}_error_producer_config", {}))
            self.error_producer = KafkaProducerWrapper(config=error_producer_conf)
            logger.info(f"[{self.service_name}] Initialized 'error' producer for topic: {self.error_topic}")

        except KeyError as e:
            logger.exception(
                f"[{self.service_name}] Configuration key error during Kafka client setup: Missing key {e} in {self.config_path}"
            )
            raise ValueError(f"Missing required Kafka configuration key: {e}")
        except Exception as e:
            logger.exception(f"[{self.service_name}] Failed to initialize Kafka clients: {e}")
            raise


    def _kafka_error_callback(self, err: KafkaException):
        """Generic callback for Kafka client errors."""
        log_prefix = f"[{self.service_name}] Kafka Error:"
        if err.code() == KafkaException._PARTITION_EOF:
            # Typically informational, can be logged at DEBUG or INFO
            logger.info(f"{log_prefix} Reached end of partition: {err}")
        elif err.fatal():
            logger.error(f"{log_prefix} FATAL Error: {err}. Stopping service.")
            self.stop() # Trigger shutdown on fatal errors
        else:
            logger.warning(f"{log_prefix} Non-fatal Error: {err}")


    def _update_and_report_stats(self, is_valid: Optional[bool] = None, is_error: bool = False) -> None:
        """Updates processing statistics and logs them periodically."""
        self.stats["processed"] += 1
        if is_error:
            self.stats["errors"] += 1
        elif is_valid is True:
            self.stats["valid"] += 1
        elif is_valid is False:
            self.stats["invalid"] += 1

        current_time = time.time()
        if current_time - self.stats["last_report_time"] >= self.stats_interval:
            self._report_stats(current_time)


    def _report_stats(self, current_time: Optional[float] = None):
        """Logs the current statistics."""
        if current_time is None:
             current_time = time.time()

        processed = self.stats["processed"]
        if processed > 0:
            valid_pct = (self.stats["valid"] / processed) * 100
            invalid_pct = (self.stats["invalid"] / processed) * 100
            error_pct = (self.stats["errors"] / processed) * 100
            logger.info(
                f"[{self.service_name}] Stats ({int(current_time - self.stats['last_report_time'])}s): "
                f"Processed={processed}, "
                f"Valid={self.stats['valid']} ({valid_pct:.1f}%), "
                f"Invalid={self.stats['invalid']} ({invalid_pct:.1f}%), "
                f"Errors={self.stats['errors']} ({error_pct:.1f}%)"
            )
        else:
            logger.info(f"[{self.service_name}] Stats ({int(current_time - self.stats['last_report_time'])}s): No messages processed yet.")

        self.stats["last_report_time"] = current_time


    def _send_to_producer(
        self,
        producer: KafkaProducerWrapper,
        topic: str,
        data: Any,
        key: Optional[str],
        context: Dict[str, Any],
        msg_type: str # e.g., "valid", "invalid", "error"
    ) -> bool:
        """Helper to send data to a producer and log outcome."""
        log_prefix = f"[{self.service_name}]"
        try:
            # Ensure data is serializable (often dict for JSON serialization)
            if not isinstance(data, (dict, list, str, int, float, bool, type(None))):
                 # Attempt model_dump if it's a Pydantic model, otherwise convert to str
                if hasattr(data, 'model_dump'):
                    payload = data.model_dump(mode="json")
                else:
                     logger.warning(f"{log_prefix} Payload for {msg_type} topic is not easily serializable ({type(data)}), converting to string. Context: {context}")
                     payload = str(data)
            else:
                payload = data

            logger.debug(f"{log_prefix} Sending {msg_type} message | Topic: {topic}, Key: {key} | Context: {context}")
            success = producer.send_message(topic=topic, value=payload, key=key)
            if success:
                 logger.debug(f"{log_prefix} Successfully queued {msg_type} message | Topic: {topic}, Key: {key} | Context: {context}")
                 return True
            else:
                 logger.error(f"{log_prefix} Failed to queue {msg_type} message (send_message returned False) | Topic: {topic}, Key: {key} | Context: {context}")
                 return False
        except Exception as e:
            logger.exception(f"{log_prefix} Exception sending {msg_type} message | Topic: {topic}, Key: {key}, Error: {e} | Context: {context}")
            return False


    def process_message(self, raw_message: Dict[str, Any]) -> None:
        """
        Processes a single raw message from Kafka.

        - Extracts context.
        - Validates the message using self.validator.
        - Sends results to the appropriate output topic (valid, invalid, or error).
        - Updates statistics.
        """
        message_value = raw_message.get("value")
        source_topic = raw_message.get("topic", "unknown")
        message_offset = raw_message.get("offset", -1)
        # Use subclass method to get message key/id
        item_id = self._get_message_key(message_value if isinstance(message_value, dict) else {}) or "UNKNOWN_ID"

        log_context = {
            "topic": source_topic,
            "offset": message_offset,
            "item_id": item_id,
        }
        log_prefix = f"[{self.service_name}]"

        logger.debug(f"{log_prefix} Received message | Value: {str(message_value)[:200]}... | Context: {log_context}")

        if not isinstance(message_value, dict):
            err_msg = f"Message value is not a dictionary (Type: {type(message_value)})"
            logger.error(f"{log_prefix} {err_msg} | Context: {log_context}")
            self._send_to_producer(
                self.error_producer, self.error_topic,
                {"original_payload": message_value, "error": err_msg, "context": log_context},
                f"error_{item_id}", log_context, "error"
            )
            self._update_and_report_stats(is_error=True)
            return

        try:
            logger.debug(f"{log_prefix} Attempting validation | Context: {log_context}")
            is_valid, validated_data, validation_errors = self.validator.validate(message_value)

            if is_valid and validated_data is not None:
                logger.debug(f"{log_prefix} Validation SUCCEEDED | Context: {log_context}")
                validated_key = self._get_validated_message_key(validated_data) or f"valid_{item_id}"
                send_success = self._send_to_producer(
                    self.valid_producer, self.valid_topic,
                    validated_data, validated_key, log_context, "valid"
                )
                self._update_and_report_stats(is_valid=True, is_error=not send_success) # Count as error if send fails

            else: # Validation failed
                logger.warning(f"{log_prefix} Validation FAILED | Errors: {validation_errors} | Context: {log_context}")
                invalid_payload = {
                    "original_message": message_value,
                    "validation_errors": validation_errors,
                    "processing_timestamp": time.time(),
                    "log_context": log_context,
                }
                invalid_key = f"invalid_{item_id}"
                send_success = self._send_to_producer(
                     self.invalid_producer, self.invalid_topic,
                     invalid_payload, invalid_key, log_context, "invalid"
                )
                self._update_and_report_stats(is_valid=False, is_error=not send_success) # Count as error if send fails

        except Exception as e:
            # Catch errors during validation or sending logic itself
            error_msg = f"Unexpected error processing message: {str(e)}"
            logger.exception(f"{log_prefix} {error_msg} | Context: {log_context}")
            error_payload = {
                 "original_message": message_value,
                 "error": error_msg,
                 "traceback": traceback.format_exc(),
                 "processing_timestamp": time.time(),
                 "log_context": log_context,
            }
            error_key = f"error_{item_id}"
            # Don't count send failure here as error again, already counted the processing error
            self._send_to_producer(
                 self.error_producer, self.error_topic,
                 error_payload, error_key, log_context, "error"
            )
            self._update_and_report_stats(is_error=True)


    def run(self) -> None:
        """Starts the main consumer loop."""
        if self.running:
            logger.warning(f"[{self.service_name}] Service is already running.")
            return

        if not self.consumer:
             logger.error(f"[{self.service_name}] Consumer not initialized. Cannot run.")
             return

        self.running = True
        logger.info(f"[{self.service_name}] Starting validation service...")
        log_prefix = f"[{self.service_name}]"

        try:
            while self.running:
                message_processed_in_cycle = False
                # Consume messages using the wrapper's generator
                message_generator = self.consumer.consume()

                try:
                    for msg in message_generator:
                        if not self.running: break

                        if msg is not None:
                             if "error" in msg and msg["error"] is not None:
                                 # Let the error callback handle logging/shutdown logic
                                 # self._kafka_error_callback(msg['error']) # Callback should already be configured
                                 # Log context if available
                                 logger.error(f"{log_prefix} Kafka consume error encountered: {msg['error']}")
                                 self.stats["errors"] += 1 # Count consume errors
                                 # Potentially add a delay or specific handling for consumer errors
                                 time.sleep(1)
                             elif "value" in msg:
                                 self.process_message(msg)
                                 message_processed_in_cycle = True
                             else:
                                 logger.warning(f"{log_prefix} Received unexpected message structure from consumer: {msg}")

                        # Optional small sleep to prevent high CPU usage in tight loops
                        time.sleep(0.01)

                except StopIteration:
                    logger.debug(f"{log_prefix} Consumer generator finished cycle or timed out.")
                # KafkaException should ideally be caught by the error_cb
                except Exception as e:
                    logger.exception(f"{log_prefix} Unexpected error in consumer loop: {e}")
                    self.stats["errors"] += 1
                    # Consider if this requires stopping the service
                    # self.stop()
                    time.sleep(5) # Pause before retrying loop after unexpected error

                if not self.running: break # Check flag again

                # If no messages processed, sleep longer
                if not message_processed_in_cycle and self.running:
                    logger.debug(f"{log_prefix} No messages consumed in this cycle. Sleeping...")
                    time.sleep(0.5)

        except KeyboardInterrupt:
            logger.info(f"{log_prefix} Keyboard interrupt received.")
        except Exception as e:
            logger.exception(f"{log_prefix} Critical error in main run loop: {e}")
        finally:
            logger.info(f"{log_prefix} Run loop exiting. Initiating shutdown...")
            self.stop()


    def stop(self) -> None:
        """Stops the service and closes Kafka clients gracefully."""
        if not self.running:
            # logger.info(f"[{self.service_name}] Stop called, but service was not running.")
            return

        logger.info(f"[{self.service_name}] Shutting down validation service...")
        self.running = False

        # Report final stats before closing
        logger.info(f"[{self.service_name}] Reporting final statistics...")
        self._report_stats(current_time=time.time())

        # Give time for the loop to exit
        time.sleep(1)

        log_prefix = f"[{self.service_name}]"
        logger.info(f"{log_prefix} Closing Kafka consumer...")
        try:
            if self.consumer:
                self.consumer.close()
                logger.info(f"{log_prefix} Consumer closed.")
        except Exception as e:
            logger.exception(f"{log_prefix} Error closing consumer: {e}")

        producers = {
            "valid": self.valid_producer,
            "invalid": self.invalid_producer,
            "error": self.error_producer,
        }
        for name, producer in producers.items():
             if producer:
                logger.info(f"{log_prefix} Flushing and closing '{name}' producer...")
                try:
                    producer.flush(timeout=10)
                    # producer.close() # If explicit close is available/needed
                    logger.info(f"{log_prefix} '{name}' producer flushed.")
                except Exception as e:
                    logger.exception(f"{log_prefix} Error flushing/closing '{name}' producer: {e}")

        logger.info(f"[{self.service_name}] Service shutdown complete.")
        # Remove handlers to prevent multiple calls if stop is called again
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    def _handle_signal(self, signum, frame):
        """Handles termination signals."""
        signal_name = signal.Signals(signum).name
        if self.running:
            logger.warning(f"[{self.service_name}] Received signal {signal_name}. Initiating graceful shutdown...")
            # Don't call stop directly from signal handler if it involves complex operations (like Kafka flush)
            # Just set the flag, the main loop will detect it and call stop.
            self.running = False
        else:
            logger.warning(f"[{self.service_name}] Received signal {signal_name}, but service already stopping.")


    @classmethod
    def run_service(cls, *args, **kwargs):
         """Class method to create and run the service instance."""
         service_instance = None
         try:
             service_instance = cls(*args, **kwargs)
             service_instance.run()
         except Exception as e:
             logger.exception(f"[{cls.__name__}] Failed to initialize or run service: {e}")
             # Attempt cleanup if instance exists and was running
             if service_instance and getattr(service_instance, 'running', False):
                 service_instance.stop()
         finally:
             logger.info(f"[{cls.__name__}] Service execution finished.") 