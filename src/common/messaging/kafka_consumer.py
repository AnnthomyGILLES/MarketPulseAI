# src/common/messaging/kafka_consumer.py

import json  # For handling deserialization errors
import threading  # Needed for graceful shutdown with blocking iteration
import time
from typing import Dict, Any, List, Optional, Generator, Union, Tuple

# Use kafka-python library
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

# Assuming these utilities exist and work as expected
from src.common.messaging.serializers import deserialize_message  #
from src.utils.logging import get_logger  # Use the central logging utility

# Get logger for this module
logger = get_logger(__name__)


class KafkaConsumerWrapper:
    """
    A Kafka consumer wrapper using kafka-python.

    Handles subscription, message iteration, deserialization, and basic error handling.
    Note: Graceful shutdown with kafka-python's blocking iterator requires careful handling (e.g., using threads or timeouts in poll). This implementation uses a flag and relies on closing the consumer to unblock iteration.
    """

    def __init__(
        self,
        topics: Union[List[str], Tuple[str, ...]],
        bootstrap_servers: Union[str, List[str]],
        group_id: str,
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = True,
        key_deserializer: Optional[callable] = lambda k: k.decode("utf-8")
        if k
        else None,
        # value_deserializer = None, # Handle deserialization manually
        client_id: Optional[str] = None,
        api_version: Optional[Tuple[int, ...]] = None,  # e.g., (2, 0, 2)
        fetch_max_wait_ms: int = 500,  # How long poll blocks if no data
        **kwargs: Any,
    ):  # Pass other kafka-python KafkaConsumer options
        """
        Initializes the KafkaConsumerWrapper using kafka-python.

        Args:
            topics: List or tuple of topics to subscribe to.
            bootstrap_servers: List of broker addresses or comma-separated string.
            group_id: The consumer group ID.
            auto_offset_reset: Consumer behavior ('earliest', 'latest').
            enable_auto_commit: If True, commits offsets automatically.
            key_deserializer: Callable to deserialize keys (defaults to UTF-8 decoding).
            client_id: Optional identifier for the consumer connection.
            api_version: Specify Kafka broker version if auto-detection fails.
            fetch_max_wait_ms: Max time (ms) consumer blocks if no new messages. Helps make iteration less blocking.
            **kwargs: Additional arguments passed directly to kafka.KafkaConsumer.
                      See kafka-python documentation (e.g., security_protocol, ssl_context, sasl options).
        """
        self.topics = topics
        self.consumer_config = {
            "bootstrap_servers": bootstrap_servers,
            "group_id": group_id,
            "auto_offset_reset": auto_offset_reset,
            "enable_auto_commit": enable_auto_commit,
            "key_deserializer": key_deserializer,
            "value_deserializer": None,  # Crucial: handle value deserialization manually
            "client_id": client_id,
            "api_version": api_version,
            "fetch_max_wait_ms": fetch_max_wait_ms,  # Makes iteration check _running flag periodically
            # Set consumer_timeout_ms to -1 to block indefinitely until a message or error,
            # or a positive value to time out the iterator itself (can be complex to handle)
            # 'consumer_timeout_ms': -1, # Default is -1 (block)
            **kwargs,
        }
        self.consumer: Optional[KafkaConsumer] = None
        self._running = False  # Flag to control consumption loop
        self._thread: Optional[threading.Thread] = (
            None  # If using a thread for consumption
        )

        try:
            logger.info(
                f"Initializing KafkaConsumer for group '{group_id}' on servers {bootstrap_servers}..."
            )
            self.consumer = KafkaConsumer(**self.consumer_config)
            # Subscribe to topics (kafka-python consumer does this implicitly when topics are provided,
            # or explicitly via subscribe method)
            # self.consumer.subscribe(topics=list(self.topics)) # Can also use subscribe method
            logger.info(f"KafkaConsumer subscribed to topics: {self.topics}")
            self._running = True
        except NoBrokersAvailable as e:
            logger.exception(
                f"Failed to initialize KafkaConsumer: No brokers available at {bootstrap_servers}. Error: {e}"
            )
            raise
        except KafkaError as e:
            logger.exception(f"Failed to initialize KafkaConsumer: {e}")
            raise
        except Exception as e:
            logger.exception(f"Unexpected error initializing KafkaConsumer: {e}")
            raise

    def _message_generator(self) -> Generator[Optional[Dict[str, Any]], None, None]:
        """Internal generator to yield messages, handling deserialization and errors."""
        if not self.consumer:
            logger.error("Consumer not initialized.")
            return

        logger.info("Starting message consumption loop...")
        while self._running:
            try:
                logger.debug(f"Attempting to iterate consumer for topics {self.topics} (will block)...")
                # Iterating the consumer blocks until fetch_max_wait_ms or a message arrives
                for msg in self.consumer:
                    if not self._running:  # Check flag immediately after waking up
                        logger.debug("Consumer woken up, but _running is False. Exiting inner loop.")
                        break
                    try:
                        # msg.key is already deserialized by key_deserializer
                        # msg.value is bytes, needs manual deserialization
                        value_bytes = msg.value
                        deserialized_value = deserialize_message(value_bytes)  #

                        logger.debug(
                            f"Consumed message from {msg.topic}[{msg.partition}] at offset {msg.offset}"
                        )
                        yield {
                            "topic": msg.topic,
                            "key": msg.key,
                            "value": deserialized_value,
                            "partition": msg.partition,
                            "offset": msg.offset,
                            "timestamp": msg.timestamp,  # timestamp in ms
                        }
                        # If using manual commit (enable_auto_commit=False), commit here or batch commits
                        # self.consumer.commit()

                    except json.JSONDecodeError as e:
                        logger.error(
                            f"JSON deserialization error for message at {msg.topic}[{msg.partition}] offset {msg.offset}: {e}. Value(bytes): {value_bytes[:100]}..."
                        )
                        # Optionally yield an error object or None
                        yield None
                    except Exception as e:
                        logger.exception(
                            f"Error processing message at {msg.topic}[{msg.partition}] offset {msg.offset}: {e}"
                        )
                        yield None  # Yield None on processing error

                # If the loop finishes naturally without break (e.g., consumer closed externally?), ensure flag is False
                if self._running:
                    logger.warning(
                        "Consumer iteration loop exited unexpectedly while running flag was True."
                    )
                    self._running = False
                else:
                    logger.debug("Exited consumer iteration loop because _running is False.")

            except StopIteration:
                # This can happen if consumer_timeout_ms is set and reached
                logger.info(
                    "Consumer iteration timed out (StopIteration). Continuing poll if running."
                )
                if not self._running:
                    break  # Exit main while loop if we were stopped
                # Otherwise, the outer while loop continues
            except KafkaError as e:
                logger.error(f"KafkaError during consumption: {e}", exc_info=True)
                # Depending on the error, might need to stop
                if "Broker disconnected" in str(e) or "Connection refused" in str(e):
                    self._running = False  # Stop on likely persistent connection issues
                    logger.error("Stopping consumption due to Kafka connection error.")
                time.sleep(5)  # Wait before retrying iteration
            except Exception as e:
                logger.exception(f"Unexpected error during consumer iteration: {e}")
                self._running = False  # Stop on unexpected errors
                break  # Exit main while loop

        logger.info("Message consumption loop finished.")

    def consume(self) -> Generator[Optional[Dict[str, Any]], None, None]:
        """
        Provides a generator to consume messages from the subscribed topics.

        Yields:
            A dictionary containing the message details ('topic', 'key', 'value', 'partition', 'offset', 'timestamp')
            or None if an error occurs during processing/deserialization or if no message is available within the poll timeout.
            Yields None repeatedly if consumer is stopped.
        """
        if not self._running:
            logger.warning("Consume called but consumer is not running.")
            while True:
                yield None  # Yield None indefinitely if stopped

        yield from self._message_generator()

    def stop(self):
        """Signals the consumer to stop consuming messages."""
        logger.info("Stop requested for KafkaConsumerWrapper.")
        self._running = False
        # Closing the consumer is necessary to unblock the iterator
        # Call close here or ensure it's called externally after stop()
        self.close()

    def close(self):
        """Closes the Kafka consumer connection."""
        logger.info("Closing Kafka consumer...")
        self._running = False  # Ensure flag is set
        if self.consumer:
            try:
                # Close will also trigger commit if auto-commit is enabled and needed
                self.consumer.close()
                logger.info("Kafka consumer closed successfully.")
            except Exception as e:
                logger.exception(f"Error closing Kafka consumer: {e}")
            finally:
                self.consumer = None
        else:
            logger.info("Consumer already closed or not initialized.")

    def __iter__(self) -> Generator[Optional[Dict[str, Any]], None, None]:
        """Allows iterating over the consumer instance directly."""
        yield from self.consume()

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager, ensuring consumer is closed."""
        self.close()
