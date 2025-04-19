# src/common/messaging/kafka_consumer.py

import json  # For handling deserialization errors
from typing import Dict, Any, List, Optional, Generator, Union, Tuple

# Use kafka-python library
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable, KafkaTimeoutError
from loguru import logger

# Assuming these utilities exist and work as expected
from src.common.messaging.serializers import deserialize_message  #



class KafkaConsumerWrapper:
    """
    A Kafka consumer wrapper using kafka-python.

    Handles connection, subscription, message iteration with deserialization,
    and error management. Provides a generator interface for message consumption.
    Closing the consumer via `close()` or the context manager stops iteration.
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
        # Setting consumer_timeout_ms makes the iterator raise StopIteration after that duration
        # consumer_timeout_ms: int = -1, # Default is -1 (block indefinitely)
        max_poll_interval_ms: int = 300000, # Default for kafka-python
        session_timeout_ms: int = 10000, # Default for kafka-python
        **kwargs: Any,
    ):
        """
        Initializes the KafkaConsumerWrapper.

        Args:
            topics: Topic(s) to subscribe to.
            bootstrap_servers: Broker address list or comma-separated string.
            group_id: Consumer group ID.
            auto_offset_reset: 'earliest' or 'latest'.
            enable_auto_commit: If True, commits offsets automatically in the background.
            key_deserializer: Callable for key deserialization (defaults to UTF-8 decoding).
            client_id: Optional identifier for the consumer.
            api_version: Specify Kafka broker version tuple, e.g., (2, 8, 1). Auto-detects if None.
            fetch_max_wait_ms: Max time (ms) consumer's poll() blocks if no messages.
            consumer_timeout_ms: If positive, iterator raises StopIteration after timeout.
                                 If -1, blocks indefinitely. (Passed via kwargs if needed).
            max_poll_interval_ms: Max delay between poll() calls before considered dead.
            session_timeout_ms: Timeout for broker to detect client failure.
            **kwargs: Additional arguments for kafka.KafkaConsumer (e.g., security settings).
        """
        self.topics = topics
        self._closed = True # Start as closed until successfully initialized
        self.consumer: Optional[KafkaConsumer] = None

        consumer_config = {
            "bootstrap_servers": bootstrap_servers,
            "group_id": group_id,
            "auto_offset_reset": auto_offset_reset,
            "enable_auto_commit": enable_auto_commit,
            "key_deserializer": key_deserializer,
            "value_deserializer": None, # Handle value deserialization manually
            "client_id": client_id,
            "api_version": api_version,
            "fetch_max_wait_ms": fetch_max_wait_ms,
            "max_poll_interval_ms": max_poll_interval_ms,
            "session_timeout_ms": session_timeout_ms,
            **kwargs,
        }

        try:
            logger.info(
                f"Initializing KafkaConsumer for group '{group_id}' on servers {bootstrap_servers}..."
            )
            # KafkaConsumer constructor handles subscription
            self.consumer = KafkaConsumer(*self.topics, **consumer_config)
            self._closed = False # Mark as open
            logger.info(
                f"KafkaConsumer initialized and subscribed to topics: {self.topics}"
            )

        except NoBrokersAvailable as e:
            logger.error(
                f"Failed to connect to Kafka brokers at {bootstrap_servers}. Ensure brokers are running and accessible. Error: {e}"
            )
            # Keep self.consumer as None and self._closed as True
            raise # Re-raise for the caller to handle
        except KafkaError as e:
            logger.error(f"KafkaError during KafkaConsumer initialization: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error during KafkaConsumer initialization: {e}", exc_info=True)
            raise


    def consume(self) -> Generator[Dict[str, Any], None, None]:
        """
        Generator that yields consumed messages after deserialization.

        Iterates indefinitely (or until `consumer_timeout_ms`) over the Kafka consumer,
        handling message deserialization and basic error logging. Skips messages
        that fail deserialization.

        Yields:
            dict: A dictionary containing message details:
                  {'topic', 'key', 'value', 'partition', 'offset', 'timestamp'}

        Raises:
            RuntimeError: If the consumer is closed or was never initialized.
            KafkaError: If a critical Kafka error occurs during consumption (re-raised).
            StopIteration: If consumer_timeout_ms is set and the timeout is reached.
        """
        if self._closed or self.consumer is None:
            raise RuntimeError("Kafka consumer is closed or not initialized.")

        logger.info(f"Starting message consumption from topics: {self.topics}")
        try:
            # The consumer object itself is iterable and blocks until messages arrive
            # or timeouts occur (fetch_max_wait_ms, consumer_timeout_ms).
            for msg in self.consumer:
                try:
                    value_bytes = msg.value
                    if value_bytes is None:
                         logger.warning(f"Received message with None value at {msg.topic}[{msg.partition}] offset {msg.offset}. Skipping.")
                         continue

                    deserialized_value = deserialize_message(value_bytes)

                    logger.debug(f"Consumed message from {msg.topic}[{msg.partition}] at offset {msg.offset}")
                    yield {
                        "topic": msg.topic,
                        "key": msg.key, # Already deserialized by key_deserializer
                        "value": deserialized_value,
                        "partition": msg.partition,
                        "offset": msg.offset,
                        "timestamp": msg.timestamp, # Timestamp in ms from Kafka
                    }
                    # Automatic commit handled by kafka-python if enable_auto_commit=True
                    # If enable_auto_commit=False, manual commit would be needed here or in batches.

                except json.JSONDecodeError as e:
                    logger.error(
                        f"JSON deserialization error for message at {msg.topic}[{msg.partition}] offset {msg.offset}: {e}. Value(bytes): {value_bytes[:100]}... Skipping message."
                    )
                    # Continue to the next message
                except Exception as e:
                    logger.exception(
                        f"Unexpected error processing message at {msg.topic}[{msg.partition}] offset {msg.offset}: {e}. Skipping message."
                    )
                    # Continue to the next message

        except StopIteration:
            # Occurs if consumer_timeout_ms is set and reached. The generator ends naturally.
            logger.info("Consumer iteration stopped (StopIteration, likely due to consumer_timeout_ms).")
            # Let StopIteration propagate up to the caller
            raise
        except KafkaTimeoutError as e:
             logger.warning(f"KafkaTimeoutError during consumption: {e}. This might indicate broker issues or network latency.")
             # Depending on the policy, we might continue or raise. Raising signals a potential issue.
             raise e
        except KafkaError as e:
            # Log other Kafka errors encountered during consumption. These might be critical.
            logger.error(f"KafkaError during consumption: {e}", exc_info=True)
            self._closed = True # Mark as closed because the consumer might be unusable
            # Re-raise the error to signal a potentially fatal state to the caller.
            raise e
        except Exception as e:
            # Log unexpected errors during the iteration.
            logger.exception(f"Unexpected error during consumer iteration: {e}")
            self._closed = True # Mark as closed on unexpected errors too
            raise e
        finally:
            # This block executes when the loop terminates (normally, via error, or StopIteration).
            logger.info("Message consumption loop finished.")
            # Consumer closing is handled by external call to close() or context manager exit.

    def close(self):
        """
        Closes the Kafka consumer connection cleanly.

        Safe to call multiple times. Idempotent.
        """
        if self._closed or self.consumer is None:
            logger.info("Consumer close() called, but already closed or not initialized.")
            return

        logger.info("Closing Kafka consumer...")
        try:
            # Close() handles committing offsets if enable_auto_commit=True
            self.consumer.close()
            logger.info("Kafka consumer closed successfully.")
        except Exception as e:
            # Log error during close, but still mark as closed.
            logger.exception(f"Error closing Kafka consumer: {e}")
        finally:
            # Ensure it's marked as closed regardless of errors during close()
            self._closed = True
            self.consumer = None


    def __iter__(self) -> Generator[Dict[str, Any], None, None]:
        """Allows iterating directly over the KafkaConsumerWrapper instance."""
        yield from self.consume()

    def __enter__(self):
        """Enter context manager. Ensures consumer is initialized."""
        if self._closed or self.consumer is None:
             # This case should ideally be prevented by handling errors in __init__
             raise RuntimeError("Cannot enter context: Consumer is closed or failed to initialize.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager, ensuring the consumer is closed."""
        self.close()
