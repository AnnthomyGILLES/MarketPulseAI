# src/common/messaging/kafka_consumer.py

import json  # For handling deserialization errors
import time  # Keep for potential error handling delays
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
    A simplified Kafka consumer wrapper using kafka-python.

    Handles subscription, message iteration, deserialization, and basic error handling.
    Iteration relies on the blocking nature of the kafka-python consumer.
    Closing the consumer is the way to stop iteration.
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
        # consumer_timeout_ms can be set to make the iterator time out
        # consumer_timeout_ms: int = -1, # Default is -1 (block indefinitely)
        **kwargs: Any,
    ):
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
            # consumer_timeout_ms: Timeout (ms) for the consumer iterator. Use -1 for indefinite blocking.
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
            "fetch_max_wait_ms": fetch_max_wait_ms,
            # Add consumer_timeout_ms if you want the iterator itself to time out
            # 'consumer_timeout_ms': kwargs.pop('consumer_timeout_ms', -1),
            **kwargs,
        }
        self.consumer: Optional[KafkaConsumer] = None

        try:
            logger.info(
                f"Initializing KafkaConsumer for group '{group_id}' on servers {bootstrap_servers}..."
            )
            # kafka-python subscribes to topics passed during initialization
            self.consumer = KafkaConsumer(*self.topics, **self.consumer_config)
            logger.info(f"KafkaConsumer subscribed to topics: {self.topics}")
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
            logger.error("Consumer not initialized. Cannot generate messages.")
            return # Or raise an exception

        logger.info("Starting message consumption loop (iterating over consumer)...")
        try:
            # Iterating the consumer blocks until fetch_max_wait_ms or a message arrives,
            # or indefinitely if consumer_timeout_ms is -1.
            # This loop continues until the consumer is closed or a critical error occurs.
            for msg in self.consumer:
                try:
                    # msg.key is already deserialized by key_deserializer
                    # msg.value is bytes, needs manual deserialization
                    value_bytes = msg.value
                    deserialized_value = deserialize_message(value_bytes)

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
                    yield None # Continue processing next message
                except Exception as e:
                    logger.exception(
                        f"Error processing message at {msg.topic}[{msg.partition}] offset {msg.offset}: {e}"
                    )
                    yield None # Continue processing next message

        except StopIteration:
            # This happens if consumer_timeout_ms is set and reached.
            # The generator naturally ends here. The caller needs to decide whether to re-iterate.
            logger.info("Consumer iteration finished (StopIteration, likely due to consumer_timeout_ms).")
        except KafkaError as e:
            # Log Kafka errors during consumption. Depending on the error, the consumer might be unusable.
            logger.error(f"KafkaError during consumption: {e}", exc_info=True)
            # Consider raising the error to signal a potentially fatal state
            # raise e
        except Exception as e:
            # Log unexpected errors.
            logger.exception(f"Unexpected error during consumer iteration: {e}")
            # Consider raising the error
            # raise e
        finally:
            # This block executes when the loop terminates (normally or via error/close)
            logger.info("Message consumption loop finished.")
            # Note: We don't automatically close the consumer here. Closing is managed externally or via context manager.


    def consume(self) -> Generator[Optional[Dict[str, Any]], None, None]:
        """
        Provides a generator to consume messages from the subscribed topics.

        Yields:
            A dictionary containing the message details ('topic', 'key', 'value', 'partition', 'offset', 'timestamp')
            or None if an error occurs during processing/deserialization.
            The generator stops when the consumer is closed or encounters a fatal error.
        """
        if self.consumer is None:
            logger.error("Cannot consume messages: consumer is not initialized.")
            # Decide behavior: raise error or yield nothing?
            # Option 1: Raise
            # raise RuntimeError("Consumer is not initialized.")
            # Option 2: Return an empty generator
            return
            # (The original code yielded None indefinitely, which might hide issues)

        # Yield directly from the internal generator
        yield from self._message_generator()


    def stop(self):
        """
        Stops the consumer by closing the underlying connection.
        This will cause the message generator loop to terminate.
        """
        logger.info("Stop requested for KafkaConsumerWrapper. Closing consumer.")
        self.close()

    def close(self):
        """Closes the Kafka consumer connection."""
        if self.consumer:
            logger.info("Closing Kafka consumer...")
            try:
                # Close will also trigger commit if auto-commit is enabled and needed
                self.consumer.close()
                logger.info("Kafka consumer closed successfully.")
                self.consumer = None # Mark as closed
            except Exception as e:
                logger.exception(f"Error closing Kafka consumer: {e}")
                # Ensure consumer is marked as None even if close fails
                self.consumer = None
        else:
            logger.info("Consumer already closed or not initialized.")

    def __iter__(self) -> Generator[Optional[Dict[str, Any]], None, None]:
        """Allows iterating over the consumer instance directly."""
        yield from self.consume()

    def __enter__(self):
        """Enter context manager."""
        if self.consumer is None:
             # Ensure consumer is initialized before entering context, or handle appropriately
             raise RuntimeError("Cannot enter context: Consumer failed to initialize.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager, ensuring consumer is closed."""
        self.close()
