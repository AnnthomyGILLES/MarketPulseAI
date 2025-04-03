# src/common/messaging/kafka_producer.py

import json
import time
from pathlib import Path
from typing import Any, Dict, Union, Optional, List

# Use kafka-python as in the original file
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

# Assuming these utilities exist and work as expected
from src.common.messaging.serializers import serialize_message  #
from src.utils.logging import get_logger  # Use the central logging utility

# Get logger for this module
logger = get_logger(__name__)


class KafkaProducerWrapper:
    """
    A Kafka producer wrapper using kafka-python.

    Handles message serialization, optional key encoding, and basic retry logic.
    Producers are generally created once and used for multiple topics.
    """

    def __init__(
        self,
        bootstrap_servers: Union[str, List[str]],
        client_id: Optional[str] = None,
        value_serializer: callable = serialize_message,
        key_serializer: Optional[callable] = lambda k: k.encode("utf-8")
        if isinstance(k, str)
        else k,
        acks: Union[int, str] = 1,
        retries: int = 3,  # Default retries for producer config
        **kwargs,
    ):  # Pass other kafka-python KafkaProducer options
        """
        Initializes the KafkaProducerWrapper.

        Args:
            bootstrap_servers: List of broker addresses (e.g., ['host1:9092', 'host2:9092']) or comma-separated string.
            client_id: An optional identifier for the producer connection.
            value_serializer: Callable to serialize message values (defaults to custom JSON serializer).
            key_serializer: Callable to serialize message keys (defaults to UTF-8 encoding for strings).
            acks: The number of acknowledgments required from brokers (0, 1, 'all').
            retries: Number of times to retry sending a failing message batch.
            **kwargs: Additional arguments passed directly to kafka.KafkaProducer.
                      See kafka-python documentation for options like `linger_ms`, `batch_size`, etc.
        """
        self.bootstrap_servers = bootstrap_servers
        producer_config = {
            "bootstrap_servers": bootstrap_servers,
            "client_id": client_id,
            "value_serializer": value_serializer,
            "key_serializer": key_serializer,
            "acks": acks,
            "retries": retries,
            **kwargs,  # Merge additional options
        }
        try:
            self.producer = KafkaProducer(**producer_config)
            logger.info(
                f"KafkaProducer initialized. Servers: {bootstrap_servers}, Client ID: {client_id}"
            )
        except KafkaError as e:
            logger.exception(f"Failed to initialize KafkaProducer: {e}")
            raise

    def send_message(
        self,
        topic: str,
        value: Dict[str, Any],
        key: Optional[Any] = None,
        send_retries: int = 2,  # Retries specific to this send call
        retry_delay_secs: float = 0.5,
    ) -> bool:
        """
        Sends a single message to a specified Kafka topic.

        Args:
            topic: The target Kafka topic.
            value: The message value (dictionary, will be serialized).
            key: The message key (optional, will be serialized).
            send_retries: Number of attempts for this specific send operation.
            retry_delay_secs: Delay between retries in seconds.

        Returns:
            True if the message was successfully sent (queued by the producer), False otherwise.
            Note: This does not guarantee delivery; use callbacks for confirmation if needed.
        """
        future = None
        for attempt in range(send_retries + 1):
            try:
                logger.debug(
                    f"Attempt {attempt + 1}: Sending message to topic '{topic}' (Key: {key})"
                )
                # Send the message - kafka-python handles serialization via configured serializers
                future = self.producer.send(topic=topic, value=value, key=key)
                # Optional: Wait for send confirmation (can impact performance)
                # record_metadata = future.get(timeout=10) # Waits for ack based on 'acks' config
                # logger.debug(f"Message sent: {record_metadata.topic} [{record_metadata.partition}] @ {record_metadata.offset}")
                return True  # Message accepted by producer buffer

            except KafkaTimeoutError as e:
                logger.warning(
                    f"Attempt {attempt + 1} failed: Timeout sending message to topic '{topic}'. Error: {e}"
                )
                if attempt < send_retries:
                    logger.info(f"Retrying in {retry_delay_secs} seconds...")
                    time.sleep(retry_delay_secs)
                else:
                    logger.error(
                        f"Failed to send message to topic '{topic}' after {send_retries + 1} attempts due to timeout."
                    )
                    return False
            except KafkaError as e:
                # Handle other potential Kafka errors during send
                logger.error(
                    f"Attempt {attempt + 1} failed: KafkaError sending message to topic '{topic}'. Error: {e}",
                    exc_info=True,
                )
                # Depending on the error, retry might not help, but we follow the retry logic
                if attempt < send_retries:
                    logger.info(f"Retrying in {retry_delay_secs} seconds...")
                    time.sleep(retry_delay_secs)
                else:
                    logger.error(
                        f"Failed to send message to topic '{topic}' after {send_retries + 1} attempts due to KafkaError."
                    )
                    return False
            except Exception as e:
                # Catch unexpected errors
                logger.exception(
                    f"Unexpected error sending message to topic '{topic}' on attempt {attempt + 1}: {e}"
                )
                # Stop retrying on unexpected errors
                return False
        return False  # Should not be reached if loop completes normally

    def send_json_stream(
        self, file_path: Union[str, Path], topic: str, send_retries: int = 2
    ) -> int:
        """
        Reads a JSON Lines file (one JSON object per line) and sends each object to Kafka.

        Args:
            file_path: Path to the JSON Lines file (.jsonl).
            topic: The target Kafka topic for all messages in the file.
            send_retries: Number of send attempts per message.

        Returns:
            The number of messages successfully sent (queued).
        """
        sent_count = 0
        line_num = 0
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                for line in file:
                    line_num += 1
                    if not line.strip():  # Skip empty lines
                        continue
                    try:
                        obj = json.loads(line)
                        if not isinstance(obj, dict):
                            logger.warning(
                                f"Skipping non-dictionary object in {file_path}:{line_num}"
                            )
                            continue
                        # Use the standard send_message method with retries
                        success = self.send_message(
                            topic=topic, value=obj, send_retries=send_retries
                        )
                        if success:
                            sent_count += 1
                        else:
                            logger.error(
                                f"Failed to send object from {file_path}:{line_num}. See previous errors."
                            )
                            # Decide whether to stop or continue on failure
                    except json.JSONDecodeError as e:
                        logger.error(
                            f"JSON decoding error in {file_path}:{line_num}: {e}"
                        )
                    except Exception as e:
                        logger.exception(
                            f"Error processing line {line_num} from {file_path}: {e}"
                        )
            logger.info(
                f"Finished sending stream from {file_path}. Sent {sent_count} messages to topic '{topic}'."
            )
        except FileNotFoundError:
            logger.error(f"File not found for JSON stream: {file_path}")
        except Exception as e:
            logger.exception(
                f"Error reading or sending JSON stream from {file_path}: {e}"
            )
        return sent_count

    def flush(self, timeout: Optional[float] = None):
        """
        Wait for all outstanding messages to be sent.

        Args:
            timeout: Maximum time in seconds to wait. Waits indefinitely if None.
        """
        try:
            logger.info(f"Flushing producer (Timeout: {timeout}s)...")
            self.producer.flush(timeout=timeout)
            logger.info("Producer flushed.")
        except KafkaError as e:
            logger.error(f"Error during producer flush: {e}", exc_info=True)

    def close(self, timeout: Optional[float] = 10.0):
        """
        Flush outstanding messages and close the Kafka producer.

        Args:
            timeout: Maximum time in seconds to wait for flush before closing.
        """
        logger.info("Closing Kafka producer...")
        try:
            self.flush(timeout=timeout)
            self.producer.close()
            logger.info("Kafka producer closed successfully.")
        except KafkaError as e:
            logger.error(f"Error closing Kafka producer: {e}", exc_info=True)
        except Exception as e:
            logger.exception(f"Unexpected error during producer close: {e}")

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager, ensuring producer is closed."""
        self.close()
