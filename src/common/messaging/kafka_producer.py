import json
import time
from pathlib import Path
from typing import Any, Dict, Union, Optional, List

# Use kafka-python as in the original file
from kafka import KafkaProducer
from kafka.errors import KafkaError
from loguru import logger

# Assuming these utilities exist and work as expected
from src.common.messaging.serializers import serialize_message


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
    ) -> bool:
        """
        Sends a single message to a specified Kafka topic using the configured producer.

        Relies on the KafkaProducer's internal retry mechanism ('retries' parameter during initialization).

        Args:
            topic: The target Kafka topic.
            value: The message value (dictionary, will be serialized).
            key: The message key (optional, will be serialized).

        Returns:
            True if the message was successfully accepted into the producer's buffer for sending,
            False if an error occurred during the initial send attempt.
            Note: True does not guarantee delivery; inspect the Future object returned by
                  producer.send() or use callbacks for confirmation if needed.
        """
        try:
            logger.debug(f"Attempting to send message to topic '{topic}' (Key: {key})")
            # Send the message - kafka-python handles serialization and configured retries.
            # The send() call is asynchronous and returns a Future.
            # We are not waiting for the future here, just checking for immediate errors.
            self.producer.send(topic=topic, value=value, key=key)
            # Optional: To wait for acknowledgment, you could do:
            # future = self.producer.send(topic=topic, value=value, key=key)
            # record_metadata = future.get(timeout=10) # Raises KafkaTimeoutError on timeout
            # logger.debug(f"Message sent and acknowledged: {record_metadata.topic}...")
            logger.debug(f"Message for topic '{topic}' accepted by producer buffer.")
            return True
        except KafkaError as e:
            # Errors that might occur immediately (e.g., serialization errors, certain connection issues)
            logger.error(
                f"KafkaError occurred immediately when trying to send to topic '{topic}'. Error: {e}"
            )
            return False
        except Exception as e:
            # Catch other potential immediate exceptions
            logger.exception(
                f"Unexpected error occurred immediately when trying to send to topic '{topic}': {e}"
            )
            return False

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
                    if not line.strip():
                        continue
                    try:
                        obj = json.loads(line)
                        if not isinstance(obj, dict):
                            logger.warning(
                                f"Skipping non-dictionary object in {file_path}:{line_num}"
                            )
                            continue
                        success = self.send_message(topic=topic, value=obj)
                        if success:
                            sent_count += 1
                        else:
                            logger.error(
                                f"Failed to send object from {file_path}:{line_num}. See previous errors."
                            )
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
            logger.error(f"Error during producer flush: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error during producer flush: {e}")

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
            logger.error(f"Error closing Kafka producer: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error during producer close: {e}")

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager, ensuring producer is closed."""
        self.close()


# Example Usage
if __name__ == "__main__":
    import sys
    import os
    import tempfile

    # Configure Loguru for console output
    logger.remove()  # Remove default handler
    logger.add(sys.stderr, level="DEBUG")

    KAFKA_BROKERS = os.getenv("KAFKA_BROKERS")
    TEST_TOPIC = "my-test-topic"

    logger.info(f"Attempting to connect to Kafka brokers at {KAFKA_BROKERS}")

    try:
        # Use context manager to ensure close() is called
        with KafkaProducerWrapper(bootstrap_servers=KAFKA_BROKERS) as producer:
            logger.info("KafkaProducerWrapper instantiated successfully.")

            # 1. Send a single message
            test_message = {"id": 1, "payload": "Hello Kafka!"}
            logger.info(
                f"Sending single message to topic '{TEST_TOPIC}': {test_message}"
            )
            success = producer.send_message(
                topic=TEST_TOPIC, value=test_message, key="message1"
            )
            if success:
                logger.info("Single message accepted by producer buffer.")
            else:
                logger.warning(
                    "Failed to send single message (check broker availability/config)."
                )

            # Allow some time for the message to potentially be sent before the next step
            time.sleep(1)

            # 2. Send messages from a JSON Lines stream
            logger.info("Creating temporary JSON Lines file for stream test...")
            # Create a temporary file
            with tempfile.NamedTemporaryFile(
                mode="w+", suffix=".jsonl", delete=False
            ) as temp_file:
                temp_file_path = temp_file.name
                json_objects = [
                    {"id": 10, "data": "stream_data_1", "timestamp": time.time()},
                    {"id": 11, "data": "stream_data_2", "timestamp": time.time()},
                    {"invalid": "this line is not a dict"},  # Test invalid line
                    json.dumps(
                        {"id": 12, "data": "stream_data_3", "timestamp": time.time()}
                    )
                    + "\n",  # Already string
                ]
                for item in json_objects:
                    if isinstance(item, dict):
                        # Use the same serializer as the producer might use internally for consistency demo
                        serialized_line = producer.producer.config["value_serializer"](
                            item
                        )
                        temp_file.write(
                            serialized_line.decode("utf-8") + "\n"
                        )  # Write serialized string
                    else:
                        temp_file.write(
                            str(item) + "\n"
                        )  # Write non-dict/pre-string directly

                logger.info(f"Temporary file created: {temp_file_path}")

            logger.info(
                f"Sending JSON stream from '{temp_file_path}' to topic '{TEST_TOPIC}'..."
            )
            sent_count = producer.send_json_stream(
                file_path=temp_file_path, topic=TEST_TOPIC
            )
            logger.info(f"Finished sending stream. {sent_count} messages sent/queued.")

            # Clean up the temporary file
            try:
                os.remove(temp_file_path)
                logger.info(f"Temporary file '{temp_file_path}' removed.")
            except OSError as e:
                logger.error(f"Error removing temporary file '{temp_file_path}': {e}")

            # The producer will be flushed and closed automatically by the __exit__ method here
            logger.info("Exiting 'with' block, producer will be flushed and closed.")

    except KafkaError as e:
        logger.error(
            f"Kafka connection or setup error: {e}. Ensure Kafka is running at {KAFKA_BROKERS}"
        )
    except Exception as e:
        logger.exception(
            f"An unexpected error occurred during the example execution: {e}"
        )

    logger.info("Example script finished.")
