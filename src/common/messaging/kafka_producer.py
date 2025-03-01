import json
from pathlib import Path
from typing import Any, Dict, Union

from kafka import KafkaProducer
from kafka.errors import KafkaError
from loguru import logger

from common.kafka_utils.serializers import serialize_message


class KafkaProducerWrapper:
    """A Kafka producer for sending messages to a specified topic with JSON streaming capability."""

    def __init__(self, bootstrap_servers: list, topic: str, **kwargs):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: serialize_message(v),
            **kwargs,
        )
        logger.info(f"Kafka producer initialized for topic: {self.topic}")

    def send_message(self, message: Dict[str, Any], retries: int = 3) -> bool:
        for attempt in range(retries):
            try:
                self.producer.send(self.topic, message)
                self.producer.flush()
                logger.info("Message sent successfully")
                return True
            except KafkaError as e:
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == retries - 1:
                    logger.error(
                        f"Failed to send message after {retries} attempts",
                        exc_info=True,
                    )
                    return False
        return False

    def send_json_stream(self, file_path: Union[str, Path], retries: int = 3) -> None:
        with open(file_path, "r") as file:
            for line in file:
                obj = json.loads(line)
                success = self.send_message(obj, retries)
                if not success:
                    logger.error(f"Failed to send object: {obj}")

    def close(self):
        """Close the Kafka producer."""
        self.producer.flush()
        self.producer.close()
        logger.info("Kafka producer closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


if __name__ == "__main__":
    input_json = (
        Path().cwd().parent.parent / "data" / "raw" / "startup_demo_sample.jsonl"
    )
    with KafkaProducerWrapper(["localhost:9093"], "qdrant_startups") as producer:
        producer.send_json_stream(input_json)
        print("JSON file processed and sent to Kafka")
