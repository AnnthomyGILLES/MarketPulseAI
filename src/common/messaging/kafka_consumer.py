from typing import Any, Dict, Optional

from kafka import KafkaConsumer
from loguru import logger

from src.common.messaging.serializers import deserialize_message


class KafkaConsumerWrapper:
    """A Kafka consumer for receiving messages from a specified topic with JSON deserialization capability."""

    def __init__(self, bootstrap_servers: list, topic: str, **kwargs):
        self.topic = topic
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: deserialize_message(x),
            **kwargs,
        )
        logger.info(f"Kafka consumer initialized for topic: {self.topic}")

    def consume(self) -> Optional[Dict[str, Any]]:
        for message in self.consumer:
            document = {
                "value": message.value,
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "timestamp": message.timestamp,
            }
            logger.info(f"Received message: {document}")
            yield document

    def close(self):
        """Close the Kafka consumer."""
        self.consumer.close()
        logger.info("Kafka consumer closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


if __name__ == "__main__":
    with KafkaConsumerWrapper(["localhost:9093"], "qdrant_startups") as consumer:
        while True:
            message = consumer.consume()
            if message:
                print(message)
            else:
                break  # Exit if no more messages (for demonstration purposes)
