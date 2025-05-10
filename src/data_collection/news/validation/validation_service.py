"""
Kafka-based validation service for news data.
"""

from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger

from src.common.validation import BaseValidationService
from src.data_collection.news.validation.news_validator import NewsDataValidator


class NewsValidationService(BaseValidationService):
    """
    Service that consumes raw news data from Kafka, validates it,
    and produces validated and invalid records to separate topics.
    """

    # Define default topic keys for Kafka
    DEFAULT_INPUT_TOPICS = ["news_data_raw"]
    DEFAULT_CONSUMER_GROUP = "news_validation"
    DEFAULT_VALID_TOPIC = "news_data_validated"
    DEFAULT_INVALID_TOPIC = "news_data_invalid"
    DEFAULT_ERROR_TOPIC = "news_data_error"

    DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[4] / "config" / "kafka" / "kafka_config.yaml"

    def __init__(
        self,
        config_path: Optional[str] = None,
        service_name: str = "NewsValidationService",
    ):
        """
        Initialize the validation service.

        Args:
            config_path: Path to the Kafka config file
            service_name: Name identifier for the service
        """
        # Initialize validator which is needed for the base class
        self.validator = NewsDataValidator()
        
        # Call the base class __init__ with required parameters
        super().__init__(
            service_name=service_name,
            validator=self.validator,
            input_topics_config_keys=self.DEFAULT_INPUT_TOPICS,
            consumer_group_config_key=self.DEFAULT_CONSUMER_GROUP,
            valid_topic_config_key=self.DEFAULT_VALID_TOPIC,
            invalid_topic_config_key=self.DEFAULT_INVALID_TOPIC,
            error_topic_config_key=self.DEFAULT_ERROR_TOPIC,
            config_path=config_path or self.DEFAULT_CONFIG_PATH,
        )
        
        logger.info(f"{self.service_name} initialized")

    def _get_message_key(self, data: Dict[str, Any]) -> Optional[str]:
        """Extracts a unique identifier from the raw message."""
        if isinstance(data, dict):
            # Use URL as a unique identifier if available, otherwise use timestamp or generate a key
            return data.get("url") or data.get("id") or f"news_{data.get('publishedAt', '')}"
        return None

    def _get_validated_message_key(self, validated_data: Any) -> Optional[str]:
        """Extracts a key for the validated message."""
        # If validated_data is a Pydantic model
        if hasattr(validated_data, "url"):
            return validated_data.url
        elif hasattr(validated_data, "id"):
            return validated_data.id
            
        # If it's a dictionary
        if isinstance(validated_data, dict):
            return validated_data.get("url") or validated_data.get("id")
            
        return None


if __name__ == "__main__":
    # Set up logger
    logger.add(
        "logs/validation_service_{time}.log",
        rotation="500 MB",
        retention="10 days",
        level="INFO",
    )

    # Resolve paths for configuration files using Pathlib
    base_path = Path(__file__).resolve().parents[4]  # Adjusting to get the base directory
    kafka_config_path = base_path / "config/kafka/kafka_config.yaml"

    # Initialize the validation service
    validation_service = NewsValidationService(config_path=str(kafka_config_path))
    
    try:
        validation_service.run()
    except KeyboardInterrupt:
        logger.info("Validation service stopped by user") 