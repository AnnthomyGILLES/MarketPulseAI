# src/data_collection/market_data/validation/validation_service.py

from pathlib import Path
from typing import Dict, Any, Optional

# Import BaseValidationService and MarketDataValidator
from src.common.validation import BaseValidationService
from src.data_collection.market_data.validation.market_data_validator import (
    MarketDataValidator,
)

# Removed imports handled by base class: time, traceback, KafkaConsumerWrapper, KafkaProducerWrapper, load_config, logger (base class configures it)


# Inherit from BaseValidationService
class MarketDataValidationService(BaseValidationService):
    """
    Service that consumes raw market data from Kafka, validates it using MarketDataValidator,
    and produces results to downstream Kafka topics. Inherits from BaseValidationService.
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the validation service using the base class constructor.

        Args:
            config_path: Optional path to the Kafka configuration file.
        """
        # Define Kafka topic keys based on the existing service's usage
        # The original service sent both invalid and error data to 'market_data_error'.
        # We map both invalid_topic_config_key and error_topic_config_key to it.
        # If separate topics are desired later, the kafka_config.yaml can be updated.
        input_topics = ["market_data_raw"]
        consumer_group = "market_data_validation"
        valid_topic = "market_data_validated"
        invalid_output_topic = "market_data_error" # Where failed validations go
        error_output_topic = "market_data_error"   # Where processing errors go

        # Instantiate the specific validator
        validator = MarketDataValidator()

        # Call the base class __init__
        super().__init__(
            service_name="MarketDataValidationService",
            validator=validator,
            input_topics_config_keys=input_topics,
            consumer_group_config_key=consumer_group,
            valid_topic_config_key=valid_topic,
            invalid_topic_config_key=invalid_output_topic,
            error_topic_config_key=error_output_topic,
            config_path=config_path,
        )

    def _get_message_key(self, data: Optional[Dict[str, Any]]) -> Optional[str]:
        """Extracts the 'symbol' as the message key if available."""
        if isinstance(data, dict):
            return data.get("symbol")
        return None

    def _get_validated_message_key(self, validated_data: Dict[str, Any]) -> Optional[str]:
        """Extracts the 'symbol' from the validated data dictionary as the key."""
        # validated_data is the dict returned by MarketDataValidator on success
        return validated_data.get("symbol")

    # Methods removed as they are handled by BaseValidationService:
    # - _setup_kafka_clients
    # - _kafka_error_callback (using base implementation)
    # - _update_and_report_stats / _report_stats
    # - process_message
    # - run
    # - stop
    # - _handle_signal
    # - _setup_logging


if __name__ == "__main__":
    # Use the base class's run_service method for standalone execution
    # This automatically handles initialization and the run loop.
    # It assumes the config file is in the default location or specified via an arg parser (not implemented here)
    MarketDataValidationService.run_service()

    # The old way:
    # validation_service = MarketDataValidationService()
    # try:
    #     validation_service.run()
    # except KeyboardInterrupt:
    #     print("Service interrupted by user. Shutting down...")
    # # Base class handles stop in its finally block
