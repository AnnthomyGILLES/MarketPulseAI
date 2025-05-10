import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

from loguru import logger

from src.common.validation import BaseValidationService
from src.data_collection.social_media.reddit.validation.schema import (
    RedditPost,
    RedditComment,
    ValidatedRedditItem,
)
from src.data_collection.social_media.reddit.validation.validator import (
    RedditDataValidator,
)

class RedditValidationService(BaseValidationService):
    """
    Kafka consumer service for validating and enriching Reddit data.

    Consumes from raw Reddit topics, validates using the RedditDataValidator,
    and produces results to downstream topics (validated, invalid, error).
    """

    # Define default topic keys (can be overridden by kafka_config.yaml)
    DEFAULT_INPUT_TOPICS = ["social_media_reddit_posts", "social_media_reddit_comments"]
    DEFAULT_CONSUMER_GROUP = "reddit_validation"
    DEFAULT_VALID_TOPIC = "social_media_reddit_validated"
    DEFAULT_VALID_POST_TOPIC = "social_media_reddit_validated"
    DEFAULT_VALID_COMMENT_TOPIC = "social_media_reddit_comments_validated"
    DEFAULT_VALID_SYMBOL_TOPIC = "social_media_reddit_symbols_validated"
    DEFAULT_INVALID_TOPIC = "social_media_reddit_invalid"
    DEFAULT_ERROR_TOPIC = "social_media_reddit_error"

    DEFAULT_CONFIG_PATH = (
        Path(__file__).resolve().parents[5] / "config" / "kafka" / "kafka_config.yaml"
    )

    def __init__(self, config_path: Optional[str] = None):
        """Initialize the service, loading configuration and setting up components."""
        # Initialize validator first as it's needed for the base class
        self.validator = RedditDataValidator()
        
        # Load any additional configuration here if needed before calling super().__init__
        # self.custom_config = ...
        
        # Call the base class __init__ with all required parameters
        super().__init__(
            service_name="RedditValidationService",
            validator=self.validator,
            input_topics_config_keys=self.DEFAULT_INPUT_TOPICS,
            consumer_group_config_key=self.DEFAULT_CONSUMER_GROUP,
            valid_topic_config_key=self.DEFAULT_VALID_TOPIC,
            invalid_topic_config_key=self.DEFAULT_INVALID_TOPIC,
            error_topic_config_key=self.DEFAULT_ERROR_TOPIC,
            config_path=config_path or self.DEFAULT_CONFIG_PATH,
        )
        
        # Store additional topic names needed for routing
        self.post_topic = self.config["topics"].get(self.DEFAULT_VALID_POST_TOPIC)
        self.comment_topic = self.config["topics"].get(self.DEFAULT_VALID_COMMENT_TOPIC)
        self.symbol_topic = self.config["topics"].get(self.DEFAULT_VALID_SYMBOL_TOPIC)

    def _determine_target_producer_and_topic(
        self, validated_model: ValidatedRedditItem, source_topic: str
    ) -> tuple[Optional[str], Optional[str]]:
        """
        Determines the appropriate producer and topic based on the validated data and source.
        
        Args:
            validated_model: The validated Reddit item (post or comment)
            source_topic: The original Kafka topic the message came from
            
        Returns:
            Tuple of (producer_instance, target_topic) or (None, None) if no valid target
        """
        # Start with the default valid producer and topic
        producer = self.valid_producer
        topic = self.valid_topic
        
        if validated_model.detected_symbols:
            if self.symbol_topic:
                topic = self.symbol_topic
                logger.debug(f"Routing item with symbols {validated_model.detected_symbols} to symbol topic")
                
        elif isinstance(validated_model, RedditPost):
            # Check if it came from the posts topic initially
            posts_topic = self.config["topics"]["social_media_reddit_posts"]
            if source_topic == posts_topic and self.post_topic:
                topic = self.post_topic
                logger.debug(f"Routing post {validated_model.id} to posts topic")
            else:
                logger.warning(
                    f"Post {validated_model.id} received from non-post topic '{source_topic}'. Using default topic."
                )
                
        elif isinstance(validated_model, RedditComment):
            comments_topic = self.config["topics"]["social_media_reddit_comments"]
            if source_topic == comments_topic and self.comment_topic:
                topic = self.comment_topic
                logger.debug(f"Routing comment {validated_model.id} to comments topic")
            else:
                logger.warning(
                    f"Comment {validated_model.id} received from non-comment topic '{source_topic}'. Using default topic."
                )
        else:
            logger.error(
                f"Cannot determine target for unknown validated model type: {type(validated_model)}"
            )
            return None, None
            
        return producer, topic

    def process_message(self, raw_message: Dict[str, Any]) -> None:
        """
        Override the base process_message to handle Reddit-specific routing of validated messages.
        """
        message_value = raw_message.get("value")
        source_topic = raw_message.get("topic", "unknown")
        message_offset = raw_message.get("offset", -1)
        
        item_id = self._get_message_key(
            message_value if isinstance(message_value, dict) else {}
        ) or "UNKNOWN_ID"

        log_context = {
            "topic": source_topic,
            "offset": message_offset,
            "item_id": item_id,
        }
        log_prefix = f"[{self.service_name}]"

        logger.debug(
            f"{log_prefix} Received message | Context: {log_context}"
        )
        
        if not isinstance(message_value, dict):
            err_msg = f"Message value is not a dictionary (Type: {type(message_value)})"
            logger.error(f"{log_prefix} {err_msg} | Context: {log_context}")
            self._send_to_producer(
                self.error_producer,
                self.error_topic,
                {
                    "original_payload": message_value,
                    "error": err_msg,
                    "context": log_context,
                },
                f"error_{item_id}",
                log_context,
                "error",
            )
            self._update_and_report_stats(is_error=True)
            return

        try:
            logger.debug(f"{log_prefix} Attempting validation | Context: {log_context}")
            is_valid, validated_data, validation_errors = self.validator.validate(
                message_value
            )

            if is_valid and validated_data is not None:
                # This is where we handle Reddit-specific routing
                producer, target_topic = self._determine_target_producer_and_topic(
                    validated_data, source_topic
                )
                
                if producer and target_topic:
                    logger.debug(f"{log_prefix} Validation SUCCEEDED | Type: {validated_data.content_type} | Context: {log_context}")
                    validated_key = self._get_validated_message_key(validated_data) or f"valid_{item_id}"
                    
                    # If symbols were detected, send one message per symbol
                    if hasattr(validated_data, "detected_symbols") and validated_data.detected_symbols:
                        symbols_sent = 0
                        validated_data_dict = validated_data.model_dump(mode="json")
                        
                        for symbol in validated_data.detected_symbols:
                            symbol_data = validated_data_dict.copy()
                            symbol_data["symbol"] = symbol
                            success = self._send_to_producer(
                                producer,
                                target_topic,
                                symbol_data,
                                f"symbol_{symbol}_{validated_key}",
                                log_context,
                                "symbol"
                            )
                            if success:
                                symbols_sent += 1
                                
                        logger.debug(f"{log_prefix} Sent {symbols_sent} symbol messages for {validated_data.id}")
                        self._update_and_report_stats(is_valid=True)
                        
                    else:
                        # Standard message send
                        send_success = self._send_to_producer(
                            producer,
                            target_topic,
                            validated_data,
                            validated_key,
                            log_context,
                            "valid"
                        )
                        self._update_and_report_stats(is_valid=True, is_error=not send_success)
                else:
                    logger.error(f"{log_prefix} No valid producer/topic determined for message. Context: {log_context}")
                    self._send_to_producer(
                        self.error_producer,
                        self.error_topic,
                        {
                            "original_payload": validated_data,
                            "error": "Failed to determine target producer/topic",
                            "context": log_context,
                        },
                        f"error_{item_id}",
                        log_context,
                        "error",
                    )
                    self._update_and_report_stats(is_error=True)
            else:
                # Invalid message handling - use the base class approach
                logger.warning(
                    f"{log_prefix} Validation FAILED | Errors: {validation_errors} | Context: {log_context}"
                )
                invalid_payload = {
                    "original_message": message_value,
                    "validation_errors": validation_errors,
                    "processing_timestamp": datetime.now(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z"),
                    "log_context": log_context,
                }
                invalid_key = f"invalid_{item_id}"
                send_success = self._send_to_producer(
                    self.invalid_producer,
                    self.invalid_topic,
                    invalid_payload,
                    invalid_key,
                    log_context,
                    "invalid",
                )
                self._update_and_report_stats(
                    is_valid=False, is_error=not send_success
                )
                
        except Exception as e:
            # Error handling - use the base class approach
            error_msg = f"Unexpected error processing message: {str(e)}"
            logger.exception(f"{log_prefix} {error_msg} | Context: {log_context}")
            error_payload = {
                "original_message": message_value,
                "error": error_msg,
                "traceback": str(e),
                "processing_timestamp": datetime.now(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "log_context": log_context,
            }
            error_key = f"error_{item_id}"
            self._send_to_producer(
                self.error_producer,
                self.error_topic,
                error_payload,
                error_key,
                log_context,
                "error",
            )
            self._update_and_report_stats(is_error=True)

    def _get_message_key(self, data: Optional[Dict[str, Any]]) -> Optional[str]:
        """Extracts the 'id' field from the raw message."""
        if isinstance(data, dict):
            return data.get("id")
        return None

    def _get_validated_message_key(
        self, validated_data: ValidatedRedditItem
    ) -> Optional[str]:
        """Extracts the 'id' field from the validated Pydantic model."""
        return validated_data.id


if __name__ == "__main__":
    # Basic configuration for running standalone
    log_path = Path("logs") / "reddit_validation_service_{time}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)  # Ensure logs directory exists
    logger.add(
        str(log_path),
        rotation="100 MB",
        retention="10 days",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
    )

    service_instance = None
    try:
        service_instance = RedditValidationService()  # Uses default config path
        service_instance.run()
    except Exception as e:
        logger.exception(
            f"Failed to initialize or run the RedditValidationService: {e}"
        )
        if service_instance and service_instance.running:
            service_instance.stop()  # Attempt cleanup even on init/run failure
    finally:
        logger.info("RedditValidationService standalone execution finished.")
