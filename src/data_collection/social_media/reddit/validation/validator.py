import json
from typing import Dict, Any, List, Tuple, Optional

from loguru import logger
from pydantic import ValidationError

from .rules import RedditValidationRules
from .schema import RedditPost, RedditComment, ValidatedRedditItem


class RedditDataValidator:
    """
    Validator for Reddit data (Posts and Comments) using Pydantic schemas and business rules.
    """

    def __init__(self):
        """Initialize the validator."""
        self.rules = RedditValidationRules()
        logger.info("RedditDataValidator initialized.")

    def validate(
        self, data: Dict[str, Any]
    ) -> Tuple[bool, Optional[ValidatedRedditItem], List[str]]:
        """
        Validate raw Reddit data against schema and business rules.

        Args:
            data: Dictionary containing raw Reddit data.

        Returns:
            Tuple containing:
                - bool: True if data is valid, False otherwise.
                - Optional[ValidatedRedditItem]: The validated Pydantic model instance if valid, else None.
                - List[str]: A list of validation error messages if invalid.
        """
        validation_errors = []
        validated_model: Optional[ValidatedRedditItem] = None
        content_type = data.get("content_type")
        item_id = data.get("id", "UNKNOWN_ID")

        # Step 1: Schema Validation (and type determination)
        try:
            if content_type == "post":
                validated_model = RedditPost(**data)
                logger.debug(f"Schema validation passed for post {item_id}")
            elif content_type == "comment":
                validated_model = RedditComment(**data)
                logger.debug(f"Schema validation passed for comment {item_id}")
            else:
                validation_errors.append(
                    f"Schema error: Invalid or missing 'content_type': {content_type}"
                )
                logger.warning(
                    f"Schema validation failed for item {item_id}: Invalid content_type '{content_type}'"
                )
                return False, None, validation_errors

        except ValidationError as e:
            for error in e.errors():
                field = ".".join(map(str, error["loc"])) if error["loc"] else "general"
                validation_errors.append(f"Schema error: Field '{field}': {error['msg']}")
            logger.warning(
                f"Schema validation failed for {content_type or 'item'} {item_id}. "
                f"Errors: {validation_errors}"
            )
            # Return original data? No, return None for the model if schema fails.
            return False, None, validation_errors
        except Exception as e:
            # Catch unexpected errors during Pydantic validation
            error_msg = f"Unexpected schema validation error: {str(e)}"
            validation_errors.append(error_msg)
            logger.exception(
                f"Unexpected schema validation error for {content_type or 'item'} {item_id}"
            )
            return False, None, validation_errors

        # Step 2: Business Rule Validation (only if schema validation passed)
        rule_validations = []
        if validated_model:
            # Common rules for both types
            rule_validations.append(self.rules.check_timestamp_consistency(validated_model))

            # Type-specific rules
            if isinstance(validated_model, RedditPost):
                 rule_validations.append(self.rules.check_post_quality(validated_model))
            elif isinstance(validated_model, RedditComment):
                rule_validations.append(self.rules.check_comment_quality(validated_model))

            # Collect rule validation errors
            for is_valid, error_message in rule_validations:
                if not is_valid and error_message:
                    # Prefix to distinguish from schema errors
                    validation_errors.append(f"Business rule error: {error_message}")

        # Final Decision
        is_data_valid = len(validation_errors) == 0
        if is_data_valid:
            logger.debug(f"All validations passed for {content_type} {item_id}")
            return True, validated_model, []
        else:
            logger.warning(
                f"Validation failed for {content_type} {item_id}. Errors: {validation_errors}"
            )
            # Even if only business rules failed, we return False and no validated model for consistency.
            # The consumer service can decide how to handle rule failures vs schema failures if needed (e.g., different logging/metrics)
            return False, None, validation_errors 