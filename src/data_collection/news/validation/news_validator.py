"""
Validator for news data from various sources.
"""

import json
import pendulum
from typing import Dict, Any, List, Tuple, Optional

from loguru import logger
from pydantic import ValidationError

from src.common.validation import BaseValidator
from src.data_collection.news.validation.schema import (
    NewsArticleSchema,
    ValidatedNewsArticle,
)
from src.data_collection.news.validation.validation_rules import NewsValidationRules


class NewsDataValidator(BaseValidator):
    """
    Validator for news article data to ensure quality before further processing.

    Inherits from BaseValidator and uses NewsArticleSchema and NewsValidationRules.
    """

    def __init__(self):
        """Initialize the validator."""
        super().__init__(validator_name="NewsDataValidator")
        self.validation_rules = NewsValidationRules()

    def validate(
        self, data: Dict[str, Any]
    ) -> Tuple[bool, Optional[ValidatedNewsArticle], List[str]]:
        """
        Validate news article data against schema and business rules.

        Args:
            data: Dictionary containing news article data

        Returns:
            Tuple containing:
                - bool: True if data is valid, False otherwise
                - Optional[ValidatedNewsArticle]: Validated model if valid, else None
                - List[str]: List of validation error messages if any
        """
        validation_errors = []
        validated_model: Optional[ValidatedNewsArticle] = None
        article_id = data.get("article_id") or data.get("url", "UNKNOWN")

        # Step 1: Schema validation
        try:
            # First validate with base schema
            base_validated = NewsArticleSchema(**data)

            # Then create the full validated model with additional fields
            validated_model = ValidatedNewsArticle(
                **base_validated.model_dump(),
                validated_at=pendulum.now().to_iso8601_string(),
            )

            logger.debug(f"Schema validation passed for article {article_id}")

        except ValidationError as e:
            for error in e.errors():
                field = ".".join(map(str, error["loc"])) if error["loc"] else "general"
                validation_errors.append(
                    f"Schema error: Field '{field}': {error['msg']}"
                )

            logger.warning(
                f"Schema validation failed for article {article_id}: {validation_errors} | "
                f"Data: {json.dumps(data)[:200]}..."
            )
            return False, None, validation_errors

        except Exception as e:
            error_msg = f"Unexpected schema validation error: {str(e)}"
            validation_errors.append(error_msg)
            logger.exception(
                f"Unexpected schema validation error for article {article_id}"
            )
            return False, None, validation_errors

        # Step 2: Business rule validation
        # This block runs only if schema validation passed
        rule_validations = [
            self.validation_rules.check_title_quality(validated_model.model_dump()),
            self.validation_rules.check_content_quality(validated_model.model_dump()),
            self.validation_rules.check_timestamp_recency(validated_model.model_dump()),
        ]

        # Collect rule validation errors
        for is_rule_valid, error_message in rule_validations:
            if not is_rule_valid and error_message:
                validation_errors.append(f"Business rule error: {error_message}")

        # Final decision
        is_data_valid = len(validation_errors) == 0
        if is_data_valid:
            logger.debug(f"All validations passed for article {article_id}")
            return True, validated_model, []
        else:
            logger.warning(
                f"Business rule validation failed for article {article_id}. "
                f"Errors: {validation_errors}"
            )
            return False, None, validation_errors