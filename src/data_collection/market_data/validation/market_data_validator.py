import json
from typing import Dict, Any, List, Tuple, Optional

from loguru import logger
from pydantic import ValidationError

# Import BaseValidator
from src.common.validation import BaseValidator
from src.data_collection.market_data.validation.validation_rules import ValidationRules
from src.data_collection.market_data.validation.validation_schema import (
    MarketDataSchema,
)


# Inherit from BaseValidator
class MarketDataValidator(BaseValidator):
    """
    Validator for market data to ensure quality before further processing.

    Inherits from BaseValidator and uses MarketDataSchema and ValidationRules.
    """

    def __init__(self):
        """Initialize the validator and the cache for previous data."""
        # Call super().__init__ for potential base class initialization logic
        super().__init__(validator_name="MarketDataValidator")
        self.validation_rules = ValidationRules()
        self._symbol_last_data: Dict[str, Dict[str, Any]] = {}  # Cache last valid data point per symbol

    # Ensure the signature matches BaseValidator.validate
    def validate(
        self, data: Dict[str, Any]
    ) -> Tuple[bool, Optional[Dict[str, Any]], List[str]]:
        """
        Validate market data against schema and business rules.

        Args:
            data: Dictionary containing market data

        Returns:
            Tuple containing:
                - bool: True if data is valid, False otherwise.
                - Optional[Dict[str, Any]]: Cleaned/normalized data dict if valid, else None.
                - List[str]: List of validation error messages if any.
        """
        validation_errors = []
        validated_data_dict: Optional[Dict[str, Any]] = None
        symbol = data.get("symbol", "UNKNOWN") # Get symbol early for logging

        # Step 1: Schema validation
        try:
            # Validate and normalize using Pydantic
            # .model_dump() returns a dict, which matches the Optional[Dict[str, Any]] return type
            validated_data_dict = MarketDataSchema(**data).model_dump()
            logger.debug(f"Schema validation passed for symbol {symbol}")
        except ValidationError as e:
            for error in e.errors():
                field = ".".join(map(str, error["loc"])) if error["loc"] else "general"
                validation_errors.append(
                    f"Schema error: Field '{field}': {error['msg']}"
                )
            logger.warning(
                f"Schema validation failed for symbol {symbol}: {validation_errors} "
                f"| Data: {json.dumps(data)[:200]}..."
            )
            # Return False, None (for validated data), and errors
            return False, None, validation_errors
        except Exception as e:
            # Catch unexpected errors during schema validation
            error_msg = f"Unexpected schema validation error: {str(e)}"
            validation_errors.append(error_msg)
            logger.exception(f"Unexpected schema validation error for symbol {symbol}")
            return False, None, validation_errors


        # Step 2: Business rule validation (only if schema validation passed and validated_data_dict is not None)
        # This block is guaranteed to run only if schema validation succeeded.
        rule_validations = [
            self.validation_rules.check_price_consistency(validated_data_dict),
            self.validation_rules.check_timestamp_recency(validated_data_dict),
            self.validation_rules.check_price_range(validated_data_dict),
        ]

        # Add previous data check if available for this symbol
        current_symbol = validated_data_dict.get("symbol") # Should always exist after schema validation
        if current_symbol and current_symbol in self._symbol_last_data:
            rule_validations.append(
                self.validation_rules.check_for_sudden_change(
                    validated_data_dict, self._symbol_last_data[current_symbol]
                )
            )

        # Collect rule validation errors
        for is_rule_valid, error_message in rule_validations:
            if not is_rule_valid and error_message:
                validation_errors.append(f"Business rule error: {error_message}")

        # If all validations (schema + rules) pass, update last data for this symbol
        is_data_valid = len(validation_errors) == 0
        if is_data_valid:
             if current_symbol:
                self._symbol_last_data[current_symbol] = validated_data_dict
             logger.debug(f"All validations passed for symbol {current_symbol}")
             # Return True, the validated data dict, and empty error list
             return True, validated_data_dict, []
        else:
            logger.warning(
                f"Business rule validation failed for symbol {current_symbol}. "
                f"Errors: {validation_errors}"
            )
            # Return False, None (as data is invalid), and the combined errors
            return False, None, validation_errors
