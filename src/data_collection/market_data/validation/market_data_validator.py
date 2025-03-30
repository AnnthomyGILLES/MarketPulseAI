import json
from typing import Dict, Any, List, Tuple

from loguru import logger
from pydantic import ValidationError

from src.data_collection.market_data.validation.validation_rules import ValidationRules
from src.data_collection.market_data.validation.validation_schema import (
    MarketDataSchema,
)


class MarketDataValidator:
    """
    Validator for market data to ensure quality before further processing.

    This class validates incoming market data against schema requirements
    and business rules, producing validation reports and filtering out invalid data.
    """

    def __init__(self):
        """Initialize the validator."""
        self.validation_rules = ValidationRules()
        self._symbol_last_data = {}  # Cache last valid data point per symbol

    def validate(self, data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], List[str]]:
        """
        Validate market data against schema and business rules.

        Args:
            data: Dictionary containing market data

        Returns:
            Tuple containing:
                - Boolean indicating if data is valid
                - Cleaned/normalized data if valid, original data if invalid
                - List of validation error messages if any
        """
        validation_errors = []

        # Step 1: Schema validation
        try:
            # Validate and normalize using Pydantic
            validated_data = MarketDataSchema(**data).model_dump()
            logger.debug(f"Schema validation passed for symbol {data.get('symbol')}")
        except ValidationError as e:
            # Extract validation error messages
            for error in e.errors():
                validation_errors.append(
                    f"Schema error: {error['loc'][0]} - {error['msg']}"
                )

            logger.warning(
                f"Schema validation failed for data: {json.dumps(data)[:200]}... "
                f"Errors: {validation_errors}"
            )
            return False, data, validation_errors

        # Step 2: Business rule validation
        rule_validations = [
            self.validation_rules.check_price_consistency(validated_data),
            self.validation_rules.check_timestamp_recency(validated_data),
            self.validation_rules.check_price_range(validated_data),
        ]

        # Add previous data check if available for this symbol
        symbol = validated_data.get("symbol")
        if symbol in self._symbol_last_data:
            rule_validations.append(
                self.validation_rules.check_for_sudden_change(
                    validated_data, self._symbol_last_data[symbol]
                )
            )

        # Collect rule validation errors
        for is_valid, error_message in rule_validations:
            if not is_valid:
                validation_errors.append(f"Business rule error: {error_message}")

        # If validation passes, update last data for this symbol
        if not validation_errors and symbol:
            self._symbol_last_data[symbol] = validated_data

        is_valid = len(validation_errors) == 0
        if is_valid:
            logger.debug(f"All validations passed for symbol {symbol}")
            return True, validated_data, []
        else:
            logger.warning(
                f"Business rule validation failed for symbol {symbol}. "
                f"Errors: {validation_errors}"
            )
            return False, data, validation_errors
