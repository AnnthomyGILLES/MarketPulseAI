import abc
from typing import Dict, Any, List, Tuple, Optional

from loguru import logger


class BaseValidator(abc.ABC):
    """
    Abstract base class for data validators.

    Defines the common interface for validating data records.
    Subclasses must implement the `validate` method.
    """

    def __init__(self, validator_name: Optional[str] = None):
        """
        Initialize the base validator.

        Args:
            validator_name: Optional name for logging purposes. Defaults to class name.
        """
        self.validator_name = validator_name or self.__class__.__name__
        logger.info(f"{self.validator_name} initialized.")

    @abc.abstractmethod
    def validate(
        self, data: Dict[str, Any]
    ) -> Tuple[bool, Optional[Any], List[str]]:
        """
        Validate a single data record.

        Args:
            data: The raw data dictionary to validate.

        Returns:
            A tuple containing:
            - bool: True if the data is valid according to schema and rules, False otherwise.
            - Optional[Any]: The validated and potentially transformed data object/dict if valid,
                             otherwise None. The specific type depends on the implementation.
            - List[str]: A list of validation error messages if the data is invalid.
                         Should be empty if the data is valid.
        """
        raise NotImplementedError 