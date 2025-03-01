"""
Base classes for market data validation.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Union

from src.data_collection import logger
from src.data_collection.models import MarketData


class BaseValidator(ABC):
    """Base class for all data validators."""
    
    def __init__(self):
        """Initialize the validator."""
        logger.info(f"Initialized {self.__class__.__name__}")
    
    @abstractmethod
    def validate(self, data: MarketData) -> Tuple[bool, Optional[str]]:
        """
        Validate a MarketData object.
        
        Args:
            data: The MarketData object to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        pass
    
    def validate_batch(self, data_batch: List[MarketData]) -> Dict[str, Union[List[MarketData], List[Tuple[MarketData, str]]]]:
        """
        Validate a batch of MarketData objects.
        
        Args:
            data_batch: List of MarketData objects to validate
            
        Returns:
            Dictionary with 'valid' and 'invalid' keys, where 'valid' contains valid MarketData objects
            and 'invalid' contains tuples of (MarketData, error_message)
        """
        valid = []
        invalid = []
        
        for data in data_batch:
            is_valid, error = self.validate(data)
            if is_valid:
                valid.append(data)
            else:
                invalid.append((data, error))
                logger.warning(f"Validation failed for {data.symbol}: {error}")
        
        return {
            'valid': valid,
            'invalid': invalid
        } 