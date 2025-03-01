"""
Validators for market data.
"""

from typing import Optional, Tuple

from src.data_collection import logger
from src.data_collection.market_data.validation import BaseValidator
from src.data_collection.models import MarketData


class MarketDataValidator(BaseValidator):
    """Validates market data."""
    
    def __init__(self, max_price: float = 100000.0, min_price: float = 0.0):
        """
        Initialize the market data validator.
        
        Args:
            max_price: Maximum allowed price
            min_price: Minimum allowed price
        """
        super().__init__()
        self.max_price = max_price
        self.min_price = min_price
    
    def validate(self, data: MarketData) -> Tuple[bool, Optional[str]]:
        """
        Validate a MarketData object.
        
        Args:
            data: The MarketData object to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check for missing required fields
        if not data.symbol:
            return False, "Symbol is required"
        
        if data.timestamp <= 0:
            return False, "Invalid timestamp"
        
        # Validate price ranges
        if not (self.min_price <= data.open_price <= self.max_price):
            return False, f"Open price {data.open_price} out of range [{self.min_price}, {self.max_price}]"
        
        if not (self.min_price <= data.high_price <= self.max_price):
            return False, f"High price {data.high_price} out of range [{self.min_price}, {self.max_price}]"
        
        if not (self.min_price <= data.low_price <= self.max_price):
            return False, f"Low price {data.low_price} out of range [{self.min_price}, {self.max_price}]"
        
        if not (self.min_price <= data.close_price <= self.max_price):
            return False, f"Close price {data.close_price} out of range [{self.min_price}, {self.max_price}]"
        
        # Validate price relationships
        if data.low_price > data.high_price:
            return False, f"Low price {data.low_price} greater than high price {data.high_price}"
        
        if data.open_price > data.high_price or data.open_price < data.low_price:
            return False, f"Open price {data.open_price} outside range [{data.low_price}, {data.high_price}]"
        
        if data.close_price > data.high_price or data.close_price < data.low_price:
            return False, f"Close price {data.close_price} outside range [{data.low_price}, {data.high_price}]"
        
        # Validate volume
        if data.volume < 0:
            return False, f"Negative volume {data.volume}"
        
        return True, None 