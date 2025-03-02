"""
Validator for YFinance data.
"""

from typing import Optional, Tuple, Dict, Any

from src.data_collection import logger
from src.data_collection.market_data.validation.market_validators import MarketDataValidator
from src.data_collection.models import MarketData


class YFinanceValidator(MarketDataValidator):
    """Validates data specifically from Yahoo Finance."""
    
    def __init__(self, max_price: float = 100000.0, min_price: float = 0.0, 
                 max_volume_change_pct: float = 500.0):
        """
        Initialize the YFinance validator.
        
        Args:
            max_price: Maximum allowed price
            min_price: Minimum allowed price
            max_volume_change_pct: Maximum allowed volume change percentage
        """
        super().__init__(max_price, min_price)
        self.max_volume_change_pct = max_volume_change_pct
        self.last_valid_data = {}  # Store last valid data by symbol for comparison
    
    def validate(self, data: MarketData) -> Tuple[bool, Optional[str]]:
        """
        Validate a MarketData object from YFinance.
        
        Args:
            data: The MarketData object to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # First run the base validation
        is_valid, error = super().validate(data)
        if not is_valid:
            return False, error
        
        # YFinance-specific validations
        
        # Check if the source is correct
        if data.source != 'yfinance':
            return False, f"Invalid source: {data.source}, expected 'yfinance'"
        
        # Check for missing data (NaN values converted to 0)
        if data.open_price == 0 and data.close_price == 0 and data.high_price == 0 and data.low_price == 0:
            return False, "All price values are zero, likely missing data"
        
        # Check for stale data
        if data.symbol in self.last_valid_data:
            last_data = self.last_valid_data[data.symbol]
            
            # Check if timestamp is newer
            if data.timestamp <= last_data.timestamp:
                return False, f"Stale data: timestamp {data.timestamp} not newer than previous {last_data.timestamp}"
            
            # Check for unrealistic price changes (e.g., more than 20% in a minute)
            if last_data.close_price > 0:
                price_change_pct = abs(data.close_price - last_data.close_price) / last_data.close_price * 100
                if price_change_pct > 20:  # 20% change threshold
                    return False, f"Suspicious price change: {price_change_pct:.2f}% from previous close"
            
            # Check for unrealistic volume changes
            if last_data.volume > 0:
                volume_change_pct = abs(data.volume - last_data.volume) / last_data.volume * 100
                if volume_change_pct > self.max_volume_change_pct:
                    return False, f"Suspicious volume change: {volume_change_pct:.2f}% from previous volume"
        
        # Store this data for future comparisons if it's valid
        self.last_valid_data[data.symbol] = data
        
        return True, None
    
    def validate_raw_data(self, raw_data: Dict[str, Any], symbol: str) -> Tuple[bool, Optional[str]]:
        """
        Validate raw YFinance data before parsing.
        
        Args:
            raw_data: Raw data from YFinance collector
            symbol: Symbol being validated
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Check if data exists
            if 'data' not in raw_data:
                return False, "Missing 'data' key in raw data"
            
            data = raw_data['data']
            
            # Check if data is empty
            if data.empty:
                return False, f"Empty data frame for symbol {symbol}"
            
            # Check for specific symbol data
            if symbol in data.columns:
                symbol_data = data[symbol]
            else:
                # For single symbol case
                symbol_data = data
            
            # Check if we have any rows
            if len(symbol_data) == 0:
                return False, f"No rows in data for symbol {symbol}"
            
            # Check for all NaN values in the latest row
            latest = symbol_data.iloc[-1]
            if latest.isnull().all():
                return False, f"All values are NaN for latest data point of {symbol}"
            
            return True, None
            
        except Exception as e:
            return False, f"Error validating raw YFinance data: {e}" 