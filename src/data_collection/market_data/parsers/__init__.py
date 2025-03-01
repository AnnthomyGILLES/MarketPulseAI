"""
Base classes for market data parsers.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from src.data_collection import logger
from src.data_collection.models import MarketData


class BaseParser(ABC):
    """Base class for all data parsers."""
    
    def __init__(self):
        """Initialize the parser."""
        logger.info(f"Initialized {self.__class__.__name__}")
    
    @abstractmethod
    def parse(self, raw_data: Dict[str, Any], symbol: str) -> Optional[MarketData]:
        """
        Parse raw data into a standardized MarketData object.
        
        Args:
            raw_data: The raw data from a collector
            symbol: The stock symbol this data is for
            
        Returns:
            A MarketData object or None if parsing fails
        """
        pass
    
    def parse_batch(self, raw_data_batch: Dict[str, Dict[str, Any]]) -> List[MarketData]:
        """
        Parse a batch of raw data for multiple symbols.
        
        Args:
            raw_data_batch: Dictionary mapping symbols to their raw data
            
        Returns:
            List of MarketData objects
        """
        results = []
        for symbol, raw_data in raw_data_batch.items():
            try:
                parsed_data = self.parse(raw_data, symbol)
                if parsed_data:
                    results.append(parsed_data)
            except Exception as e:
                logger.error(f"Error parsing data for {symbol}: {e}")
        
        return results 