"""
Base classes for market data collectors.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from src.data_collection import logger


class BaseCollector(ABC):
    """Base class for all data collectors."""
    
    def __init__(self, symbols: List[str]):
        """Initialize the collector with symbols to collect."""
        self.symbols = symbols
        self.last_collection_time = {}
        logger.info(f"Initialized {self.__class__.__name__} with {len(symbols)} symbols")
    
    @abstractmethod
    def collect(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """
        Collect raw data for a symbol or all symbols.
        
        Args:
            symbol: Optional specific symbol to collect. If None, collect all symbols.
            
        Returns:
            Dictionary containing the raw collected data.
        """
        pass
    
    def collect_all(self) -> Dict[str, Dict[str, Any]]:
        """
        Collect data for all symbols.
        
        Returns:
            Dictionary mapping symbols to their raw collected data.
        """
        results = {}
        for symbol in self.symbols:
            try:
                results[symbol] = self.collect(symbol)
            except Exception as e:
                logger.error(f"Error collecting data for {symbol}: {e}")
        
        return results