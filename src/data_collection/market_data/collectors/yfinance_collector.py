"""
Yahoo Finance data collector.
"""

import time
from typing import Any, Dict, List, Optional

import yfinance as yf

from src.data_collection import logger, STOCK_SYMBOLS
from src.data_collection.market_data.collectors import BaseCollector


class YFinanceCollector(BaseCollector):
    """Collects market data from Yahoo Finance."""
    
    def __init__(self, symbols: List[str] = STOCK_SYMBOLS):
        """Initialize the Yahoo Finance collector."""
        super().__init__(symbols)
    
    def collect(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """
        Collect data from Yahoo Finance.
        
        Args:
            symbol: Optional specific symbol to collect. If None, collect all symbols.
            
        Returns:
            Dictionary containing the raw collected data.
        """
        try:
            symbols_to_collect = [symbol] if symbol else self.symbols
            
            # Get data for symbols
            data = yf.download(symbols_to_collect, period="1d", interval="1m", group_by="ticker")
            
            # Record collection time
            collection_time = time.time()
            
            # Format the response
            result = {
                'data': data,
                'collection_time': collection_time,
                'source': 'yfinance'
            }
            
            logger.info(f"Collected Yahoo Finance data for {len(symbols_to_collect)} symbols")
            return result
            
        except Exception as e:
            logger.error(f"Error collecting Yahoo Finance data: {e}")
            raise 