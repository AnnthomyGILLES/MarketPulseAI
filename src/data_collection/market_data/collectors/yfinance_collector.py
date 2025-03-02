"""
Yahoo Finance data collector.
"""
import time
from typing import Any, Dict, List, Optional

import yfinance as yf

from src.data_collection import logger, STOCK_SYMBOLS
from src.data_collection.market_data.collectors import BaseCollector
from src.data_collection.market_data.parsers.yfinance_parser import YFinanceParser
from src.data_collection.market_data.validation.yfinance_validator import YFinanceValidator


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


if __name__ == "__main__":
    # Create an instance of the collector and validator
    collector = YFinanceCollector(symbols=["AAPL", "MSFT", "GOOG"])
    validator = YFinanceValidator()
    parser = YFinanceParser()
    
    # Simulate streaming data by collecting at regular intervals
    print("\nSimulating streaming data with validation (press Ctrl+C to stop)...")
    try:
        # Track the last price to detect changes
        last_prices = {}
        
        while True:
            # Collect fresh data for a single symbol
            symbol = "AAPL"
            fresh_data = collector.collect(symbol=symbol)
            
            # Validate raw data
            is_raw_valid, raw_error = validator.validate_raw_data(fresh_data, symbol)
            if not is_raw_valid:
                print(f"[{time.strftime('%H:%M:%S')}] Invalid raw data: {raw_error}")
                time.sleep(60)
                continue
                
            # Parse the data
            market_data = parser.parse(fresh_data, symbol)
            if not market_data:
                print(f"[{time.strftime('%H:%M:%S')}] Failed to parse data")
                time.sleep(60)
                continue
                
            # Validate the parsed data
            is_valid, error = validator.validate(market_data)
            if not is_valid:
                print(f"[{time.strftime('%H:%M:%S')}] Invalid data: {error}")
                time.sleep(60)
                continue
            
            # Extract the latest price
            latest_data = market_data.close_price
            
            # Check if price changed
            if symbol in last_prices and last_prices[symbol] != latest_data:
                print(f"[{time.strftime('%H:%M:%S')}] {symbol}: ${latest_data:.2f} (changed by ${latest_data - last_prices[symbol]:.2f}) ✓")
            elif symbol not in last_prices:
                print(f"[{time.strftime('%H:%M:%S')}] {symbol}: ${latest_data:.2f} ✓")
            
            # Update last known price
            last_prices[symbol] = latest_data
            
            # Wait for 60 seconds before next request
            time.sleep(60)
            
    except KeyboardInterrupt:
        print("\nStopped data collection.")
    except Exception as e:
        print(f"\nError during data collection: {e}")