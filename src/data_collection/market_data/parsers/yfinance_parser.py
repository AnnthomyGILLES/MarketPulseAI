"""
Yahoo Finance data parser.
"""

import time
from typing import Any, Dict, Optional

import pandas as pd

from src.data_collection import logger
from src.data_collection.market_data.parsers import BaseParser
from src.data_collection.models import MarketData


class YFinanceParser(BaseParser):
    """Parses market data from Yahoo Finance."""
    
    def __init__(self):
        """Initialize the Yahoo Finance parser."""
        super().__init__()
    
    def _calculate_technical_indicators(self, data: pd.DataFrame) -> Dict[str, float]:
        """Calculate technical indicators for the latest data point."""
        if data.empty:
            return {}
            
        # Get the close prices
        close_prices = data['Close']
        
        # Calculate indicators
        indicators = {}
        
        # Simple Moving Averages
        indicators['SMA_5'] = close_prices.rolling(window=5).mean().iloc[-1] if len(close_prices) >= 5 else None
        indicators['SMA_20'] = close_prices.rolling(window=20).mean().iloc[-1] if len(close_prices) >= 20 else None
        
        # RSI
        if len(close_prices) >= 14:
            indicators['RSI'] = self._calculate_rsi(close_prices).iloc[-1]
        
        # MACD
        if len(close_prices) >= 26:
            macd, signal = self._calculate_macd(close_prices)
            indicators['MACD'] = macd.iloc[-1]
            indicators['MACD_Signal'] = signal.iloc[-1]
            
        # Filter out None values
        return {k: float(v) for k, v in indicators.items() if v is not None}
    
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """Calculate Relative Strength Index."""
        delta = prices.diff()
        gain = delta.where(delta > 0, 0).rolling(window=period).mean()
        loss = -delta.where(delta < 0, 0).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def _calculate_macd(self, prices: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
        """Calculate MACD (Moving Average Convergence Divergence)."""
        ema_fast = prices.ewm(span=fast, adjust=False).mean()
        ema_slow = prices.ewm(span=slow, adjust=False).mean()
        macd = ema_fast - ema_slow
        macd_signal = macd.ewm(span=signal, adjust=False).mean()
        return macd, macd_signal
    
    def parse(self, raw_data: Dict[str, Any], symbol: str) -> Optional[MarketData]:
        """
        Parse raw Yahoo Finance data into a standardized MarketData object.
        
        Args:
            raw_data: The raw data from the YFinanceCollector
            symbol: The stock symbol this data is for
            
        Returns:
            A MarketData object or None if parsing fails
        """
        try:
            data = raw_data['data']
            collection_time = raw_data['collection_time']
            source = raw_data['source']
            
            # Handle multi-level column structure for multiple symbols
            if symbol in data.columns:
                symbol_data = data[symbol]
            else:
                # Handle single-level column structure (for single symbol)
                symbol_data = data
            
            if symbol_data.empty:
                logger.warning(f"No data available for {symbol}")
                return None
            
            # Get the latest data point
            latest = symbol_data.iloc[-1]
            
            # Calculate technical indicators
            indicators = self._calculate_technical_indicators(symbol_data)
            
            # Create MarketData object
            market_data = MarketData(
                symbol=symbol,
                timestamp=collection_time,
                open_price=float(latest.get('Open', 0.0)),
                high_price=float(latest.get('High', 0.0)),
                low_price=float(latest.get('Low', 0.0)),
                close_price=float(latest.get('Close', 0.0)),
                volume=int(latest.get('Volume', 0)),
                source=source,
                indicators=indicators
            )
            
            return market_data
            
        except Exception as e:
            logger.error(f"Error parsing Yahoo Finance data for {symbol}: {e}")
            return None 