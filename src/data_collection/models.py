"""
Data models for the collection system.
"""

from dataclasses import dataclass
from typing import Dict, List, Any, Optional
import time


@dataclass
class MarketData:
    """Class for holding market data."""
    symbol: str
    timestamp: float
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    source: str
    indicators: Dict[str, float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert data to a dictionary for Kafka."""
        return {
            'symbol': self.symbol,
            'timestamp': self.timestamp,
            'open': self.open_price,
            'high': self.high_price,
            'low': self.low_price,
            'close': self.close_price,
            'volume': self.volume,
            'source': self.source,
            'indicators': self.indicators or {},
            'collection_time': time.time()
        }


@dataclass
class SocialMediaData:
    """Class for holding social media data."""
    source: str
    timestamp: float
    content: str
    author: str
    post_id: str
    symbols: List[str]
    likes: int = 0
    shares: int = 0
    comments: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert data to a dictionary for Kafka."""
        return {
            'source': self.source,
            'timestamp': self.timestamp,
            'content': self.content,
            'author': self.author,
            'post_id': self.post_id,
            'symbols': self.symbols,
            'likes': self.likes,
            'shares': self.shares,
            'comments': self.comments,
            'collection_time': time.time()
        }


@dataclass
class NewsData:
    """Class for holding news article data."""
    source: str
    timestamp: float
    title: str
    content: str
    url: str
    symbols: List[str]
    author: str = ''

    def to_dict(self) -> Dict[str, Any]:
        """Convert data to a dictionary for Kafka."""
        return {
            'source': self.source,
            'timestamp': self.timestamp,
            'title': self.title,
            'content': self.content,
            'url': self.url,
            'symbols': self.symbols,
            'author': self.author,
            'collection_time': time.time()
        } 