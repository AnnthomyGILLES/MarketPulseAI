"""
Data models for the collection system.
"""

from typing import Dict, List, Any, Optional
import time
from pydantic import BaseModel, Field, validator


class MarketData(BaseModel):
    """Class for holding market data."""
    symbol: str
    timestamp: float
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    source: str
    indicators: Optional[Dict[str, float]] = Field(default_factory=dict)
    collection_time: float = Field(default_factory=time.time)


class SocialMediaData(BaseModel):
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
    collection_time: float = Field(default_factory=time.time)


class NewsData(BaseModel):
    """Class for holding news article data."""
    source: str
    timestamp: float
    title: str
    content: str
    url: str
    symbols: List[str]
    author: str = ''
    collection_time: float = Field(default_factory=time.time)