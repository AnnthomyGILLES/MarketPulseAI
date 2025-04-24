"""
Data models for news collection and validation.

These models define the structure and validation rules for news data
collected from various sources including NewsAPI.org.
"""

from datetime import datetime
from typing import List, Optional, Dict, Any

from pydantic import BaseModel, Field, HttpUrl, validator


class NewsApiSource(BaseModel):
    """
    Represents a news source from NewsAPI.
    """
    id: Optional[str] = Field(None, description="Source ID")
    name: str = Field(..., description="Source name")


class NewsApiArticle(BaseModel):
    """
    Represents a news article from NewsAPI.
    """
    source: NewsApiSource
    author: Optional[str] = Field(None, description="Author of the article")
    title: str = Field(..., description="Article title")
    description: Optional[str] = Field(None, description="Article description or snippet")
    url: HttpUrl = Field(..., description="URL to the article")
    urlToImage: Optional[HttpUrl] = Field(None, description="URL to article thumbnail image")
    publishedAt: datetime = Field(..., description="Publication date and time")
    content: Optional[str] = Field(None, description="Article content or snippet")


class NewsApiResponse(BaseModel):
    """
    Represents the response from NewsAPI.
    """
    status: str = Field(..., description="Response status ('ok' or 'error')")
    totalResults: int = Field(..., description="Total number of results available")
    articles: List[NewsApiArticle] = Field(..., description="List of articles")
    code: Optional[str] = Field(None, description="Error code if status is 'error'")
    message: Optional[str] = Field(None, description="Error message if status is 'error'")
    
    @validator('status')
    def status_must_be_ok(cls, v):
        if v != 'ok':
            raise ValueError(f"API returned error status: {v}")
        return v


class EnrichedNewsArticle(BaseModel):
    """
    Enriched news article with additional metadata for MarketPulseAI.
    """
    # Original NewsAPI data
    article_id: str = Field(..., description="Unique ID for the article")
    source_id: Optional[str] = Field(None, description="Source ID")
    source_name: str = Field(..., description="Source name")
    author: Optional[str] = Field(None, description="Author of the article")
    title: str = Field(..., description="Article title")
    description: Optional[str] = Field(None, description="Article description")
    url: HttpUrl = Field(..., description="URL to the article")
    image_url: Optional[HttpUrl] = Field(None, description="URL to article image")
    published_at: datetime = Field(..., description="Publication date and time")
    content: Optional[str] = Field(None, description="Article content or snippet")
    
    # Enriched metadata
    symbols: List[str] = Field(default_factory=list, description="Stock symbols mentioned")
    categories: List[str] = Field(default_factory=list, description="News categories")
    relevance_score: float = Field(0.0, description="Relevance score (0-1)")
    collection_timestamp: datetime = Field(..., description="When the article was collected")
    
    # Processing metadata
    processed: bool = Field(False, description="Whether sentiment analysis has been performed")
    raw_data: Dict[str, Any] = Field(default_factory=dict, description="Original raw data") 