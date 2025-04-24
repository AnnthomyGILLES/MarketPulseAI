"""
Pydantic schema definitions for news data validation.
"""

from typing import Dict, Any, Optional

from pydantic import BaseModel, Field, model_validator


class NewsArticleSchema(BaseModel):
    """
    Schema for validating news articles from NewsAPI.
    """

    # Required fields
    title: str = Field(..., min_length=1)
    url: str = Field(..., min_length=10)
    publishedAt: str = Field(...)
    source: Dict[str, Any] = Field(...)

    # Optional fields with defaults
    author: Optional[str] = None
    description: Optional[str] = None
    content: Optional[str] = None
    urlToImage: Optional[str] = None

    # Collection metadata
    collected_at: str
    collector_id: str
    source_type: str = "newsapi"

    # Extended model fields (will be added during validation)
    article_id: Optional[str] = None
    processed_title: Optional[str] = None

    @model_validator(mode="after")
    def set_article_id(self) -> "NewsArticleSchema":
        """Set a unique article ID and any other derived fields."""
        if not self.article_id:
            self.article_id = self.url
        return self


class ValidatedNewsArticle(NewsArticleSchema):
    """
    Schema for validated news articles with additional fields.
    """

    # Validation metadata
    validated_at: str
    validation_version: str = "1.0.0" 