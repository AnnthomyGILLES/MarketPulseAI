"""
Validators for news data from various sources.

This module provides validation logic for news data, ensuring it meets
the quality standards required for processing in MarketPulseAI.
"""

import hashlib
import uuid
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional, Set

from loguru import logger
from pydantic import ValidationError

from src.common.validation.base_validator import BaseValidator
from src.data_collection.social_media.news.models import NewsApiArticle, EnrichedNewsArticle


class NewsArticleValidator(BaseValidator):
    """
    Validator for news articles from NewsAPI.
    
    Validates and enriches raw news data before it's published to Kafka.
    """
    
    def __init__(
        self,
        min_title_length: int = 10,
        min_content_length: int = 50,
        required_fields: Optional[Set[str]] = None,
        validator_name: str = "NewsArticleValidator"
    ):
        """
        Initialize the news article validator.
        
        Args:
            min_title_length: Minimum acceptable title length
            min_content_length: Minimum acceptable content length
            required_fields: Set of fields that must be present in the article
            validator_name: Name of the validator for logging
        """
        super().__init__(validator_name=validator_name)
        self.min_title_length = min_title_length
        self.min_content_length = min_content_length
        self.required_fields = required_fields or {
            "title", "source", "url", "publishedAt"
        }
        logger.info(f"{self.validator_name} initialized with "
                    f"min_title_length={min_title_length}, "
                    f"min_content_length={min_content_length}")
    
    def validate(self, data: Dict[str, Any]) -> Tuple[bool, Optional[EnrichedNewsArticle], List[str]]:
        """
        Validate a news article from NewsAPI.
        
        Args:
            data: Raw news article data from NewsAPI
            
        Returns:
            Tuple containing:
            - Boolean indicating if validation passed
            - Enriched article object if validation passed, otherwise None
            - List of validation error messages
        """
        errors = []
        
        # Check for required fields
        for field in self.required_fields:
            if field not in data or data[field] is None:
                errors.append(f"Missing required field: {field}")
        
        if errors:
            return False, None, errors
        
        # Validate with Pydantic model
        try:
            article = NewsApiArticle(**data)
        except ValidationError as e:
            return False, None, [f"Validation error: {err['msg']} (field: {err['loc'][0]})" 
                                for err in e.errors()]
        
        # Additional quality checks
        if len(article.title) < self.min_title_length:
            errors.append(f"Title too short: {len(article.title)} chars (min: {self.min_title_length})")
        
        if article.content and len(article.content) < self.min_content_length:
            errors.append(f"Content too short: {len(article.content)} chars (min: {self.min_content_length})")
        
        if errors:
            return False, None, errors
        
        # Generate unique ID for the article
        article_id = self._generate_article_id(article)
        
        # Create enriched article
        enriched = EnrichedNewsArticle(
            article_id=article_id,
            source_id=article.source.id,
            source_name=article.source.name,
            author=article.author,
            title=article.title,
            description=article.description,
            url=article.url,
            image_url=article.urlToImage,
            published_at=article.publishedAt,
            content=article.content,
            collection_timestamp=datetime.now(),
            raw_data=data
        )
        
        return True, enriched, []
    
    def _generate_article_id(self, article: NewsApiArticle) -> str:
        """
        Generate a deterministic ID for the article based on its URL and title.
        
        Args:
            article: The validated article
            
        Returns:
            Unique ID string for the article
        """
        # Use URL and title to create a deterministic hash
        # This helps avoid duplicates if we fetch the same article multiple times
        base_string = f"{article.url}:{article.title}"
        hasher = hashlib.sha256(base_string.encode('utf-8'))
        return hasher.hexdigest()[:24]  # First 24 chars of the hash 