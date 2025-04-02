"""
Data validation module for Reddit data collected by RedditCollector.

This module provides schema validation and data quality checks for Reddit posts
and comments before they are processed by the MarketPulseAI system.
"""

import re
from datetime import datetime
from typing import List, Optional, Dict, Any, Union

from loguru import logger
from pydantic import BaseModel, Field, field_validator, model_validator


class RedditPostBase(BaseModel):
    """Base schema for Reddit data with common fields."""

    id: str
    source: str = Field(..., description="Source platform, should be 'reddit'")
    collection_timestamp: str

    @field_validator("collection_timestamp")
    @classmethod
    def validate_collection_timestamp(cls, v):
        """Validate that collection_timestamp is in ISO format."""
        try:
            datetime.fromisoformat(v)
            return v
        except ValueError:
            raise ValueError("collection_timestamp must be in ISO format")

    @field_validator("source")
    @classmethod
    def validate_source(cls, v):
        """Validate that source is 'reddit'."""
        if v != "reddit":
            raise ValueError("source must be 'reddit'")
        return v


class RedditPost(RedditPostBase):
    """Schema for Reddit posts with validation."""

    content_type: str = Field(..., description="Content type, should be 'post'")
    subreddit: str
    title: str
    author: str
    created_utc: float
    post_created_datetime: str
    score: int
    upvote_ratio: float
    num_comments: int
    selftext: str
    url: str
    is_self: bool
    permalink: str
    collection_method: str
    post_age_days: int
    detected_symbols: Optional[List[str]] = None

    @field_validator("content_type")
    @classmethod
    def validate_content_type(cls, v):
        """Validate that content_type is 'post'."""
        if v != "post":
            raise ValueError("content_type must be 'post'")
        return v

    @field_validator("post_created_datetime")
    @classmethod
    def validate_post_created_datetime(cls, v):
        """Validate that post_created_datetime is in ISO format."""
        try:
            datetime.fromisoformat(v)
            return v
        except ValueError:
            raise ValueError("post_created_datetime must be in ISO format")

    @field_validator("score")
    @classmethod
    def validate_score(cls, v):
        """Validate that score is a reasonable value."""
        if v < 0 or v > 1_000_000:  # Reasonable upper bound for Reddit posts
            logger.warning(f"Unusual score value: {v}")
        return v

    @field_validator("upvote_ratio")
    @classmethod
    def validate_upvote_ratio(cls, v):
        """Validate that upvote_ratio is between 0 and 1."""
        if v < 0 or v > 1:
            raise ValueError("upvote_ratio must be between 0 and 1")
        return v

    @field_validator("num_comments")
    @classmethod
    def validate_num_comments(cls, v):
        """Validate that num_comments is a non-negative integer."""
        if v < 0:
            raise ValueError("num_comments must be non-negative")
        return v

    @field_validator("post_age_days")
    @classmethod
    def validate_post_age_days(cls, v):
        """Validate that post_age_days is reasonable."""
        if v < 0 or v > 365 * 10:  # Cap at 10 years to catch timestamp errors
            raise ValueError(f"post_age_days has unusual value: {v}")
        return v

    @field_validator("detected_symbols")
    @classmethod
    def validate_detected_symbols(cls, v):
        """Validate that detected_symbols contains valid stock symbols."""
        if v is None:
            return v
        # Basic validation - stock symbols are typically 1-5 uppercase letters
        for symbol in v:
            if not re.match(r"^[A-Z]{1,5}$", symbol):
                logger.warning(f"Potentially invalid stock symbol: {symbol}")
        return v

    @model_validator(mode="after")
    @classmethod
    def check_content_quality(cls, model: "RedditPost"):
        """Perform quality checks on the content."""
        title = model.title or ""
        selftext = model.selftext or ""

        # Check for minimum content length
        if len(title) < 3:
            logger.warning(f"Very short post title: '{title}'")

        # Check if the post has meaningful content
        if len(selftext) < 10 and model.is_self:
            logger.warning("Self post with minimal content")

        # Check for deleted content
        if title == "[deleted]" or selftext == "[deleted]":
            logger.warning("Post contains deleted content")

        return model


class RedditComment(RedditPostBase):
    """Schema for Reddit comments with validation."""

    post_id: str
    body: str
    author: str
    created_utc: float
    comment_created_datetime: str
    score: int
    subreddit: str
    parent_id: str
    detected_symbols: Optional[List[str]] = None

    @field_validator("comment_created_datetime")
    @classmethod
    def validate_comment_created_datetime(cls, v):
        """Validate that comment_created_datetime is in ISO format."""
        try:
            datetime.fromisoformat(v)
            return v
        except ValueError:
            raise ValueError("comment_created_datetime must be in ISO format")

    @field_validator("score")
    @classmethod
    def validate_score(cls, v):
        """Validate that score is a reasonable value."""
        if v < -1000 or v > 100_000:  # Reasonable bounds for Reddit comments
            logger.warning(f"Unusual comment score value: {v}")
        return v

    @field_validator("body")
    @classmethod
    def validate_body(cls, v):
        """Validate comment body."""
        if len(v) < 1:
            logger.warning("Empty comment body")
        elif v == "[deleted]":
            logger.warning("Deleted comment")
        return v

    @field_validator("detected_symbols")
    @classmethod
    def validate_detected_symbols(cls, v):
        """Validate that detected_symbols contains valid stock symbols."""
        if v is None:
            return v
        # Basic validation - stock symbols are typically 1-5 uppercase letters
        for symbol in v:
            if not re.match(r"^[A-Z]{1,5}$", symbol):
                logger.warning(f"Potentially invalid stock symbol: {symbol}")
        return v


class RedditDataValidator:
    """Validates and processes Reddit data."""

    def __init__(self):
        """Initialize the validator."""
        self.valid_count = 0
        self.invalid_count = 0

    def validate_reddit_data(self, data: Dict[str, Any]) -> Union[Dict[str, Any], None]:
        """
        Validate Reddit data against the appropriate schema.

        Args:
            data: The Reddit data to validate.

        Returns:
            The validated data if valid, None otherwise.
        """
        try:
            # Determine if this is a post or comment
            is_post = data.get("content_type") == "post"

            # Validate against the appropriate schema
            if is_post:
                validated_data = RedditPost(**data).model_dump()
            else:
                validated_data = RedditComment(**data).model_dump()

            # Track validation success
            self.valid_count += 1

            return validated_data

        except Exception as e:
            self.invalid_count += 1
            logger.error(
                f"Validation error for data with ID {data.get('id', 'unknown')}: {str(e)}"
            )
            return None

    def get_validation_stats(self) -> Dict[str, int]:
        """Get statistics about validation results."""
        return {
            "valid_count": self.valid_count,
            "invalid_count": self.invalid_count,
            "warning_count": self.warning_count,
        }

    def reset_stats(self):
        """Reset validation statistics."""
        self.valid_count = 0
        self.invalid_count = 0
        self.warning_count = 0


class RedditDataEnricher:
    """Enriches validated Reddit data with additional information."""

    def enrich_post(self, post_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich Reddit post data with additional information.

        Args:
            post_data: Validated Reddit post data.

        Returns:
            Enriched Reddit post data.
        """
        enriched_data = post_data.copy()

        # Add sentiment analysis placeholder
        enriched_data["sentiment_placeholder"] = "neutral"

        # Add content length metrics
        enriched_data["title_length"] = len(post_data.get("title", ""))
        enriched_data["selftext_length"] = len(post_data.get("selftext", ""))

        # Calculate engagement metrics
        enriched_data["engagement_score"] = (
            post_data.get("score", 0)
            + post_data.get("num_comments", 0)
            * 2  # Comments weighted more than upvotes
        )

        # Add data quality score
        quality_score = self._calculate_quality_score(post_data)
        enriched_data["quality_score"] = quality_score

        return enriched_data

    def enrich_comment(self, comment_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich Reddit comment data with additional information.

        Args:
            comment_data: Validated Reddit comment data.

        Returns:
            Enriched Reddit comment data.
        """
        enriched_data = comment_data.copy()

        # Add sentiment analysis placeholder
        enriched_data["sentiment_placeholder"] = "neutral"

        # Add content length metric
        enriched_data["body_length"] = len(comment_data.get("body", ""))

        # Calculate simple comment quality score
        quality_score = self._calculate_comment_quality_score(comment_data)
        enriched_data["quality_score"] = quality_score

        return enriched_data

    def _calculate_quality_score(self, post_data: Dict[str, Any]) -> float:
        """
        Calculate a quality score for a Reddit post.

        Args:
            post_data: Validated Reddit post data.

        Returns:
            Quality score between 0 and 1.
        """
        score = 0.0

        # Factor 1: Content length
        title_length = len(post_data.get("title", ""))
        selftext_length = len(post_data.get("selftext", ""))

        if title_length > 20:
            score += 0.2

        if selftext_length > 100:
            score += 0.3
        elif selftext_length > 50:
            score += 0.1

        # Factor 2: Upvote ratio
        upvote_ratio = post_data.get("upvote_ratio", 0.5)
        score += min(0.2, upvote_ratio - 0.5)  # Max 0.2 for perfect ratio

        # Factor 3: Comment count
        num_comments = post_data.get("num_comments", 0)
        score += min(0.3, (num_comments / 100) * 0.3)  # Max 0.3 for 100+ comments

        # Ensure score is between 0 and 1
        return max(0.0, min(1.0, score))

    def _calculate_comment_quality_score(self, comment_data: Dict[str, Any]) -> float:
        """
        Calculate a quality score for a Reddit comment.

        Args:
            comment_data: Validated Reddit comment data.

        Returns:
            Quality score between 0 and 1.
        """
        score = 0.0

        # Factor 1: Content length
        body_length = len(comment_data.get("body", ""))

        if body_length > 100:
            score += 0.5
        elif body_length > 50:
            score += 0.3
        elif body_length > 20:
            score += 0.1

        # Factor 2: Score
        comment_score = comment_data.get("score", 0)
        score += min(0.5, (comment_score / 50) * 0.5)  # Max 0.5 for 50+ score

        # Ensure score is between 0 and 1
        return max(0.0, min(1.0, score))
