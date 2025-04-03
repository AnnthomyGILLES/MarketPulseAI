# src/data_collection/social_media/reddit/validation/reddit_validation.py

import logging
import re
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any, Union, Tuple

from pydantic import BaseModel, Field, field_validator, model_validator, ValidationError

# Setup logger for this module
# Assuming a central logging setup exists and is configured elsewhere
logger = logging.getLogger(__name__)

# Basic regex for common stock symbols (1-5 uppercase letters)
# Allows optional '$' prefix
SYMBOL_REGEX = re.compile(r"^\$?[A-Z]{1,5}$")


def validate_iso_utc_datetime(timestamp: str) -> str:
    """Validates if a string is a valid ISO 8601 UTC timestamp."""
    try:
        # Ensure it ends with 'Z' and parses correctly
        if not timestamp.endswith("Z"):
            raise ValueError("Timestamp must be in UTC and end with 'Z'")
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if dt.tzinfo is None or dt.tzinfo != timezone.utc:
            raise ValueError("Timestamp must have UTC timezone info")
        return timestamp
    except (ValueError, TypeError) as e:
        raise ValueError(
            f"Invalid ISO 8601 UTC timestamp format: {timestamp}. Error: {e}"
        )


def validate_symbol_list(symbols: Optional[List[str]]) -> Optional[List[str]]:
    """Validates a list of potential stock symbols."""
    if symbols is None:
        return None
    if not isinstance(symbols, list):
        raise ValueError("detected_symbols must be a list or null")

    validated_symbols = []
    for symbol in symbols:
        if isinstance(symbol, str) and SYMBOL_REGEX.match(symbol):
            validated_symbols.append(symbol.lstrip("$"))  # Store without '$' prefix
        else:
            logger.warning(f"Invalid or non-standard symbol format found: '{symbol}'")
            # Decide whether to discard or keep - currently discarding invalid ones
            # If keeping, add: validated_symbols.append(symbol)
    return validated_symbols if validated_symbols else None


class RedditBase(BaseModel):
    """Base schema for Reddit items (Posts and Comments)."""

    id: str = Field(..., description="Unique Reddit ID for the item.")
    source: str = Field("reddit", description="Source platform, must be 'reddit'.")
    content_type: str = Field(..., description="Type of content: 'post' or 'comment'.")
    collection_timestamp: str = Field(
        ..., description="UTC timestamp when the item was collected (ISO 8601 format)."
    )
    created_utc: float = Field(
        ..., description="Original creation timestamp (epoch seconds)."
    )
    author: str = Field(
        ..., description="Reddit username of the author, or '[deleted]'."
    )
    score: int = Field(..., description="Score (upvotes - downvotes) of the item.")
    subreddit: str = Field(..., description="Subreddit where the item originated.")
    permalink: str = Field(..., description="Full permalink URL to the item.")
    detected_symbols: Optional[List[str]] = Field(
        None, description="List of stock symbols detected in the content."
    )

    @field_validator("source")
    @classmethod
    def check_source(cls, v):
        if v != "reddit":
            raise ValueError("Source must be 'reddit'")
        return v

    @field_validator("content_type")
    @classmethod
    def check_content_type(cls, v):
        if v not in ["post", "comment"]:
            raise ValueError("content_type must be 'post' or 'comment'")
        return v

    @field_validator("collection_timestamp")
    @classmethod
    def check_collection_timestamp(cls, v):
        return validate_iso_utc_datetime(v)

    # Convert created_utc (float epoch) to a standard ISO string field
    # This makes the schema consistent in using ISO strings for datetimes
    created_datetime: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def convert_created_utc(cls, values):
        """Convert created_utc float to ISO string and add to values."""
        created_utc_float = values.get("created_utc")
        if isinstance(created_utc_float, (int, float)):
            try:
                dt_utc = datetime.fromtimestamp(created_utc_float, timezone.utc)
                values["created_datetime"] = dt_utc.isoformat().replace("+00:00", "Z")
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"Could not convert created_utc '{created_utc_float}' to datetime: {e}"
                )
                # Keep created_utc field but created_datetime will be None or invalid later
        elif "created_datetime" not in values:
            # Handle cases where created_utc might be missing but a string version exists
            # (e.g., post_created_datetime from original collector)
            dt_str = values.get("post_created_datetime") or values.get(
                "comment_created_datetime"
            )
            if dt_str:
                try:
                    values["created_datetime"] = validate_iso_utc_datetime(dt_str)
                except ValueError:
                    logger.warning(
                        f"Could not validate existing datetime string: {dt_str}"
                    )

        # Ensure created_datetime exists after potential conversion attempts
        if "created_datetime" not in values or not values["created_datetime"]:
            raise ValueError(
                "Missing or invalid creation timestamp (created_utc or created_datetime)"
            )

        return values

    @field_validator("created_datetime")
    @classmethod
    def check_created_datetime(cls, v):
        """Validates the derived created_datetime field."""
        if v is None:
            # This should ideally be caught by the model_validator, but double-check
            raise ValueError("Creation datetime could not be determined or validated.")
        # The format itself is validated by validate_iso_utc_datetime during creation/assignment
        return v

    @field_validator("detected_symbols")
    @classmethod
    def check_detected_symbols(cls, v):
        return validate_symbol_list(v)

    # Model-level validation for cross-field checks if needed
    @model_validator(mode="after")
    def check_times(self):
        """Optional: Check if collection time is after creation time."""
        try:
            created_dt = datetime.fromisoformat(
                self.created_datetime.replace("Z", "+00:00")
            )
            collected_dt = datetime.fromisoformat(
                self.collection_timestamp.replace("Z", "+00:00")
            )
            if collected_dt < created_dt - timedelta(
                minutes=1
            ):  # Allow small tolerance
                logger.warning(
                    f"Collection timestamp {self.collection_timestamp} is before creation timestamp {self.created_datetime} for item {self.id}"
                )
                # Raise ValueError here if this should be a hard failure
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not compare timestamps for item {self.id}: {e}")
        return self


class RedditPost(RedditBase):
    """Schema for validating Reddit posts."""

    content_type: str = Field("post", description="Content type, must be 'post'.")
    title: str = Field(..., min_length=1, description="Title of the post.")
    selftext: str = Field(
        "", description="Body text of the post (if any)."
    )  # Allow empty string
    url: str = Field(..., description="URL link associated with the post.")
    is_self: bool = Field(..., description="True if it's a self-post (text only).")
    upvote_ratio: float = Field(
        ..., ge=0.0, le=1.0, description="Ratio of upvotes to total votes."
    )
    num_comments: int = Field(..., ge=0, description="Number of comments on the post.")
    # Deprecated fields from original collector (handled by base model now)
    # post_created_datetime: Optional[str] = Field(None, deprecated=True)
    # post_age_days: Optional[int] = Field(None, deprecated=True)
    collection_method: Optional[str] = Field(
        None,
        description="Method used to collect this post (e.g., 'hot', 'symbol_search').",
    )

    @field_validator("content_type")
    @classmethod
    def check_post_content_type(cls, v):
        if v != "post":
            raise ValueError("content_type must be 'post' for RedditPost schema")
        return v

    @model_validator(mode="after")
    def check_post_quality(self):
        """Perform basic quality checks specific to posts."""
        if self.author == "[deleted]" and (not self.title or self.title == "[deleted]"):
            logger.warning(f"Post {self.id} appears to be deleted (author and title).")
        elif len(self.title) < 5:
            logger.debug(f"Post {self.id} has a very short title: '{self.title}'")
        if self.is_self and not self.selftext and len(self.title) < 20:
            logger.debug(f"Post {self.id} is a self-post with no body and short title.")
        return self


class RedditComment(RedditBase):
    """Schema for validating Reddit comments."""

    content_type: str = Field("comment", description="Content type, must be 'comment'.")
    post_id: str = Field(..., description="ID of the parent post.")
    body: str = Field(..., min_length=1, description="Body text of the comment.")
    parent_id: str = Field(
        ..., description="ID of the parent item (post or another comment)."
    )
    is_submitter: Optional[bool] = Field(
        None, description="True if the commenter is the post's author."
    )
    # Deprecated fields from original collector (handled by base model now)
    # comment_created_datetime: Optional[str] = Field(None, deprecated=True)

    @field_validator("content_type")
    @classmethod
    def check_comment_content_type(cls, v):
        if v != "comment":
            raise ValueError("content_type must be 'comment' for RedditComment schema")
        return v

    @field_validator("post_id", "parent_id")
    @classmethod
    def check_id_format(cls, v):
        # Reddit IDs usually start with t1_, t3_ etc. but let's just check non-empty for now
        if not v or not isinstance(v, str):
            raise ValueError("post_id and parent_id must be non-empty strings")
        # More specific regex could be added if needed, e.g., r'^[t][13]_\w+$'
        return v

    @model_validator(mode="after")
    def check_comment_quality(self):
        """Perform basic quality checks specific to comments."""
        if self.author == "[deleted]" and self.body == "[deleted]":
            logger.warning(f"Comment {self.id} appears to be fully deleted.")
        elif len(self.body) < 3:
            logger.debug(f"Comment {self.id} has a very short body.")
        return self


# --- Validation Function ---


def validate_reddit_data(
    data: Dict[str, Any],
) -> Tuple[Union[RedditPost, RedditComment, None], List[str]]:
    """
    Validates raw Reddit data against Post or Comment schemas.

    Args:
        data: A dictionary representing a Reddit post or comment.

    Returns:
        A tuple containing:
        - The validated Pydantic model instance (RedditPost or RedditComment) or None if invalid.
        - A list of validation error messages, if any.
    """
    validation_errors = []
    validated_model = None

    content_type = data.get("content_type")
    item_id = data.get("id", "UNKNOWN_ID")

    try:
        if content_type == "post":
            validated_model = RedditPost(**data)
            logger.debug(f"Successfully validated post: {item_id}")
        elif content_type == "comment":
            validated_model = RedditComment(**data)
            logger.debug(f"Successfully validated comment: {item_id}")
        else:
            validation_errors.append(
                f"Missing or invalid 'content_type': {content_type}"
            )
            logger.warning(
                f"Validation failed for item {item_id}: Invalid content_type '{content_type}'"
            )

    except ValidationError as e:
        # Log detailed errors from Pydantic
        for error in e.errors():
            field = ".".join(map(str, error["loc"])) if error["loc"] else "general"
            msg = f"Field '{field}': {error['msg']}"
            validation_errors.append(msg)
        logger.warning(
            f"Validation failed for {content_type or 'item'} {item_id}: {len(validation_errors)} error(s). First error: {validation_errors[0]}"
        )
        # Optionally log all errors: logger.debug(f"All validation errors for {item_id}: {validation_errors}")

    except Exception as e:
        # Catch unexpected errors during validation
        error_msg = f"Unexpected validation error: {str(e)}"
        validation_errors.append(error_msg)
        logger.error(
            f"Unexpected error validating {content_type or 'item'} {item_id}: {e}",
            exc_info=True,
        )  # Include stack trace

    return validated_model, validation_errors


# --- Enrichment Class (Example) ---


class RedditDataEnricher:
    """
    Enriches validated Reddit data with additional derived metrics or scores.
    Note: Enrichment logic is highly application-specific.
    """

    def enrich_data(
        self, validated_data: Union[RedditPost, RedditComment]
    ) -> Dict[str, Any]:
        """
        Enriches validated Reddit post or comment data.

        Args:
            validated_data: A validated RedditPost or RedditComment Pydantic model.

        Returns:
            Enriched data as a dictionary.
        """
        enriched_dict = (
            validated_data.model_dump()
        )  # Convert Pydantic model back to dict

        # --- Common Enrichment ---
        enriched_dict["processing_timestamp"] = (
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        )
        enriched_dict["sentiment_score"] = (
            None  # Placeholder for sentiment analysis result
        )
        enriched_dict["sentiment_label"] = "neutral"  # Placeholder

        # --- Post-Specific Enrichment ---
        if isinstance(validated_data, RedditPost):
            enriched_dict["title_length"] = len(validated_data.title)
            enriched_dict["selftext_length"] = len(validated_data.selftext)
            # Example: Simple engagement score
            enriched_dict["engagement_score"] = validated_data.score + (
                validated_data.num_comments * 2
            )  # Weight comments higher
            enriched_dict["quality_score"] = self._calculate_post_quality(
                validated_data
            )

        # --- Comment-Specific Enrichment ---
        elif isinstance(validated_data, RedditComment):
            enriched_dict["body_length"] = len(validated_data.body)
            enriched_dict["quality_score"] = self._calculate_comment_quality(
                validated_data
            )

        logger.debug(f"Enriched {validated_data.content_type} {validated_data.id}")
        return enriched_dict

    def _calculate_post_quality(self, post: RedditPost) -> float:
        """Calculates a simple quality score for a post (0.0 - 1.0). Example logic."""
        score = 0.0
        # Length contributes
        score += min(0.3, len(post.title) / 100.0)
        score += min(
            0.4, len(post.selftext) / 500.0 if post.is_self else len(post.url) / 100.0
        )
        # Engagement contributes
        score += min(0.1, post.score / 100.0)
        score += min(0.2, post.num_comments / 50.0)
        # Ratio contributes
        if post.upvote_ratio > 0.7:
            score += 0.1
        # Penalize deleted items
        if (
            post.author == "[deleted]"
            or post.title == "[deleted]"
            or post.selftext == "[deleted]"
        ):
            score *= 0.5
        return round(max(0.0, min(1.0, score)), 3)

    def _calculate_comment_quality(self, comment: RedditComment) -> float:
        """Calculates a simple quality score for a comment (0.0 - 1.0). Example logic."""
        score = 0.0
        # Length contributes
        score += min(0.7, len(comment.body) / 300.0)
        # Score contributes
        score += min(0.3, comment.score / 50.0)
        # Penalize deleted items
        if comment.author == "[deleted]" or comment.body == "[deleted]":
            score *= 0.5
        return round(max(0.0, min(1.0, score)), 3)
