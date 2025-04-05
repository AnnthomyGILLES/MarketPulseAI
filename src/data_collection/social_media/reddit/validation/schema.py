import re
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Union

from loguru import logger
from pydantic import BaseModel, Field, field_validator, model_validator

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
    created_datetime: Optional[str] = None  # Will be populated by model_validator

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

    @model_validator(mode="before")
    @classmethod
    def convert_created_utc(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Convert created_utc float to ISO string and add to values."""
        created_utc_float = values.get("created_utc")
        created_dt_iso = None
        if isinstance(created_utc_float, (int, float)):
            try:
                dt_utc = datetime.fromtimestamp(created_utc_float, timezone.utc)
                created_dt_iso = dt_utc.isoformat().replace("+00:00", "Z")
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"Could not convert created_utc '{created_utc_float}' to datetime: {e}"
                )
                # Let potential validation error be raised later if needed
        elif "created_datetime" not in values:
            # Handle cases where created_utc might be missing but a string version exists
            dt_str = values.get("post_created_datetime") or values.get(
                "comment_created_datetime"
            )
            if dt_str:
                try:
                    created_dt_iso = validate_iso_utc_datetime(dt_str)
                except ValueError:
                    logger.warning(
                        f"Could not validate existing datetime string: {dt_str}"
                    )

        if created_dt_iso:
            values["created_datetime"] = created_dt_iso
        elif "created_datetime" not in values:
             # If still no valid created_datetime, set to None to trigger validation later
            values["created_datetime"] = None

        return values

    @field_validator("created_datetime")
    @classmethod
    def check_created_datetime(cls, v: Optional[str]) -> str:
        """Validates the derived created_datetime field."""
        if v is None:
            raise ValueError("Creation datetime could not be determined or validated.")
        # Format validation happens during assignment or via validate_iso_utc_datetime
        return v

    @field_validator("detected_symbols")
    @classmethod
    def check_detected_symbols(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        return validate_symbol_list(v)


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
    collection_method: Optional[str] = Field(
        None,
        description="Method used to collect this post (e.g., 'hot', 'symbol_search').",
    )

    @field_validator("content_type")
    @classmethod
    def check_post_content_type(cls, v: str) -> str:
        if v != "post":
            raise ValueError("content_type must be 'post' for RedditPost schema")
        return v


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

    @field_validator("content_type")
    @classmethod
    def check_comment_content_type(cls, v: str) -> str:
        if v != "comment":
            raise ValueError("content_type must be 'comment' for RedditComment schema")
        return v

    @field_validator("post_id", "parent_id")
    @classmethod
    def check_id_format(cls, v: str) -> str:
        if not v or not isinstance(v, str):
            raise ValueError("post_id and parent_id must be non-empty strings")
        return v

# Union type for validated models
ValidatedRedditItem = Union[RedditPost, RedditComment] 