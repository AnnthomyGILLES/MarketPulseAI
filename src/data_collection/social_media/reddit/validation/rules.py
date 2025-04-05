from datetime import datetime, timezone, timedelta
from typing import Tuple, Optional, Dict, Any

from loguru import logger

from .schema import RedditPost, RedditComment, ValidatedRedditItem


class RedditValidationRules:
    """Business rules for validating Reddit data beyond schema validation."""

    @staticmethod
    def check_timestamp_consistency(
        data: ValidatedRedditItem,
    ) -> Tuple[bool, Optional[str]]:
        """Check if collection time is reasonably after creation time."""
        try:
            # Timestamps are already validated ISO strings in UTC ('Z') by the schema
            created_dt = datetime.fromisoformat(
                data.created_datetime.replace("Z", "+00:00")
            )
            collected_dt = datetime.fromisoformat(
                data.collection_timestamp.replace("Z", "+00:00")
            )

            # Allow for some clock skew or processing delay, but collection shouldn't be significantly before creation
            if collected_dt < created_dt - timedelta(minutes=5):
                msg = f"Collection timestamp {data.collection_timestamp} is significantly before creation timestamp {data.created_datetime}"
                logger.warning(f"{msg} for item {data.id}")
                return False, msg

            # Check if collection timestamp is excessively old (e.g., > 7 days), might indicate stale collector data
            if datetime.now(timezone.utc) - collected_dt > timedelta(days=7):
                 msg = f"Collection timestamp {data.collection_timestamp} is older than 7 days"
                 logger.warning(f"{msg} for item {data.id}")
                 # Decide if this is an error or just a warning. Returning True for now.
                 # return False, msg

            # Check if creation timestamp is suspiciously far in the future
            if created_dt > datetime.now(timezone.utc) + timedelta(minutes=10):
                msg = f"Creation timestamp {data.created_datetime} is suspiciously far in the future"
                logger.warning(f"{msg} for item {data.id}")
                return False, msg


            return True, None
        except (ValueError, TypeError, AttributeError) as e:
            # Should not happen if schema validation passed, but good to have a failsafe
            msg = f"Could not compare timestamps: {e}"
            logger.warning(f"{msg} for item {getattr(data, 'id', 'UNKNOWN_ID')}")
            return False, msg

    @staticmethod
    def check_post_quality(data: RedditPost) -> Tuple[bool, Optional[str]]:
        """Perform basic quality checks specific to posts."""
        warnings = []
        if data.author == "[deleted]" and (not data.title or data.title == "[deleted]"):
            warnings.append("Post appears to be deleted (author and title).")
            logger.warning(f"Post {data.id} appears deleted.")
            # Depending on requirements, this could be a failure: return False, warnings[0]

        if len(data.title) < 5:
            warnings.append("Post has a very short title.")
            logger.debug(f"Post {data.id} has a very short title: '{data.title}'")

        if data.is_self and not data.selftext and len(data.title) < 20:
            warnings.append("Self-post with no body and short title.")
            logger.debug(f"Post {data.id} is a self-post with no body and short title.")

        # For now, these are warnings, not validation failures
        return True, "; ".join(warnings) if warnings else None

    @staticmethod
    def check_comment_quality(data: RedditComment) -> Tuple[bool, Optional[str]]:
        """Perform basic quality checks specific to comments."""
        warnings = []
        if data.author == "[deleted]" and data.body == "[deleted]":
            warnings.append("Comment appears to be fully deleted.")
            logger.warning(f"Comment {data.id} appears fully deleted.")
            # Depending on requirements, this could be a failure: return False, warnings[0]

        if len(data.body) < 3:
            warnings.append("Comment has a very short body.")
            logger.debug(f"Comment {data.id} has a very short body.")

        # For now, these are warnings, not validation failures
        return True, "; ".join(warnings) if warnings else None 