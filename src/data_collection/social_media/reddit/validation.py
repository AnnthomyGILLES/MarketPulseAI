# src/data_collection/social_media/reddit/validation.py
import json
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("reddit_validator")


class RedditContentValidator:
    """
    Validates Reddit content for data quality and completeness.
    """

    def __init__(self, config=None):
        """
        Initialize the content validator.

        Args:
            config (dict, optional): Configuration dictionary.
        """
        self.config = config or {}

        # Required fields for different content types
        self.required_fields = {
            "post": ["id", "title", "subreddit", "author", "created_utc"],
            "comment": ["id", "body", "subreddit", "author", "created_utc", "post_id"],
        }

        # Maximum content lengths
        self.max_lengths = {
            "title": 300,  # Reddit's max title length
            "selftext": 40000,  # Reddit's max post length
            "body": 10000,  # Reddit's max comment length
        }

        logger.info("Content validator initialized")

    def validate_required_fields(self, content):
        """
        Check if content has all required fields.

        Args:
            content (dict): Content to validate

        Returns:
            bool: Whether content has all required fields
        """
        content_type = content.get("content_type")
        if not content_type:
            logger.warning("Content missing content_type field")
            return False

        if content_type not in self.required_fields:
            logger.warning(f"Unknown content_type: {content_type}")
            return False

        for field in self.required_fields[content_type]:
            if field not in content or content[field] is None:
                logger.warning(f"Content missing required field: {field}")
                return False

        return True

    def validate_field_lengths(self, content):
        """
        Validate field lengths against maximum allowed.

        Args:
            content (dict): Content to validate

        Returns:
            bool: Whether all fields are within length limits
        """
        for field, max_length in self.max_lengths.items():
            if field in content and isinstance(content[field], str):
                if len(content[field]) > max_length:
                    logger.warning(f"Field {field} exceeds maximum length")
                    return False

        return True

    def validate_timestamps(self, content):
        """
        Validate timestamp fields.

        Args:
            content (dict): Content to validate

        Returns:
            bool: Whether timestamps are valid
        """
        # Check created_utc is a valid timestamp
        if "created_utc" in content:
            try:
                created_utc = float(content["created_utc"])
                # Check if timestamp is in the future
                if created_utc > datetime.now().timestamp():
                    logger.warning("created_utc timestamp is in the future")
                    return False

                # Check if timestamp is too old (older than 5 years)
                if created_utc < (
                    datetime.now().timestamp() - 157680000
                ):  # 5 years in seconds
                    logger.warning("created_utc timestamp is too old")
                    return False
            except (ValueError, TypeError):
                logger.warning("Invalid created_utc timestamp")
                return False

        # Check collected_at is a valid timestamp
        if "collected_at" in content:
            try:
                collected_at = float(content["collected_at"])
                # Check if timestamp is in the future
                if collected_at > datetime.now().timestamp():
                    logger.warning("collected_at timestamp is in the future")
                    return False
            except (ValueError, TypeError):
                logger.warning("Invalid collected_at timestamp")
                return False

        return True

    def check_for_spam(self, content):
        """
        Check if content is likely spam.

        Args:
            content (dict): Content to validate

        Returns:
            bool: Whether content is not spam (True if valid)
        """
        # Check for [deleted] or [removed] content
        if content.get("content_type") == "post":
            if (
                content.get("title") == "[deleted]"
                or content.get("selftext") == "[removed]"
            ):
                logger.warning("Post is deleted or removed")
                return False
        elif content.get("content_type") == "comment":
            if content.get("body") == "[deleted]" or content.get("body") == "[removed]":
                logger.warning("Comment is deleted or removed")
                return False

        # Check for extremely short content
        if content.get("content_type") == "post":
            if len(content.get("title", "")) < 3:
                logger.warning("Post title too short")
                return False
            if content.get("is_self", False) and len(content.get("selftext", "")) < 5:
                logger.warning("Self post text too short")
                return False
        elif content.get("content_type") == "comment":
            if len(content.get("body", "")) < 5:
                logger.warning("Comment too short")
                return False

        return True

    def validate(self, content):
        """
        Perform all validations on content.

        Args:
            content (dict): Content to validate

        Returns:
            tuple: (is_valid, validation_errors)
        """
        validation_errors = []

        # Check required fields
        if not self.validate_required_fields(content):
            validation_errors.append("Missing required fields")

        # Check field lengths
        if not self.validate_field_lengths(content):
            validation_errors.append("Field length exceeded")

        # Check timestamps
        if not self.validate_timestamps(content):
            validation_errors.append("Invalid timestamps")

        # Check for spam
        if not self.check_for_spam(content):
            validation_errors.append("Content appears to be spam or deleted")

        # Content is valid if no errors found
        is_valid = len(validation_errors) == 0

        # Add validation result to content
        validated_content = content.copy()
        validated_content["is_valid"] = is_valid

        if not is_valid:
            validated_content["validation_errors"] = validation_errors
            logger.warning(
                f"Content {content.get('id')} failed validation: {', '.join(validation_errors)}"
            )

        return is_valid, validated_content


# Example usage
if __name__ == "__main__":
    # Test the validator
    validator = RedditContentValidator()

    test_content = {
        "id": "abc123",
        "content_type": "post",
        "title": "Test Post",
        "selftext": "This is a test post",
        "subreddit": "wallstreetbets",
        "author": "test_user",
        "created_utc": datetime.now().timestamp() - 3600,  # 1 hour ago
        "collected_at": datetime.now().timestamp(),
    }

    is_valid, validated_content = validator.validate(test_content)
    print(f"Content valid: {is_valid}")
    print(json.dumps(validated_content, indent=2))
