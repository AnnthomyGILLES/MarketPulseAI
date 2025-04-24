"""
Business validation rules for news data.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, Optional


class NewsValidationRules:
    """
    Business rules for validating news articles beyond schema validation.
    """
    
    @staticmethod
    def check_title_quality(data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Check if the article title meets quality standards.
        
        Args:
            data: The article data dictionary
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        title = data.get("title", "")
        
        if not title:
            return False, "Missing title"
            
        min_length = 10  # Minimum meaningful title length
        if len(title) < min_length:
            return False, f"Title too short (minimum {min_length} characters)"
            
        # Title should not be all uppercase (indicates clickbait)
        if title.isupper():
            return False, "Title is all uppercase"
            
        return True, None
        
    @staticmethod
    def check_content_quality(data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Check if the article content meets quality standards.
        
        Args:
            data: The article data dictionary
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check either description or content exists
        description = data.get("description") or ""
        content = data.get("content") or ""
        
        if not description and not content:
            return False, "Both description and content are missing"
            
        # Check minimum content length across either field
        min_content_length = 50
        max_content_length = len(description) if description else 0
        if content:
            max_content_length = max(max_content_length, len(content))
            
        if max_content_length < min_content_length:
            return False, f"Content too short (minimum {min_content_length} characters)"
            
        return True, None
        
    @staticmethod
    def check_timestamp_recency(data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Check if the article timestamp is recent relative to the collection time.
        
        Args:
            data: The article data dictionary
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        published_at = data.get("publishedAt")
        if not published_at:
            return False, "Missing publication timestamp"
            
        try:
            # Parse the timestamp
            pub_time = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            
            # Check if publication date is in the future
            now = datetime.utcnow()
            if pub_time > now + timedelta(hours=1):  # Allow 1 hour for timezone differences
                return False, f"Publication date is in the future: {published_at}"
                
            # Check if article is too old (e.g., more than 30 days)
            max_age_days = 30
            if pub_time < now - timedelta(days=max_age_days):
                return False, f"Article too old (> {max_age_days} days)"
                
            return True, None
        except (ValueError, TypeError) as e:
            return False, f"Invalid publication timestamp format: {e}" 