"""
Validators for social media data.
"""

import re
from typing import Optional, Tuple

from src.data_collection import logger
from src.data_collection.social_data.validation import BaseSocialValidator
from src.data_collection.models import SocialMediaData


class TwitterValidator(BaseSocialValidator):
    """Validates Twitter data."""
    
    def __init__(self, max_content_length: int = 280, min_content_length: int = 1):
        """
        Initialize the Twitter validator.
        
        Args:
            max_content_length: Maximum allowed content length
            min_content_length: Minimum allowed content length
        """
        super().__init__()
        self.max_content_length = max_content_length
        self.min_content_length = min_content_length
    
    def validate(self, data: SocialMediaData) -> Tuple[bool, Optional[str]]:
        """
        Validate a SocialMediaData object from Twitter.
        
        Args:
            data: The SocialMediaData object to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check source
        if data.source != 'twitter':
            return False, f"Invalid source: {data.source}, expected 'twitter'"
        
        # Check for missing required fields
        if not data.post_id:
            return False, "Post ID is required"
        
        if not data.author:
            return False, "Author is required"
        
        if data.timestamp <= 0:
            return False, "Invalid timestamp"
        
        # Validate content
        if not data.content:
            return False, "Content is required"
        
        content_length = len(data.content)
        if not (self.min_content_length <= content_length <= self.max_content_length):
            return False, f"Content length {content_length} out of range [{self.min_content_length}, {self.max_content_length}]"
        
        # Validate metrics
        if data.likes < 0:
            return False, f"Negative likes count: {data.likes}"
        
        if data.shares < 0:
            return False, f"Negative shares count: {data.shares}"
        
        if data.comments < 0:
            return False, f"Negative comments count: {data.comments}"
        
        # Check for spam or bot content (simple heuristic)
        if self._is_likely_spam(data.content):
            return False, "Content appears to be spam"
        
        return True, None
    
    def _is_likely_spam(self, content: str) -> bool:
        """
        Check if content is likely to be spam.
        
        Args:
            content: The text content to check
            
        Returns:
            True if content appears to be spam, False otherwise
        """
        # Simple spam detection heuristics
        
        # Check for excessive hashtags
        hashtag_count = content.count('#')
        if hashtag_count > 10:
            return True
        
        # Check for excessive URLs
        url_pattern = re.compile(r'https?://\S+')
        url_count = len(re.findall(url_pattern, content))
        if url_count > 3:
            return True
        
        # Check for all caps
        words = content.split()
        if len(words) > 5:
            caps_words = [w for w in words if w.isupper() and len(w) > 3]
            if len(caps_words) / len(words) > 0.7:  # More than 70% of words are all caps
                return True
        
        # Check for repetitive characters
        if re.search(r'(.)\1{4,}', content):  # Same character repeated 5+ times
            return True
        
        return False 