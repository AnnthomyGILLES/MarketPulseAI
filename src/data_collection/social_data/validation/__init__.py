"""
Base classes for social media data validation.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Union

from src.data_collection import logger
from src.data_collection.models import SocialMediaData


class BaseSocialValidator(ABC):
    """Base class for all social media data validators."""
    
    def __init__(self):
        """Initialize the validator."""
        logger.info(f"Initialized {self.__class__.__name__}")
    
    @abstractmethod
    def validate(self, data: SocialMediaData) -> Tuple[bool, Optional[str]]:
        """
        Validate a SocialMediaData object.
        
        Args:
            data: The SocialMediaData object to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        pass
    
    def validate_batch(self, data_batch: List[SocialMediaData]) -> Dict[str, Union[List[SocialMediaData], List[Tuple[SocialMediaData, str]]]]:
        """
        Validate a batch of SocialMediaData objects.
        
        Args:
            data_batch: List of SocialMediaData objects to validate
            
        Returns:
            Dictionary with 'valid' and 'invalid' keys, where 'valid' contains valid SocialMediaData objects
            and 'invalid' contains tuples of (SocialMediaData, error_message)
        """
        valid = []
        invalid = []
        
        for data in data_batch:
            is_valid, error = self.validate(data)
            if is_valid:
                valid.append(data)
            else:
                invalid.append((data, error))
                logger.warning(f"Validation failed for social media post {data.post_id}: {error}")
        
        return {
            'valid': valid,
            'invalid': invalid
        } 