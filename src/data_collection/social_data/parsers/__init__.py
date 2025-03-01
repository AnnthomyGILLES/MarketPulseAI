"""
Base classes for social media data parsers.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from src.data_collection import logger
from src.data_collection.models import SocialMediaData


class BaseSocialParser(ABC):
    """Base class for all social media data parsers."""
    
    def __init__(self):
        """Initialize the parser."""
        logger.info(f"Initialized {self.__class__.__name__}")
    
    @abstractmethod
    def parse(self, raw_data: Dict[str, Any]) -> List[SocialMediaData]:
        """
        Parse raw social media data into standardized SocialMediaData objects.
        
        Args:
            raw_data: The raw data from a collector
            
        Returns:
            List of SocialMediaData objects
        """
        pass
    
    def parse_batch(self, raw_data_batch: Dict[str, Dict[str, Any]]) -> List[SocialMediaData]:
        """
        Parse a batch of raw data for multiple search terms.
        
        Args:
            raw_data_batch: Dictionary mapping search terms to their raw data
            
        Returns:
            List of SocialMediaData objects
        """
        results = []
        for search_term, raw_data in raw_data_batch.items():
            try:
                parsed_data = self.parse(raw_data)
                results.extend(parsed_data)
            except Exception as e:
                logger.error(f"Error parsing data for search term '{search_term}': {e}")
        
        return results