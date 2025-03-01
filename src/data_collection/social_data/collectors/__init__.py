"""
Base classes for social media data collectors.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from src.data_collection import logger


class BaseSocialCollector(ABC):
    """Base class for all social media data collectors."""
    
    def __init__(self, search_terms: List[str]):
        """Initialize the collector with search terms."""
        self.search_terms = search_terms
        self.last_collection_time = {}
        logger.info(f"Initialized {self.__class__.__name__} with {len(search_terms)} search terms")
    
    @abstractmethod
    def collect(self, search_term: Optional[str] = None, count: int = 100) -> Dict[str, Any]:
        """
        Collect raw data for a search term.
        
        Args:
            search_term: Optional specific search term to collect. If None, use first search term.
            count: Maximum number of items to collect
            
        Returns:
            Dictionary containing the raw collected data.
        """
        pass
    
    def collect_all(self, count: int = 100) -> Dict[str, Dict[str, Any]]:
        """
        Collect data for all search terms.
        
        Args:
            count: Maximum number of items to collect per search term
            
        Returns:
            Dictionary mapping search terms to their raw collected data.
        """
        results = {}
        for term in self.search_terms:
            try:
                results[term] = self.collect(term, count)
            except Exception as e:
                logger.error(f"Error collecting data for search term '{term}': {e}")
        
        return results 