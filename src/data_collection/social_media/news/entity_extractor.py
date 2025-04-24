"""
Entity extraction utilities for news articles.

This module provides functionality to extract relevant entities like stock symbols,
company names, and categories from news articles.
"""

import re
from typing import List, Set, Dict, Optional, Tuple

import nltk
from loguru import logger

# Download required NLTK data if not already present
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', quiet=True)
    
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords', quiet=True)


class EntityExtractor:
    """
    Extracts relevant entities from news articles like stock symbols and categories.
    """
    
    def __init__(
        self,
        stock_symbols_path: Optional[str] = None,
        company_names_path: Optional[str] = None,
        financial_terms_path: Optional[str] = None
    ):
        """
        Initialize the entity extractor.
        
        Args:
            stock_symbols_path: Path to file containing stock symbols (one per line)
            company_names_path: Path to file containing company names (one per line)
            financial_terms_path: Path to file containing financial terms (one per line)
        """
        self.stock_symbols = self._load_set_from_file(stock_symbols_path) if stock_symbols_path else set()
        self.company_names = self._load_set_from_file(company_names_path) if company_names_path else set()
        self.financial_terms = self._load_set_from_file(financial_terms_path) if financial_terms_path else set()
        
        # Fallback to basic default symbols if no file provided
        if not self.stock_symbols:
            self.stock_symbols = {
                'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'AMZN', 'META', 'TSLA', 'NVDA', 'BRK-A', 'BRK-B',
                'JPM', 'V', 'PG', 'DIS', 'NFLX', 'INTC', 'CSCO', 'KO', 'PEP', 'ADBE'
            }
            
        # Common financial categories
        self.categories = {
            'earnings', 'merger', 'acquisition', 'ipo', 'bankruptcy', 'stock', 'market',
            'economy', 'finance', 'banking', 'investment', 'trading', 'cryptocurrency',
            'regulation', 'tech', 'energy', 'healthcare', 'retail', 'real-estate'
        }
        
        # Regex for extracting stock symbols (typically 1-5 uppercase letters, may include -)
        self.symbol_pattern = re.compile(r'\b[A-Z]{1,5}(?:-[A-Z])?(?:\.?[A-Z])?(?:\.[A-Z]){0,1}\b')
        
        logger.info(f"EntityExtractor initialized with {len(self.stock_symbols)} stock symbols")
    
    def extract_symbols(self, text: str) -> List[str]:
        """
        Extract stock symbols from text.
        
        Args:
            text: The text to analyze
            
        Returns:
            List of stock symbols found in the text
        """
        if not text:
            return []
            
        # Find all potential stock symbols using regex
        potential_symbols = self.symbol_pattern.findall(text)
        
        # Filter to only known symbols
        known_symbols = [symbol for symbol in potential_symbols if symbol in self.stock_symbols]
        
        return list(set(known_symbols))  # Remove duplicates
    
    def extract_categories(self, text: str) -> List[str]:
        """
        Extract financial categories from text.
        
        Args:
            text: The text to analyze
            
        Returns:
            List of categories found in the text
        """
        if not text:
            return []
            
        # Convert to lowercase for category matching
        lower_text = text.lower()
        
        # Find all matching categories
        found_categories = [category for category in self.categories 
                            if category in lower_text]
        
        return list(set(found_categories))  # Remove duplicates
    
    def calculate_relevance_score(self, text: str, title: str = "") -> float:
        """
        Calculate relevance score for financial context based on content analysis.
        
        Args:
            text: The main text content
            title: Optional title text
            
        Returns:
            Relevance score between 0.0 and 1.0
        """
        if not text:
            return 0.0
            
        # Combine title and text, with title weighted more heavily
        combined_text = f"{title} {title} {text}" if title else text
        combined_text = combined_text.lower()
        
        # Count financial terms
        term_count = sum(1 for term in self.financial_terms if term.lower() in combined_text)
        
        # Count stock symbols
        symbols = self.extract_symbols(combined_text)
        symbol_count = len(symbols)
        
        # Count financial categories
        categories = self.extract_categories(combined_text)
        category_count = len(categories)
        
        # Calculate weighted score
        # Higher weights for symbols (direct stock mentions)
        base_score = (symbol_count * 3 + term_count + category_count * 2) / 20
        
        # Clamp to range 0.0 - 1.0
        return min(max(base_score, 0.0), 1.0)
    
    def enrich_article(self, article_dict: Dict) -> Dict:
        """
        Enrich a news article with extracted entities and relevance score.
        
        Args:
            article_dict: Dictionary containing article data
            
        Returns:
            Enriched article dictionary
        """
        if not article_dict:
            return article_dict
            
        # Create combined text for entity extraction
        title = article_dict.get('title', '')
        description = article_dict.get('description', '')
        content = article_dict.get('content', '')
        
        combined_text = f"{title} {description} {content}"
        
        # Extract entities
        symbols = self.extract_symbols(combined_text)
        categories = self.extract_categories(combined_text)
        relevance_score = self.calculate_relevance_score(content, title)
        
        # Add to article dict
        article_dict['symbols'] = symbols
        article_dict['categories'] = categories
        article_dict['relevance_score'] = relevance_score
        
        return article_dict
    
    @staticmethod
    def _load_set_from_file(file_path: str) -> Set[str]:
        """
        Load a set of strings from a file (one string per line).
        
        Args:
            file_path: Path to the file
            
        Returns:
            Set of strings from the file
        """
        try:
            with open(file_path, 'r') as f:
                return {line.strip() for line in f if line.strip()}
        except Exception as e:
            logger.error(f"Error loading data from {file_path}: {e}")
            return set() 