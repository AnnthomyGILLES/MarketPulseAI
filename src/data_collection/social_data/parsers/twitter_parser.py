"""
Twitter data parser.
"""

from typing import Any, Dict, List, Optional

from src.data_collection import logger
from src.data_collection.social_data.parsers import BaseSocialParser
from src.data_collection.models import SocialMediaData


class TwitterParser(BaseSocialParser):
    """Parses data from Twitter."""
    
    def __init__(self, extract_symbols_func=None):
        """
        Initialize the Twitter parser.
        
        Args:
            extract_symbols_func: Optional function to extract stock symbols from text
        """
        super().__init__()
        self.extract_symbols_func = extract_symbols_func
    
    def parse(self, raw_data: Dict[str, Any]) -> List[SocialMediaData]:
        """
        Parse raw Twitter data into standardized SocialMediaData objects.
        
        Args:
            raw_data: The raw data from the TwitterCollector
            
        Returns:
            List of SocialMediaData objects
        """
        try:
            tweets = raw_data['tweets']
            collection_time = raw_data['collection_time']
            source = raw_data['source']
            search_term = raw_data['search_term']
            
            parsed_tweets = []
            
            for tweet in tweets:
                try:
                    # Extract text, handling retweets
                    if hasattr(tweet, 'retweeted_status'):
                        text = tweet.retweeted_status.full_text
                    else:
                        text = tweet.full_text
                    
                    # Extract stock symbols from tweet text if function provided
                    symbols = []
                    if self.extract_symbols_func:
                        symbols = self.extract_symbols_func(text)
                    
                    # Create SocialMediaData object
                    tweet_data = SocialMediaData(
                        source='twitter',
                        timestamp=tweet.created_at.timestamp(),
                        content=text,
                        author=tweet.user.screen_name,
                        post_id=tweet.id_str,
                        symbols=symbols,
                        likes=tweet.favorite_count,
                        shares=tweet.retweet_count,
                        comments=0  # Twitter API doesn't easily provide this
                    )
                    
                    parsed_tweets.append(tweet_data)
                    
                except Exception as e:
                    logger.error(f"Error parsing individual tweet: {e}")
            
            logger.info(f"Parsed {len(parsed_tweets)} tweets for search term '{search_term}'")
            return parsed_tweets
            
        except Exception as e:
            logger.error(f"Error parsing Twitter data: {e}")
            return [] 