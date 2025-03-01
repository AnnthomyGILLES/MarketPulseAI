"""
Twitter data collector.
"""

import time
from typing import Any, Dict, List, Optional

import tweepy

from src.data_collection import (
    logger, TWITTER_SEARCH_TERMS, TWITTER_API_KEY, TWITTER_API_SECRET,
    TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET
)
from src.data_collection.social_data.collectors import BaseSocialCollector


class TwitterCollector(BaseSocialCollector):
    """Collects data from Twitter using the Twitter API."""
    
    def __init__(self, 
                 search_terms: List[str] = TWITTER_SEARCH_TERMS,
                 api_key: str = TWITTER_API_KEY,
                 api_secret: str = TWITTER_API_SECRET,
                 access_token: str = TWITTER_ACCESS_TOKEN,
                 access_secret: str = TWITTER_ACCESS_SECRET):
        """
        Initialize the Twitter collector.
        
        Args:
            search_terms: List of terms to search for on Twitter
            api_key: Twitter API key
            api_secret: Twitter API secret
            access_token: Twitter access token
            access_secret: Twitter access token secret
        """
        super().__init__(search_terms)
        
        # Initialize the Twitter API client
        auth = tweepy.OAuthHandler(api_key, api_secret)
        auth.set_access_token(access_token, access_secret)
        self.api = tweepy.API(auth, wait_on_rate_limit=True)
        
        # Validate API credentials
        try:
            self.api.verify_credentials()
            logger.info("Twitter API credentials verified successfully")
        except Exception as e:
            logger.error(f"Failed to verify Twitter API credentials: {e}")
    
    def collect(self, search_term: Optional[str] = None, count: int = 100) -> Dict[str, Any]:
        """
        Collect tweets matching a search term.
        
        Args:
            search_term: Term to search for. If None, use the first search term.
            count: Maximum number of tweets to collect
            
        Returns:
            Dictionary containing the raw collected tweets.
        """
        try:
            # Use the first search term if none provided
            term = search_term if search_term else self.search_terms[0]
            
            # Record collection time
            collection_time = time.time()
            self.last_collection_time[term] = collection_time
            
            # Collect tweets
            tweets = self.api.search_tweets(q=term, count=count, tweet_mode='extended')
            
            # Format the response
            result = {
                'search_term': term,
                'tweets': tweets,
                'collection_time': collection_time,
                'source': 'twitter'
            }
            
            logger.info(f"Collected {len(tweets)} tweets for search term '{term}'")
            return result
            
        except Exception as e:
            logger.error(f"Error collecting tweets for '{search_term}': {e}")
            raise 