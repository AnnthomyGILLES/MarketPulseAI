"""
MongoDB repository for news articles.

This module provides functionality to store and retrieve news articles from MongoDB,
supporting the MarketPulseAI news data pipeline.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union

import pymongo
from bson import ObjectId
from loguru import logger
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError, PyMongoError


class NewsArticleRepository:
    """
    Repository for news articles in MongoDB.
    
    Handles persistence of news articles and provides query methods for
    retrieving and analyzing news data.
    """
    
    def __init__(
        self,
        connection_string: str,
        database_name: str = "social_media",
        collection_name: str = "news_articles",
        sentiment_collection_name: str = "news_sentiment_timeseries"
    ):
        """
        Initialize the news article repository.
        
        Args:
            connection_string: MongoDB connection string
            database_name: Name of the database
            collection_name: Name of the news article collection
            sentiment_collection_name: Name of the sentiment timeseries collection
        """
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]
        self.sentiment_collection = self.db[sentiment_collection_name]
        
        logger.info(f"NewsArticleRepository initialized - connected to {database_name}.{collection_name}")
    
    def insert_article(self, article: Dict[str, Any]) -> Optional[str]:
        """
        Insert a news article into MongoDB.
        
        Args:
            article: Dictionary containing article data
            
        Returns:
            Document ID if successful, None if error
        """
        try:
            # Convert string dates to datetime objects if needed
            if isinstance(article.get("published_at"), str):
                article["published_at"] = datetime.fromisoformat(article["published_at"].replace("Z", "+00:00"))
                
            if isinstance(article.get("collection_timestamp"), str):
                article["collection_timestamp"] = datetime.fromisoformat(article["collection_timestamp"].replace("Z", "+00:00"))
            
            # Insert the article
            result = self.collection.insert_one(article)
            logger.debug(f"Inserted article with ID: {result.inserted_id}")
            return str(result.inserted_id)
            
        except DuplicateKeyError as e:
            logger.warning(f"Duplicate article not inserted: {article.get('article_id')} - {e}")
            return None
            
        except Exception as e:
            logger.error(f"Error inserting article: {e}")
            return None
    
    def update_article(self, article_id: str, update_data: Dict[str, Any]) -> bool:
        """
        Update an existing article in MongoDB.
        
        Args:
            article_id: The article_id field value of the article to update
            update_data: Dictionary containing fields to update
            
        Returns:
            True if successful, False otherwise
        """
        try:
            result = self.collection.update_one(
                {"article_id": article_id},
                {"$set": update_data}
            )
            
            success = result.modified_count > 0
            if success:
                logger.debug(f"Updated article: {article_id}")
            else:
                logger.warning(f"Article not found for update: {article_id}")
                
            return success
            
        except Exception as e:
            logger.error(f"Error updating article {article_id}: {e}")
            return False
    
    def get_article_by_id(self, article_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve an article by its article_id.
        
        Args:
            article_id: The article_id field value
            
        Returns:
            Dictionary containing the article or None if not found
        """
        try:
            article = self.collection.find_one({"article_id": article_id})
            return article
            
        except Exception as e:
            logger.error(f"Error retrieving article {article_id}: {e}")
            return None
    
    def get_articles_by_symbol(
        self,
        symbol: str,
        days: int = 7,
        limit: int = 100,
        min_relevance: float = 0.0,
        processed_only: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Retrieve articles mentioning a specific stock symbol.
        
        Args:
            symbol: The stock symbol to search for
            days: Number of days to look back
            limit: Maximum number of articles to return
            min_relevance: Minimum relevance score threshold
            processed_only: If True, only return articles with sentiment processed
            
        Returns:
            List of articles sorted by published_at (newest first)
        """
        try:
            # Calculate date cutoff
            cutoff_date = datetime.now() - timedelta(days=days)
            
            # Build query
            query = {
                "symbols": symbol,
                "published_at": {"$gte": cutoff_date},
                "relevance_score": {"$gte": min_relevance}
            }
            
            if processed_only:
                query["processed"] = True
            
            # Execute query
            articles = list(self.collection.find(
                query,
                sort=[("published_at", pymongo.DESCENDING)],
                limit=limit
            ))
            
            logger.debug(f"Retrieved {len(articles)} articles for symbol: {symbol}")
            return articles
            
        except Exception as e:
            logger.error(f"Error retrieving articles for symbol {symbol}: {e}")
            return []
    
    def get_unprocessed_articles(
        self,
        limit: int = 100,
        min_relevance: float = 0.3
    ) -> List[Dict[str, Any]]:
        """
        Retrieve articles that haven't had sentiment analysis performed.
        
        Args:
            limit: Maximum number of articles to return
            min_relevance: Minimum relevance score threshold
            
        Returns:
            List of unprocessed articles
        """
        try:
            query = {
                "processed": False,
                "relevance_score": {"$gte": min_relevance}
            }
            
            articles = list(self.collection.find(
                query,
                sort=[("published_at", pymongo.DESCENDING)],
                limit=limit
            ))
            
            return articles
            
        except Exception as e:
            logger.error(f"Error retrieving unprocessed articles: {e}")
            return []
    
    def mark_article_processed(
        self,
        article_id: str,
        sentiment_data: Dict[str, Any]
    ) -> bool:
        """
        Mark an article as having sentiment processed and store results.
        
        Args:
            article_id: The article_id field value
            sentiment_data: Dictionary containing sentiment analysis results
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Add processed timestamp
            sentiment_data["processed_at"] = datetime.now()
            
            result = self.collection.update_one(
                {"article_id": article_id},
                {
                    "$set": {
                        "processed": True,
                        "sentiment": sentiment_data
                    }
                }
            )
            
            return result.modified_count > 0
            
        except Exception as e:
            logger.error(f"Error marking article {article_id} as processed: {e}")
            return False
    
    def insert_sentiment_datapoint(
        self,
        symbol: str,
        sentiment_score: float,
        sentiment_magnitude: float,
        timestamp: datetime,
        article_ids: List[str],
        sources: List[str]
    ) -> Optional[str]:
        """
        Insert a sentiment timeseries datapoint for a symbol.
        
        Args:
            symbol: Stock symbol
            sentiment_score: Aggregated sentiment score
            sentiment_magnitude: Aggregated sentiment magnitude
            timestamp: Timestamp for the datapoint
            article_ids: List of article IDs included in the aggregation
            sources: List of news sources included in the aggregation
            
        Returns:
            Document ID if successful, None otherwise
        """
        try:
            datapoint = {
                "symbol": symbol,
                "sentiment_score": sentiment_score,
                "sentiment_magnitude": sentiment_magnitude,
                "timestamp": timestamp,
                "article_count": len(article_ids),
                "article_ids": article_ids,
                "sources": sources
            }
            
            result = self.sentiment_collection.insert_one(datapoint)
            return str(result.inserted_id)
            
        except Exception as e:
            logger.error(f"Error inserting sentiment datapoint for {symbol}: {e}")
            return None
    
    def get_sentiment_timeseries(
        self,
        symbol: str,
        days: int = 30,
        interval_hours: int = 4
    ) -> List[Dict[str, Any]]:
        """
        Get sentiment timeseries data for a symbol.
        
        Args:
            symbol: Stock symbol
            days: Number of days to look back
            interval_hours: Interval size in hours for aggregation
            
        Returns:
            List of sentiment datapoints
        """
        try:
            # Calculate date cutoff
            cutoff_date = datetime.now() - timedelta(days=days)
            
            pipeline = [
                # Match documents for the symbol and timeframe
                {
                    "$match": {
                        "symbol": symbol,
                        "timestamp": {"$gte": cutoff_date}
                    }
                },
                # Sort by timestamp
                {
                    "$sort": {"timestamp": 1}
                }
            ]
            
            results = list(self.sentiment_collection.aggregate(pipeline))
            return results
            
        except Exception as e:
            logger.error(f"Error retrieving sentiment timeseries for {symbol}: {e}")
            return []
    
    def close(self):
        """Close the MongoDB connection."""
        if self.client:
            self.client.close()
            logger.debug("MongoDB connection closed") 