"""
NewsAPI data collector for MarketPulseAI.

This module provides functionality to collect news articles from NewsAPI.org,
validate them, and publish them to Kafka topics for further processing.
"""

import asyncio
import datetime
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Union

import aiohttp
import backoff
from loguru import logger

from src.common.messaging.kafka_producer import KafkaProducerWrapper
from src.data_collection.base_collector import BaseCollector
from src.data_collection.social_media.news.entity_extractor import EntityExtractor
from src.data_collection.social_media.news.models import NewsApiResponse, EnrichedNewsArticle
from src.data_collection.social_media.news.validator import NewsArticleValidator


class NewsApiCollector(BaseCollector):
    """
    Collects financial news data from NewsAPI.org.
    
    Features:
    - Configurable API parameters (queries, sources, categories)
    - Rate limit handling with exponential backoff
    - Data validation and enrichment
    - Kafka integration for streaming pipeline
    - Persistence for deduplication and tracking
    """
    
    # Constants for NewsAPI
    BASE_URL = "https://newsapi.org/v2"
    ENDPOINTS = {
        "everything": f"{BASE_URL}/everything",
        "top_headlines": f"{BASE_URL}/top-headlines"
    }
    
    # Constants for rate limiting
    MAX_REQUESTS_PER_DAY = 100  # Free tier limit, adjust as needed
    REQUEST_COOLDOWN = 60  # seconds between requests to avoid rate limits
    
    def __init__(
        self,
        api_key: str,
        bootstrap_servers: Union[str, List[str]],
        topic_name: str = "news-articles",
        error_topic_name: str = "news-errors",
        queries: Optional[List[str]] = None,
        sources: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        countries: Optional[List[str]] = None,
        languages: Optional[List[str]] = None,
        page_size: int = 100,
        days_to_fetch: int = 7,
        persistence_dir: Optional[str] = None,
        stock_symbols_path: Optional[str] = None,
        collector_name: str = "NewsApiCollector",
    ):
        """
        Initialize the NewsAPI collector.
        
        Args:
            api_key: NewsAPI API key
            bootstrap_servers: Kafka bootstrap servers
            topic_name: Kafka topic for valid articles
            error_topic_name: Kafka topic for invalid articles
            queries: List of search queries to use
            sources: List of news sources to filter by
            categories: List of news categories to filter by
            countries: List of country codes to filter by
            languages: List of language codes to filter by
            page_size: Number of articles per request
            days_to_fetch: Number of days of news to fetch
            persistence_dir: Directory to store processed article IDs
            stock_symbols_path: Path to file with stock symbols
            collector_name: Name for the collector instance
        """
        super().__init__(collector_name=collector_name)
        
        self.api_key = api_key
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.error_topic_name = error_topic_name
        
        # Default to financial news queries if none provided
        self.queries = queries or [
            "stock market", "financial news", "stock price", 
            "market analysis", "investment", "earnings"
        ]
        
        self.sources = sources
        self.categories = categories or ["business", "finance"]
        self.countries = countries
        self.languages = languages or ["en"]
        
        self.page_size = page_size
        self.days_to_fetch = days_to_fetch
        
        # Set up persistence directory
        self.persistence_dir = Path(persistence_dir) if persistence_dir else None
        if self.persistence_dir:
            self.persistence_dir.mkdir(exist_ok=True, parents=True)
            self.processed_ids_file = self.persistence_dir / "processed_news_ids.json"
            self.processed_ids = self._load_processed_ids()
        else:
            self.processed_ids = set()
            
        # Create components
        self.validator = NewsArticleValidator()
        self.entity_extractor = EntityExtractor(stock_symbols_path=stock_symbols_path)
        
        # Internal state
        self.producer = None
        self.running = False
        self.last_request_time = 0
        self.session = None
        
        logger.info(f"{collector_name} initialized with {len(self.queries)} queries")
    
    async def async_collect(self) -> None:
        """
        Main collection method that runs the async collection process.
        """
        if self.running:
            logger.warning(f"{self.collector_name} is already running")
            return
            
        self.running = True
        
        try:
            # Initialize Kafka producer
            self.producer = KafkaProducerWrapper(
                bootstrap_servers=self.bootstrap_servers,
                client_id=f"{self.collector_name}-producer",
                acks="all"
            )
            
            # Initialize HTTP session
            self.session = aiohttp.ClientSession(
                headers={"X-Api-Key": self.api_key}
            )
            
            logger.info(f"{self.collector_name} starting collection")
            
            # Process each query
            for query in self.queries:
                if not self.running:
                    break
                    
                await self._collect_for_query(query)
                
                # Save progress after each query
                if self.persistence_dir:
                    self._save_processed_ids()
            
            logger.info(f"{self.collector_name} collection completed")
        
        except Exception as e:
            logger.exception(f"Error in news collection: {e}")
        finally:
            self.running = False
            await self._cleanup_async()
    
    def collect(self) -> None:
        """
        Synchronous wrapper for the async collection process.
        """
        asyncio.run(self.async_collect())
    
    def stop(self) -> None:
        """
        Stop the collection process gracefully.
        """
        super().stop()
        self.running = False
        
        # Save progress
        if self.persistence_dir:
            self._save_processed_ids()
    
    def cleanup(self) -> None:
        """
        Clean up resources.
        """
        super().cleanup()
        
        # Close Kafka producer
        if self.producer:
            self.producer.close()
            self.producer = None
            
        logger.info(f"{self.collector_name} cleanup completed")
    
    async def _cleanup_async(self) -> None:
        """
        Async cleanup for resources.
        """
        # Close HTTP session
        if self.session:
            await self.session.close()
            self.session = None
            
        # Close Kafka producer
        if self.producer:
            self.producer.close()
            self.producer = None
    
    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=3,
        max_time=30
    )
    async def _fetch_articles(self, params: Dict[str, Any]) -> Optional[NewsApiResponse]:
        """
        Fetch articles from NewsAPI with retry logic.
        
        Args:
            params: Query parameters for the API
            
        Returns:
            NewsApiResponse object if successful, None otherwise
        """
        # Respect rate limits
        await self._respect_rate_limit()
        
        endpoint = self.ENDPOINTS["everything"]
        
        try:
            logger.debug(f"Requesting from {endpoint} with params: {params}")
            
            async with self.session.get(endpoint, params=params) as response:
                self.last_request_time = time.time()
                
                # Check response status
                if response.status == 429:  # Too many requests
                    retry_after = int(response.headers.get("Retry-After", "60"))
                    logger.warning(f"Rate limited by NewsAPI. Retry after {retry_after}s")
                    await asyncio.sleep(retry_after)
                    return None
                
                if response.status != 200:
                    logger.error(f"NewsAPI request failed: {response.status} - {await response.text()}")
                    return None
                
                data = await response.json()
                
                if data.get("status") == "error":
                    logger.error(f"NewsAPI error: {data.get('code')} - {data.get('message')}")
                    return None
                
                # Validate response
                try:
                    return NewsApiResponse(**data)
                except Exception as e:
                    logger.error(f"Failed to validate NewsAPI response: {e}")
                    return None
        
        except Exception as e:
            logger.exception(f"Error fetching from NewsAPI: {e}")
            return None
    
    async def _collect_for_query(self, query: str) -> None:
        """
        Collect and process articles for a specific query.
        
        Args:
            query: The search query to use
        """
        logger.info(f"Collecting articles for query: '{query}'")
        
        # Calculate date range (from days_to_fetch days ago until today)
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=self.days_to_fetch)
        
        # Format dates for NewsAPI
        from_date = start_date.strftime("%Y-%m-%d")
        to_date = end_date.strftime("%Y-%m-%d")
        
        # Build query parameters
        params = {
            "q": query,
            "pageSize": self.page_size,
            "sortBy": "publishedAt",
            "from": from_date,
            "to": to_date,
        }
        
        # Add optional filters
        if self.sources:
            params["sources"] = ",".join(self.sources)
        if self.languages:
            params["language"] = ",".join(self.languages)
        
        # Fetch and process articles
        response = await self._fetch_articles(params)
        
        if not response:
            logger.warning(f"No response for query: '{query}'")
            return
            
        processed_count = 0
        
        for article in response.articles:
            if not self.running:
                break
                
            # Convert to dict for validation
            article_dict = article.dict()
            
            # Skip if already processed
            article_id = self.validator._generate_article_id(article)
            if article_id in self.processed_ids:
                logger.debug(f"Skipping already processed article: {article_id}")
                continue
                
            # Validate article
            is_valid, validated_article, errors = self.validator.validate(article_dict)
            
            if is_valid and validated_article:
                # Enrich with entity extraction
                validated_dict = validated_article.dict()
                enriched_dict = self.entity_extractor.enrich_article(validated_dict)
                
                # Publish to Kafka
                success = self.producer.send_message(
                    topic=self.topic_name,
                    value=enriched_dict,
                    key=article_id
                )
                
                if success:
                    # Mark as processed
                    self.processed_ids.add(article_id)
                    processed_count += 1
                else:
                    logger.error(f"Failed to publish article to Kafka: {article_id}")
            else:
                # Log validation errors
                logger.warning(f"Validation failed for article: {errors}")
                
                # Publish error to error topic
                error_data = {
                    "article": article_dict,
                    "errors": errors,
                    "timestamp": datetime.datetime.now().isoformat()
                }
                
                self.producer.send_message(
                    topic=self.error_topic_name,
                    value=error_data
                )
        
        logger.info(f"Processed {processed_count} new articles for query: '{query}'")
    
    async def _respect_rate_limit(self) -> None:
        """
        Ensure we don't exceed API rate limits by waiting if needed.
        """
        elapsed = time.time() - self.last_request_time
        
        if elapsed < self.REQUEST_COOLDOWN:
            wait_time = self.REQUEST_COOLDOWN - elapsed
            logger.debug(f"Rate limiting: waiting {wait_time:.2f}s before next request")
            await asyncio.sleep(wait_time)
    
    def _load_processed_ids(self) -> Set[str]:
        """
        Load the set of already processed article IDs from disk.
        
        Returns:
            Set of processed article IDs
        """
        if not self.persistence_dir or not self.processed_ids_file.exists():
            return set()
            
        try:
            with open(self.processed_ids_file, 'r') as f:
                return set(json.load(f))
        except Exception as e:
            logger.error(f"Error loading processed IDs: {e}")
            return set()
    
    def _save_processed_ids(self) -> None:
        """
        Save the set of processed article IDs to disk.
        """
        if not self.persistence_dir:
            return
            
        try:
            with open(self.processed_ids_file, 'w') as f:
                json.dump(list(self.processed_ids), f)
            logger.debug(f"Saved {len(self.processed_ids)} processed IDs to {self.processed_ids_file}")
        except Exception as e:
            logger.error(f"Error saving processed IDs: {e}") 