"""
NewsAPI collector for financial news data.

Fetches financial news from NewsAPI.org and publishes to Kafka.
"""

import os
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Set

import requests
from loguru import logger

from src.data_collection.base_collector import BaseCollector
from src.common.messaging.kafka_producer import KafkaProducerWrapper
from src.utils.config import load_config


class NewsAPICollector(BaseCollector):
    """
    Collector for financial news data from NewsAPI.org.

    Fetches articles based on configured queries and filters,
    then publishes to Kafka for downstream processing.
    """

    def __init__(
        self,
        config_path: str,
        kafka_config_path: Optional[str] = None,
        collector_name: str = "NewsAPICollector",
    ):
        """
        Initialize the NewsAPI collector.

        Args:
            config_path: Path to the NewsAPI configuration file
            kafka_config_path: Path to the Kafka configuration file (optional)
            collector_name: Name identifier for the collector
        """
        super().__init__(collector_name=collector_name)

        # Load configurations
        self.config = load_config(config_path)
        self.api_key = os.getenv("NEWSAPI_KEY") or self.config["api"]["key"].replace(
            "${NEWSAPI_KEY}", ""
        )
        if not self.api_key or "${NEWSAPI_KEY}" in self.api_key:
            raise ValueError("NEWSAPI_KEY environment variable must be set")

        self.base_url = self.config["api"]["base_url"]

        # Set up persistence directory
        self.persistence_dir = Path(self.config["collection"]["persistence_dir"])
        self.persistence_dir.mkdir(parents=True, exist_ok=True)
        self.processed_ids_file = self.persistence_dir / "processed_article_ids.json"
        self.processed_ids = self._load_processed_ids()

        # Set up Kafka producer if config provided
        self.kafka_producer = None
        self.output_topic = None
        if kafka_config_path:
            self._initialize_kafka_producer(kafka_config_path)

        # Runtime control
        self.running = False
        logger.info(f"{self.collector_name} initialized with config from {config_path}")

    def _load_processed_ids(self) -> Set[str]:
        """Load previously processed article IDs to avoid duplicates."""
        if self.processed_ids_file.exists():
            try:
                with open(self.processed_ids_file, "r") as f:
                    return set(json.load(f))
            except (json.JSONDecodeError, FileNotFoundError) as e:
                logger.warning(f"Error loading processed IDs, starting fresh: {e}")
        return set()

    def _save_processed_ids(self) -> None:
        """Save the set of processed article IDs."""
        with open(self.processed_ids_file, "w") as f:
            json.dump(list(self.processed_ids), f)

    def _initialize_kafka_producer(self, kafka_config_path: str) -> None:
        """Initialize the Kafka producer for news data."""
        try:
            kafka_config = load_config(kafka_config_path)

            # Get broker addresses - use the appropriate key based on environment
            if os.getenv("CONTAINER_ENV") == "true":
                bootstrap_servers = kafka_config["bootstrap_servers_container"]
            else:
                bootstrap_servers = kafka_config["bootstrap_servers"]

            # Set output topic
            topics = kafka_config.get("topics", {})
            self.output_topic = topics.get("news_data_raw", "news-data-raw")

            # Get producer settings
            producer_settings = kafka_config.get("producer", {})
            client_id = f"newsapi-collector-{os.getpid()}"

            # Initialize producer
            self.kafka_producer = KafkaProducerWrapper(
                bootstrap_servers=bootstrap_servers,
                client_id=client_id,
                acks=producer_settings.get("acks", "all"),
                retries=producer_settings.get("retries", 3),
            )
            logger.info(f"Kafka producer initialized for topic: {self.output_topic}")
        except Exception as e:
            logger.exception(f"Failed to initialize Kafka producer: {e}")
            self.kafka_producer = None

    def collect(self) -> None:
        """
        Start collecting news data from NewsAPI based on configuration.

        This method runs continuously until stopped, with configurable frequency.
        """
        self.running = True

        frequency_minutes = self.config["collection"]["frequency"]
        logger.info(f"Starting news collection cycle every {frequency_minutes} minutes")

        try:
            while self.running:
                start_time = time.time()
                logger.info("Starting news collection cycle")

                # Collect for each query in the configuration
                for query in self.config["filters"]["queries"]:
                    try:
                        articles = self._fetch_articles_for_query(query)
                        self._process_articles(articles)
                    except Exception as e:
                        logger.exception(f"Error processing query '{query}': {e}")

                # Save updated processed IDs
                self._save_processed_ids()

                # Calculate sleep time for next cycle
                elapsed = time.time() - start_time
                sleep_time = max(0, (frequency_minutes * 60) - elapsed)

                if sleep_time > 0 and self.running:
                    logger.info(
                        f"Sleeping for {sleep_time:.1f} seconds until next collection cycle"
                    )
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Collection interrupted by user")
        except Exception as e:
            logger.exception(f"Unexpected error in collection loop: {e}")
        finally:
            self.stop()

    def _fetch_articles_for_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Fetch articles for a specific query from NewsAPI.

        Args:
            query: The search query string

        Returns:
            List of article dictionaries
        """
        days_to_fetch = self.config["collection"]["days_to_fetch"]
        page_size = self.config["collection"]["page_size"]
        cooldown = self.config["collection"]["request_cooldown"]

        # Calculate date range
        from_date = (datetime.utcnow() - timedelta(days=days_to_fetch)).strftime(
            "%Y-%m-%d"
        )
        to_date = datetime.utcnow().strftime("%Y-%m-%d")

        # Build request parameters
        params = {
            "q": query,
            "from": from_date,
            "to": to_date,
            "pageSize": page_size,
            "sortBy": "publishedAt",
            "language": ",".join(self.config["filters"]["languages"]),
            "apiKey": self.api_key,
        }

        # Add optional parameters if configured
        if sources := self.config["filters"]["sources"]:
            params["sources"] = ",".join(sources)

        if categories := self.config["filters"]["categories"]:
            params["category"] = categories[
                0
            ]  # NewsAPI only supports one category per request

        if countries := self.config["filters"]["countries"]:
            params["country"] = countries[
                0
            ]  # NewsAPI only supports one country per request

        # Make the API request
        logger.info(f"Fetching news for query: '{query}' from {from_date} to {to_date}")

        try:
            response = requests.get(f"{self.base_url}/everything", params=params)
            response.raise_for_status()

            result = response.json()
            articles = result.get("articles", [])
            logger.info(f"Retrieved {len(articles)} articles for query '{query}'")

            # Apply cooldown to avoid rate limits
            if cooldown > 0:
                time.sleep(cooldown)

            return articles

        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for query '{query}': {e}")
            if hasattr(e.response, "text"):
                logger.error(f"Response content: {e.response.text}")
            return []

    def _process_articles(self, articles: List[Dict[str, Any]]) -> None:
        """
        Process retrieved articles and send to Kafka if not already processed.

        Args:
            articles: List of article dictionaries from NewsAPI
        """
        if not articles:
            return

        new_count = 0
        for article in articles:
            # Generate consistent ID or use the one provided by the API
            article_id = article.get("url", "")

            # Skip if already processed
            if article_id in self.processed_ids:
                continue

            # Enrich the article with metadata
            processed_article = self._enrich_article(article)

            # Send to Kafka if producer is available
            if self.kafka_producer and self.output_topic:
                success = self.kafka_producer.send_message(
                    topic=self.output_topic, value=processed_article, key=article_id
                )
                if success:
                    new_count += 1
                    # Mark as processed only if successfully sent
                    self.processed_ids.add(article_id)
            else:
                # For testing/development without Kafka
                logger.debug(
                    f"Would send to Kafka: {json.dumps(processed_article)[:100]}..."
                )
                new_count += 1
                self.processed_ids.add(article_id)

        logger.info(f"Processed {new_count} new articles")

    def _enrich_article(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich the article with additional metadata.

        Args:
            article: Raw article from NewsAPI

        Returns:
            Enriched article with additional fields
        """
        # Create a copy to avoid modifying the original
        enriched = article.copy()

        # Add collection metadata
        enriched["collected_at"] = datetime.utcnow().isoformat()
        enriched["collector_id"] = self.collector_name

        # Add source type
        enriched["source_type"] = "newsapi"

        # Standardize dates if present
        if published_at := enriched.get("publishedAt"):
            try:
                # Ensure consistent datetime format
                dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                enriched["publishedAt"] = dt.isoformat()
            except (ValueError, TypeError):
                # Keep original if parsing fails
                pass

        return enriched

    def stop(self) -> None:
        """Stop the collector and clean up resources."""
        logger.info(f"Stopping {self.collector_name}")
        self.running = False
        self._save_processed_ids()
        self.cleanup()

    def cleanup(self) -> None:
        """Clean up resources."""
        super().cleanup()
        if self.kafka_producer:
            try:
                self.kafka_producer.close()
                logger.info("Kafka producer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")


if __name__ == "__main__":
    # Resolve paths for configuration files using Pathlib
    base_path = (
        Path(__file__).resolve().parents[4]
    )  # Adjusting to get the base directory
    config_path = base_path / "config/news_api_config.yaml"
    kafka_config_path = (
        base_path / "config/kafka/kafka_config.yaml"
    )  # Optional, can be set to None if not needed
    
    collector = NewsAPICollector(config_path=str(config_path), kafka_config_path=str(kafka_config_path))
    try:
        collector.collect()
    except KeyboardInterrupt:
        logger.info("Collector stopped by user") 