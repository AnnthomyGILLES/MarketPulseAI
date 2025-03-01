"""
Data Collection Manager for coordinating all data collection activities.
"""

import threading
import time
from typing import Dict, List

from src.common.messaging.kafka_producer import KafkaProducerWrapper
from src.data_collection import (
    logger, KAFKA_BOOTSTRAP_SERVERS, MARKET_DATA_TOPIC, 
    TWITTER_DATA_TOPIC, REDDIT_DATA_TOPIC, NEWS_DATA_TOPIC,
    COLLECTION_INTERVAL
)
from src.data_collection.market_data.collectors.yfinance_collector import YFinanceCollector
from src.data_collection.market_data.parsers.yfinance_parser import YFinanceParser
from src.data_collection.market_data.validation.market_validators import MarketDataValidator
from src.data_collection.models import MarketData, SocialMediaData, NewsData


class KafkaPublisher:
    """Handles publishing collected data to Kafka topics using the common messaging module."""

    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS):
        """Initialize the Kafka producers for different topics."""
        bootstrap_servers_list = bootstrap_servers.split(',')
        
        # Create separate producers for each topic
        self.market_data_producer = KafkaProducerWrapper(
            bootstrap_servers=bootstrap_servers_list,
            topic=MARKET_DATA_TOPIC
        )
        
        self.twitter_data_producer = KafkaProducerWrapper(
            bootstrap_servers=bootstrap_servers_list,
            topic=TWITTER_DATA_TOPIC
        )
        
        self.reddit_data_producer = KafkaProducerWrapper(
            bootstrap_servers=bootstrap_servers_list,
            topic=REDDIT_DATA_TOPIC
        )
        
        self.news_data_producer = KafkaProducerWrapper(
            bootstrap_servers=bootstrap_servers_list,
            topic=NEWS_DATA_TOPIC
        )
        
        logger.info(f"Initialized Kafka publishers with bootstrap servers: {bootstrap_servers}")

    def publish_market_data(self, data: MarketData) -> None:
        """Publish market data to the appropriate Kafka topic."""
        try:
            self.market_data_producer.send_message(data.to_dict())
            logger.debug(f"Published market data for {data.symbol} to Kafka")
        except Exception as e:
            logger.error(f"Error publishing market data to Kafka: {e}")

    def publish_social_data(self, data: SocialMediaData) -> None:
        """Publish social media data to the appropriate Kafka topic."""
        try:
            if data.source == 'twitter':
                self.twitter_data_producer.send_message(data.to_dict())
            else:
                self.reddit_data_producer.send_message(data.to_dict())
            logger.debug(f"Published social media data from {data.source} to Kafka")
        except Exception as e:
            logger.error(f"Error publishing social media data to Kafka: {e}")

    def publish_news_data(self, data: NewsData) -> None:
        """Publish news data to the appropriate Kafka topic."""
        try:
            self.news_data_producer.send_message(data.to_dict())
            logger.debug(f"Published news data from {data.source} to Kafka")
        except Exception as e:
            logger.error(f"Error publishing news data to Kafka: {e}")

    def close(self) -> None:
        """Close all Kafka producers."""
        self.market_data_producer.close()
        self.twitter_data_producer.close()
        self.reddit_data_producer.close()
        self.news_data_producer.close()
        logger.info("Closed all Kafka producers")


class DataCollectionManager:
    """Manages the data collection process from all sources."""

    def __init__(self, collection_interval: int = COLLECTION_INTERVAL):
        """Initialize the data collection manager."""
        self.collection_interval = collection_interval
        self.running = False
        self.kafka_publisher = KafkaPublisher()
        
        # Initialize market data components
        self.market_collector = YFinanceCollector()
        self.market_parser = YFinanceParser()
        self.market_validator = MarketDataValidator()
        
        # Initialize collection threads
        self.collection_threads = {}
        
        logger.info(f"Initialized DataCollectionManager with interval: {collection_interval} seconds")

    def _market_data_collection_job(self) -> None:
        """Job function for collecting market data."""
        while self.running:
            try:
                logger.info("Starting market data collection cycle")
                
                # Collect raw data
                raw_data = self.market_collector.collect()
                
                # Parse data for each symbol
                parsed_data = []
                for symbol in self.market_collector.symbols:
                    try:
                        market_data = self.market_parser.parse(raw_data, symbol)
                        if market_data:
                            parsed_data.append(market_data)
                    except Exception as e:
                        logger.error(f"Error parsing data for {symbol}: {e}")
                
                # Validate data
                validation_results = self.market_validator.validate_batch(parsed_data)
                valid_data = validation_results['valid']
                
                # Log validation failures
                for invalid_data, error in validation_results['invalid']:
                    logger.warning(f"Invalid market data for {invalid_data.symbol}: {error}")
                
                # Publish valid data to Kafka
                for data in valid_data:
                    self.kafka_publisher.publish_market_data(data)
                
                logger.info(f"Completed market data collection cycle. Collected {len(valid_data)} valid records.")
            except Exception as e:
                logger.error(f"Error in market data collection job: {e}")

            # Sleep until next collection cycle
            time.sleep(self.collection_interval)

    def start_collection(self) -> None:
        """Start the data collection process for all sources."""
        if self.running:
            logger.warning("Data collection is already running")
            return

        self.running = True

        # Start collection threads
        self.collection_threads['market'] = threading.Thread(target=self._market_data_collection_job)
        self.collection_threads['market'].start()
        
        # Add more collection threads for social media and news here
        
        logger.info("Data collection process started")
    
    def stop_collection(self) -> None:
        """Stop the data collection process."""
        if not self.running:
            logger.warning("Data collection is not running")
            return
        
        self.running = False
        
        # Wait for threads to finish
        for name, thread in self.collection_threads.items():
            logger.info(f"Waiting for {name} collection thread to finish...")
            thread.join(timeout=30)
            if thread.is_alive():
                logger.warning(f"{name} collection thread did not finish gracefully")
        
        # Close Kafka publisher
        self.kafka_publisher.close()
        
        logger.info("Data collection process stopped")


if __name__ == "__main__":
    # Example usage
    manager = DataCollectionManager()
    try:
        manager.start_collection()
        # Keep the main thread running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping data collection due to keyboard interrupt")
        manager.stop_collection() 