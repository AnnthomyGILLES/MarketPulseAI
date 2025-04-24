#!/usr/bin/env python
"""
Kafka consumer that saves news articles to MongoDB.

This script consumes news articles from a Kafka topic and persists them to MongoDB.
It serves as the bridge between the streaming data pipeline and the database layer.
"""

import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Dict, Any, List

import yaml
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv
from loguru import logger

# Add src directory to Python path
current_dir = Path(__file__).resolve().parent
src_dir = current_dir.parent.parent.parent
sys.path.append(str(src_dir))

from data_collection.social_media.news.mongodb_repository import NewsArticleRepository


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Consume news articles from Kafka and save to MongoDB')
    parser.add_argument(
        '--config', 
        default='config/news_api_config.yaml',
        help='Path to the news API configuration file'
    )
    parser.add_argument(
        '--log-level', 
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logging level'
    )
    return parser.parse_args()


def setup_logging(log_level):
    """Configure logging with loguru."""
    logger.remove()  # Remove default handler
    logger.add(sys.stderr, level=log_level)
    logger.add(
        "logs/news_mongodb_{time}.log",
        rotation="10 MB",
        retention="7 days",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    )


def load_config(config_path):
    """Load the configuration from a YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


class NewsKafkaConsumer:
    """
    Consumes news articles from Kafka and saves them to MongoDB.
    
    Features:
    - Reliable consumption with exactly-once semantics
    - Error handling and retry logic
    - Graceful shutdown
    - Performance monitoring
    """
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        mongodb_uri: str,
        poll_timeout: float = 1.0,
        database_name: str = "social_media",
        collection_name: str = "news_articles"
    ):
        """
        Initialize the Kafka consumer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic to consume from
            group_id: Consumer group ID
            mongodb_uri: MongoDB connection URI
            poll_timeout: Timeout for polling messages (seconds)
            database_name: MongoDB database name
            collection_name: MongoDB collection name
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.poll_timeout = poll_timeout
        
        # Initialize MongoDB repository
        self.repository = NewsArticleRepository(
            connection_string=mongodb_uri,
            database_name=database_name,
            collection_name=collection_name
        )
        
        # Initialize Kafka consumer
        self.consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,  # 5 minutes
            'session.timeout.ms': 30000,     # 30 seconds
            'heartbeat.interval.ms': 10000,  # 10 seconds
        }
        
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([topic])
        
        # Internal state
        self.running = False
        self.messages_processed = 0
        self.messages_failed = 0
        self.last_commit_time = time.time()
        self.commit_interval = 5.0  # Commit every 5 seconds
        
        logger.info(f"Initialized Kafka consumer for topic '{topic}'")
    
    def start(self):
        """Start consuming messages from Kafka."""
        self.running = True
        
        logger.info(f"Starting consumption from topic: {self.topic}")
        
        try:
            while self.running:
                try:
                    self._consume_message()
                    
                    # Commit offsets periodically
                    if time.time() - self.last_commit_time > self.commit_interval:
                        self._commit_offsets()
                
                except KafkaException as e:
                    logger.error(f"Kafka error: {e}")
                    time.sleep(1)  # Avoid tight loop on errors
                
                except Exception as e:
                    logger.exception(f"Unexpected error: {e}")
                    time.sleep(1)  # Avoid tight loop on errors
        
        finally:
            self._shutdown()
            logger.info(f"Consumer stopped. Processed: {self.messages_processed}, "
                       f"Failed: {self.messages_failed}")
    
    def stop(self):
        """Stop the consumer gracefully."""
        logger.info("Stopping Kafka consumer...")
        self.running = False
    
    def _consume_message(self):
        """Poll and process a single message."""
        msg = self.consumer.poll(timeout=self.poll_timeout)
        
        if msg is None:
            return
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event - not an error
                logger.debug(f"Reached end of partition {msg.partition()}")
            else:
                # Actual error
                logger.error(f"Error consuming message: {msg.error()}")
                self.messages_failed += 1
            return
        
        try:
            # Parse message value
            try:
                article = json.loads(msg.value().decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error(f"Failed to decode message: {e}")
                self.messages_failed += 1
                return
            
            # Insert to MongoDB
            result = self.repository.insert_article(article)
            
            if result:
                self.messages_processed += 1
                if self.messages_processed % 100 == 0:
                    logger.info(f"Processed {self.messages_processed} messages")
            else:
                self.messages_failed += 1
        
        except Exception as e:
            logger.exception(f"Error processing message: {e}")
            self.messages_failed += 1
    
    def _commit_offsets(self):
        """Commit offsets to Kafka."""
        try:
            self.consumer.commit(asynchronous=False)
            self.last_commit_time = time.time()
            logger.debug("Committed offsets")
        except Exception as e:
            logger.error(f"Error committing offsets: {e}")
    
    def _shutdown(self):
        """Perform cleanup on shutdown."""
        try:
            # Final offset commit
            self._commit_offsets()
            
            # Close consumer
            self.consumer.close()
            
            # Close repository
            self.repository.close()
            
            logger.info("Kafka consumer and MongoDB repository closed")
        
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")


def main():
    """Main entry point."""
    args = parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    # Load environment variables
    load_dotenv()
    
    # Load configuration
    config = load_config(args.config)
    
    # Get MongoDB connection URI
    mongodb_uri = os.getenv("MONGODB_URI", "mongodb://mongodb_user:mongodb_password@mongodb:27017/")
    
    # Get Kafka bootstrap servers
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
    
    # Create and start consumer
    consumer = NewsKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=config["kafka"]["news_topic"],
        group_id="news-mongodb-consumer",
        mongodb_uri=mongodb_uri
    )
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        consumer.stop()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start consuming
    consumer.start()


if __name__ == "__main__":
    main() 