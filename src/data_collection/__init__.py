"""
Data Collection Module for Real-Time Stock Market Analysis System
"""

import configparser
import logging
import os
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_collection.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("data_collector")

# Load configuration
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                          'config', 'collector_config.ini')
config.read(config_path)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = config.get('kafka', 'bootstrap_servers', fallback='redpanda:29092')
MARKET_DATA_TOPIC = config.get('kafka', 'market_data_topic', fallback='market_data')
TWITTER_DATA_TOPIC = config.get('kafka', 'twitter_data_topic', fallback='twitter_data')
REDDIT_DATA_TOPIC = config.get('kafka', 'reddit_data_topic', fallback='reddit_data')
NEWS_DATA_TOPIC = config.get('kafka', 'news_data_topic', fallback='news_data')

# Cassandra configuration
CASSANDRA_HOST = config.get('cassandra', 'host', fallback='localhost')
CASSANDRA_PORT = config.getint('cassandra', 'port', fallback=9042)
CASSANDRA_USER = config.get('cassandra', 'username', fallback='cassandra')
CASSANDRA_PASSWORD = config.get('cassandra', 'password', fallback='cassandra')
CASSANDRA_KEYSPACE = config.get('cassandra', 'keyspace', fallback='market_data')

# API keys and secrets
ALPHA_VANTAGE_API_KEY = config.get('alpha_vantage', 'api_key', fallback='')
TWITTER_API_KEY = config.get('twitter', 'api_key', fallback='')
TWITTER_API_SECRET = config.get('twitter', 'api_secret', fallback='')
TWITTER_ACCESS_TOKEN = config.get('twitter', 'access_token', fallback='')
TWITTER_ACCESS_SECRET = config.get('twitter', 'access_secret', fallback='')
REDDIT_CLIENT_ID = config.get('reddit', 'client_id', fallback='')
REDDIT_CLIENT_SECRET = config.get('reddit', 'client_secret', fallback='')
REDDIT_USER_AGENT = config.get('reddit', 'user_agent', fallback='')
NEWS_API_KEY = config.get('news_api', 'api_key', fallback='')

# Collection parameters
STOCK_SYMBOLS = config.get('market_data', 'symbols', fallback='AAPL,MSFT,GOOGL,AMZN,TSLA').split(',')
COLLECTION_INTERVAL = config.getint('general', 'collection_interval_seconds', fallback=60)
TWITTER_SEARCH_TERMS = config.get('twitter', 'search_terms', fallback='stock market,investing').split(',')
REDDIT_SUBREDDITS = config.get('reddit', 'subreddits', fallback='stocks,investing,wallstreetbets').split(',')
NEWS_SOURCES = config.get('news_api', 'sources', fallback='bloomberg,financial-times,business-insider')
