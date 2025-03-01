"""
Data Collection Module for Real-Time Stock Market Analysis System
----------------------------------------------------------------

This module handles the collection of data from various sources:
1. Stock market data via yfinance/Alpha Vantage
2. Twitter data via Twitter API
3. Reddit data via Reddit API
4. Financial news via News API

The collected data is validated, transformed into a standardized format,
and then passed to the Apache Kafka streaming pipeline.
"""

import os
import sys
import time
import json
import logging
import datetime
import pandas as pd
import numpy as np
import requests
import yfinance as yf
import tweepy
import praw
import configparser
import threading
import queue
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass
from bs4 import BeautifulSoup
from newsapi import NewsApiClient

# Import the KafkaProducerWrapper from common messaging
from common.messaging.kafka_producer import KafkaProducerWrapper

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
config.read('config/collector_config.ini')

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = config.get('kafka', 'bootstrap_servers', fallback='localhost:9092')
MARKET_DATA_TOPIC = config.get('kafka', 'market_data_topic', fallback='market_data')
TWITTER_DATA_TOPIC = config.get('kafka', 'twitter_data_topic', fallback='twitter_data')
REDDIT_DATA_TOPIC = config.get('kafka', 'reddit_data_topic', fallback='reddit_data')
NEWS_DATA_TOPIC = config.get('kafka', 'news_data_topic', fallback='news_data')

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


@dataclass
class MarketData:
    """Class for holding market data."""
    symbol: str
    timestamp: float
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    source: str
    indicators: Dict[str, float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert data to a dictionary for Kafka."""
        return {
            'symbol': self.symbol,
            'timestamp': self.timestamp,
            'open': self.open_price,
            'high': self.high_price,
            'low': self.low_price,
            'close': self.close_price,
            'volume': self.volume,
            'source': self.source,
            'indicators': self.indicators or {},
            'collection_time': time.time()
        }


@dataclass
class SocialMediaData:
    """Class for holding social media data."""
    source: str
    timestamp: float
    content: str
    author: str
    post_id: str
    symbols: List[str]
    likes: int = 0
    shares: int = 0
    comments: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert data to a dictionary for Kafka."""
        return {
            'source': self.source,
            'timestamp': self.timestamp,
            'content': self.content,
            'author': self.author,
            'post_id': self.post_id,
            'symbols': self.symbols,
            'likes': self.likes,
            'shares': self.shares,
            'comments': self.comments,
            'collection_time': time.time()
        }


@dataclass
class NewsData:
    """Class for holding news article data."""
    source: str
    timestamp: float
    title: str
    content: str
    url: str
    symbols: List[str]
    author: str = ''

    def to_dict(self) -> Dict[str, Any]:
        """Convert data to a dictionary for Kafka."""
        return {
            'source': self.source,
            'timestamp': self.timestamp,
            'title': self.title,
            'content': self.content,
            'url': self.url,
            'symbols': self.symbols,
            'author': self.author,
            'collection_time': time.time()
        }


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


class StockDataCollector:
    """Collects stock market data from various sources."""

    def __init__(self, symbols: List[str] = STOCK_SYMBOLS, publisher: KafkaPublisher = None):
        """Initialize the stock data collector."""
        self.symbols = symbols
        self.publisher = publisher or KafkaPublisher()
        self.alpha_vantage_url = "https://www.alphavantage.co/query"
        self.alpha_vantage_api_key = ALPHA_VANTAGE_API_KEY
        self.last_collection_time = {}
        logger.info(f"Initialized StockDataCollector with {len(symbols)} symbols")

    def _extract_stock_symbols_from_text(self, text: str) -> List[str]:
        """Extract potential stock symbols from text.

        This is a simple implementation that looks for words that could be stock symbols.
        A more sophisticated implementation would use NLP to identify company mentions and map to symbols.
        """
        words = text.upper().split()
        # Simple heuristic: symbols are all caps between 1-5 letters
        potential_symbols = [word for word in words if word.isalpha() and 1 <= len(word) <= 5]
        # Filter to only include known symbols
        return [symbol for symbol in potential_symbols if symbol in self.symbols]

    def collect_yfinance_data(self) -> List[MarketData]:
        """Collect market data using yfinance."""
        market_data = []
        try:
            # Get data for all symbols at once
            data = yf.download(self.symbols, period="1d", interval="1m", group_by="ticker")

            current_time = time.time()
            for symbol in self.symbols:
                try:
                    if symbol in data.columns:
                        # Handle multi-level column structure
                        symbol_data = data[symbol]
                    else:
                        # Handle single-level column structure (for single symbol)
                        symbol_data = data

                    latest = symbol_data.iloc[-1]

                    market_record = MarketData(
                        symbol=symbol,
                        timestamp=current_time,
                        open_price=float(latest.get('Open', 0.0)),
                        high_price=float(latest.get('High', 0.0)),
                        low_price=float(latest.get('Low', 0.0)),
                        close_price=float(latest.get('Close', 0.0)),
                        volume=int(latest.get('Volume', 0)),
                        source='yfinance',
                        indicators={}
                    )
                    market_data.append(market_record)

                    # Publish to Kafka
                    if self.publisher:
                        self.publisher.publish_market_data(market_record)

                except Exception as e:
                    logger.error(f"Error processing data for symbol {symbol}: {e}")

            logger.info(f"Collected market data for {len(market_data)} symbols using yfinance")
        except Exception as e:
            logger.error(f"Error collecting market data via yfinance: {e}")

        return market_data

    def collect_alpha_vantage_data(self, symbol: str) -> Optional[MarketData]:
        """Collect market data using Alpha Vantage API."""
        try:
            # Check if we need to respect rate limits
            if symbol in self.last_collection_time:
                elapsed_time = time.time() - self.last_collection_time[symbol]
                if elapsed_time < 15:  # Alpha Vantage limits: 5 requests per minute, 500 per day
                    time.sleep(15 - elapsed_time)  # Respect the rate limit

            # Record the collection time
            self.last_collection_time[symbol] = time.time()

            # Make API request
            params = {
                'function': 'TIME_SERIES_INTRADAY',
                'symbol': symbol,
                'interval': '1min',
                'apikey': self.alpha_vantage_api_key,
                'outputsize': 'compact'
            }

            response = requests.get(self.alpha_vantage_url, params=params)
            response.raise_for_status()
            data = response.json()

            # Extract the latest data point
            time_series = data.get('Time Series (1min)', {})
            if not time_series:
                logger.warning(f"No time series data returned for {symbol}")
                return None

            # Get the most recent timestamp and data
            latest_timestamp = list(time_series.keys())[0]
            latest_data = time_series[latest_timestamp]

            # Create market data record
            market_record = MarketData(
                symbol=symbol,
                timestamp=datetime.datetime.strptime(latest_timestamp, '%Y-%m-%d %H:%M:%S').timestamp(),
                open_price=float(latest_data.get('1. open', 0.0)),
                high_price=float(latest_data.get('2. high', 0.0)),
                low_price=float(latest_data.get('3. low', 0.0)),
                close_price=float(latest_data.get('4. close', 0.0)),
                volume=int(latest_data.get('5. volume', 0)),
                source='alpha_vantage',
                indicators={}
            )

            # Publish to Kafka
            if self.publisher:
                self.publisher.publish_market_data(market_record)

            logger.info(f"Collected Alpha Vantage data for {symbol}")
            return market_record

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error collecting Alpha Vantage data for {symbol}: {e}")
        except Exception as e:
            logger.error(f"Error collecting Alpha Vantage data for {symbol}: {e}")

        return None

    def collect_all_data(self) -> List[MarketData]:
        """Collect data from all configured sources."""
        all_data = []

        # Start with yfinance as it can batch requests
        yf_data = self.collect_yfinance_data()
        all_data.extend(yf_data)

        # If Alpha Vantage API key is provided, collect that data too
        if self.alpha_vantage_api_key:
            # Only fetch a subset of symbols to respect rate limits
            for symbol in self.symbols[:5]:  # Limit to 5 symbols
                av_data = self.collect_alpha_vantage_data(symbol)
                if av_data:
                    all_data.append(av_data)

        return all_data


class TwitterCollector:
    """Collects data from Twitter using the Twitter API."""

    def __init__(self,
                 search_terms: List[str] = TWITTER_SEARCH_TERMS,
                 publisher: KafkaPublisher = None,
                 stock_collector: StockDataCollector = None):
        """Initialize the Twitter collector."""
        self.search_terms = search_terms
        self.publisher = publisher or KafkaPublisher()
        self.stock_collector = stock_collector or StockDataCollector()

        # Initialize the Twitter API client
        auth = tweepy.OAuthHandler(TWITTER_API_KEY, TWITTER_API_SECRET)
        auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET)
        self.api = tweepy.API(auth, wait_on_rate_limit=True)

        logger.info(f"Initialized TwitterCollector with search terms: {search_terms}")

    def collect_tweets(self, search_term: str, count: int = 100) -> List[SocialMediaData]:
        """Collect tweets matching the search term."""
        collected_data = []
        try:
            tweets = self.api.search_tweets(q=search_term, count=count, tweet_mode='extended')

            for tweet in tweets:
                # Extract text, handling retweets
                if hasattr(tweet, 'retweeted_status'):
                    text = tweet.retweeted_status.full_text
                else:
                    text = tweet.full_text

                # Extract stock symbols from tweet text
                symbols = self.stock_collector._extract_stock_symbols_from_text(text)

                # Create social media data record
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

                collected_data.append(tweet_data)

                # Publish to Kafka
                if self.publisher:
                    self.publisher.publish_social_data(tweet_data)

            logger.info(f"Collected {len(collected_data)} tweets for search term: {search_term}")
        except Exception as e:
            logger.error(f"Error collecting tweets for {search_term}: {e}")

        return collected_data

    def collect_all_tweets(self) -> List[SocialMediaData]:
        """Collect tweets for all search terms."""
        all_tweets = []

        for term in self.search_terms:
            term_tweets = self.collect_tweets(term)
            all_tweets.extend(term_tweets)
            # Sleep to respect rate limits
            time.sleep(2)

        return all_tweets


class RedditCollector:
    """Collects data from Reddit using the Reddit API."""

    def __init__(self,
                 subreddits: List[str] = REDDIT_SUBREDDITS,
                 publisher: KafkaPublisher = None,
                 stock_collector: StockDataCollector = None):
        """Initialize the Reddit collector."""
        self.subreddits = subreddits
        self.publisher = publisher or KafkaPublisher()
        self.stock_collector = stock_collector or StockDataCollector()

        # Initialize the Reddit API client
        self.reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )

        logger.info(f"Initialized RedditCollector with subreddits: {subreddits}")

    def collect_subreddit_posts(self, subreddit_name: str, limit: int = 25) -> List[SocialMediaData]:
        """Collect posts from a subreddit."""
        collected_data = []
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            new_posts = subreddit.new(limit=limit)

            for post in new_posts:
                # Extract stock symbols from post title and text
                text = f"{post.title} {post.selftext}"
                symbols = self.stock_collector._extract_stock_symbols_from_text(text)

                # Create social media data record
                post_data = SocialMediaData(
                    source='reddit',
                    timestamp=post.created_utc,
                    content=text,
                    author=post.author.name if post.author else "[deleted]",
                    post_id=post.id,
                    symbols=symbols,
                    likes=post.score,
                    comments=post.num_comments,
                    shares=0  # Reddit doesn't have a shares concept
                )

                collected_data.append(post_data)

                # Publish to Kafka
                if self.publisher:
                    self.publisher.publish_social_data(post_data)

                # Also collect comments if there are any
                if post.num_comments > 0:
                    post.comments.replace_more(limit=0)  # Only get the directly available comments
                    for comment in post.comments.list():
                        comment_symbols = self.stock_collector._extract_stock_symbols_from_text(comment.body)

                        comment_data = SocialMediaData(
                            source='reddit_comment',
                            timestamp=comment.created_utc,
                            content=comment.body,
                            author=comment.author.name if comment.author else "[deleted]",
                            post_id=f"{post.id}_{comment.id}",
                            symbols=comment_symbols,
                            likes=comment.score,
                            comments=len(comment.replies),
                            shares=0
                        )

                        collected_data.append(comment_data)

                        # Publish to Kafka
                        if self.publisher:
                            self.publisher.publish_social_data(comment_data)

            logger.info(f"Collected {len(collected_data)} Reddit posts and comments from r/{subreddit_name}")
        except Exception as e:
            logger.error(f"Error collecting Reddit data from r/{subreddit_name}: {e}")

        return collected_data

    def collect_all_reddit_data(self) -> List[SocialMediaData]:
        """Collect data from all configured subreddits."""
        all_data = []

        for subreddit_name in self.subreddits:
            subreddit_data = self.collect_subreddit_posts(subreddit_name)
            all_data.extend(subreddit_data)
            # Sleep to respect rate limits
            time.sleep(2)

        return all_data


class NewsCollector:
    """Collects news articles from various financial news sources."""

    def __init__(self,
                 sources: str = NEWS_SOURCES,
                 publisher: KafkaPublisher = None,
                 stock_collector: StockDataCollector = None):
        """Initialize the news collector."""
        self.sources = sources
        self.publisher = publisher or KafkaPublisher()
        self.stock_collector = stock_collector or StockDataCollector()

        # Initialize the News API client
        self.newsapi = NewsApiClient(api_key=NEWS_API_KEY) if NEWS_API_KEY else None

        logger.info(f"Initialized NewsCollector with sources: {sources}")

    def collect_news_api_articles(self) -> List[NewsData]:
        """Collect articles using the News API."""
        collected_data = []

        if not self.newsapi:
            logger.warning("News API key not provided, skipping News API collection")
            return collected_data

        try:
            # Query for financial/stock market news
            articles = self.newsapi.get_everything(
                q='stock market OR investing OR finance',
                sources=self.sources,
                language='en',
                sort_by='publishedAt',
                page_size=100
            )

            for article in articles['articles']:
                # Combine title and content for symbol extraction
                text = f"{article['title']} {article['description'] or ''} {article['content'] or ''}"
                symbols = self.stock_collector._extract_stock_symbols_from_text(text)

                # Parse the publication date
                pub_date = datetime.datetime.strptime(
                    article['publishedAt'],
                    '%Y-%m-%dT%H:%M:%SZ'
                ).timestamp()

                # Create news data record
                news_data = NewsData(
                    source=article['source']['name'],
                    timestamp=pub_date,
                    title=article['title'],
                    content=article['description'] or '',
                    url=article['url'],
                    symbols=symbols,
                    author=article['author'] or ''
                )

                collected_data.append(news_data)

                # Publish to Kafka
                if self.publisher:
                    self.publisher.publish_news_data(news_data)

            logger.info(f"Collected {len(collected_data)} news articles from News API")
        except Exception as e:
            logger.error(f"Error collecting news from News API: {e}")

        return collected_data

    def scrape_financial_news(self) -> List[NewsData]:
        """Scrape news from financial news websites.

        This is a simple implementation that could be expanded with more sources.
        In a production environment, you would respect robots.txt and implement
        more sophisticated scraping with proper error handling.
        """
        collected_data = []

        # Example: Scrape Yahoo Finance headlines
        try:
            url = "https://finance.yahoo.com/news/"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }

            response = requests.get(url, headers=headers)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            articles = soup.select('div.Cf')

            current_time = time.time()

            for article in articles[:20]:  # Limit to 20 articles
                try:
                    title_elem = article.select_one('h3')
                    if not title_elem:
                        continue

                    title = title_elem.text.strip()

                    link_elem = article.select_one('a')
                    if link_elem and 'href' in link_elem.attrs:
                        url = 'https://finance.yahoo.com' + link_elem['href'] if link_elem['href'].startswith('/') else link_elem['href']
                    else:
                        url = ''

                    # Try to get description or summary
                    desc_elem = article.select_one('p')
                    content = desc_elem.text.strip() if desc_elem else ''

                    # Extract potential stock symbols
                    symbols = self.stock_collector._extract_stock_symbols_from_text(f"{title} {content}")

                    # Create news data record
                    news_data = NewsData(
                        source='Yahoo Finance',
                        timestamp=current_time,
                        title=title,
                        content=content,
                        url=url,
                        symbols=symbols,
                        author=''
                    )

                    collected_data.append(news_data)

                    # Publish to Kafka
                    if self.publisher:
                        self.publisher.publish_news_data(news_data)

                except Exception as e:
                    logger.error(f"Error processing Yahoo Finance article: {e}")

            logger.info(f"Scraped {len(collected_data)} articles from Yahoo Finance")
        except Exception as e:
            logger.error(f"Error scraping Yahoo Finance: {e}")

        return collected_data

    def collect_all_news(self) -> List[NewsData]:
        """Collect news from all configured sources."""
        all_news = []

        # Collect from News API
        api_news = self.collect_news_api_articles()
        all_news.extend(api_news)

        # Collect from web scraping
        scraped_news = self.scrape_financial_news()
        all_news.extend(scraped_news)

        return all_news


class DataCollectionManager:
    """Manages the data collection process from all sources."""

    def __init__(self, collection_interval: int = COLLECTION_INTERVAL):
        """Initialize the data collection manager."""
        self.collection_interval = collection_interval
        self.running = False
        self.kafka_publisher = KafkaPublisher()

        # Initialize collectors
        self.stock_collector = StockDataCollector(publisher=self.kafka_publisher)
        self.twitter_collector = TwitterCollector(publisher=self.kafka_publisher, stock_collector=self.stock_collector)
        self.reddit_collector = RedditCollector(publisher=self.kafka_publisher, stock_collector=self.stock_collector)
        self.news_collector = NewsCollector(publisher=self.kafka_publisher, stock_collector=self.stock_collector)

        # Initialize collection threads
        self.collection_threads = {}
        self.collection_queue = queue.Queue()

        logger.info(f"Initialized DataCollectionManager with interval: {collection_interval} seconds")

    def _stock_collection_job(self) -> None:
        """Job function for collecting stock data."""
        while self.running:
            try:
                logger.info("Starting stock data collection cycle")
                self.stock_collector.collect_all_data()
                logger.info("Completed stock data collection cycle")
            except Exception as e:
                logger.error(f"Error in stock collection job: {e}")

            # Sleep until next collection cycle
            time.sleep(self.collection_interval)

    def _twitter_collection_job(self) -> None:
        """Job function for collecting Twitter data."""
        while self.running:
            try:
                logger.info("Starting Twitter data collection cycle")
                self.twitter_collector.collect_all_tweets()
                logger.info("Completed Twitter data collection cycle")
            except Exception as e:
                logger.error(f"Error in Twitter collection job: {e}")

            # Sleep until next collection cycle
            time.sleep(self.collection_interval * 5)  # Collect less frequently than stock data

    def _reddit_collection_job(self) -> None:
        """Job function for collecting Reddit data."""
        while self.running:
            try:
                logger.info("Starting Reddit data collection cycle")
                self.reddit_collector.collect_all_reddit_data()
                logger.info("Completed Reddit data collection cycle")
            except Exception as e:
                logger.error(f"Error in Reddit collection job: {e}")

            # Sleep until next collection cycle
            time.sleep(self.collection_interval * 10)  # Collect less frequently than Twitter

    def _news_collection_job(self) -> None:
        """Job function for collecting news data."""
        while self.running:
            try:
                logger.info("Starting news data collection cycle")
                self.news_collector.collect_all_news()
                logger.info("Completed news data collection cycle")
            except Exception as e:
                logger.error(f"Error in news collection job: {e}")

            # Sleep until next collection cycle
            time.sleep(self.collection_interval * 15)  # Collect less frequently than Reddit

    def start_collection(self) -> None:
        """Start the data collection process for all sources."""
        if self.running:
            logger.warning("Data collection is already running")
            return

        self.running = True

        # Start collection threads
        self.collection_threads['stock'] = threading.Thread(target=self._stock_collection_job)
        self.collection_threads['twitter'] = threading.Thread(target=self._twitter_collection_job)
        self.collection_threads['reddit'] = threading.Thread(target=self._reddit_collection_job)
        self.collection_threads['news'] = threading.Thread(target=self._news_collection_job)
        for thread in self.collection_threads.values():
            thread.start()

        logger.info("Data collection process started")