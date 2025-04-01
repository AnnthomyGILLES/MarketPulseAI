import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import praw
from dotenv import load_dotenv

from src.data_collection.base_collector import BaseCollector


class RedditCollector(BaseCollector):
    """
    Enhanced Reddit collector that supports:
    1. Getting top posts by subreddit
    2. Getting posts by symbol mentions
    3. Tracking posts with delayed popularity growth
    """

    def __init__(self, config_path: str = None):
        if config_path is None:
            base_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
            config_path = str(base_dir / "config" / "kafka" / "kafka_config.yaml")

        super().__init__(config_path, "reddit_data_collector")

        # Configurable settings
        self.subreddits = ["wallstreetbets", "investing", "stocks", "StockMarket"]
        self.symbols = ["AAPL"]  # Example stock symbols to track
        self.running = False
        self.collection_interval = 60  # seconds between collection cycles

        # Time windows for different types of collection
        self.daily_window = 1  # days
        self.rising_window = 7  # days
        self.historical_window = 30  # days

        # Collection limits
        self.posts_per_subreddit = 50
        self.comments_per_post = 30
        self.posts_per_symbol = 25

        # Post tracking settings
        self.historical_posts_cache = {}  # To store IDs of previously collected posts

        # Initialize Reddit API client
        self.reddit = self._initialize_reddit_client()

    def _initialize_reddit_client(self):
        """Initialize the Reddit API client with credentials from environment variables"""
        # Load environment variables from config directory
        env_path = Path(__file__).parents[4] / "config" / "reddit" / ".env"
        load_dotenv(dotenv_path=env_path)

        # Reddit API credentials
        self.client_id = os.getenv("REDDIT_CLIENT_ID")
        self.client_secret = os.getenv("REDDIT_CLIENT_SECRET")
        self.username = os.getenv("REDDIT_USERNAME")
        self.password = os.getenv("REDDIT_PASSWORD")
        self.user_agent = os.getenv("REDDIT_USER_AGENT")

        return praw.Reddit(
            client_id=self.client_id,
            client_secret=self.client_secret,
            user_agent=self.user_agent,
            username=self.username,
            password=self.password,
        )

    def collect_posts_by_subreddit(
        self,
        subreddit_name: str,
        sort_methods: List[str] = None,
        time_filters: List[str] = None,
    ):
        """
        Collect posts from a specific subreddit using multiple sort methods and time filters.

        Args:
            subreddit_name: Name of the subreddit to collect from
            sort_methods: List of sort methods to use ('hot', 'new', 'top', 'rising')
            time_filters: List of time filters to use for 'top' sort ('hour', 'day', 'week', 'month', 'year', 'all')
        """
        if sort_methods is None:
            sort_methods = ["hot", "top", "rising"]

        if time_filters is None:
            time_filters = ["day", "week"]

        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            self.logger.info(
                f"Collecting posts from r/{subreddit_name} using multiple methods"
            )

            collected_ids = set()  # Track IDs to avoid duplicates across sort methods

            for sort_method in sort_methods:
                if sort_method == "top":
                    for time_filter in time_filters:
                        self._collect_posts_with_method(
                            subreddit,
                            sort_method,
                            limit=self.posts_per_subreddit,
                            time_filter=time_filter,
                            collected_ids=collected_ids,
                        )
                else:
                    self._collect_posts_with_method(
                        subreddit,
                        sort_method,
                        limit=self.posts_per_subreddit,
                        collected_ids=collected_ids,
                    )

            self.logger.info(
                f"Collected {len(collected_ids)} unique posts from r/{subreddit_name}"
            )
        except Exception as e:
            self.logger.error(
                f"Error collecting posts from r/{subreddit_name}: {str(e)}"
            )

    def _collect_posts_with_method(
        self,
        subreddit,
        sort_method: str,
        limit: int,
        time_filter: str = None,
        collected_ids: set = None,
    ):
        """Helper method to collect posts using a specific sort method"""
        try:
            if collected_ids is None:
                collected_ids = set()

            # Get the appropriate iterator based on sort method
            if sort_method == "hot":
                posts_iterator = subreddit.hot(limit=limit)
            elif sort_method == "new":
                posts_iterator = subreddit.new(limit=limit)
            elif sort_method == "top":
                posts_iterator = subreddit.top(time_filter=time_filter, limit=limit)
            elif sort_method == "rising":
                posts_iterator = subreddit.rising(limit=limit)
            else:
                self.logger.error(f"Unknown sort method: {sort_method}")
                return

            method_desc = f"{sort_method}"
            if time_filter:
                method_desc += f"/{time_filter}"

            self.logger.info(
                f"Collecting {limit} {method_desc} posts from r/{subreddit.display_name}"
            )

            for post in posts_iterator:
                # Skip if we've already collected this post
                if post.id in collected_ids:
                    continue

                collected_ids.add(post.id)

                # Process the post
                self._process_post(post, collection_method=method_desc)

        except Exception as e:
            self.logger.error(
                f"Error in _collect_posts_with_method ({sort_method}): {str(e)}"
            )

    def _process_post(self, post, collection_method: str = "unknown"):
        """Process a Reddit post and send to Kafka with enhanced metadata"""
        try:
            # Create post data dictionary with additional metadata
            post_data = {
                "id": post.id,
                "source": "reddit",
                "content_type": "post",
                "subreddit": post.subreddit.display_name,
                "title": post.title,
                "author": str(post.author) if post.author else "[deleted]",
                "created_utc": post.created_utc,
                "post_created_datetime": datetime.fromtimestamp(
                    post.created_utc
                ).isoformat(),
                "score": post.score,
                "upvote_ratio": post.upvote_ratio,
                "num_comments": post.num_comments,
                "selftext": post.selftext,
                "url": post.url,
                "is_self": post.is_self,
                "permalink": post.permalink,
                "collection_method": collection_method,
                "collection_timestamp": datetime.now().isoformat(),
                # Add post age for easy filtering
                "post_age_days": (
                    datetime.now() - datetime.fromtimestamp(post.created_utc)
                ).days,
            }

            # Update cache with current data
            self.historical_posts_cache[post.id] = post_data

            # Extract any stock symbols mentioned in the post
            detected_symbols = self._extract_symbols(post.title, post.selftext)
            if detected_symbols:
                post_data["detected_symbols"] = detected_symbols

            # Send to Kafka primary topic
            self.send_to_kafka(
                self.config["kafka"]["topics"]["social_media_reddit_raw"],
                post_data,
                key=f"reddit_post_{post.id}",
            )

            # For each detected symbol, also send to symbol-specific topic
            for symbol in detected_symbols:
                symbol_data = post_data.copy()
                symbol_data["symbol"] = symbol
                self.send_to_kafka(
                    self.config["kafka"]["topics"]["social_media_reddit_symbols"],
                    symbol_data,
                    key=f"reddit_symbol_{symbol}_{post.id}",
                )

            # Collect comments for the post if it's popular
            if post.score > 10 or post.num_comments > 5:
                self.collect_comments(post.id, limit=self.comments_per_post)

        except Exception as e:
            self.logger.error(f"Error processing post {post.id}: {str(e)}")

    def _extract_symbols(self, title: str, selftext: str) -> List[str]:
        """Extract stock symbols from post title and content"""
        # Simple implementation - just check for exact matches
        # A more sophisticated version would use regex for $SYMBOL patterns
        # and NLP for context-aware extraction
        detected_symbols = []

        text_to_search = (title + " " + selftext).upper()

        for symbol in self.symbols:
            if symbol in text_to_search.split():
                detected_symbols.append(symbol)

        return detected_symbols

    def collect_comments(self, post_id: str, limit: Optional[int] = None):
        """
        Collect comments for a specific post.

        Args:
            post_id: Reddit post ID
            limit: Maximum number of comments to collect (None for all)
        """
        try:
            submission = self.reddit.submission(id=post_id)
            submission.comments.replace_more(limit=0)  # Skip "more comments" links

            self.logger.info(f"Collecting comments for post {post_id}")

            comment_count = 0
            for comment in submission.comments.list():
                # Create comment data dictionary
                comment_data = {
                    "id": comment.id,
                    "post_id": post_id,
                    "body": comment.body,
                    "author": str(comment.author) if comment.author else "[deleted]",
                    "created_utc": comment.created_utc,
                    "comment_created_datetime": datetime.fromtimestamp(
                        comment.created_utc
                    ).isoformat(),
                    "score": comment.score,
                    "subreddit": comment.subreddit.display_name,
                    "parent_id": comment.parent_id,
                    "collection_timestamp": datetime.now().isoformat(),
                    # Extract symbols from comment
                    "detected_symbols": self._extract_symbols("", comment.body),
                }

                # Send to Kafka
                self.send_to_kafka(
                    self.config["kafka"]["topics"]["social_media_reddit_comments"],
                    comment_data,
                    key=f"reddit_comment_{comment.id}",
                )

                comment_count += 1
                if limit and comment_count >= limit:
                    break

            self.logger.info(f"Collected {comment_count} comments for post {post_id}")
        except Exception as e:
            self.logger.error(f"Error collecting comments for post {post_id}: {str(e)}")

    def search_for_symbols(self, time_filters: List[str] = None):
        """
        Search for posts mentioning the tracked stock symbols across multiple time periods.

        Args:
            time_filters: List of time filters to use ('hour', 'day', 'week', 'month', 'year', 'all')
        """
        if time_filters is None:
            time_filters = ["day", "week"]

        for symbol in self.symbols:
            try:
                self.logger.info(f"Searching Reddit for posts mentioning {symbol}")

                for time_filter in time_filters:
                    # Search across Reddit with time filter
                    search_results = self.reddit.subreddit("all").search(
                        f"{symbol}",
                        sort="relevance",
                        time_filter=time_filter,
                        limit=self.posts_per_symbol,
                    )

                    result_count = 0
                    for post in search_results:
                        # Process and send the post
                        self._process_post(
                            post, collection_method=f"symbol_search/{time_filter}"
                        )
                        result_count += 1

                    self.logger.info(
                        f"Found {result_count} {time_filter} posts mentioning {symbol}"
                    )

            except Exception as e:
                self.logger.error(f"Error searching for {symbol}: {str(e)}")

    def recollect_historical_posts(self):
        """
        Re-collect posts we've seen before to track their engagement growth over time.
        """
        self.logger.info(
            f"Re-collecting {len(self.historical_posts_cache)} historical posts to track engagement"
        )

        # Take a sample of historical posts to avoid hitting rate limits
        posts_to_check = list(self.historical_posts_cache.keys())

        # If we have too many posts, prioritize those collected 1-7 days ago
        if len(posts_to_check) > 100:
            filtered_posts = []
            current_time = datetime.now()

            for post_id, post_data in self.historical_posts_cache.items():
                try:
                    collection_time = datetime.fromisoformat(
                        post_data["collection_timestamp"]
                    )
                    days_since_collection = (current_time - collection_time).days

                    # Prioritize posts we haven't checked recently
                    if 1 <= days_since_collection <= 7:
                        filtered_posts.append(post_id)
                except:
                    continue

            posts_to_check = filtered_posts[:100]  # Limit to 100 posts per cycle

        # Re-collect each post
        for post_id in posts_to_check:
            try:
                post = self.reddit.submission(id=post_id)
                self._process_post(post, collection_method="historical_recollection")
            except Exception as e:
                self.logger.error(
                    f"Error re-collecting historical post {post_id}: {str(e)}"
                )
                # If we can't find the post, remove it from our cache
                if "received 404 HTTP response" in str(e):
                    self.historical_posts_cache.pop(post_id, None)

    def cleanup_historical_cache(self):
        """Remove very old posts from the tracking cache"""
        posts_to_remove = []
        current_time = datetime.now()

        for post_id, post_data in self.historical_posts_cache.items():
            try:
                # Remove posts older than historical_window
                post_age_days = (
                    current_time - datetime.fromtimestamp(post_data["created_utc"])
                ).days
                if post_age_days > self.historical_window:
                    posts_to_remove.append(post_id)
            except:
                # If we can't parse the date, remove it
                posts_to_remove.append(post_id)

        for post_id in posts_to_remove:
            self.historical_posts_cache.pop(post_id, None)

        self.logger.info(
            f"Cleaned up {len(posts_to_remove)} old posts from tracking cache"
        )

    def collect(self):
        """
        Run the enhanced Reddit data collection process.

        Implements a multi-stage collection approach:
        1. Daily posts from specified subreddits
        2. Symbol-specific searches
        3. Re-collection of previously seen posts to track engagement growth
        """
        self.running = True
        self.logger.info(
            f"Starting enhanced Reddit collection for subreddits: {', '.join(self.subreddits)} "
            f"and symbols: {', '.join(self.symbols)}"
        )

        # Set up collection schedule
        last_daily_collection = datetime.now() - timedelta(
            hours=1
        )  # Start with immediate collection
        last_symbol_search = datetime.now() - timedelta(hours=1)
        last_recollection = datetime.now() - timedelta(hours=1)
        last_cache_cleanup = datetime.now() - timedelta(days=1)

        try:
            while self.running:
                start_time = time.time()
                current_time = datetime.now()

                # 1. Daily subreddit collection (every 1 hour)
                if (current_time - last_daily_collection).total_seconds() >= 3600:
                    for subreddit in self.subreddits:
                        self.collect_posts_by_subreddit(
                            subreddit,
                            sort_methods=["hot", "new", "top", "rising"],
                            time_filters=["day"],
                        )
                    last_daily_collection = current_time

                # 2. Symbol search (every 3 hours)
                if (current_time - last_symbol_search).total_seconds() >= 10800:
                    self.search_for_symbols(time_filters=["day", "week"])
                    last_symbol_search = current_time

                # 3. Re-collect historical posts (every 12 hours)
                if (current_time - last_recollection).total_seconds() >= 43200:
                    self.recollect_historical_posts()
                    last_recollection = current_time

                # 4. Clean up historical cache (every 3 days)
                if (current_time - last_cache_cleanup).total_seconds() >= 259200:
                    self.cleanup_historical_cache()
                    last_cache_cleanup = current_time

                # Calculate sleep time to maintain collection interval
                elapsed_time = time.time() - start_time
                sleep_time = max(0, self.collection_interval - elapsed_time)

                self.logger.info(
                    f"Collection cycle completed in {elapsed_time:.2f} seconds. "
                    f"Cache size: {len(self.historical_posts_cache)} posts. "
                    f"Sleeping for {sleep_time:.2f} seconds"
                )

                # Sleep until next collection cycle
                time_slept = 0
                # TODO Remove this for prod
                self.running = False
                while self.running and time_slept < sleep_time:
                    time.sleep(1)
                    time_slept += 1

        except KeyboardInterrupt:
            self.logger.info("Reddit data collection stopped by user")
        except Exception as e:
            self.logger.error(f"Reddit data collection failed: {str(e)}")
        finally:
            self.running = False
            self.cleanup()

    def stop(self):
        """Stop the data collection process"""
        self.running = False
        self.logger.info("Stopping Reddit data collection")
        self.cleanup()


if __name__ == "__main__":
    # Create an instance of the enhanced collector
    collector = RedditCollector()

    try:
        # Start the collection process
        collector.collect()
    except KeyboardInterrupt:
        # Handle graceful shutdown on Ctrl+C
        print("Collection interrupted. Shutting down...")
    finally:
        # Ensure resources are cleaned up
        collector.stop()
