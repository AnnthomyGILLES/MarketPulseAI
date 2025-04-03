# src/data_collection/social_media/reddit/collectors/reddit_collector.py

import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Set

import praw
import prawcore
import yaml  # Added for config loading
from dotenv import load_dotenv

from src.data_collection.base_collector import BaseCollector
from src.utils.logging import setup_logger  # Assuming a central logging setup exists

# Setup logger for this module
setup_logger("Collections")
logger = logging.getLogger(__name__)


class RedditCollector(BaseCollector):
    """
    Collects Reddit posts and comments based on subreddits and symbol mentions.

    Features:
    - Fetches posts using various sorting methods (hot, new, top, rising).
    - Searches for posts mentioning specific stock symbols.
    - Collects comments for fetched posts.
    - Sends data to Kafka topics.
    - Includes basic handling for rate limits and API errors.
    - Uses external configuration for settings.
    """

    DEFAULT_CONFIG_PATH = (
        Path(__file__).resolve().parents[4]
        / "config"
        / "reddit"
        / "reddit_collector_config.yaml"
    )
    DEFAULT_ENV_PATH = (
        Path(__file__).resolve().parents[5] / "config" / "reddit" / ".env"
    )

    def __init__(
        self,
        kafka_config_path: Optional[str] = None,
        reddit_config_path: Optional[str] = None,
        env_path: Optional[str] = None,
    ):
        # Initialize BaseCollector first (handles Kafka setup)
        super().__init__(
            "reddit_data_collector", kafka_config_path,
        )  # BaseCollector likely loads kafka_config internally

        # Load Reddit specific configuration
        self.config_path = Path(reddit_config_path or self.DEFAULT_CONFIG_PATH)
        self.env_path = Path(env_path or self.DEFAULT_ENV_PATH)
        self._load_reddit_config()

        # Initialize Reddit API client
        self.reddit = self._initialize_reddit_client()

        self.running = False
        # Post tracking (consider more robust storage if memory becomes an issue)
        self.collected_post_ids_this_run: Set[str] = set()

    def _load_reddit_config(self):
        """Loads collector settings from a YAML configuration file."""
        logger.info(f"Loading Reddit collector configuration from: {self.config_path}")
        try:
            with open(self.config_path, "r") as f:
                self.reddit_config = yaml.safe_load(f)

            # Apply settings from config
            self.subreddits: List[str] = self.reddit_config.get("subreddits", [])
            self.symbols: List[str] = self.reddit_config.get("symbols", [])
            self.collection_interval: int = self.reddit_config.get(
                "collection_interval_seconds", 60
            )
            self.posts_per_subreddit: int = self.reddit_config.get("limits", {}).get(
                "posts_per_subreddit", 10
            )
            self.comments_per_post: int = self.reddit_config.get("limits", {}).get(
                "comments_per_post", 20
            )
            self.posts_per_symbol: int = self.reddit_config.get("limits", {}).get(
                "posts_per_symbol", 10
            )
            self.default_sort_methods: List[str] = self.reddit_config.get(
                "collection_params", {}
            ).get("default_sort_methods", ["hot", "top", "rising"])
            self.default_time_filters: List[str] = self.reddit_config.get(
                "collection_params", {}
            ).get("default_time_filters", ["day", "week"])
            self.comment_collection_min_score: int = self.reddit_config.get(
                "collection_params", {}
            ).get("comment_collection_min_score", 10)
            self.comment_collection_min_comments: int = self.reddit_config.get(
                "collection_params", {}
            ).get("comment_collection_min_comments", 5)

            logger.info(
                f"Reddit collector configured. Subreddits: {len(self.subreddits)}, Symbols: {len(self.symbols)}"
            )

        except FileNotFoundError:
            logger.error(
                f"Reddit configuration file not found at {self.config_path}. Using defaults."
            )
            # Set minimal defaults if config load fails
            self.subreddits = ["wallstreetbets", "investing", "stocks"]
            self.symbols = ["AAPL", "GOOGL", "MSFT"]
            self.collection_interval = 60
            self.posts_per_subreddit = 10
            self.comments_per_post = 20
            self.posts_per_symbol = 10
            self.default_sort_methods = ["hot", "top", "rising"]
            self.default_time_filters = ["day", "week"]
            self.comment_collection_min_score = 10
            self.comment_collection_min_comments = 5
        except Exception as e:
            logger.exception(
                f"Error loading Reddit configuration from {self.config_path}: {e}"
            )
            raise  # Re-raise exception as config is critical

    def _initialize_reddit_client(self):
        """Initializes the PRAW Reddit API client using credentials from .env."""
        logger.info(f"Loading Reddit credentials from: {self.env_path}")
        load_dotenv(dotenv_path=self.env_path)

        client_id = os.getenv("REDDIT_CLIENT_ID")
        client_secret = os.getenv("REDDIT_CLIENT_SECRET")
        username = os.getenv("REDDIT_USERNAME")
        password = os.getenv("REDDIT_PASSWORD")
        user_agent = os.getenv("REDDIT_USER_AGENT", "MarketPulseAI Collector v1.0")

        if not all([client_id, client_secret, user_agent]):
            logger.error(
                "Missing required Reddit API credentials (ID, Secret, User-Agent) in environment variables."
            )
            raise ValueError("Missing Reddit API credentials.")

        try:
            reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent=user_agent,
                username=username,  # Optional, needed for certain actions
                password=password,  # Optional
                # Consider adding timeout settings if needed
                # request_timeout=30
            )
            # Test connection
            reddit.user.me()  # Accessing me() requires username/password and checks auth
            logger.info(
                f"PRAW Reddit client initialized successfully. Read-only: {reddit.read_only}"
            )
            return reddit
        except prawcore.exceptions.OAuthException as e:
            logger.exception(f"Authentication error initializing Reddit client: {e}")
            raise
        except Exception as e:
            logger.exception(f"Failed to initialize Reddit client: {e}")
            raise

    def _handle_api_error(self, error: Exception, context: str):
        """Handles common PRAW API errors with logging and potential backoff."""
        if isinstance(error, prawcore.exceptions.RequestException):
            # Network errors, timeouts
            logger.warning(
                f"Network error during {context}: {error}. Check connection."
            )
            time.sleep(5)  # Simple backoff
        elif isinstance(error, prawcore.exceptions.ResponseException):
            # HTTP errors (4xx, 5xx)
            logger.error(
                f"HTTP error during {context}: {error}. Status: {error.response.status_code}"
            )
            if error.response.status_code == 401:  # Unauthorized
                logger.error("Reddit API authentication failed. Check credentials.")
                self.stop()  # Stop collector if auth fails
            elif error.response.status_code == 403:  # Forbidden
                logger.warning(
                    f"Forbidden access during {context}. Check permissions or endpoint."
                )
            elif error.response.status_code == 404:  # Not Found
                logger.warning(f"Resource not found during {context}: {error}")
            elif error.response.status_code == 429:  # Rate limited
                logger.warning(
                    f"Rate limited during {context}. PRAW should handle waits, but logging explicitely."
                )
                # PRAW usually handles sleep, but adding a small extra delay if needed
                time.sleep(10)
            elif error.response.status_code >= 500:  # Server error
                logger.warning(
                    f"Reddit server error ({error.response.status_code}) during {context}. Retrying later."
                )
                time.sleep(15)
        elif isinstance(error, praw.exceptions.RedditAPIException):
            # Specific Reddit API issues (e.g., invalid parameters)
            logger.error(f"Reddit API error during {context}: {error}")
            for sub_error in error.items:
                logger.error(
                    f"  - API Error Type: {sub_error.error_type}, Message: {sub_error.message}"
                )
        else:
            # General unexpected errors
            logger.exception(f"Unexpected error during {context}: {error}")

    def collect_posts_by_subreddit(
        self,
        subreddit_name: str,
        sort_methods: Optional[List[str]] = None,
        time_filters: Optional[List[str]] = None,
    ):
        """Collects posts from a subreddit using specified methods and filters."""
        if sort_methods is None:
            sort_methods = self.default_sort_methods
        if time_filters is None:
            time_filters = self.default_time_filters

        subreddit_instance = None
        try:
            subreddit_instance = self.reddit.subreddit(subreddit_name)
            # Check if subreddit exists and is accessible (light check)
            _ = subreddit_instance.display_name
            logger.info(
                f"Starting post collection for r/{subreddit_name} (Methods: {', '.join(sort_methods)})"
            )
        except (prawcore.exceptions.Redirect, prawcore.exceptions.NotFound):
            logger.error(f"Subreddit r/{subreddit_name} not found or redirected.")
            return
        except Exception as e:
            self._handle_api_error(e, f"accessing subreddit r/{subreddit_name}")
            return  # Stop collection for this subreddit if initial access fails

        initial_collected_count = len(self.collected_post_ids_this_run)

        for sort_method in sort_methods:
            try:
                if sort_method == "top":
                    for time_filter in time_filters:
                        self._collect_posts_with_method(
                            subreddit_instance,
                            sort_method,
                            limit=self.posts_per_subreddit,
                            time_filter=time_filter,
                        )
                else:
                    self._collect_posts_with_method(
                        subreddit_instance,
                        sort_method,
                        limit=self.posts_per_subreddit,
                    )
            except Exception as e:
                # Catch errors at the sort method level to continue with others
                self._handle_api_error(
                    e, f"collecting {sort_method} posts from r/{subreddit_name}"
                )
                time.sleep(5)  # Wait a bit before next method

        newly_collected = (
            len(self.collected_post_ids_this_run) - initial_collected_count
        )
        logger.info(
            f"Finished collection for r/{subreddit_name}. Collected {newly_collected} new unique posts in this cycle."
        )

    def _collect_posts_with_method(
        self,
        subreddit: praw.models.Subreddit,
        sort_method: str,
        limit: int,
        time_filter: Optional[str] = None,
    ):
        """Helper to collect posts using a specific sort method/filter."""
        method_desc = f"{sort_method}" + (f"/{time_filter}" if time_filter else "")
        logger.debug(
            f"Fetching {limit} {method_desc} posts from r/{subreddit.display_name}"
        )

        posts_iterator = None
        try:
            if sort_method == "hot":
                posts_iterator = subreddit.hot(limit=limit)
            elif sort_method == "new":
                posts_iterator = subreddit.new(limit=limit)
            elif sort_method == "top" and time_filter:
                posts_iterator = subreddit.top(time_filter=time_filter, limit=limit)
            elif sort_method == "rising":
                posts_iterator = subreddit.rising(limit=limit)
            else:
                logger.warning(
                    f"Unsupported sort method/filter combination: {method_desc}"
                )
                return

            count = 0
            for post in posts_iterator:
                if post.id not in self.collected_post_ids_this_run:
                    self.collected_post_ids_this_run.add(post.id)
                    self._process_post(post, collection_method=method_desc)
                    count += 1
                else:
                    logger.debug(
                        f"Skipping already collected post {post.id} from r/{subreddit.display_name}"
                    )

            logger.debug(
                f"Fetched {count} new {method_desc} posts from r/{subreddit.display_name}"
            )

        except Exception as e:
            self._handle_api_error(
                e, f"fetching {method_desc} posts from r/{subreddit.display_name}"
            )
            # Re-raise to be caught by the outer loop if necessary, or handle more gracefully
            raise

    def _process_post(self, post: praw.models.Submission, collection_method: str):
        """Processes a single Reddit post and sends it to Kafka."""
        try:
            post_data = {
                "id": post.id,
                "source": "reddit",
                "content_type": "post",  # Differentiates posts from comments
                "subreddit": post.subreddit.display_name,
                "title": post.title,
                "author": str(post.author) if post.author else "[deleted]",
                "created_utc": post.created_utc,
                "post_created_datetime": datetime.fromtimestamp(
                    post.created_utc
                ).isoformat()
                + "Z",  # Use UTC ISO format
                "score": post.score,
                "upvote_ratio": post.upvote_ratio,
                "num_comments": post.num_comments,
                "selftext": post.selftext,
                "url": post.url,
                "is_self": post.is_self,
                "permalink": f"https://www.reddit.com{post.permalink}",  # Full permalink
                "collection_method": collection_method,
                "collection_timestamp": datetime.now().isoformat()
                + "Z",  # Use UTC ISO format
                # Calculate age on demand during processing if needed, avoids staleness
            }

            # Simple Symbol Extraction (Placeholder for potentially more complex logic)
            # Consider case-insensitivity and patterns like $AAPL
            detected_symbols = self._extract_symbols(post.title + " " + post.selftext)
            if detected_symbols:
                post_data["detected_symbols"] = list(
                    detected_symbols
                )  # Ensure list format

            # Send raw post data
            kafka_topic_raw = self.kafka_config["topics"]["social_media_reddit_raw"]
            self.send_to_kafka(kafka_topic_raw, post_data, key=f"reddit_post_{post.id}")
            logger.debug(
                f"Sent post {post.id} from r/{post.subreddit.display_name} to Kafka topic {kafka_topic_raw}"
            )

            # Send to symbol-specific topic if symbols detected
            if detected_symbols:
                kafka_topic_symbols = self.kafka_config["topics"][
                    "social_media_reddit_symbols"
                ]
                for symbol in detected_symbols:
                    symbol_data = post_data.copy()
                    symbol_data["symbol"] = symbol  # Add symbol context
                    self.send_to_kafka(
                        kafka_topic_symbols,
                        symbol_data,
                        key=f"reddit_symbol_{symbol}_{post.id}",
                    )
                logger.debug(
                    f"Sent post {post.id} to Kafka topic {kafka_topic_symbols} for symbols: {', '.join(detected_symbols)}"
                )

            # Collect comments if post meets criteria
            if (
                post.score >= self.comment_collection_min_score
                or post.num_comments >= self.comment_collection_min_comments
            ):
                self.collect_comments(post.id, limit=self.comments_per_post)

        except Exception as e:
            # Log error but don't let one post failure stop the collector
            logger.error(
                f"Error processing post {getattr(post, 'id', 'N/A')}: {e}",
                exc_info=True,
            )

    def _extract_symbols(self, text: str) -> Set[str]:
        """Extracts potential stock symbols mentioned in text."""
        # Basic implementation: case-insensitive match against configured symbols
        # Improvement idea: Use regex for $SYMBOL or common patterns
        # Improvement idea: Use NLP for better context checking
        detected = set()
        # Normalize text for easier matching
        text_upper = text.upper()
        # Use word boundaries or splits to avoid partial matches (e.g., 'CAPITAL' matching 'AAPL')
        words = set(text_upper.split())  # Simple split, regex \b would be better

        for symbol in self.symbols:
            # Check for symbol possibly surrounded by common punctuation or as whole word
            # This is still basic and can be improved significantly.
            if symbol.upper() in words:
                detected.add(symbol)
            elif f"${symbol.upper()}" in words:
                detected.add(symbol)

        return detected

    def collect_comments(self, post_id: str, limit: Optional[int] = None):
        """Collects top-level comments for a given post ID."""
        logger.debug(f"Collecting comments for post {post_id} (Limit: {limit})")
        try:
            submission = self.reddit.submission(id=post_id)
            # Fetch comments, replace_more(limit=0) avoids loading deeper replies efficiently
            submission.comments.replace_more(limit=0)

            comment_count = 0
            kafka_topic_comments = self.kafka_config["topics"][
                "social_media_reddit_comments"
            ]

            for comment in submission.comments.list():
                # Ensure it's a Comment object, not MoreComments
                if not isinstance(comment, praw.models.Comment):
                    continue

                comment_data = {
                    "id": comment.id,
                    "source": "reddit",
                    "content_type": "comment",  # Differentiate comments
                    "post_id": post_id,
                    "subreddit": comment.subreddit.display_name,
                    "body": comment.body,
                    "author": str(comment.author) if comment.author else "[deleted]",
                    "created_utc": comment.created_utc,
                    "comment_created_datetime": datetime.fromtimestamp(
                        comment.created_utc
                    ).isoformat()
                    + "Z",
                    "score": comment.score,
                    "parent_id": comment.parent_id,  # Indicates if it's a reply
                    "is_submitter": comment.is_submitter,
                    "permalink": f"https://www.reddit.com{comment.permalink}",  # Full permalink
                    "collection_timestamp": datetime.now().isoformat() + "Z",
                }

                # Extract symbols from comment body
                detected_symbols = self._extract_symbols(comment.body)
                if detected_symbols:
                    comment_data["detected_symbols"] = list(detected_symbols)

                # Send comment data
                self.send_to_kafka(
                    kafka_topic_comments,
                    comment_data,
                    key=f"reddit_comment_{comment.id}",
                )

                comment_count += 1
                if limit is not None and comment_count >= limit:
                    logger.debug(f"Reached comment limit ({limit}) for post {post_id}")
                    break

            logger.debug(f"Collected {comment_count} comments for post {post_id}")

        except prawcore.exceptions.NotFound:
            logger.warning(f"Post {post_id} not found when trying to collect comments.")
        except Exception as e:
            self._handle_api_error(e, f"collecting comments for post {post_id}")

    def search_for_symbols(self, time_filters: Optional[List[str]] = None):
        """Searches Reddit globally for posts mentioning tracked symbols."""
        if time_filters is None:
            time_filters = self.default_time_filters

        logger.info(f"Starting symbol search for: {', '.join(self.symbols)}")

        for symbol in self.symbols:
            logger.debug(f"Searching for symbol: {symbol}")
            for time_filter in time_filters:
                try:
                    # Search across all of Reddit ('all' subreddit)
                    # Consider searching within finance-related subreddits only for higher relevance
                    # Use relevance sort, could experiment with 'new' or 'comments'
                    search_results = self.reddit.subreddit(
                        "all"
                    ).search(
                        f'"{symbol}" OR ${symbol}',  # More specific query, include $ prefix
                        sort="relevance",  # or 'new'
                        time_filter=time_filter,
                        limit=self.posts_per_symbol,  # Limit per symbol per time filter
                    )

                    result_count = 0
                    for post in search_results:
                        if post.id not in self.collected_post_ids_this_run:
                            self.collected_post_ids_this_run.add(post.id)
                            self._process_post(
                                post, collection_method=f"symbol_search/{time_filter}"
                            )
                            result_count += 1
                        else:
                            logger.debug(
                                f"Skipping already collected post {post.id} found via symbol search for {symbol}"
                            )

                    logger.debug(
                        f"Found {result_count} new posts mentioning '{symbol}' in time filter '{time_filter}'"
                    )
                    time.sleep(2)  # Small delay between searches to be polite to API

                except Exception as e:
                    self._handle_api_error(
                        e,
                        f"searching for symbol {symbol} with time filter {time_filter}",
                    )
                    time.sleep(5)  # Wait before next search if error occurs

        logger.info("Finished symbol search cycle.")

    def collect(self):
        """Main collection loop."""
        self.running = True
        logger.info(
            f"Starting Reddit collector."
            f" Interval: {self.collection_interval}s."
            f" Subreddits: {len(self.subreddits)}."
            f" Symbols: {len(self.symbols)}."
        )

        try:
            while self.running:
                start_time = time.time()
                logger.info("Starting new collection cycle...")
                self.collected_post_ids_this_run.clear()  # Reset for this cycle

                # --- Collect posts from configured subreddits ---
                for subreddit_name in self.subreddits:
                    if not self.running:
                        break  # Allow graceful stop
                    self.collect_posts_by_subreddit(subreddit_name)
                    time.sleep(1)  # Small delay between subreddits

                if not self.running:
                    break

                # --- Search for symbol mentions ---
                self.search_for_symbols()

                # --- Cycle complete ---
                end_time = time.time()
                elapsed_time = end_time - start_time
                logger.info(
                    f"Collection cycle finished in {elapsed_time:.2f} seconds. "
                    f"Collected {len(self.collected_post_ids_this_run)} unique posts/comments this cycle."
                )

                # --- Sleep until next cycle ---
                sleep_time = max(0, self.collection_interval - elapsed_time)
                logger.info(f"Sleeping for {sleep_time:.2f} seconds...")
                # Use a loop with smaller sleeps to allow faster shutdown
                sleep_end = time.time() + sleep_time
                # TODO Remove this for prod and uncomment below
                break
                # while self.running and time.time() < sleep_end:
                #     time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received. Stopping collector.")
        except Exception as e:
            logger.exception(f"Critical error in main collection loop: {e}")
        finally:
            self.stop()

    def stop(self):
        """Stops the data collection loop gracefully."""
        if self.running:
            self.running = False
            logger.info("Stopping Reddit data collector...")
            self.cleanup()  # Call cleanup from BaseCollector if needed
            logger.info("Reddit data collector stopped.")


# Example Usage (if run directly)
if __name__ == "__main__":
    # Configure logging for standalone run
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Create collector instance (adjust paths if needed)
    collector = RedditCollector()

    try:
        collector.collect()
    except Exception:
        logger.exception("Collector failed to run.")
    finally:
        # Ensure stop is called even if collect loop exits unexpectedly
        collector.stop()
