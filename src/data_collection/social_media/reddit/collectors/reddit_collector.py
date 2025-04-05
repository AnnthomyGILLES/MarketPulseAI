# src/data_collection/social_media/reddit/collectors/reddit_collector.py

import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Set

import praw
import prawcore
import yaml
from dotenv import load_dotenv
from loguru import logger

from src.common.messaging.kafka_producer import KafkaProducerWrapper
from src.data_collection.base_collector import BaseCollector


class RedditCollector(BaseCollector):
    """
    Collects Reddit posts and comments based on subreddits and symbol mentions.

    Features:
    - Fetches posts using various sorting methods (hot, new, top, rising).
    - Searches for posts mentioning specific stock symbols.
    - Collects comments for fetched posts.
    - Sends data to Kafka topics using KafkaProducerWrapper.
    - Includes basic handling for rate limits and API errors.
    - Uses external configuration for settings.
    - Uses Loguru logger provided by BaseCollector.
    """

    DEFAULT_REDDIT_CONFIG_PATH = (
        Path(__file__).resolve().parents[5]
        / "config"
        / "reddit"
        / "reddit_collector_config.yaml"
    )
    DEFAULT_ENV_PATH = (
        Path(__file__).resolve().parents[5] / "config" / "reddit" / ".env"
    )
    # Define a default Kafka config path (adjust if needed)
    DEFAULT_KAFKA_CONFIG_PATH = (
        Path(__file__).resolve().parents[5] / "config" / "kafka" / "kafka_config.yaml"
    )

    def __init__(
        self,
        kafka_config_path: Optional[str] = None,
        reddit_config_path: Optional[str] = None,
        env_path: Optional[str] = None,
    ):
        # Initialize BaseCollector (which also logs collector initialization)
        super().__init__(collector_name="RedditDataCollector")

        # Load Kafka Config
        self.kafka_config_path = Path(
            kafka_config_path or self.DEFAULT_KAFKA_CONFIG_PATH
        )
        self._load_kafka_config()  # Load Kafka settings first

        # Initialize Kafka Producer
        self.kafka_producer: Optional[KafkaProducerWrapper] = None
        self._initialize_kafka_producer()  # Initialize after loading config

        # Load Reddit specific configuration
        self.reddit_config_path = Path(
            reddit_config_path or self.DEFAULT_REDDIT_CONFIG_PATH
        )
        self.env_path = Path(env_path or self.DEFAULT_ENV_PATH)
        self._load_reddit_config()

        # Initialize Reddit API client
        self.reddit = self._initialize_reddit_client()

        self.running = False
        # Post tracking
        self.collected_post_ids_this_run: Set[str] = set()

    def _load_kafka_config(self):
        """Loads Kafka configuration from a YAML file."""
        logger.info(f"Loading Kafka configuration from: {self.kafka_config_path}")
        try:
            with open(self.kafka_config_path, "r") as f:
                self.kafka_config = yaml.safe_load(f)
                if not self.kafka_config:
                    raise ValueError("Kafka config file is empty or invalid.")
            logger.info("Kafka configuration loaded successfully.")
        except FileNotFoundError:
            logger.error(
                f"Kafka configuration file not found at {self.kafka_config_path}. Cannot initialize producer."
            )
            self.kafka_config = {}  # Ensure kafka_config exists but is empty
            # Decide if this is a fatal error
            # raise # Option: re-raise if Kafka is essential
        except Exception as e:
            logger.exception(
                f"Error loading Kafka configuration from {self.kafka_config_path}: {e}"
            )
            raise  # Re-raise as Kafka config is critical

    def _initialize_kafka_producer(self):
        """Initializes the KafkaProducerWrapper."""
        if not self.kafka_config or "bootstrap_servers" not in self.kafka_config:
            logger.error(
                "Cannot initialize Kafka producer: Missing bootstrap_servers in Kafka config."
            )
            return  # Producer remains None

        try:
            bootstrap_servers = self.kafka_config["bootstrap_servers"]
            producer_settings = self.kafka_config.get("producer_settings", {})
            client_id = producer_settings.get(
                "client_id", f"reddit-collector-{os.getpid()}"
            )  # Example client_id
            acks = producer_settings.get("acks", 1)
            retries = producer_settings.get("retries", 3)
            # Pass other settings directly
            extra_kwargs = {
                k: v
                for k, v in producer_settings.items()
                if k not in ["client_id", "acks", "retries"]
            }

            self.kafka_producer = KafkaProducerWrapper(
                bootstrap_servers=bootstrap_servers,
                client_id=client_id,
                acks=acks,
                retries=retries,
                **extra_kwargs,  # Pass additional kafka-python args
            )
            logger.info("KafkaProducerWrapper initialized successfully.")
        except Exception:
            logger.exception("Failed to initialize KafkaProducerWrapper.")
            self.kafka_producer = None  # Ensure it's None on failure
            # Decide if this should halt initialization
            # raise # Option: re-raise

    def _load_reddit_config(self):
        """Loads collector settings from a YAML configuration file."""
        logger.info(
            f"Loading Reddit collector configuration from: {self.reddit_config_path}"
        )
        try:
            with open(self.reddit_config_path, "r") as f:
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
                f"Reddit configuration file not found at {self.reddit_config_path}. Using defaults."
            )
            raise
        except Exception as e:
            logger.exception(
                f"Error loading Reddit configuration from {self.reddit_config_path}: {e}"
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
                username=username,
                password=password,
            )
            # Test connection
            reddit.user.me()
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
            logger.warning(
                f"Network error during {context}: {error}. Check connection."
            )
            time.sleep(5)
        elif isinstance(error, prawcore.exceptions.ResponseException):
            logger.error(
                f"HTTP error during {context}: {error}. Status: {error.response.status_code}"
            )
            if error.response.status_code == 401:
                logger.error("Reddit API authentication failed. Check credentials.")
                self.stop()
            elif error.response.status_code == 403:
                logger.warning(
                    f"Forbidden access during {context}. Check permissions or endpoint."
                )
            elif error.response.status_code == 404:
                logger.warning(f"Resource not found during {context}: {error}")
            elif error.response.status_code == 429:
                logger.warning(
                    f"Rate limited during {context}. PRAW should handle waits, but logging explicitely."
                )
                time.sleep(10)
            elif error.response.status_code >= 500:
                logger.warning(
                    f"Reddit server error ({error.response.status_code}) during {context}. Retrying later."
                )
                time.sleep(15)
        elif isinstance(error, praw.exceptions.RedditAPIException):
            logger.error(f"Reddit API error during {context}: {error}")
            for sub_error in error.items:
                logger.error(
                    f"  - API Error Type: {sub_error.error_type}, Message: {sub_error.message}"
                )
        else:
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
            _ = subreddit_instance.display_name
            logger.info(
                f"Starting post collection for r/{subreddit_name} (Methods: {', '.join(sort_methods)})"
            )
        except (prawcore.exceptions.Redirect, prawcore.exceptions.NotFound):
            logger.error(f"Subreddit r/{subreddit_name} not found or redirected.")
            return
        except Exception as e:
            self._handle_api_error(e, f"accessing subreddit r/{subreddit_name}")
            return

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
                self._handle_api_error(
                    e, f"collecting {sort_method} posts from r/{subreddit_name}"
                )
                time.sleep(5)

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
            raise

    def _process_post(self, post: praw.models.Submission, collection_method: str):
        """Processes a single Reddit post and sends it to Kafka."""
        if not self.kafka_producer:
            logger.error("Kafka producer not available. Cannot send post data.")
            return  # Cannot proceed without producer

        try:
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
                ).isoformat()
                + "Z",
                "score": post.score,
                "upvote_ratio": post.upvote_ratio,
                "num_comments": post.num_comments,
                "selftext": post.selftext,
                "url": post.url,
                "is_self": post.is_self,
                "permalink": f"https://www.reddit.com{post.permalink}",
                "collection_method": collection_method,
                "collection_timestamp": datetime.now().isoformat() + "Z",
            }

            detected_symbols = self._extract_symbols(post.title + " " + post.selftext)
            if detected_symbols:
                post_data["detected_symbols"] = list(detected_symbols)

            # Send raw post data
            kafka_topic_raw = self.kafka_config["topics"]["social_media_reddit_raw"]
            success_raw = self.kafka_producer.send_message(
                topic=kafka_topic_raw, value=post_data, key=f"reddit_post_{post.id}"
            )
            if success_raw:
                logger.debug(
                    f"Sent post {post.id} from r/{post.subreddit.display_name} to Kafka topic {kafka_topic_raw}"
                )
            # KafkaProducerWrapper already logs errors on failure

            # Send to symbol-specific topic if symbols detected
            if detected_symbols:
                kafka_topic_symbols = self.kafka_config["topics"][
                    "social_media_reddit_symbols"
                ]
                symbols_sent_count = 0
                for symbol in detected_symbols:
                    symbol_data = post_data.copy()
                    symbol_data["symbol"] = symbol
                    success_symbol = self.kafka_producer.send_message(
                        topic=kafka_topic_symbols,
                        value=symbol_data,
                        key=f"reddit_symbol_{symbol}_{post.id}",
                    )
                    if success_symbol:
                        symbols_sent_count += 1
                if symbols_sent_count > 0:
                    logger.debug(
                        f"Sent post {post.id} to Kafka topic {kafka_topic_symbols} for {symbols_sent_count} symbols: {', '.join(detected_symbols)}"
                    )

            # Collect comments if post meets criteria
            if (
                post.score >= self.comment_collection_min_score
                or post.num_comments >= self.comment_collection_min_comments
            ):
                self.collect_comments(post.id, limit=self.comments_per_post)

        except KeyError as e:
            logger.error(
                f"Missing key in Kafka config topics section: {e}. Cannot send message."
            )
        except Exception as e:
            logger.error(
                f"Error processing post {getattr(post, 'id', 'N/A')}: {e}",
                exc_info=True,
            )

    def _extract_symbols(self, text: str) -> Set[str]:
        """
        Extracts potential stock symbols mentioned in text using regex.
        Matches symbols like $AAPL or AAPL (case-insensitive) as whole words.
        """
        if not self.symbols:
            return set()
        if not text:
            return set()

        symbol_map = {
            str(s).upper(): str(s) for s in self.symbols if isinstance(s, (str, bytes))
        }
        if not symbol_map:
            logger.warning("No valid string symbols available for extraction.")
            return set()

        try:
            escaped_symbols = [re.escape(s_upper) for s_upper in symbol_map.keys()]
            symbols_pattern_part = "|".join(escaped_symbols)
            pattern = rf"(?:\$)?\b(?:{symbols_pattern_part})\b"
        except re.error as e:
            logger.error(f"Failed to build regex pattern for symbols: {e}")
            return set()

        detected = set()
        try:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                cleaned_match = match.lstrip("$").upper()
                if cleaned_match in symbol_map:
                    detected.add(symbol_map[cleaned_match])
        except re.error as e:
            logger.error(f"Regex error during symbol extraction: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error during symbol extraction: {e}")

        return detected

    def collect_comments(self, post_id: str, limit: Optional[int] = None):
        """Collects top-level comments for a given post ID."""
        if not self.kafka_producer:
            logger.error("Kafka producer not available. Cannot send comment data.")
            return  # Cannot proceed without producer

        logger.debug(f"Collecting comments for post {post_id} (Limit: {limit})")
        try:
            submission = self.reddit.submission(id=post_id)
            submission.comments.replace_more(limit=0)

            comment_count = 0
            kafka_topic_comments = self.kafka_config["topics"][
                "social_media_reddit_comments"
            ]

            for comment in submission.comments.list():
                if not isinstance(comment, praw.models.Comment):
                    continue

                comment_data = {
                    "id": comment.id,
                    "source": "reddit",
                    "content_type": "comment",
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
                    "parent_id": comment.parent_id,
                    "is_submitter": comment.is_submitter,
                    "permalink": f"https://www.reddit.com{comment.permalink}",
                    "collection_timestamp": datetime.now().isoformat() + "Z",
                }

                detected_symbols = self._extract_symbols(comment.body)
                if detected_symbols:
                    comment_data["detected_symbols"] = list(detected_symbols)

                # Send comment data using KafkaProducerWrapper
                success = self.kafka_producer.send_message(
                    topic=kafka_topic_comments,
                    value=comment_data,
                    key=f"reddit_comment_{comment.id}",
                )
                # KafkaProducerWrapper handles logging errors on send failure

                if success:
                    comment_count += 1
                    if limit is not None and comment_count >= limit:
                        logger.debug(
                            f"Reached comment limit ({limit}) for post {post_id}"
                        )
                        break

            logger.debug(
                f"Collected and attempted to send {comment_count} comments for post {post_id}"
            )

        except prawcore.exceptions.NotFound:
            logger.warning(f"Post {post_id} not found when trying to collect comments.")
        except KeyError as e:
            logger.error(
                f"Missing key in Kafka config topics section: {e}. Cannot send comments."
            )
        except Exception as e:
            self._handle_api_error(e, f"collecting comments for post {post_id}")

    def search_for_symbols(self, time_filters: Optional[List[str]] = None):
        """Searches Reddit globally for posts mentioning tracked symbols."""
        if not self.kafka_producer:
            logger.error(
                "Kafka producer not available. Cannot process symbols found via search."
            )
            return  # Cannot proceed without producer

        if time_filters is None:
            time_filters = self.default_time_filters

        logger.info(f"Starting symbol search for: {', '.join(self.symbols)}")

        for symbol in self.symbols:
            logger.debug(f"Searching for symbol: {symbol}")
            for time_filter in time_filters:
                try:
                    search_results = self.reddit.subreddit("all").search(
                        f'"{symbol}" OR ${symbol}',
                        sort="relevance",
                        time_filter=time_filter,
                        limit=self.posts_per_symbol,
                    )

                    result_count = 0
                    for post in search_results:
                        if post.id not in self.collected_post_ids_this_run:
                            self.collected_post_ids_this_run.add(post.id)
                            # Reuses _process_post which handles Kafka sending
                            self._process_post(
                                post, collection_method=f"symbol_search/{time_filter}"
                            )
                            result_count += 1
                        else:
                            logger.debug(
                                f"Skipping already collected post {post.id} found via symbol search for {symbol}"
                            )

                    logger.debug(
                        f"Found and processed {result_count} new posts mentioning '{symbol}' in time filter '{time_filter}'"
                    )
                    time.sleep(2)

                except Exception as e:
                    self._handle_api_error(
                        e,
                        f"searching for symbol {symbol} with time filter {time_filter}",
                    )
                    time.sleep(5)

        logger.info("Finished symbol search cycle.")

    def collect(self):
        """Main collection loop."""
        if not self.kafka_producer:
            logger.error("Cannot start collection: Kafka producer is not initialized.")
            return
        if not self.reddit:
            logger.error("Cannot start collection: Reddit client is not initialized.")
            return

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
                self.collected_post_ids_this_run.clear()

                # --- Collect posts from configured subreddits ---
                for subreddit_name in self.subreddits:
                    if not self.running:
                        break
                    self.collect_posts_by_subreddit(subreddit_name)
                    if not self.running:
                        break  # Check again after potentially long call
                    time.sleep(1)

                if not self.running:
                    break

                # --- Search for symbol mentions ---
                self.search_for_symbols()

                # --- Cycle complete ---
                end_time = time.time()
                elapsed_time = end_time - start_time
                logger.info(
                    f"Collection cycle finished in {elapsed_time:.2f} seconds. "
                    f"Processed {len(self.collected_post_ids_this_run)} unique posts/comments this cycle."
                )

                # --- Sleep until next cycle ---
                sleep_time = max(0, self.collection_interval - elapsed_time)
                logger.info(f"Sleeping for {sleep_time:.2f} seconds...")
                sleep_end = time.time() + sleep_time
                # TODO: Remove break and uncomment loop for production
                break  # Keep break for testing/dev
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
            self.running = False  # Signal loops to stop
            logger.info("Stopping Reddit data collector...")
            self.cleanup()  # Perform cleanup actions
            logger.info("Reddit data collector stopped.")
        else:
            logger.info("Stop called, but collector was not running.")

    def cleanup(self) -> None:
        """Closes the Kafka producer and performs base cleanup."""
        logger.info("Cleaning up Reddit collector resources...")
        if self.kafka_producer:
            logger.info("Closing Kafka producer...")
            self.kafka_producer.close()  # Use wrapper's close method
        else:
            logger.warning("Kafka producer was not initialized, nothing to close.")
        super().cleanup()  # Call base cleanup if it exists and does something
        logger.info("Reddit collector cleanup finished.")


# Example Usage (if run directly)
if __name__ == "__main__":
    # Configure Loguru for standalone run
    logger.remove()
    logger.add(
        sys.stderr,
        level="DEBUG",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True,
    )

    # Create collector instance (will use default paths or env vars)
    # Assumes config files and .env are correctly placed relative to this script
    # or absolute paths are provided via environment variables if needed.
    collector = None
    try:
        logger.info("Creating RedditCollector instance for standalone run...")
        collector = RedditCollector()
        logger.info("RedditCollector instance created.")

        logger.info("Starting collector...")
        collector.collect()

    except ValueError as e:
        logger.error(f"Configuration error during startup: {e}")
    except Exception as e:
        logger.exception(f"Collector failed to run due to an unexpected error: {e}")
    finally:
        logger.info("Ensuring collector is stopped in finally block...")
        # Ensure stop is called even if collect loop exits unexpectedly or initialization fails partially
        if collector:
            collector.stop()
        else:
            logger.warning("Collector instance was not successfully created.")
        logger.info("Standalone script finished.")
