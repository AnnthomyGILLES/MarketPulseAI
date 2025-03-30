import os
import time
from datetime import datetime
from pathlib import Path

import praw
from dotenv import load_dotenv

from src.data_collection.base_collector import BaseCollector


class RedditCollector(BaseCollector):
    def __init__(self, config_path: str = None):
        if config_path is None:
            base_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
            config_path = str(base_dir / "config" / "kafka" / "kafka_config.yaml")

        super().__init__(config_path, "reddit_data_collector")
        self.subreddits = ["wallstreetbets", "investing", "stocks", "StockMarket"]
        self.symbols = ["AAPL"]  # Example stock symbols to track
        self.running = False
        self.collection_interval = 60  # seconds
        self.reddit = self._initialize_reddit_client()

    def _initialize_reddit_client(self):
        """Initialize the Reddit API client"""

        # Load environment variables from config directory
        env_path = Path(__file__).parents[4] / "config" / "reddit" / ".env"
        load_dotenv(dotenv_path=env_path)

        # Reddit API credentials
        self.client_id = os.getenv("REDDIT_CLIENT_ID")
        self.client_secret = os.getenv("REDDIT_CLIENT_SECRET")
        self.username = os.getenv("REDDIT_USERNAME")
        self.password = os.getenv("REDDIT_PASSWORD")
        self.user_agent = os.getenv(
            "REDDIT_USER_AGENT", "MarketPulseAI:v1.0 (by /u/your_username)"
        )

        return praw.Reddit(
            client_id=self.client_id,
            client_secret=self.client_secret,
            user_agent=self.user_agent,
            username=self.username,
            password=self.password,
        )

    def collect_subreddit_posts(self, subreddit_name, limit=100, time_filter="day"):
        """
        Collect posts from a specific subreddit.

        Args:
            subreddit_name: Name of the subreddit to collect from
            limit: Maximum number of posts to collect
            time_filter: Time filter for posts (hour, day, week, month, year, all)
        """
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            self.logger.info(
                f"Collecting top {limit} posts from r/{subreddit_name} for the past {time_filter}"
            )

            for post in subreddit.top(time_filter=time_filter, limit=limit):
                # Create post data dictionary
                post_data = {
                    "id": post.id,
                    "source": "reddit",
                    "content_type": "post",
                    "subreddit": post.subreddit.display_name,
                    "title": post.title,
                    "author": str(post.author) if post.author else "[deleted]",
                    "created_utc": post.created_utc,
                    "score": post.score,
                    "upvote_ratio": post.upvote_ratio,
                    "num_comments": post.num_comments,
                    "selftext": post.selftext,
                    "url": post.url,
                    "is_self": post.is_self,
                    "permalink": post.permalink,
                    "collection_timestamp": datetime.now().isoformat(),
                }

                # Send to Kafka
                self.send_to_kafka(
                    self.config["kafka"]["topics"]["social_media_reddit_raw"],
                    post_data,
                    key=f"reddit_post_{post.id}",
                )

            self.logger.info(f"Collected posts from r/{subreddit_name}")
        except Exception as e:
            self.logger.error(
                f"Error collecting posts from r/{subreddit_name}: {str(e)}"
            )

    def collect_comments(self, post_id, limit=None):
        """
        Collect comments for a specific post.

        Args:
            post_id: Reddit post ID
            limit: Maximum number of comments to collect (None for all)
        """
        try:
            submission = self.reddit.submission(id=post_id)
            submission.comments.replace_more(
                limit=0
            )  # Retrieve all comments and skip "more comments" links

            self.logger.info(f"Collecting comments for post {post_id}")

            comment_count = 0
            for comment in submission.comments.list():
                # Create comment data dictionary
                comment_data = {
                    "id": comment.id,
                    "post_id": post_id,
                    "body": comment.body,
                    "author": str(comment.author),
                    "created_utc": comment.created_utc,
                    "score": comment.score,
                    "subreddit": comment.subreddit.display_name,
                    "parent_id": comment.parent_id,
                    "collection_timestamp": datetime.now().isoformat(),
                }

                # Send to Kafka
                self.send_to_kafka(
                    self.config["kafka"]["topics"]["social_media_reddit_raw"],
                    comment_data,
                    key=f"reddit_comment_{comment.id}",
                )

                comment_count += 1
                if limit and comment_count >= limit:
                    break

            self.logger.info(f"Collected {comment_count} comments for post {post_id}")
        except Exception as e:
            self.logger.error(f"Error collecting comments for post {post_id}: {str(e)}")

    def search_for_symbols(self, limit=100):
        """
        Search for posts mentioning the tracked stock symbols.

        Args:
            limit: Maximum number of search results per symbol
        """
        for symbol in self.symbols:
            try:
                self.logger.info(f"Searching Reddit for posts mentioning {symbol}")

                # Search across Reddit
                search_results = self.reddit.subreddit("all").search(
                    f"{symbol}", sort="new", time_filter="day", limit=limit
                )

                result_count = 0
                for post in search_results:
                    # Create search result data dictionary
                    post_data = {
                        "id": post.id,
                        "title": post.title,
                        "author": str(post.author),
                        "created_utc": post.created_utc,
                        "score": post.score,
                        "upvote_ratio": post.upvote_ratio,
                        "num_comments": post.num_comments,
                        "subreddit": post.subreddit.display_name,
                        "selftext": post.selftext,
                        "url": post.url,
                        "permalink": post.permalink,
                        "symbol": symbol,
                        "collection_timestamp": datetime.now().isoformat(),
                    }

                    # Send to Kafka
                    self.send_to_kafka(
                        self.config["kafka"]["topics"]["social_media_reddit_raw"],
                        post_data,
                        key=f"reddit_symbol_{symbol}_{post.id}",
                    )

                    result_count += 1

                self.logger.info(f"Found {result_count} posts mentioning {symbol}")
            except Exception as e:
                self.logger.error(f"Error searching for {symbol}: {str(e)}")

    def collect(self):
        """
        Run the Reddit data collection process.

        Collects posts from specified subreddits and searches for posts
        mentioning tracked stock symbols.
        """
        self.running = True
        self.logger.info(
            f"Starting Reddit data collection for subreddits: {', '.join(self.subreddits)}"
        )

        try:
            while self.running:
                start_time = time.time()

                # Collect posts from each subreddit
                for subreddit in self.subreddits:
                    self.collect_subreddit_posts(subreddit, limit=1, time_filter="day")

                # Search for stock symbols
                self.search_for_symbols(limit=1)

                # Collect comments from some top posts
                for subreddit in self.subreddits:
                    try:
                        top_posts = list(self.reddit.subreddit(subreddit).hot(limit=1))
                        for post in top_posts:
                            self.collect_comments(post.id, limit=5)
                    except Exception as e:
                        self.logger.error(
                            f"Error collecting comments from r/{subreddit}: {str(e)}"
                        )

                # Calculate sleep time to maintain collection interval
                elapsed_time = time.time() - start_time
                sleep_time = max(0, self.collection_interval - elapsed_time)

                self.logger.info(
                    f"Collection cycle completed in {elapsed_time:.2f} seconds. Sleeping for {sleep_time:.2f} seconds"
                )

                # Sleep until next collection cycle
                time_slept = 0
                while self.running and time_slept < sleep_time:
                    time.sleep(1)
                    time_slept += 1
            self.running = False
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
    # Create an instance of the collector
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
