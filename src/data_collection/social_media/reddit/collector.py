import json
import logging
import os
import time
from datetime import datetime

import praw
from dotenv import load_dotenv

from src.common.messaging.kafka_producer import KafkaProducerWrapper

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("reddit_collector")


class RedditCollector:
    """
    Collects Reddit posts and comments related to stocks and financial markets,
    then publishes them to Kafka topics for further processing.
    """

    def __init__(self, config=None):
        """
        Initialize the Reddit collector with configuration.

        Args:
            config (dict, optional): Configuration dictionary. If None, loads from environment.
        """
        # Load environment variables
        load_dotenv()

        self.config = config or {}

        # Reddit API credentials
        self.client_id = self.config.get("REDDIT_CLIENT_ID") or os.getenv(
            "REDDIT_CLIENT_ID"
        )
        self.client_secret = self.config.get("REDDIT_CLIENT_SECRET") or os.getenv(
            "REDDIT_CLIENT_SECRET"
        )
        self.user_agent = self.config.get("REDDIT_USER_AGENT") or os.getenv(
            "REDDIT_USER_AGENT", "MarketPulseAI:v1.0 (by /u/your_username)"
        )

        # Kafka configuration
        self.kafka_bootstrap_servers = self.config.get(
            "KAFKA_BOOTSTRAP_SERVERS"
        ) or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.kafka_topic = self.config.get("REDDIT_KAFKA_TOPIC") or os.getenv(
            "REDDIT_KAFKA_TOPIC", "social-media-reddit-raw"
        )

        # Reddit configuration
        self.subreddits = self.config.get("REDDIT_SUBREDDITS") or os.getenv(
            "REDDIT_SUBREDDITS", "wallstreetbets,stocks,investing,StockMarket,options"
        )
        self.post_limit = int(
            self.config.get("REDDIT_POST_LIMIT") or os.getenv("REDDIT_POST_LIMIT", 100)
        )
        self.comment_limit = int(
            self.config.get("REDDIT_COMMENT_LIMIT")
            or os.getenv("REDDIT_COMMENT_LIMIT", 100)
        )
        self.polling_interval = int(
            self.config.get("REDDIT_POLLING_INTERVAL")
            or os.getenv("REDDIT_POLLING_INTERVAL", 60)
        )

        # Initialize Reddit and Kafka clients
        try:
            self._init_reddit_client()
            self._init_kafka_producer()
            logger.info("Reddit collector initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Reddit collector: {str(e)}")
            raise

    def _init_reddit_client(self):
        """Initialize the Reddit PRAW client."""
        if not all([self.client_id, self.client_secret, self.user_agent]):
            raise ValueError("Reddit API credentials not properly configured")

        self.reddit = praw.Reddit(
            client_id=self.client_id,
            client_secret=self.client_secret,
            user_agent=self.user_agent,
        )
        logger.info("Reddit client initialized")

    def _init_kafka_producer(self):
        """Initialize the Kafka producer using the KafkaProducerWrapper."""
        try:
            bootstrap_servers = self.kafka_bootstrap_servers.split(",")
            self.producer = KafkaProducerWrapper(
                bootstrap_servers=bootstrap_servers,
                topic=self.kafka_topic
            )
            logger.info(f"Kafka producer connected to {self.kafka_bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {str(e)}")
            raise

    def _publish_to_kafka(self, data, key=None):
        """
        Publish data to Kafka topic.

        Args:
            data (dict): Data to publish
            key (str, optional): Kafka message key (unused with KafkaProducerWrapper)

        Returns:
            bool: Whether the message was sent successfully
        """
        try:
            success = self.producer.send_message(data)
            return success
        except Exception as e:
            logger.error(f"Failed to publish data to Kafka: {str(e)}")
            return False

    def collect_posts(self, subreddits=None, time_filter="day"):
        """
        Collect posts from specified subreddits.

        Args:
            subreddits (str, optional): Comma-separated list of subreddits to search
            time_filter (str, optional): Time filter for posts ('hour', 'day', 'week', 'month', 'year', 'all')

        Returns:
            int: Number of posts collected
        """
        if not subreddits:
            subreddits = self.subreddits

        subreddit_list = [s.strip() for s in subreddits.split(",")]
        logger.info(f"Collecting posts from subreddits: {subreddit_list}")

        # Combine subreddits into a single query
        combined_subreddits = "+".join(subreddit_list)
        subreddit = self.reddit.subreddit(combined_subreddits)

        posts_count = 0

        try:
            # Get hot posts
            for post in subreddit.hot(limit=self.post_limit):
                post_data = self._extract_post_data(post)

                # Publish to Kafka
                if post_data:
                    success = self._publish_to_kafka(
                        data=post_data, key=post_data.get("id")
                    )
                    if success:
                        posts_count += 1

            logger.info(f"Collected and published {posts_count} posts")
            return posts_count

        except Exception as e:
            logger.error(f"Error collecting posts: {str(e)}")
            return posts_count

    def _extract_post_data(self, post):
        """
        Extract relevant data from a Reddit post.

        Args:
            post: PRAW post object

        Returns:
            dict: Extracted post data
        """
        try:
            # Basic post data
            post_data = {
                "id": post.id,
                "source": "reddit",
                "content_type": "post",
                "subreddit": post.subreddit.display_name,
                "title": post.title,
                "selftext": post.selftext,
                "url": post.url,
                "author": str(post.author) if post.author else "[deleted]",
                "created_utc": post.created_utc,
                "score": post.score,
                "upvote_ratio": post.upvote_ratio,
                "num_comments": post.num_comments,
                "permalink": post.permalink,
                "is_self": post.is_self,
                "collected_at": datetime.now().timestamp(),
            }

            return post_data

        except Exception as e:
            logger.error(f"Error extracting post data: {str(e)}")
            return None

    def collect_comments(self, post_ids=None, limit=None):
        """
        Collect comments from specific posts or from recent posts.

        Args:
            post_ids (list, optional): List of post IDs to collect comments from
            limit (int, optional): Maximum number of comments to collect per post

        Returns:
            int: Number of comments collected
        """
        if limit is None:
            limit = self.comment_limit

        comments_count = 0

        try:
            # If no post IDs provided, collect from recent posts
            if not post_ids:
                subreddit_list = [s.strip() for s in self.subreddits.split(",")]
                combined_subreddits = "+".join(subreddit_list)
                subreddit = self.reddit.subreddit(combined_subreddits)

                # Get post IDs from hot posts
                post_ids = [post.id for post in subreddit.hot(limit=10)]

            # Collect comments for each post
            for post_id in post_ids:
                submission = self.reddit.submission(id=post_id)
                submission.comments.replace_more(
                    limit=0
                )  # Replace MoreComments objects with actual comments

                for comment in submission.comments.list()[:limit]:
                    comment_data = self._extract_comment_data(comment, post_id)

                    # Publish to Kafka
                    if comment_data:
                        success = self._publish_to_kafka(
                            data=comment_data, key=comment_data.get("id")
                        )
                        if success:
                            comments_count += 1

            logger.info(f"Collected and published {comments_count} comments")
            return comments_count

        except Exception as e:
            logger.error(f"Error collecting comments: {str(e)}")
            return comments_count

    def _extract_comment_data(self, comment, post_id):
        """
        Extract relevant data from a Reddit comment.

        Args:
            comment: PRAW comment object
            post_id: ID of the parent post

        Returns:
            dict: Extracted comment data
        """
        try:
            # Basic comment data
            comment_data = {
                "id": comment.id,
                "source": "reddit",
                "content_type": "comment",
                "post_id": post_id,
                "subreddit": comment.subreddit.display_name,
                "body": comment.body,
                "author": str(comment.author) if comment.author else "[deleted]",
                "created_utc": comment.created_utc,
                "score": comment.score,
                "permalink": comment.permalink,
                "is_submitter": comment.is_submitter,
                "parent_id": comment.parent_id,
                "collected_at": datetime.now().timestamp(),
            }

            return comment_data

        except Exception as e:
            logger.error(f"Error extracting comment data: {str(e)}")
            return None

    def run(self, continuous=True):
        """
        Run the collector, either once or continuously.

        Args:
            continuous (bool): Whether to run continuously

        Returns:
            dict: Collection statistics
        """
        stats = {"posts_collected": 0, "comments_collected": 0, "iterations": 0}

        try:
            if continuous:
                logger.info(
                    f"Starting continuous collection (polling every {self.polling_interval} seconds)"
                )

                while True:
                    stats["iterations"] += 1
                    logger.info(f"Collection iteration {stats['iterations']}")

                    # Collect posts
                    posts_count = self.collect_posts()
                    stats["posts_collected"] += posts_count

                    # Collect comments
                    comments_count = self.collect_comments()
                    stats["comments_collected"] += comments_count

                    logger.info(f"Sleeping for {self.polling_interval} seconds")
                    time.sleep(self.polling_interval)
            else:
                # Single run
                logger.info("Starting one-time collection")

                # Collect posts
                posts_count = self.collect_posts()
                stats["posts_collected"] += posts_count

                # Collect comments
                comments_count = self.collect_comments()
                stats["comments_collected"] += comments_count

                logger.info("One-time collection completed")

            return stats

        except KeyboardInterrupt:
            logger.info("Collection stopped by user")
            return stats
        except Exception as e:
            logger.error(f"Error in collection run: {str(e)}")
            return stats

    def close(self):
        """Clean up resources."""
        try:
            if hasattr(self, "producer"):
                self.producer.close()
                logger.info("Kafka producer closed")
        except Exception as e:
            logger.error(f"Error closing resources: {str(e)}")


# Example usage
if __name__ == "__main__":
    collector = RedditCollector()
    try:
        stats = collector.run(continuous=True)
    finally:
        collector.close()
