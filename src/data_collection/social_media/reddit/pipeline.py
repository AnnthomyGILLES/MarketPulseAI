# src/data_collection/social_media/reddit/pipeline.py
import logging
import time
from datetime import datetime
from pathlib import Path

from src.common.messaging.kafka_producer import KafkaProducerWrapper
from src.data_collection.social_media.reddit.collector import RedditCollector
from src.data_collection.social_media.reddit.filters import RedditContentFilter
from src.data_collection.social_media.reddit.validation import RedditContentValidator
from src.utils.config import load_config

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("reddit_pipeline")


class RedditPipeline:
    """
    End-to-end pipeline for Reddit data collection, filtering, validation, and publication.
    Implements the BaseCollector pattern seen in the stock market collector.
    """

    def __init__(self, config_path: str = None):
        """
        Initialize the Reddit pipeline.

        Args:
            config_path (str, optional): Path to configuration file
        """
        # Set default config path if not provided
        if config_path is None:
            base_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
            config_path = str(base_dir / "config" / "kafka" / "kafka_config.yaml")

        # Load configuration
        self.config = load_config(config_path)

        # Initialize components
        self.collector = RedditCollector(self.config)
        self.filter = RedditContentFilter(self.config)
        self.validator = RedditContentValidator(self.config)

        # Initialize Kafka producers
        bootstrap_servers = self.config["kafka"]["bootstrap_servers_dev"]
        self.raw_producer = KafkaProducerWrapper(
            bootstrap_servers=bootstrap_servers,
            topic=self.config["kafka"]["topics"]["social_media_reddit_raw"],
        )

        self.validated_producer = KafkaProducerWrapper(
            bootstrap_servers=bootstrap_servers,
            topic=self.config["kafka"]["topics"]["social_media_reddit_validated"],
        )

        self.error_producer = KafkaProducerWrapper(
            bootstrap_servers=bootstrap_servers,
            topic=self.config["kafka"]["topics"]["social_media_reddit_error"],
        )

        # Statistics tracking
        self.stats = {
            "total_collected": 0,
            "valid": 0,
            "invalid": 0,
            "financially_relevant": 0,
            "with_symbols": 0,
            "start_time": datetime.now(),
        }

        # Control flag
        self.running = False

        logger.info("Reddit pipeline initialized")

    def process_content(self, content):
        """
        Process a single piece of Reddit content through the pipeline.

        Args:
            content (dict): Reddit content to process

        Returns:
            bool: Whether content was successfully processed and sent to Kafka
        """
        try:
            # 1. Track raw content
            self.stats["total_collected"] += 1

            # 2. Filter for financial relevance and extract symbols
            filtered_content = self.filter.filter_content(content)

            # Skip if not financially relevant
            if filtered_content is None:
                return False

            self.stats["financially_relevant"] += 1

            if filtered_content.get("symbol_count", 0) > 0:
                self.stats["with_symbols"] += 1

            # 3. Validate the content
            is_valid, validated_content = self.validator.validate(filtered_content)

            # 4. Send to appropriate Kafka topic
            if is_valid:
                self.stats["valid"] += 1
                return self.validated_producer.send_message(validated_content)
            else:
                self.stats["invalid"] += 1
                return self.error_producer.send_message(validated_content)

        except Exception as e:
            logger.error(f"Error processing content: {str(e)}")
            return False

    def process_posts(self, subreddits=None, time_filter="day"):
        """
        Process posts from specified subreddits.

        Args:
            subreddits (str, optional): Comma-separated list of subreddits
            time_filter (str): Time filter for posts

        Returns:
            dict: Processing statistics
        """
        try:
            logger.info(f"Processing posts from {subreddits or 'default subreddits'}")

            # Collect posts from Reddit
            posts = self.collector.collect_posts(subreddits, time_filter)

            # For each post, process through the pipeline
            processed_count = 0
            for post in posts:
                if self.process_content(post):
                    processed_count += 1

            logger.info(f"Processed {processed_count} posts")
            return processed_count

        except Exception as e:
            logger.error(f"Error processing posts: {str(e)}")
            return 0

    def process_comments(self, post_ids=None, limit=None):
        """
        Process comments from specified posts.

        Args:
            post_ids (list, optional): List of post IDs
            limit (int, optional): Maximum number of comments per post

        Returns:
            dict: Processing statistics
        """
        try:
            logger.info(
                f"Processing comments from {len(post_ids) if post_ids else 'recent'} posts"
            )

            # Collect comments from Reddit
            comments = self.collector.collect_comments(post_ids, limit)

            # For each comment, process through the pipeline
            processed_count = 0
            for comment in comments:
                if self.process_content(comment):
                    processed_count += 1

            logger.info(f"Processed {processed_count} comments")
            return processed_count

        except Exception as e:
            logger.error(f"Error processing comments: {str(e)}")
            return 0

    def run(self, continuous=True, interval=60):
        """
        Run the pipeline, either once or continuously.

        Args:
            continuous (bool): Whether to run continuously
            interval (int): Polling interval in seconds for continuous mode

        Returns:
            dict: Pipeline statistics
        """
        self.running = True

        try:
            if continuous:
                logger.info(
                    f"Starting continuous pipeline (polling every {interval} seconds)"
                )

                while self.running:
                    start_time = time.time()

                    # Process posts
                    self.process_posts()

                    # Process comments
                    self.process_comments()

                    # Report statistics
                    self._report_statistics()

                    # Sleep until next interval
                    elapsed = time.time() - start_time
                    sleep_time = max(1, interval - elapsed)
                    logger.info(
                        f"Completed iteration in {elapsed:.2f}s, sleeping for {sleep_time:.2f}s"
                    )
                    time.sleep(sleep_time)
            else:
                # Single run
                logger.info("Running one-time pipeline")

                # Process posts
                self.process_posts()

                # Process comments
                self.process_comments()

                # Report statistics
                self._report_statistics()

            return self.stats

        except KeyboardInterrupt:
            logger.info("Pipeline stopped by user")
            return self.stats
        except Exception as e:
            logger.error(f"Error in pipeline run: {str(e)}")
            return self.stats
        finally:
            self.running = False

    def _report_statistics(self):
        """Report current pipeline statistics."""
        runtime = (datetime.now() - self.stats["start_time"]).total_seconds() / 60.0

        logger.info(
            f"Pipeline statistics after {runtime:.1f} minutes:\n"
            f"  Total collected: {self.stats['total_collected']}\n"
            f"  Financially relevant: {self.stats['financially_relevant']} "
            f"({self.stats['financially_relevant'] / max(1, self.stats['total_collected']) * 100:.1f}%)\n"
            f"  With stock symbols: {self.stats['with_symbols']} "
            f"({self.stats['with_symbols'] / max(1, self.stats['financially_relevant']) * 100:.1f}%)\n"
            f"  Valid: {self.stats['valid']} "
            f"({self.stats['valid'] / max(1, self.stats['financially_relevant']) * 100:.1f}%)\n"
            f"  Invalid: {self.stats['invalid']} "
            f"({self.stats['invalid'] / max(1, self.stats['financially_relevant']) * 100:.1f}%)"
        )

    def stop(self):
        """Stop the pipeline."""
        self.running = False
        logger.info("Stopping Reddit pipeline")

        # Close resources
        try:
            self.collector.close()
            self.raw_producer.close()
            self.validated_producer.close()
            self.error_producer.close()
            logger.info("Closed all pipeline resources")
        except Exception as e:
            logger.error(f"Error closing resources: {str(e)}")


# Example usage
if __name__ == "__main__":
    pipeline = RedditPipeline()
    try:
        pipeline.run(continuous=True, interval=300)  # Run every 5 minutes
    except KeyboardInterrupt:
        print("Pipeline interrupted by user. Shutting down...")
    finally:
        pipeline.stop()
