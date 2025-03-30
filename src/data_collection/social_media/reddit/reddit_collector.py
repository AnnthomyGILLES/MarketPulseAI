# src/data_collection/social_media/reddit/reddit_collector.py
import time
from datetime import datetime
from pathlib import Path

from src.data_collection.base_collector import BaseCollector
from src.data_collection.social_media.reddit.pipeline import RedditPipeline


class RedditCollector(BaseCollector):
    """
    Reddit data collector that follows the BaseCollector pattern.
    This class adapts the RedditPipeline to the BaseCollector interface.
    """

    def __init__(self, config_path: str = None):
        """
        Initialize the Reddit collector.

        Args:
            config_path (str, optional): Path to configuration file
        """
        if config_path is None:
            base_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
            config_path = str(base_dir / "config" / "kafka" / "kafka_config.yaml")

        super().__init__(config_path, "reddit_data_collector")

        # Configure collection parameters
        self.subreddits = self.config.get(
            "REDDIT_SUBREDDITS", "wallstreetbets,stocks,investing,options,pennystocks"
        )
        self.collection_interval = int(
            self.config.get("REDDIT_POLLING_INTERVAL", 300)
        )  # 5 minutes default
        self.running = False

        # Initialize the pipeline
        self.pipeline = RedditPipeline(config_path)

        self.logger.info(
            f"Reddit collector initialized for subreddits: {self.subreddits}"
        )

    def collect(self) -> None:
        """
        Run the collection process for Reddit data.
        """
        self.running = True
        self.logger.info(
            f"Starting Reddit data collection for subreddits: {self.subreddits}"
        )

        try:
            last_run_time = 0

            while self.running:
                current_time = time.time()
                elapsed = current_time - last_run_time

                # Run collection if it's time or first run
                if elapsed >= self.collection_interval or last_run_time == 0:
                    self.logger.info(
                        f"Running Reddit collection cycle at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    )

                    # Split subreddits string into list
                    subreddit_list = self.subreddits.split(",")

                    # Process in batches to avoid rate limiting
                    for i in range(0, len(subreddit_list), 3):
                        batch = subreddit_list[i : i + 3]
                        batch_str = ",".join(batch)

                        self.logger.info(f"Processing subreddit batch: {batch_str}")

                        # Collect posts
                        post_count = self.pipeline.process_posts(batch_str)
                        self.logger.info(
                            f"Processed {post_count} posts from {batch_str}"
                        )

                        # Collect comments from recent posts
                        comment_count = self.pipeline.process_comments()
                        self.logger.info(
                            f"Processed {comment_count} comments from {batch_str}"
                        )

                        # Sleep between batches to avoid rate limiting
                        if i + 3 < len(subreddit_list):
                            time.sleep(5)

                    # Update last run time
                    last_run_time = time.time()

                    # Report collection statistics
                    self.pipeline._report_statistics()

                    self.logger.info(
                        f"Completed collection cycle. Next run in {self.collection_interval} seconds"
                    )

                # Sleep for a bit to avoid busy-waiting
                time.sleep(min(10, self.collection_interval / 10))

        except KeyboardInterrupt:
            self.logger.info("Reddit data collection stopped by user")
        except Exception as e:
            self.logger.error(f"Reddit data collection failed: {str(e)}")
        finally:
            self.running = False
            self.cleanup()

    def stop(self) -> None:
        """Stop the data collection process"""
        self.running = False
        self.logger.info("Stopping Reddit data collection")
        self.pipeline.stop()
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
