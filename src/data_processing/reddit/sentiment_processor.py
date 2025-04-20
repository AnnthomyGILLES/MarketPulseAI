import os
import sys
from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, lit, current_timestamp, when
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    FloatType,
    ArrayType,
)
from loguru import logger
from pathlib import Path

from src.data_processing.common.base_processor import BaseStreamProcessor

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


class RedditSentimentProcessor(BaseStreamProcessor):
    """Processor for Reddit posts streamed from Kafka to MongoDB."""

    def __init__(self, config_path: str):
        """Initialize the Reddit post processor.

        Args:
            config_path: Path to the configuration file
        """
        super().__init__(config_path)
        self.reddit_schema = self._create_schema()

    def _create_schema(self) -> StructType:
        """Create the schema for Reddit post data.

        Returns:
            StructType schema for Reddit posts
        """
        return StructType(
            [
                StructField("id", StringType(), True),
                StructField("source", StringType(), True),
                StructField("content_type", StringType(), True),
                StructField("collection_timestamp", StringType(), True),
                StructField("created_utc", IntegerType(), True),
                StructField("author", StringType(), True),
                StructField("score", IntegerType(), True),
                StructField("subreddit", StringType(), True),
                StructField("permalink", StringType(), True),
                StructField("detected_symbols", ArrayType(StringType()), True),
                StructField("created_datetime", StringType(), True),
                StructField("title", StringType(), True),
                StructField("selftext", StringType(), True),
                StructField("url", StringType(), True),
                StructField("is_self", BooleanType(), True),
                StructField("upvote_ratio", FloatType(), True),
                StructField("num_comments", IntegerType(), True),
                StructField("collection_method", StringType(), True),
            ]
        )

    def process_reddit_posts(self, kafka_df: DataFrame) -> DataFrame:
        """Process Reddit posts from Kafka.

        Args:
            kafka_df: Raw DataFrame from Kafka

        Returns:
            Processed DataFrame ready for output
        """
        logger.info("Processing Reddit posts")

        # Parse JSON from Kafka value field
        reddit_df = (
            kafka_df.selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), self.reddit_schema).alias("data"))
            .select("data.*")
        )

        # Enrich data
        processed_df = reddit_df.withColumn(
            "processing_timestamp", current_timestamp()
        ).withColumn(
            "detected_symbols",
            when(col("detected_symbols").isNull(), lit([])).otherwise(
                col("detected_symbols")
            ),
        )

        # Apply any additional transformations here
        # For example, sentiment analysis, entity extraction, etc.

        return processed_df

    def run(self) -> None:
        """Main execution method to process Reddit posts from Kafka to MongoDB."""
        logger.info("Starting Reddit post processing pipeline")

        try:
            # Get configuration values
            kafka_topics = "social-media-reddit-posts-validated"
            kafka_topics = f"{self.config['kafka']['topics']['social_media_reddit_validated']}, {self.config['kafka']['topics']['social_media_reddit_comments_validated']}"
            checkpoint_location = self.config.get(
                "checkpoint_location", "/tmp/reddit_checkpoint"
            )

            # Get MongoDB configuration
            mongodb_database = self.config.get("mongodb", {}).get(
                "database", "social_media"
            )
            mongodb_collection = self.config.get("mongodb", {}).get(
                "collection", "reddit_posts"
            )

            # Create checkpoint directory if it doesn't exist
            Path(checkpoint_location).mkdir(parents=True, exist_ok=True)

            # Read from Kafka
            kafka_df = self.read_from_kafka(kafka_topics)

            # Process data
            processed_df = self.process_reddit_posts(kafka_df)

            # Write to MongoDB instead of CSV
            query = self.write_to_mongodb(
                processed_df,
                database=mongodb_database,
                collection=mongodb_collection,
                checkpoint_location=checkpoint_location,
                output_mode="append",
            )

            # Wait for termination
            logger.info(
                f"Streaming query started, writing to MongoDB {mongodb_database}.{mongodb_collection}"
            )
            query.awaitTermination()

        except Exception as e:
            logger.error(f"Error in Reddit post processing: {str(e)}")
            raise


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python reddit_processor.py <config_path>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    processor = RedditSentimentProcessor(config_path)
    processor.run()