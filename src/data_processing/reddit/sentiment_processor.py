import os
import sys
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    from_json,
    col,
    lit,
    current_timestamp,
    when,
    udf,
    expr,
    to_timestamp,
)
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
import redis
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime

from src.data_processing.common.base_processor import BaseStreamProcessor

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


class RedditSentimentProcessor(BaseStreamProcessor):
    """Processor for Reddit posts streamed from Kafka to MongoDB and Redis."""

    def __init__(self, config_path: str):
        """Initialize the Reddit post processor.

        Args:
            config_path: Path to the configuration file
        """
        super().__init__(config_path)
        self.reddit_schema = self._create_schema()
        self.sentiment_analyzer = SentimentIntensityAnalyzer()

        # Initialize Redis connection
        self._init_redis()

    def _init_redis(self):
        """Initialize Redis connection for real-time data storage."""
        redis_config = self.config.get("redis", {})
        redis_host = redis_config.get("host", "redis")
        redis_port = redis_config.get("port", 6379)
        redis_db = redis_config.get("db", 0)
        redis_password = redis_config.get("password", None)

        try:
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                password=redis_password,
                decode_responses=True,
            )
            logger.info(f"Connected to Redis at {redis_host}:{redis_port}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            self.redis_client = None

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

    def analyze_sentiment(self, text: str) -> float:
        """Analyze sentiment of text using VADER.

        Args:
            text: Text to analyze

        Returns:
            Sentiment score between -1 (negative) and 1 (positive)
        """
        if not text or text.strip() == "":
            return 0.0

        # Combine title and text for better context
        sentiment = self.sentiment_analyzer.polarity_scores(text)
        return sentiment["compound"]  # Return compound score

    @udf(FloatType())
    def sentiment_analysis_udf(self, title, selftext):
        """UDF wrapper for sentiment analysis.

        Args:
            title: Post title
            selftext: Post body text

        Returns:
            Sentiment score
        """
        combined_text = ""
        if title and isinstance(title, str):
            combined_text += title + " "

        if selftext and isinstance(selftext, str):
            combined_text += selftext

        if not combined_text.strip():
            return 0.0

        return self.analyze_sentiment(combined_text)

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

        # Register the UDF with the Spark session
        sentiment_udf = udf(
            lambda title, selftext: self.analyze_sentiment(
                (title or "") + " " + (selftext or "")
            ),
            FloatType(),
        )
        self.spark.udf.register("sentiment_analysis", sentiment_udf)

        # Enrich data
        processed_df = (
            reddit_df.withColumn("processing_timestamp", current_timestamp())
            .withColumn(
                "detected_symbols",
                when(col("detected_symbols").isNull(), lit([])).otherwise(
                    col("detected_symbols")
                ),
            )
            .withColumn("sentiment_score", expr("sentiment_analysis(title, selftext)"))
            .withColumn(
                "created_timestamp",
                to_timestamp(col("created_datetime"), "yyyy-MM-dd HH:mm:ss"),
            )
        )

        return processed_df

    def store_in_redis(self, df: DataFrame) -> None:
        """Store aggregated metrics in Redis for real-time dashboards.

        Args:
            df: DataFrame with processed Reddit posts
        """
        if not self.redis_client:
            logger.warning("Redis client not initialized. Skipping Redis storage.")
            return

        try:
            # Collect the DataFrame to local
            posts = df.collect()
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Store subreddit sentiment averages
            subreddit_sentiment = (
                df.groupBy("subreddit")
                .agg({"sentiment_score": "avg", "id": "count"})
                .collect()
            )

            for row in subreddit_sentiment:
                subreddit = row["subreddit"]
                avg_sentiment = row["avg(sentiment_score)"]
                post_count = row["count(id)"]

                # Store in Redis with TTL (1 hour)
                key = f"sentiment:subreddit:{subreddit}:{current_time}"
                value = json.dumps(
                    {
                        "subreddit": subreddit,
                        "sentiment_score": avg_sentiment,
                        "post_count": post_count,
                        "timestamp": current_time,
                    }
                )
                self.redis_client.setex(key, 3600, value)

                # Add to time series list (last hour)
                self.redis_client.lpush(
                    f"timeseries:sentiment:subreddit:{subreddit}", value
                )
                self.redis_client.ltrim(
                    f"timeseries:sentiment:subreddit:{subreddit}", 0, 59
                )  # Keep last 60 entries

            # Store symbol sentiment averages
            for post in posts:
                symbols = post["detected_symbols"]
                if not symbols:
                    continue

                sentiment = post["sentiment_score"]
                for symbol in symbols:
                    # Store in Redis with TTL (1 hour)
                    key = f"sentiment:symbol:{symbol}:{current_time}"
                    value = json.dumps(
                        {
                            "symbol": symbol,
                            "sentiment_score": sentiment,
                            "timestamp": current_time,
                        }
                    )
                    self.redis_client.setex(key, 3600, value)

                    # Add to time series list (last hour)
                    self.redis_client.lpush(
                        f"timeseries:sentiment:symbol:{symbol}", value
                    )
                    self.redis_client.ltrim(
                        f"timeseries:sentiment:symbol:{symbol}", 0, 59
                    )  # Keep last 60 entries

            # Update overall metrics
            overall_sentiment = df.agg({"sentiment_score": "avg"}).collect()[0][0]
            self.redis_client.setex("sentiment:overall:latest", 3600, overall_sentiment)

            # Store total post count
            total_posts = df.count()
            self.redis_client.incrby("stats:total_posts:today", total_posts)
            self.redis_client.expire("stats:total_posts:today", 86400)  # 24 hours TTL

            logger.info(f"Stored {total_posts} posts metrics in Redis")

        except Exception as e:
            logger.error(f"Error storing data in Redis: {str(e)}")

    def run(self) -> None:
        """Main execution method to process Reddit posts from Kafka to MongoDB and Redis."""
        logger.info("Starting Reddit post processing pipeline")

        try:
            # Get Kafka topics from the loaded Kafka config
            kafka_topics = (
                f"{self.config['kafka']['topics']['social_media_reddit_validated']}, "
                f"{self.config['kafka']['topics']['social_media_reddit_comments_validated']}"
            )

            checkpoint_location = self.config.get(
                "checkpoint_location", "/tmp/reddit_checkpoint"
            )

            # Get MongoDB configuration
            mongodb_database = self.config.get("mongodb", {}).get(
                "database", "social_media"
            )
            mongodb_collection = self.config.get("mongodb", {}).get(
                "collection", "reddit_sentiment"
            )

            # Create checkpoint directory if it doesn't exist
            Path(checkpoint_location).mkdir(parents=True, exist_ok=True)

            # Read from Kafka
            kafka_df = self.read_from_kafka(kafka_topics)

            # Process data
            processed_df = self.process_reddit_posts(kafka_df)

            # Write to MongoDB for historical data
            query = self.write_to_mongodb(
                processed_df,
                database=mongodb_database,
                collection=mongodb_collection,
                checkpoint_location=checkpoint_location,
                output_mode="append",
            )

            # Add a foreachBatch action to write to Redis for real-time data
            def write_to_redis_and_mongodb(batch_df, batch_id):
                if not batch_df.isEmpty():
                    # Store aggregated metrics in Redis
                    self.store_in_redis(batch_df)

            # Use foreachBatch to write to both MongoDB and Redis
            query = (
                processed_df.writeStream.foreachBatch(write_to_redis_and_mongodb)
                .option("checkpointLocation", checkpoint_location)
                .outputMode("append")
                .start()
            )

            # Wait for termination
            logger.info(
                f"Streaming query started, writing to MongoDB {mongodb_database}.{mongodb_collection} and Redis"
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