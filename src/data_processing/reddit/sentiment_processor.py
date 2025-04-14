# src/data_processing/reddit/sentiment_processor.py
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, LongType, BooleanType, IntegerType,
    MapType, ArrayType, FloatType
)
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from src.data_processing.common.base_processor import BaseStreamProcessor


class RedditSentimentProcessor(BaseStreamProcessor):
    """Processes Reddit posts for sentiment analysis."""

    def __init__(self, config_path: str):
        """Initialize the Reddit sentiment processor.

        Args:
            config_path: Path to the configuration file
        """
        super().__init__(config_path)
        self.reddit_schema = self._define_schema()
        self.sentiment_analyzer = SentimentIntensityAnalyzer()

    def _define_schema(self) -> StructType:
        """Define the schema for Reddit data.

        Returns:
            Spark schema for Reddit data
        """
        return StructType([
            StructField("author", StringType(), True),
            StructField("collection_method", StringType(), True),
            StructField("collection_timestamp", TimestampType(), True),
            StructField("content_type", StringType(), True),
            StructField("created_utc", DoubleType(), True),
            StructField("id", StringType(), True),
            StructField("is_self", BooleanType(), True),
            StructField("num_comments", IntegerType(), True),
            StructField("permalink", StringType(), True),
            StructField("post_created_datetime", TimestampType(), True),
            StructField("score", IntegerType(), True),
            StructField("selftext", StringType(), True),
            StructField("source", StringType(), True),
            StructField("subreddit", StringType(), True),
            StructField("title", StringType(), True),
            StructField("upvote_ratio", DoubleType(), True),
            StructField("url", StringType(), True),
        ])

    def _clean_text(self, text: str) -> Optional[str]:
        """Clean text for sentiment analysis.

        Args:
            text: Raw text

        Returns:
            Cleaned text or None if empty
        """
        if text is None:
            return None

        import re
        text = text.lower()
        text = re.sub(r"http\S+|www\S+|https\S+", "", text, flags=re.MULTILINE)
        text = re.sub(r"\$[a-zA-Z0-9_]+", "", text)
        text = re.sub(r"@[a-zA-Z0-9_]+", "", text)
        text = re.sub(r"[^a-zA-Z\s]", "", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text if text else None

    def _extract_symbols(self, text: str) -> Optional[List[str]]:
        """Extract stock symbols from text.

        Args:
            text: Text to analyze

        Returns:
            List of stock symbols or None if none found
        """
        if text is None:
            return None

        import re
        potential_symbols = re.findall(r"\b([A-Z]{1,5})\b|\$([A-Z]{1,5})\b", text)
        symbols = list(set(s for tup in potential_symbols for s in tup if s))
        symbols = [s for s in symbols if len(s) > 0 and s.isupper()]
        return symbols if symbols else None

    def _get_sentiment(self, text: str) -> Optional[Dict[str, float]]:
        """Calculate sentiment using VADER.

        Args:
            text: Text to analyze

        Returns:
            Dictionary with sentiment score and magnitude
        """
        if text is None:
            return None

        try:
            vs = self.sentiment_analyzer.polarity_scores(text)
            return {"score": vs["compound"], "magnitude": abs(vs["compound"])}
        except Exception as e:
            logger.warning(f"Sentiment analysis failed: {str(e)[:100]}")
            return None

    def process_reddit_data(self, df: DataFrame) -> DataFrame:
        """Process Reddit data for sentiment analysis.

        Args:
            df: Raw Kafka DataFrame

        Returns:
            Processed DataFrame with sentiment and symbols
        """
        logger.info("Processing Reddit data")

        # Register UDFs
        clean_text_udf = F.udf(self._clean_text, StringType())
        extract_symbols_udf = F.udf(self._extract_symbols, ArrayType(StringType()))

        sentiment_schema = StructType([
            StructField("score", FloatType(), True),
            StructField("magnitude", FloatType(), True),
        ])
        get_sentiment_udf = F.udf(self._get_sentiment, sentiment_schema)

        # Parse JSON value from Kafka
        parsed_df = df.select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), self.reddit_schema).alias("data")
        ).select("kafka_timestamp", "data.*")

        # Add processing timestamp
        parsed_df = parsed_df.withColumn("processing_timestamp", F.current_timestamp())

        # Basic filtering
        filtered_df = parsed_df.filter(
            F.col("id").isNotNull()
            & F.col("created_utc").isNotNull()
            & (F.col("title").isNotNull() | F.col("selftext").isNotNull())
            & F.col("subreddit").isNotNull()
        )

        # Text preprocessing
        filtered_df = filtered_df.withColumn(
            "full_text", F.concat_ws(" ", F.col("title"), F.col("selftext"))
        )
        filtered_df = filtered_df.withColumn(
            "cleaned_text", clean_text_udf(F.col("full_text"))
        )

        # Filter out rows with empty text
        filtered_df = filtered_df.filter(
            F.col("cleaned_text").isNotNull() & (F.length(F.col("cleaned_text")) > 0)
        )

        # Sentiment analysis
        sentiment_df = filtered_df.withColumn(
            "sentiment", get_sentiment_udf(F.col("cleaned_text"))
        )
        sentiment_df = (
            sentiment_df.withColumn("sentiment_score", F.col("sentiment.score"))
            .withColumn("sentiment_magnitude", F.col("sentiment.magnitude"))
            .drop("sentiment", "cleaned_text", "full_text")
        )

        # Filter out rows where sentiment analysis failed
        sentiment_df = sentiment_df.filter(F.col("sentiment_score").isNotNull())

        # Entity recognition (stock symbols)
        symbol_df = sentiment_df.withColumn(
            "symbols",
            extract_symbols_udf(F.concat_ws(" ", F.col("title"), F.col("selftext"))),
        )
        symbol_df = symbol_df.filter(
            F.col("symbols").isNotNull() & (F.size(F.col("symbols")) > 0)
        )
        exploded_df = symbol_df.withColumn("symbol", F.explode(F.col("symbols"))).drop(
            "symbols"
        )

        # Add watermark and prepare for aggregation
        final_df = exploded_df.withColumn(
            "event_timestamp", (F.col("created_utc")).cast(TimestampType())
        )

        watermarked_df = final_df.withWatermark(
            "event_timestamp", self.config["spark_settings"]["watermark_delay_threshold"]
        )

        logger.info("Reddit data processing completed")
        return watermarked_df

    def aggregate_sentiment(self, df: DataFrame) -> DataFrame:
        """Aggregate sentiment data over time windows.

        Args:
            df: Processed DataFrame with sentiment

        Returns:
            Aggregated sentiment DataFrame
        """
        logger.info("Aggregating sentiment data")

        window_duration = self.config["spark_settings"]["processing_window_duration"]

        # Group by window, symbol, subreddit, collection_method
        aggregated_df = df.groupBy(
            F.window("event_timestamp", window_duration).alias("time_window"),
            F.col("symbol"),
            F.col("subreddit"),
            F.col("collection_method"),
        ).agg(
            # Sentiment Aggregations
            F.avg("sentiment_score").alias("sentiment_score"),
            F.avg("sentiment_magnitude").alias("sentiment_magnitude"),
            # Count Aggregations
            F.count("*").alias("post_count"),
            F.approx_count_distinct("author").alias("unique_authors"),
            # Score Aggregations
            F.avg("score").alias("avg_score"),
            F.min("score").alias("min_score"),
            F.max("score").alias("max_score"),
            F.avg("upvote_ratio").alias("avg_upvote_ratio"),
            # Context Aggregations
            F.max("collection_timestamp").alias("collection_timestamp"),
            F.max("post_created_datetime").alias("post_created_datetime"),
            F.max("event_timestamp").cast(LongType()).alias("timestamp"),
            F.collect_list("permalink").alias("all_permalinks"),
        )

        # Simplified content types handling
        aggregated_df = aggregated_df.withColumn(
            "content_types",
            F.lit(None).cast(MapType(StringType(), LongType())),
        )

        # Sample permalinks
        aggregated_df = aggregated_df.withColumn(
            "permalink_sample",
            F.slice(F.col("all_permalinks"), 1, 5),
        ).drop("all_permalinks")

        # Add window details
        aggregated_df = (
            aggregated_df.withColumn("window_start", F.col("time_window.start"))
            .withColumn("window_end", F.col("time_window.end"))
            .withColumn("window_size", F.lit(window_duration))
            .drop("time_window")
        )

        logger.info("Sentiment aggregation completed")
        return aggregated_df

    def run(self) -> None:
        """Run the Reddit sentiment analysis pipeline."""
        try:
            logger.info("Starting Reddit sentiment analysis pipeline")

            # Get configuration parameters
            topics = (
                f"{self.config['kafka']['topics']['social_media_reddit_validated']},"
                f"{self.config['kafka']['topics']['social_media_reddit_comments_validated']}"
            )
            cassandra_keyspace = self.config["cassandra"]["keyspace"]
            cassandra_table = self.config["cassandra"]["table"]

            # Generate a unique checkpoint location
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            checkpoint_dir = Path(self.config["spark_settings"]["checkpoint_location_base_path"])
            checkpoint_location = str(checkpoint_dir / f"reddit_sentiment_{timestamp}")

            # Read from Kafka
            kafka_stream = self.read_from_kafka(topics)

            # Process the data
            processed_df = self.process_reddit_data(kafka_stream)
            aggregated_df = self.aggregate_sentiment(processed_df)

            # Write to Cassandra
            query = self.write_to_cassandra(
                aggregated_df,
                cassandra_keyspace,
                cassandra_table,
                checkpoint_location
            )

            logger.info("Reddit sentiment analysis pipeline started, waiting for termination")
            query.awaitTermination()

        except Exception as e:
            logger.exception(f"Error in Reddit sentiment analysis pipeline: {e}")
            raise