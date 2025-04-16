# src/data_processing/reddit/sentiment_processor.py
from typing import List, Optional, Dict, Any

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
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
        self.spark = None

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
        """Calculate sentiment scores for text.
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with sentiment scores or None if analysis fails
        """
        if text is None:
            return None
            
        try:
            vs = self.sentiment_analyzer.polarity_scores(text)
            return {"score": vs["compound"], "magnitude": abs(vs["compound"])}
        except Exception as e:
            logger.warning(f"Sentiment analysis failed for text: {text[:50]}... Error: {e}")
            return None

    def process_reddit_data(self, df: DataFrame) -> DataFrame:
        """Process Reddit data for sentiment analysis.

        Args:
            df: Raw Kafka DataFrame

        Returns:
            Processed DataFrame with sentiment and symbols
        """
        logger.info("Processing Reddit data")

        # Register UDFs using the instance methods
        clean_text_udf = F.udf(self._clean_text, StringType())
        extract_symbols_udf = F.udf(self._extract_symbols, ArrayType(StringType()))

        sentiment_schema = StructType([
            StructField("score", FloatType(), True),
            StructField("magnitude", FloatType(), True),
        ])
        
        # Define local function to avoid capturing self in UDF
        def get_sentiment_local(text):
            if text is None:
                return None
            try:
                from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
                analyzer = SentimentIntensityAnalyzer()
                vs = analyzer.polarity_scores(text)
                return {"score": vs["compound"], "magnitude": abs(vs["compound"])}
            except Exception as e:
                logger.warning(f"Sentiment analysis failed for text: {text[:50]}... Error: {e}")
                return None
                
        get_sentiment_udf = F.udf(get_sentiment_local, sentiment_schema)

        # Parse JSON value from Kafka
        parsed_df = df.select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), self.reddit_schema).alias("data")
        ).select("kafka_timestamp", "data.*")

        # Add processing timestamp
        parsed_df = parsed_df.withColumn("processing_timestamp", F.current_timestamp())

        # Basic filtering
        filtered_df = self._filter_valid_posts(parsed_df)

        # Text preprocessing
        processed_df = self._preprocess_text(filtered_df)

        # Sentiment analysis
        sentiment_df = self._apply_sentiment_analysis(processed_df)

        # Extract and explode symbols
        exploded_df = self._extract_and_explode_symbols(sentiment_df)

        # Add watermark and prepare for aggregation
        final_df = self._prepare_for_aggregation(exploded_df)

        logger.info("Reddit data processing completed")
        return final_df

    def _filter_valid_posts(self, df: DataFrame) -> DataFrame:
        """Filter for valid Reddit posts.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Filtered DataFrame
        """
        return df.filter(
            F.col("id").isNotNull()
            & F.col("created_utc").isNotNull()
            & (F.col("title").isNotNull() | F.col("selftext").isNotNull())
            & F.col("subreddit").isNotNull()
        )

    def _preprocess_text(self, df: DataFrame) -> DataFrame:
        """Preprocess text for sentiment analysis.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with preprocessed text
        """
        clean_text_udf = F.udf(self._clean_text, StringType())
        
        # Combine title and selftext
        df = df.withColumn(
            "full_text", F.concat_ws(" ", F.col("title"), F.col("selftext"))
        )
        
        # Clean text
        df = df.withColumn(
            "cleaned_text", clean_text_udf(F.col("full_text"))
        )
        
        # Filter out rows with empty text
        return df.filter(
            F.col("cleaned_text").isNotNull() & (F.length(F.col("cleaned_text")) > 0)
        )

    def _apply_sentiment_analysis(self, df: DataFrame) -> DataFrame:
        """Apply sentiment analysis to text.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with sentiment scores
        """
        sentiment_schema = StructType([
            StructField("score", FloatType(), True),
            StructField("magnitude", FloatType(), True),
        ])
        
        # Define local function to avoid capturing self in UDF
        def get_sentiment_local(text):
            if text is None:
                return None
            try:
                from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
                analyzer = SentimentIntensityAnalyzer()
                vs = analyzer.polarity_scores(text)
                return {"score": vs["compound"], "magnitude": abs(vs["compound"])}
            except Exception as e:
                logger.warning(f"Sentiment analysis failed for text: {text[:50]}... Error: {e}")
                return None
                
        get_sentiment_udf = F.udf(get_sentiment_local, sentiment_schema)
        
        # Apply sentiment analysis
        df = df.withColumn(
            "sentiment", get_sentiment_udf(F.col("cleaned_text"))
        )
        
        # Extract sentiment components
        df = (
            df.withColumn("sentiment_score", F.col("sentiment.score"))
            .withColumn("sentiment_magnitude", F.col("sentiment.magnitude"))
            .drop("sentiment", "cleaned_text", "full_text")
        )
        
        # Filter out rows where sentiment analysis failed
        return df.filter(F.col("sentiment_score").isNotNull())

    def _extract_and_explode_symbols(self, df: DataFrame) -> DataFrame:
        """Extract and explode symbols from text.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with exploded symbols
        """
        extract_symbols_udf = F.udf(self._extract_symbols, ArrayType(StringType()))
        
        # Extract symbols
        df = df.withColumn(
            "symbols",
            extract_symbols_udf(F.concat_ws(" ", F.col("title"), F.col("selftext"))),
        )
        
        # Filter for posts with symbols
        df = df.filter(
            F.col("symbols").isNotNull() & (F.size(F.col("symbols")) > 0)
        )
        
        # Explode symbols
        return df.withColumn("symbol", F.explode(F.col("symbols"))).drop(
            "symbols"
        )

    def _prepare_for_aggregation(self, df: DataFrame) -> DataFrame:
        """Prepare DataFrame for aggregation by adding watermark.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame ready for aggregation
        """
        # Add timestamp for windowing
        df = df.withColumn(
            "event_timestamp", (F.col("created_utc")).cast(TimestampType())
        )
        
        # Add watermark
        return df.withWatermark(
            "event_timestamp", self.config["spark_settings"]["watermark_delay_threshold"]
        )

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

        # Sample permalinks (take a few examples for reference)
        aggregated_df = aggregated_df.withColumn(
            "permalink_sample",
            F.expr("slice(all_permalinks, 1, least(size(all_permalinks), 3))"),
        ).drop("all_permalinks")

        # Add a unique ID
        aggregated_df = aggregated_df.withColumn(
            "_id", 
            F.concat(
                F.col("symbol"),
                F.lit("_"),
                F.col("subreddit"),
                F.lit("_"),
                F.expr("cast(time_window.start as long)"),
            )
        )

        logger.info("Sentiment aggregation completed")
        return aggregated_df

    def run(self) -> None:
        """Run the Reddit sentiment processing pipeline."""
        logger.info("Starting Reddit sentiment processing pipeline")

        try:
            # Set up Spark session with MongoDB connector
            self.spark = self._create_spark_session()
            
            # Initialize streaming with Kafka source
            kafka_stream = self._create_kafka_stream()

            # Process streaming data
            processed_stream = self.process_reddit_data(kafka_stream)
            aggregated_stream = self.aggregate_sentiment(processed_stream)

            # Write to MongoDB
            self._write_to_mongodb(aggregated_stream)

        except Exception as e:
            logger.error(f"Error in Reddit sentiment processing pipeline: {str(e)}")
            raise
        finally:
            if self.spark:
                self.spark.stop()

    def _write_to_mongodb(self, df: DataFrame) -> None:
        """Write DataFrame to MongoDB.
        
        Args:
            df: DataFrame to write
        """
        mongodb_config = self.config.get("mongodb", {})
        mongodb_uri = f"mongodb://{mongodb_config.get('connection_host')}:{mongodb_config.get('connection_port')}"
        database = mongodb_config.get("database", "social_media")
        collection = mongodb_config.get("collection", "reddit_sentiment")
        
        # Define MongoDB output options
        output_options = {
            "uri": mongodb_uri,
            "database": database,
            "collection": collection,
            "replaceDocument": "false",
            "forceInsert": "true"
        }
        
        # Add authentication if provided
        if mongodb_config.get("auth_username") and mongodb_config.get("auth_password"):
            output_options["credentials"] = {
                "user": mongodb_config.get("auth_username"),
                "password": mongodb_config.get("auth_password")
            }
        
        # Configure MongoDB output stream
        stream_query = (
            df.writeStream
            .outputMode("append")
            .format("mongodb")
            .option("checkpointLocation", f"{self.config['spark_settings']['checkpoint_location_base_path']}/mongodb")
            .options(**output_options)
            .trigger(processingTime=self.config["spark_settings"]["processing_window_duration"])
            .start()
        )

        # Wait for termination
        stream_query.awaitTermination()

    def _create_spark_session(self) -> SparkSession:
        """Create a Spark session with the required dependencies.
        
        Returns:
            Configured SparkSession
        """
        from pyspark.sql import SparkSession

        # Get package dependencies from config
        mongodb_package = self.config.get(
            "spark_mongodb_package", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"
        )
        kafka_package = self.config.get(
            "spark_kafka_package", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
        )
        
        # Combine packages
        required_packages = f"{kafka_package},{mongodb_package}"
        logger.info(f"Configuring Spark session with packages: {required_packages}")

        # Build the Spark session
        builder = self._configure_spark_builder(required_packages)
        
        # Get or create the session
        logger.info("Getting or creating Spark session...")
        spark = builder.getOrCreate()
        
        # Log actual packages configured in the active session for verification
        actual_packages = spark.conf.get("spark.jars.packages", "Not Set")
        logger.info(f"Active Spark session configured with packages: {actual_packages}")
        
        # Warn if the required packages aren't in the active session's config
        if mongodb_package not in actual_packages or kafka_package not in actual_packages:
             logger.warning(f"Required packages ('{required_packages}') may not be set in the active Spark session ('{actual_packages}'). "
                            "This usually means the session was created *before* this processor started, without the necessary packages. "
                            "Ensure packages are configured during the initial SparkSession creation.")

        return spark
        
    def _configure_spark_builder(self, required_packages: str) -> "SparkSession.Builder":
        """Configure the Spark session builder.
        
        Args:
            required_packages: Comma-separated list of packages
            
        Returns:
            Configured SparkSession builder
        """
        from pyspark.sql import SparkSession
        
        # Build the Spark session
        builder = (
            SparkSession.builder.appName(self.config.get("app_name", "RedditSentimentAnalysis"))
            .config("spark.jars.packages", required_packages)
            .config("spark.task.interruption.on.shutdown", "false")
        )
        
        # Configure MongoDB URI
        self._configure_mongodb_connection(builder)

        # Apply any additional Spark configurations from spark_settings
        self._apply_additional_spark_configs(builder)
        
        return builder
        
    def _configure_mongodb_connection(self, builder: "SparkSession.Builder") -> None:
        """Configure MongoDB connection for Spark.
        
        Args:
            builder: SparkSession builder to configure
        """
        mongodb_config = self.config.get("mongodb", {})
        mongo_host = mongodb_config.get('connection_host')
        mongo_port = mongodb_config.get('connection_port')
        
        if mongodb_config.get("auth_username") and mongodb_config.get("auth_password"):
            auth_user = mongodb_config['auth_username']
            auth_pass = mongodb_config['auth_password']
            # Construct URI with auth credentials
            mongo_uri_base = f"mongodb://{auth_user}:{auth_pass}@{mongo_host}:{mongo_port}"
        else:
            # Construct URI without auth
            mongo_uri_base = f"mongodb://{mongo_host}:{mongo_port}"
            
        # Setting both input and output URIs
        builder.config("spark.mongodb.input.uri", mongo_uri_base)
        builder.config("spark.mongodb.output.uri", mongo_uri_base)

        # Set write concern if specified
        if "write_concern" in mongodb_config:
            builder.config("spark.mongodb.output.writeConcern", mongodb_config["write_concern"])
            
    def _apply_additional_spark_configs(self, builder: "SparkSession.Builder") -> None:
        """Apply additional Spark configurations from config.
        
        Args:
            builder: SparkSession builder to configure
        """
        # Apply any additional Spark configurations from spark_settings
        for key, value in self.config.get("spark_settings", {}).items():
            # Avoid overriding packages or MongoDB URIs set above
            if key.startswith("spark.") and key not in ["spark.jars.packages", "spark.mongodb.input.uri", "spark.mongodb.output.uri"]: 
                builder.config(key, value)

    def _create_kafka_stream(self) -> DataFrame:
        """Create a Kafka stream from configured topics.
        
        Returns:
            DataFrame with raw Kafka data
        """
        # Use only validated topics
        topics = [
            "social-media-reddit-posts-validated",
            "social-media-reddit-comments-validated",
            "social-media-reddit-symbols-validated"
        ]
        
        topics_str = ",".join(topics)
        
        logger.info(f"Creating Kafka stream for topics: {topics_str}")
        return self.read_from_kafka(topics_str)