# src/data_processing/reddit/sentiment_processor.py
from typing import List, Optional, Dict

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    LongType,
    BooleanType,
    IntegerType,
    MapType,
    ArrayType,
    FloatType,
)
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from src.data_processing.common.base_processor import BaseStreamProcessor

# Define schemas and UDFs outside the class for better serialization

# Define global sentiment analyzer (stateless)
sentiment_analyzer = SentimentIntensityAnalyzer()

# Schema for Reddit data
reddit_schema = StructType(
    [
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
    ]
)

# Schema for sentiment output
sentiment_schema = StructType(
    [
        StructField("score", FloatType(), True),
        StructField("magnitude", FloatType(), True),
    ]
)


# Define UDF functions as standalone functions
def clean_text(text: str) -> Optional[str]:
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


def extract_symbols(text: str) -> Optional[List[str]]:
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


def get_sentiment(text: str) -> Optional[Dict[str, float]]:
    """Calculate sentiment scores for text.

    Args:
        text: Text to analyze

    Returns:
        Dictionary with sentiment scores or None if analysis fails
    """
    if text is None:
        return None

    try:
        # Use the global sentiment analyzer
        vs = sentiment_analyzer.polarity_scores(text)
        return {"score": vs["compound"], "magnitude": abs(vs["compound"])}
    except Exception as e:
        logger.warning(f"Sentiment analysis failed for text: {text[:50]}... Error: {e}")
        return None


# Register UDFs once
clean_text_udf = F.udf(clean_text, StringType())
extract_symbols_udf = F.udf(extract_symbols, ArrayType(StringType()))
get_sentiment_udf = F.udf(get_sentiment, sentiment_schema)


class RedditSentimentProcessor(BaseStreamProcessor):
    """Processes Reddit posts for sentiment analysis."""

    def __init__(self, config_path: str):
        """Initialize the Reddit sentiment processor.

        Args:
            config_path: Path to the configuration file
        """
        super().__init__(config_path)
        self.spark = None

    def process_reddit_data(self, df: DataFrame) -> DataFrame:
        """Process Reddit data for sentiment analysis.

        Args:
            df: Raw Kafka DataFrame

        Returns:
            Processed DataFrame with sentiment and symbols
        """
        logger.info("Processing Reddit data")

        # Parse JSON value from Kafka
        parsed_df = df.select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), reddit_schema).alias("data"),
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
        # Combine title and selftext
        df = df.withColumn(
            "full_text", F.concat_ws(" ", F.col("title"), F.col("selftext"))
        )

        # Clean text using the global UDF
        df = df.withColumn("cleaned_text", clean_text_udf(F.col("full_text")))

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
        # Apply sentiment analysis using global UDF
        df = df.withColumn("sentiment", get_sentiment_udf(F.col("cleaned_text")))

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
        # Extract symbols using global UDF
        df = df.withColumn(
            "symbols",
            extract_symbols_udf(F.concat_ws(" ", F.col("title"), F.col("selftext"))),
        )

        # Filter for posts with symbols
        df = df.filter(F.col("symbols").isNotNull() & (F.size(F.col("symbols")) > 0))

        # Explode symbols
        return df.withColumn("symbol", F.explode(F.col("symbols"))).drop("symbols")

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
            "event_timestamp",
            self.config["spark_settings"]["watermark_delay_threshold"],
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
            ),
        )

        logger.info("Sentiment aggregation completed")
        return aggregated_df

    def run(self) -> None:
        """Run the Reddit sentiment processing pipeline."""
        logger.info("Starting Reddit sentiment processing pipeline")
        warn_count = 0
        last_warn_time = None

        try:
            # Set up Spark session with MongoDB connector
            self.spark = self._create_spark_session()

            # Initialize streaming with Kafka source
            logger.info("Creating Kafka stream")
            kafka_stream = self._create_kafka_stream()

            # Add monitoring for the KAFKA-1894 warning
            def track_warnings(message):
                nonlocal warn_count, last_warn_time
                from datetime import datetime

                if "KAFKA-1894" in message:
                    warn_count += 1
                    last_warn_time = datetime.now()
                    if warn_count % 10 == 0:  # Log every 10 warnings
                        logger.warning(
                            f"KAFKA-1894 warning detected {warn_count} times. Last occurrence: {last_warn_time}"
                        )

            # Attempt to attach warning handler
            try:
                import logging

                logging.getLogger("org.apache.spark.kafka").addHandler(
                    lambda record: track_warnings(record.getMessage())
                )
                logger.info("Added warning tracking for KAFKA-1894 issue")
            except Exception as e:
                logger.warning(f"Could not attach warning handler: {e}")

            # Process streaming data
            logger.info("Processing Reddit data stream")
            processed_stream = self.process_reddit_data(kafka_stream)
            aggregated_stream = self.aggregate_sentiment(processed_stream)

            # Write to MongoDB
            logger.info("Starting MongoDB write stream")
            self._write_to_mongodb(aggregated_stream)

        except Exception as e:
            logger.error(f"Error in Reddit sentiment processing pipeline: {str(e)}")
            # Additional error details
            import traceback

            logger.error(f"Detailed error: {traceback.format_exc()}")
            logger.error(f"KAFKA-1894 warnings count before failure: {warn_count}")
            raise
        finally:
            if self.spark:
                logger.info("Stopping Spark session")
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
            "forceInsert": "true",
        }

        # Add authentication if provided
        if mongodb_config.get("auth_username") and mongodb_config.get("auth_password"):
            output_options["credentials"] = {
                "user": mongodb_config.get("auth_username"),
                "password": mongodb_config.get("auth_password"),
            }

        # Configure MongoDB output stream
        stream_query = (
            df.writeStream.outputMode("append")
            .format("mongodb")
            .option(
                "checkpointLocation",
                f"{self.config['spark_settings']['checkpoint_location_base_path']}/mongodb",
            )
            .options(**output_options)
            .trigger(
                processingTime=self.config["spark_settings"][
                    "processing_window_duration"
                ]
            )
            .start()
        )

        # Wait for termination
        stream_query.awaitTermination()

    def _create_spark_session(self) -> SparkSession:
        """Create a Spark session with the required dependencies.

        Returns:
            Configured SparkSession
        """

        # Get package dependencies from config
        mongodb_package = self.config.get(
            "spark_mongodb_package",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1",
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
        if (
            mongodb_package not in actual_packages
            or kafka_package not in actual_packages
        ):
            logger.warning(
                f"Required packages ('{required_packages}') may not be set in the active Spark session ('{actual_packages}'). "
                "This usually means the session was created *before* this processor started, without the necessary packages. "
                "Ensure packages are configured during the initial SparkSession creation."
            )

        return spark

    def _configure_spark_builder(
        self, required_packages: str
    ) -> "SparkSession.Builder":
        """Configure the Spark session builder.

        Args:
            required_packages: Comma-separated list of packages

        Returns:
            Configured SparkSession builder
        """
        from pyspark.sql import SparkSession

        # Build the Spark session
        builder = (
            SparkSession.builder.appName(
                self.config.get("app_name", "RedditSentimentAnalysis")
            )
            .config("spark.jars.packages", required_packages)
            .config("spark.task.interruption.on.shutdown", "false")
            # Adding configurations to address KAFKA-1894 warning
            .config("spark.streaming.kafka.consumer.cache.enabled", "false")
            .config(
                "spark.executor.extraJavaOptions",
                "-Dorg.apache.spark.kafka.UseUninterruptibleThread=true",
            )
            .config(
                "spark.driver.extraJavaOptions",
                "-Dorg.apache.spark.kafka.UseUninterruptibleThread=true",
            )
            .config("spark.streaming.kafka.consumer.poll.ms", "120000")
        )

        # Log the configurations being applied
        logger.info("Applying Spark configurations to address KAFKA-1894 warning")
        logger.info(
            "Setting spark.executor.extraJavaOptions to use UninterruptibleThread"
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
        mongo_host = mongodb_config.get("connection_host")
        mongo_port = mongodb_config.get("connection_port")

        if mongodb_config.get("auth_username") and mongodb_config.get("auth_password"):
            auth_user = mongodb_config["auth_username"]
            auth_pass = mongodb_config["auth_password"]
            # Construct URI with auth credentials
            mongo_uri_base = (
                f"mongodb://{auth_user}:{auth_pass}@{mongo_host}:{mongo_port}"
            )
        else:
            # Construct URI without auth
            mongo_uri_base = f"mongodb://{mongo_host}:{mongo_port}"

        # Setting both input and output URIs
        builder.config("spark.mongodb.input.uri", mongo_uri_base)
        builder.config("spark.mongodb.output.uri", mongo_uri_base)

        # Set write concern if specified
        if "write_concern" in mongodb_config:
            builder.config(
                "spark.mongodb.output.writeConcern", mongodb_config["write_concern"]
            )

    def _apply_additional_spark_configs(self, builder: "SparkSession.Builder") -> None:
        """Apply additional Spark configurations from config.

        Args:
            builder: SparkSession builder to configure
        """
        # Apply any additional Spark configurations from spark_settings
        spark_settings = self.config.get("spark_settings", {})
        kafka_config = self.config.get("kafka", {}).get(
            "config", {}
        )  # Added kafka config section

        # Combine spark_settings and kafka general config
        all_configs = {**spark_settings, **kafka_config}

        for key, value in all_configs.items():
            # Ensure keys are prefixed correctly for Spark config
            spark_key = key if key.startswith("spark.") else f"spark.{key}"

            # Avoid overriding packages or MongoDB URIs set above
            if spark_key not in [
                "spark.jars.packages",
                "spark.mongodb.input.uri",
                "spark.mongodb.output.uri",
                # Avoid Kafka options better set directly on readStream or via kafka_params
                "spark.kafka.bootstrap.servers",
                "spark.subscribe",
                "spark.startingOffsets",
                "spark.failOnDataLoss",
            ]:
                logger.debug(f"Applying additional config: {spark_key}={value}")
                builder.config(spark_key, value)

    def _create_kafka_stream(self) -> DataFrame:
        """Create a Kafka stream from configured topics, simplifying options.

        Returns:
            DataFrame with raw Kafka data
        """
        kafka_config = self.config.get("kafka", {})
        if not kafka_config:
            logger.error("Kafka configuration section is missing in self.config.")
            raise ValueError("Kafka configuration is missing.")

        # Prefer container-specific brokers if available, else default
        bootstrap_servers_list = kafka_config.get(
            "bootstrap_servers_container", kafka_config.get("bootstrap_servers")
        )
        if not bootstrap_servers_list:
            logger.error(
                "Kafka bootstrap servers are not defined in the configuration."
            )
            raise ValueError("Kafka bootstrap_servers missing.")
        bootstrap_servers = ",".join(bootstrap_servers_list)

        topics_config = kafka_config.get("topics", {})
        topics = [
            topics_config.get("social_media_reddit_validated"),
            topics_config.get("social_media_reddit_comments_validated"),
            topics_config.get("social_media_reddit_symbols_validated"),
        ]
        topics = [topic for topic in topics if topic]  # Filter out None values
        if not topics:
            logger.error(
                "No valid Kafka topics found in configuration for validated reddit data."
            )
            raise ValueError("No valid Kafka topics found in configuration.")
        topics_str = ",".join(topics)

        consumer_config = kafka_config.get("consumer", {})

        # Essential options set directly
        starting_offsets = consumer_config.get(
            "startingOffsets", "earliest"
        )  # Allow override via consumer config
        fail_on_data_loss = str(
            consumer_config.get(
                "failOnDataLoss", "false"
            )  # Allow override via consumer config
        ).lower()
        # Group ID is often managed by Spark/Kafka or set globally, but can be overridden here if needed
        # group_id = kafka_config.get("consumer_groups", {}).get("reddit_validation", "reddit-sentiment-group")

        # Parameters specifically for KAFKA-1894 mitigation
        # These could potentially be moved to the main spark config as well
        kafka_1894_params = {
            "kafka.metadata.max.age.ms": "10000",
            "kafka.max.poll.records": "500",
            "kafka.max.poll.interval.ms": "300000",  # 5 minutes
            "kafka.session.timeout.ms": "60000",  # 1 minute
            "kafka.heartbeat.interval.ms": "20000",  # 20 seconds
            "kafka.request.timeout.ms": "90000",  # 1.5 minutes
        }

        logger.info(
            f"Subscribing to Kafka topics: {topics_str} on brokers: {bootstrap_servers}"
        )
        logger.info(
            f"Applying Kafka consumer options: startingOffsets={starting_offsets}, failOnDataLoss={fail_on_data_loss}"
        )
        # logger.info(f"Using Kafka consumer group ID (likely from Spark config): {self.spark.conf.get('spark.kafka.group.id', 'Default/Not Set')}") # Example check
        logger.info(
            f"Applying specific Kafka parameters for stability: {kafka_1894_params}"
        )

        # Build the Kafka stream reader with fewer direct options
        stream_reader = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topics_str)
            .option("startingOffsets", starting_offsets)
            .option("failOnDataLoss", fail_on_data_loss)
            # .option("kafka.group.id", group_id) # Removed - Rely on Spark/Kafka defaults or global config
            # .option("kafka.auto.offset.reset", auto_offset_reset) # Removed
            # .option("kafka.enable.auto.commit", enable_auto_commit) # Removed
            # .option("kafka.auto.commit.interval.ms", auto_commit_interval_ms) # Removed
            # Apply KAFKA-1894 parameters
            .options(**kafka_1894_params)
        )

        return stream_reader.load()