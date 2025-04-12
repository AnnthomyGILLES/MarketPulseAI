import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from loguru import logger
from pydantic import BaseModel
from pyspark.sql import SparkSession, DataFrame
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


# --- Configuration Loading ---


class KafkaTopicConfig(BaseModel):
    social_media_reddit_posts: str
    social_media_reddit_comments: str
    social_media_reddit_symbols: str
    social_media_reddit_validated: str
    social_media_reddit_comments_validated: str
    social_media_reddit_symbols_validated: str
    social_media_reddit_invalid: str
    social_media_reddit_error: str
    social_media_reddit_rising_historical: str
    market_data_raw: str
    market_data_validated: str
    market_data_error: str
    sentiment_aggregated: str


class KafkaConfig(BaseModel):
    bootstrap_servers: List[str]
    topics: KafkaTopicConfig
    consumer_groups: Dict[str, str]
    config: Dict[str, object]
    producer: Dict[str, object]
    consumer: Dict[str, object]
    connect: Dict[str, object]


class CassandraConfig(BaseModel):
    keyspace: str
    table: str
    connection_host: str


class SparkPipelineConfig(BaseModel):
    app_name: str = "RedditSentimentAnalysis"
    kafka_config_path: str
    kafka: Optional[KafkaConfig] = None
    cassandra: CassandraConfig
    processing_window_duration: str = "5 minutes"
    watermark_delay_threshold: str = "10 minutes"
    checkpoint_location_base_path: str
    log_level: str = "INFO"
    # Optional: Add package/consistency loading here if fields added to SparkPipelineConfig
    spark_cassandra_package: str = (
        "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1"
    )
    spark_kafka_package: str = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
    # cassandra_write_consistency: str = "LOCAL_QUORUM"


def load_kafka_config(config_path: Path) -> KafkaConfig:
    """Loads Kafka configuration from YAML file."""
    logger.info(f"Loading Kafka configuration from: {config_path}")
    if not config_path.exists():
        logger.error(f"Kafka configuration file not found at {config_path}")
        raise FileNotFoundError(f"Kafka configuration file not found at {config_path}")

    with open(config_path, "r") as f:
        raw_config = yaml.safe_load(f)

    try:
        # Map entries to structure expected by KafkaConfig
        return KafkaConfig(
            bootstrap_servers=raw_config["bootstrap_servers"],
            topics=KafkaTopicConfig(**raw_config["topics"]),
            consumer_groups=raw_config["consumer_groups"],
            config=raw_config.get("config", {}),
            producer=raw_config.get("producer", {}),
            consumer=raw_config.get("consumer", {}),
            connect=raw_config.get("connect", {}),
        )
    except KeyError as e:
        logger.error(f"Missing Kafka configuration key: {e} in {config_path}")
        raise ValueError(f"Missing Kafka configuration key: {e}") from e
    except Exception as e:
        logger.error(f"Error parsing Kafka configuration file {config_path}: {e}")
        raise


def load_config(
    config_path: Path,
) -> SparkPipelineConfig:
    """Loads Spark and Cassandra configurations from YAML, and references Kafka config."""
    logger.info(f"Loading Spark pipeline configuration from: {config_path}")
    if not config_path.exists():
        logger.error(f"Configuration file not found at {config_path}")
        raise FileNotFoundError(f"Configuration file not found at {config_path}")

    with open(config_path, "r") as f:
        raw_config = yaml.safe_load(f)

    # Validate and parse different sections
    try:
        # Get path to Kafka config and resolve it relative to the main config file
        kafka_config_path = raw_config.get("kafka_config_path")
        if kafka_config_path:
            # Resolve relative path from the main config file's directory
            kafka_config_full_path = config_path.parent / kafka_config_path
            logger.info(f"Loading Kafka config from: {kafka_config_full_path}")
            kafka_conf = load_kafka_config(kafka_config_full_path)
        else:
            logger.warning(
                "No kafka_config_path specified, Kafka features will be unavailable"
            )
            kafka_conf = None

        # Expect Cassandra config under a 'cassandra' key in the YAML
        cassandra_conf = CassandraConfig(**raw_config["cassandra"])

        # Expect Spark settings under a 'spark' key or at the root
        # Assuming spark settings like app_name, window, watermark, checkpoint base, log_level are top-level or under a 'spark' key
        # Let's assume they are under a 'spark_settings' key for clarity
        if "spark_settings" not in raw_config:
            raise ValueError("Missing 'spark_settings' section in configuration")

        spark_settings = raw_config["spark_settings"]

        # Construct timestamped checkpoint location using the configured base path
        timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
        checkpoint_base = Path(spark_settings["checkpoint_location_base_path"])
        checkpoint_location_full = str(
            checkpoint_base / f"reddit_sentiment_{timestamp_str}"
        )
        logger.info(f"Generated unique checkpoint location: {checkpoint_location_full}")

        # Create the final config object
        pipeline_config = SparkPipelineConfig(
            app_name=spark_settings.get(
                "app_name", "RedditSentimentAnalysis"
            ),  # Allow override
            kafka_config_path=kafka_config_path,
            kafka=kafka_conf,
            cassandra=cassandra_conf,
            processing_window_duration=spark_settings.get(
                "processing_window_duration", "5 minutes"
            ),
            watermark_delay_threshold=spark_settings.get(
                "watermark_delay_threshold", "10 minutes"
            ),
            # Store the generated full path, not the base path from config directly
            checkpoint_location_base_path=checkpoint_location_full,
            log_level=spark_settings.get("log_level", "INFO"),
            # Optional: Add package/consistency loading here if fields added to SparkPipelineConfig
            spark_cassandra_package=spark_settings.get(
                "spark_cassandra_package",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1",
            ),
            spark_kafka_package=spark_settings.get(
                "spark_kafka_package",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
            ),
            # cassandra_write_consistency=spark_settings.get("cassandra_write_consistency", "LOCAL_QUORUM"),
        )
        return pipeline_config

    except KeyError as e:
        logger.error(f"Missing configuration key: {e} in {config_path}")
        raise ValueError(f"Missing configuration key: {e}") from e
    except Exception as e:
        logger.error(f"Error parsing configuration file {config_path}: {e}")
        raise


# --- Spark Session Initialization ---


def create_spark_session(config: SparkPipelineConfig) -> SparkSession:
    """Creates and configures the SparkSession."""
    logger.info(f"Creating Spark session: {config.app_name}")
    cassandra_package = config.spark_cassandra_package
    kafka_package = config.spark_kafka_package

    spark = (
        SparkSession.builder.appName(config.app_name)
        .config(
            "spark.sql.streaming.checkpointLocation",
            config.checkpoint_location_base_path,
        )
        .config("spark.jars.packages", f"{cassandra_package},{kafka_package}")
        .config("spark.cassandra.connection.host", config.cassandra.connection_host)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.log.level", config.log_level)
        .getOrCreate()
    )
    logger.info("Spark session created successfully.")
    logger.info(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    return spark


# --- Data Schemas ---

# Matches the input JSON format provided
reddit_data_schema = StructType(
    [
        StructField("author", StringType(), True),
        StructField("collection_method", StringType(), True),
        StructField(
            "collection_timestamp", TimestampType(), True
        ),  # Assuming ISO format parsable by Spark
        StructField("content_type", StringType(), True),  # "post" or "comment"
        StructField("created_utc", DoubleType(), True),  # Unix timestamp
        StructField("id", StringType(), True),
        StructField("is_self", BooleanType(), True),
        StructField("num_comments", IntegerType(), True),
        StructField("permalink", StringType(), True),
        StructField(
            "post_created_datetime", TimestampType(), True
        ),  # Assuming ISO format
        StructField("score", IntegerType(), True),
        StructField("selftext", StringType(), True),
        StructField("source", StringType(), True),
        StructField("subreddit", StringType(), True),
        StructField("title", StringType(), True),
        StructField("upvote_ratio", DoubleType(), True),
        StructField("url", StringType(), True),
    ]
)

# Matches the desired Cassandra output format
cassandra_output_schema = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField(
            "timestamp", LongType(), False
        ),  # Using created_utc as primary timestamp
        StructField(
            "collection_timestamp", TimestampType(), True
        ),  # Keep for tracking ingestion lag
        StructField("post_created_datetime", TimestampType(), True),  # Keep for context
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("window_size", StringType(), False),
        StructField("sentiment_score", FloatType(), True),
        StructField(
            "sentiment_magnitude", FloatType(), True
        ),  # Vader doesn't provide magnitude directly, using compound score again or 1.0
        StructField("post_count", LongType(), False),
        StructField("unique_authors", LongType(), True),  # Approximation
        StructField("content_types", MapType(StringType(), LongType()), True),
        StructField("subreddit", StringType(), True),
        StructField("collection_method", StringType(), True),
        StructField("avg_score", DoubleType(), True),
        StructField("min_score", IntegerType(), True),
        StructField("max_score", IntegerType(), True),
        StructField("avg_upvote_ratio", DoubleType(), True),
        StructField("permalink_sample", ArrayType(StringType()), True),
    ]
)

# --- Processing Functions ---


def clean_text(text: str) -> Optional[str]:
    """Basic text cleaning."""
    if text is None:
        return None
    text = text.lower()
    text = re.sub(
        r"http\S+|www\S+|https\S+", "", text, flags=re.MULTILINE
    )  # Remove URLs
    text = re.sub(r"\$[a-zA-Z0-9_]+", "", text)  # Remove cashtags before sentiment
    text = re.sub(r"@[a-zA-Z0-9_]+", "", text)  # Remove mentions
    text = re.sub(r"[^a-zA-Z\s]", "", text)  # Remove punctuation and numbers
    text = re.sub(r"\s+", " ", text).strip()  # Remove extra whitespace
    return text if text else None


# Register UDF for cleaning
clean_text_udf = F.udf(clean_text, StringType())


def extract_symbols(text: str) -> Optional[List[str]]:
    """Extracts potential stock symbols (e.g., $AAPL, TSLA). Simple regex approach."""
    if text is None:
        return None
    # Matches $XYZ or common patterns like TSLA (can be improved with a known list)
    potential_symbols = re.findall(r"\b([A-Z]{1,5})\b|\$([A-Z]{1,5})\b", text)
    # Flatten list and remove empty strings/duplicates
    symbols = list(set(s for tup in potential_symbols for s in tup if s))
    # Basic filtering (can be expanded with checks against actual tickers)
    symbols = [s for s in symbols if len(s) > 0 and s.isupper()]
    return symbols if symbols else None


# Register UDF for symbol extraction
extract_symbols_udf = F.udf(extract_symbols, ArrayType(StringType()))

# Initialize sentiment analyzer globally or pass to UDF if needed (Vader is stateless)
analyzer = SentimentIntensityAnalyzer()


def get_sentiment(text: str) -> Optional[Dict[str, float]]:
    """Calculates sentiment using Vader."""
    if text is None:
        return None
    try:
        vs = analyzer.polarity_scores(text)
        # Vader provides 'neg', 'neu', 'pos', 'compound'
        # We map 'compound' to sentiment_score. Magnitude isn't directly given by Vader.
        # We can use abs(compound) or a fixed value like 1.0 if magnitude is needed.
        return {"score": vs["compound"], "magnitude": abs(vs["compound"])}
    except Exception as e:
        # Log the error but don't fail the record
        logger.warning(
            f"Sentiment analysis failed for text snippet: '{text[:50]}...'. Error: {e}"
        )
        return None


# Register UDF for sentiment analysis
sentiment_schema = StructType(
    [
        StructField("score", FloatType(), True),
        StructField("magnitude", FloatType(), True),
    ]
)
get_sentiment_udf = F.udf(get_sentiment, sentiment_schema)

# --- Kafka Data Reading ---


def read_from_kafka(spark: SparkSession, config: SparkPipelineConfig) -> DataFrame:
    """Reads data from the validated Kafka topics."""
    if not config.kafka:
        logger.error("Kafka configuration is not available. Cannot read from Kafka.")
        raise ValueError("Kafka configuration is not available")

    kafka_brokers = ",".join(config.kafka.bootstrap_servers)
    # Read from both validated posts and comments topics
    topics_to_subscribe = f"{config.kafka.topics.social_media_reddit_validated},{config.kafka.topics.social_media_reddit_comments_validated}"
    logger.info(
        f"Subscribing to Kafka topics: {topics_to_subscribe} on brokers: {kafka_brokers}"
    )

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("subscribe", topics_to_subscribe)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    logger.info("Kafka stream initiated.")
    return df


# --- Data Processing Pipeline ---


def process_reddit_data(df: DataFrame, config: SparkPipelineConfig) -> DataFrame:
    """Applies the full processing pipeline to the raw Kafka stream."""
    logger.info("Starting Reddit data processing pipeline...")

    # 1. Parse JSON value from Kafka
    parsed_df = df.select(
        F.col("timestamp").alias("kafka_timestamp"),
        F.from_json(F.col("value").cast("string"), reddit_data_schema).alias("data"),
    ).select("kafka_timestamp", "data.*")

    # Add processing timestamp
    parsed_df = parsed_df.withColumn("processing_timestamp", F.current_timestamp())

    # 2. Basic Filtering (Example: ensure essential fields exist)
    filtered_df = parsed_df.filter(
        F.col("id").isNotNull()
        & F.col("created_utc").isNotNull()
        & (F.col("title").isNotNull() | F.col("selftext").isNotNull())
        & F.col("subreddit").isNotNull()
    )

    # 3. Text Preprocessing
    # Combine title and selftext for analysis, handling nulls
    # Note: Handling selftext linking to external content requires external fetching, not done here.
    filtered_df = filtered_df.withColumn(
        "full_text", F.concat_ws(" ", F.col("title"), F.col("selftext"))
    )
    filtered_df = filtered_df.withColumn(
        "cleaned_text", clean_text_udf(F.col("full_text"))
    )

    # Filter out rows where cleaning resulted in empty text
    filtered_df = filtered_df.filter(
        F.col("cleaned_text").isNotNull() & (F.length(F.col("cleaned_text")) > 0)
    )

    # 4. Sentiment Analysis
    sentiment_df = filtered_df.withColumn(
        "sentiment", get_sentiment_udf(F.col("cleaned_text"))
    )
    sentiment_df = (
        sentiment_df.withColumn("sentiment_score", F.col("sentiment.score"))
        .withColumn("sentiment_magnitude", F.col("sentiment.magnitude"))
        .drop("sentiment", "cleaned_text", "full_text")  # Clean up intermediate columns
    )  # Clean up intermediate columns

    # Filter out rows where sentiment analysis failed
    sentiment_df = sentiment_df.filter(F.col("sentiment_score").isNotNull())

    # 5. Entity Recognition (Stock Symbols)
    # Note: A single post/comment can mention multiple symbols. We explode the array.
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

    # 6. Add Watermark and Prepare for Aggregation
    # Use 'created_utc' (converted to timestamp) as the event time for windowing
    final_df = exploded_df.withColumn(
        "event_timestamp", (F.col("created_utc")).cast(TimestampType())
    )

    watermarked_df = final_df.withWatermark(
        "event_timestamp", config.watermark_delay_threshold
    )

    logger.info("Core data processing steps completed.")
    return watermarked_df


# --- Aggregation Logic ---


def aggregate_sentiment(df: DataFrame, config: SparkPipelineConfig) -> DataFrame:
    """Aggregates sentiment and other metrics over time windows."""
    logger.info(
        f"Aggregating data with window duration: {config.processing_window_duration}"
    )

    # Group by window, symbol, subreddit, collection_method
    aggregated_df = df.groupBy(
        F.window("event_timestamp", config.processing_window_duration).alias(
            "time_window"
        ),
        F.col("symbol"),
        F.col("subreddit"),
        F.col("collection_method"),  # Can add other dimensions if needed
    ).agg(
        # Sentiment Aggregations
        F.avg("sentiment_score").alias("sentiment_score"),  # Average sentiment score
        F.avg("sentiment_magnitude").alias(
            "sentiment_magnitude"
        ),  # Average magnitude (based on our mapping)
        # Count Aggregations
        F.count("*").alias("post_count"),
        F.approx_count_distinct("author").alias(
            "unique_authors"
        ),  # Approximate distinct count
        # Content Type Aggregation
        F.expr("map_from_entries(collect_list(struct(content_type, 1)))").alias(
            "content_counts_list"
        ),  # Temp structure
        # Score Aggregations
        F.avg("score").alias("avg_score"),
        F.min("score").alias("min_score"),
        F.max("score").alias("max_score"),
        F.avg("upvote_ratio").alias("avg_upvote_ratio"),
        # Context Aggregations (Collect samples, timestamps)
        # Using max as a heuristic to get the latest timestamps within the window
        F.max("collection_timestamp").alias("collection_timestamp"),
        F.max("post_created_datetime").alias("post_created_datetime"),
        F.max("event_timestamp")
        .cast(LongType())
        .alias("timestamp"),  # Max event time as primary timestamp
        F.collect_list("permalink").alias(
            "all_permalinks"
        ),  # Collect all first, then sample
    )

    # Post-process content_types aggregation (sum counts for each type)
    # This is complex in standard Spark Aggregations, might be easier in foreachBatch or simplify schema
    # Simple approach: just count posts vs comments if needed, or improve map aggregation
    # For now, skipping the complex map aggregation refinement - placeholder for content_types
    aggregated_df = aggregated_df.withColumn(
        "content_types",
        F.lit(None).cast(MapType(StringType(), LongType())),  # Placeholder
    )
    # Sample permalinks (e.g., take first 5)
    aggregated_df = aggregated_df.withColumn(
        "permalink_sample",
        F.slice(F.col("all_permalinks"), 1, 5),  # Get up to 5 samples
    ).drop("all_permalinks", "content_counts_list")

    # Add window details
    aggregated_df = (
        aggregated_df.withColumn("window_start", F.col("time_window.start"))
        .withColumn("window_end", F.col("time_window.end"))
        .withColumn("window_size", F.lit(config.processing_window_duration))
        .drop("time_window")
    )

    # Reorder columns to match Cassandra schema approximately (Cassandra write maps by name)
    # Ensure all columns needed by cassandra_output_schema are present
    # select_expr = [f.name for f in cassandra_output_schema.fields] This requires exact match + order
    # logger.debug(f"Aggregated DF Columns: {aggregated_df.columns}")
    # final_agg_df = aggregated_df.select(*select_expr) # Use selection by name during write instead

    logger.info("Aggregation completed.")
    return aggregated_df


# --- Output Writer (Cassandra) ---


def write_to_cassandra(df: DataFrame, epoch_id: int, config: SparkPipelineConfig):
    """Writes a micro-batch DataFrame to Cassandra."""
    # Note: epoch_id is provided by foreachBatch
    logger.info(
        f"Writing batch {epoch_id} to Cassandra ({config.cassandra.keyspace}.{config.cassandra.table})..."
    )
    # Use configured consistency level if available
    consistency_level = getattr(config, "cassandra_write_consistency", "LOCAL_QUORUM")
    logger.debug(f"Using Cassandra write consistency level: {consistency_level}")
    try:
        df.write.format("org.apache.spark.sql.cassandra").options(
            table=config.cassandra.table, keyspace=config.cassandra.keyspace
        ).mode("append").option(
            "spark.cassandra.output.consistency.level",
            consistency_level,  # Use variable
        ).save()
        logger.info(f"Batch {epoch_id} written successfully.")
    except Exception as e:
        logger.error(f"Error writing batch {epoch_id} to Cassandra: {e}", exc_info=True)
        # Consider adding retry logic or alternative error handling (e.g., writing failures to another sink)
        # raise e # Optionally re-raise to stop the stream


# --- Main Execution ---


def run_pipeline():
    """Main function to set up and run the Spark pipeline."""
    try:
        # Calculate paths relative to the script location *inside the container*
        script_path = Path(__file__).resolve()
        script_dir = script_path.parent
        root_dir = script_dir.parent.parent  # Should resolve to /opt/bitnami/spark
        config_path = root_dir / "config" / "spark" / "reddit_sentiment_config.yaml"

        # --- Logging Setup ---
        logger.remove()  # Remove default logger
        # Add console logger (initial level, might be updated after config load)
        initial_console_log_level = "INFO"
        logger.add(
            sink=lambda msg: print(msg, end=""),
            level=initial_console_log_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        )

        logger.info(f"Script path: {script_path}")
        logger.info(f"Calculated container root directory: {root_dir}")
        logger.info(f"Attempting to load config from: {config_path}")

        # Load Configuration
        config = load_config(config_path)

        # Update log level based on config AFTER initial logging setup
        logger.remove()  # Remove previous console logger
        logger.add(
            sink=lambda msg: print(msg, end=""),
            level=config.log_level.upper(),
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        )

        logger.info("Logging setup complete.")
        logger.info(f"Console log level: {config.log_level.upper()}")

        # Log config summary but exclude full kafka details (too verbose)
        config_dict = config.dict()
        if config.kafka:
            config_dict["kafka"] = (
                "Loaded successfully - see kafka_config_path for details"
            )
        logger.info(f"Pipeline Configuration: {config_dict}")

        # Create Spark Session
        spark = create_spark_session(config)

        # Cassandra Schema Check (Manual Step Recommended)
        logger.info(
            f"Ensure Cassandra keyspace '{config.cassandra.keyspace}' and table '{config.cassandra.table}' exist and match schema."
        )
        logger.warning(
            "Automatic schema creation/validation is not implemented. Apply schema manually if needed."
        )

        # Verify Kafka config is available
        if not config.kafka:
            raise ValueError(
                "Kafka configuration is required but was not loaded. Check kafka_config_path."
            )

        # Read from Kafka
        kafka_stream_df = read_from_kafka(spark, config)

        # Process Data
        processed_df = process_reddit_data(kafka_stream_df, config)

        # Aggregate Data
        aggregated_df = aggregate_sentiment(processed_df, config)

        # Checkpoint Location (Uses the generated path from config loading)
        checkpoint_location = config.checkpoint_location_base_path
        logger.info(f"Using checkpoint location: {checkpoint_location}")

        # Write to Cassandra using foreachBatch
        query = (
            aggregated_df.writeStream.outputMode(
                "update"
            )  # Or "append" if keys guarantee uniqueness across batches
            .option("checkpointLocation", checkpoint_location)
            .foreachBatch(lambda df, epoch_id: write_to_cassandra(df, epoch_id, config))
            # Adjust trigger as needed
            .trigger(
                processingTime=config.processing_window_duration
            )  # Trigger based on window? Or fixed interval?
            .start()
        )

        logger.info("Streaming query started. Waiting for termination...")
        query.awaitTermination()

    except FileNotFoundError as e:
        # Check if this is config related for better logging
        config_path_str = str(locals().get("config_path", "UnknownConfigPath"))
        if config_path_str in str(e):
            logger.exception(
                f"Configuration file not found. Checked path: {config_path_str}"
            )
        else:
            logger.exception(f"File not found error during setup: {e}")
    except ValueError as e:
        logger.exception(f"Configuration error: {e}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred in the pipeline: {e}")
    finally:
        if "spark" in locals() and spark:
            logger.info("Stopping Spark session.")
            spark.stop()
        logger.info("Pipeline execution finished.")


if __name__ == "__main__":
    # --- Example Unit Test Ideas (Commented Out) ---
    # ... existing test comments ...
    # -------------------------------------------------

    run_pipeline()
