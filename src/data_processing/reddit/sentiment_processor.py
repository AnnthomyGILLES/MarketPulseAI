import os
import sys

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, udf, current_timestamp, lit, concat_ws
from pyspark.sql.types import (
    StructType,
    StringType,
    TimestampType,
    DoubleType,
    LongType,
    BooleanType,
    StructField,
    MapType,
)
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from src.data_processing.common.base_processor import BaseStreamProcessor
from src.utils.config import load_config

# Define the schema for the incoming Kafka messages (adjust based on actual producer)
# Assuming the producer sends JSON strings in the 'value' field
REDDIT_POST_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("selftext", StringType(), True),  # Body of the post
        StructField("subreddit", StringType(), True),
        StructField("author", StringType(), True),
        StructField(
            "created_utc", DoubleType(), True
        ),  # Timestamp from Reddit (epoch seconds)
        StructField("url", StringType(), True),
        StructField("score", LongType(), True),  # Upvotes/Downvotes score
        StructField("upvote_ratio", DoubleType(), True),
        StructField("num_comments", LongType(), True),
        StructField("permalink", StringType(), True),
        StructField("stickied", BooleanType(), True),
        StructField("over_18", BooleanType(), True),
        StructField("spoiler", BooleanType(), True),
        StructField(
            "retrieved_on", DoubleType(), True
        ),  # Timestamp when collected (epoch seconds)
        # Add other fields as needed from your collector
    ]
)

# --- Sentiment Analysis Setup ---
# Initialize VADER sentiment analyzer (do this once globally)
analyzer = SentimentIntensityAnalyzer()


def get_sentiment_scores(text: str) -> dict:
    """
    Analyzes the sentiment of a text using VADER.
    Returns a dictionary with compound, pos, neu, neg scores.
    Handles None or empty input.
    """
    if text is None or not text.strip():
        # Return neutral scores for empty text
        return {"compound": 0.0, "pos": 0.0, "neu": 1.0, "neg": 0.0}
    try:
        # VADER analysis
        scores = analyzer.polarity_scores(text)
        return (
            scores  # e.g., {'neg': 0.0, 'neu': 0.585, 'pos': 0.415, 'compound': 0.7579}
        )
    except Exception as e:
        logger.error(
            f"Error during sentiment analysis for text: '{text[:50]}...': {e}",
            exc_info=True,
        )
        # Return neutral scores on error
        return {"compound": 0.0, "pos": 0.0, "neu": 1.0, "neg": 0.0}


# Define the UDF for sentiment analysis
sentiment_udf = udf(get_sentiment_scores, MapType(StringType(), DoubleType()))


# Helper function to derive a simple sentiment label
def get_sentiment_label(compound_score: float) -> str:
    """Categorizes sentiment based on VADER compound score."""
    if compound_score is None:  # Handle potential null scores
        return "neutral"
    if compound_score >= 0.05:
        return "positive"
    elif compound_score <= -0.05:
        return "negative"
    else:
        return "neutral"


sentiment_label_udf = udf(get_sentiment_label, StringType())

# --- Spark Processor Class ---


class RedditSentimentProcessor(BaseStreamProcessor):
    """
    PySpark streaming job to process Reddit posts from Kafka,
    perform sentiment analysis, and write results to MongoDB.
    """

    def __init__(self, config_path: str):
        """
        Initializes the processor with configuration.

        Args:
            config_path: Path to the YAML configuration file.
        """
        logger.info(f"Loading configuration from: {config_path}")
        self.config = load_config(config_path)
        # Extract necessary configurations with defaults
        self.app_name = self.config.get("app_name", "RedditSentimentProcessor")
        
        # Check if we have a reference to external Kafka config
        if "kafka_config_path" in self.config:
            logger.info(f"Loading Kafka config from: {self.config['kafka_config_path']}")
            kafka_external_config = load_config(self.config['kafka_config_path'])
            
            # Create Kafka config structure from external file
            self.kafka_config = {
                # Use container servers by default, can be changed to bootstrap_servers or bootstrap_servers_prod
                "brokers": ",".join(kafka_external_config.get("bootstrap_servers_container", ["redpanda:29092"])),
                # Use the reddit posts topic
                "topic": kafka_external_config.get("topics", {}).get("social_media_reddit_posts", "social-media-reddit-posts")
            }
            logger.info(f"Using Kafka brokers: {self.kafka_config['brokers']}")
            logger.info(f"Using Kafka topic: {self.kafka_config['topic']}")
        else:
            # Use direct Kafka config if specified
            self.kafka_config = self.config.get("kafka", {})
        
        self.mongo_config = self.config.get("mongodb", {})
        
        # Build MongoDB URI from individual components if uri not directly specified
        if "uri" not in self.mongo_config and "connection_host" in self.mongo_config:
            host = self.mongo_config.get("connection_host")
            port = self.mongo_config.get("connection_port", "27017")
            user = self.mongo_config.get("auth_username")
            password = self.mongo_config.get("auth_password")
            
            if user and password:
                uri = f"mongodb://{user}:{password}@{host}:{port}"
            else:
                uri = f"mongodb://{host}:{port}"
            
            # Add additional connection options if available
            if "connection_options" in self.mongo_config:
                options = []
                for key, value in self.mongo_config["connection_options"].items():
                    options.append(f"{key}={value}")
                
                if options:
                    uri = f"{uri}/?{'&'.join(options)}"
                
            self.mongo_config["uri"] = uri
            logger.info(f"Built MongoDB URI from components: {uri.replace(password, '*****') if password else uri}")
        
        # Use spark_settings structure for processing config if available
        if "spark_settings" in self.config:
            self.processing_config = {
                "checkpoint_location": self.config["spark_settings"].get("checkpoint_location_base_path", "/tmp/checkpoints/reddit"),
                "output_mode": "append",
                "trigger_interval": self.config["spark_settings"].get("processing_window_duration", "1 minute")
            }
        else:
            self.processing_config = self.config.get("processing", {})

        # Validate essential configurations
        if not all([self.kafka_config.get("brokers"), self.kafka_config.get("topic")]):
            logger.error("Kafka brokers or topic missing in configuration.")
            raise ValueError("Kafka brokers and topic must be specified in config.")
        if not all(
            [
                self.mongo_config.get("uri"),
                self.mongo_config.get("database"),
                self.mongo_config.get("collection"),
            ]
        ):
            logger.error(
                "MongoDB URI, database, or collection missing in configuration."
            )
            raise ValueError(
                "MongoDB URI, database, and collection must be specified in config."
            )
        if not self.processing_config.get("checkpoint_location"):
            logger.error("Processing checkpoint_location missing in configuration.")
            raise ValueError("Checkpoint location must be specified in config.")

        logger.info(f"Initializing Spark session for app: {self.app_name}")
        # Pass relevant configs to the base class constructor if it expects them
        # Assuming BaseStreamProcessor sets up SparkSession based on app_name and potentially mongo_config
        super().__init__(config_path=config_path)
        logger.info("Spark session initialized successfully.")

    def _read_kafka_stream(self) -> DataFrame:
        """Reads data stream from the configured Kafka topic."""
        logger.info(
            f"Setting up Kafka read stream from topic: {self.kafka_config['topic']}"
        )
        try:
            kafka_df = (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", self.kafka_config["brokers"])
                .option("subscribe", self.kafka_config["topic"])
                .option(
                    "startingOffsets",
                    "earliest"  # Changed from 'latest' to read all existing data
                )
                .option(
                    "failOnDataLoss",
                    self.kafka_config.get("fail_on_data_loss", "false"),
                )
                .load()
            )
            logger.info("Kafka stream reader configured with startingOffsets=earliest to process existing data")
            return kafka_df
        except Exception as e:
            logger.error(f"Failed to configure Kafka read stream: {e}", exc_info=True)
            raise

    def _process_stream(self, kafka_df: DataFrame) -> DataFrame:
        """Parses Kafka messages, applies sentiment analysis, and prepares for sink."""
        logger.info("Processing Kafka stream data...")
        
        # Check if the input DataFrame is streaming
        is_streaming = kafka_df.isStreaming
        logger.info(f"Input Kafka DataFrame is streaming: {is_streaming}")
        
        # Decode Kafka value from bytes to string, then parse JSON
        # Corrected Indentation applied here
        processed_df = (
            kafka_df.selectExpr("CAST(value AS STRING)")
            .filter(col("value").isNotNull() & (col("value") != ""))
            .select(from_json(col("value"), REDDIT_POST_SCHEMA).alias("data"))
            .select("data.*")
            .filter(col("id").isNotNull() & col("title").isNotNull())
        )  # Filter after parsing

        # --- Perform Sentiment Analysis ---
        # Combine title and selftext for a more comprehensive analysis
        # Use concat_ws which handles nulls gracefully
        processed_df = processed_df.withColumn(
            "text_for_sentiment",
            concat_ws(
                ". ", col("title"), col("selftext")
            ),  # Combine title and body text
        )

        # Apply the sentiment UDF
        processed_df = processed_df.withColumn(
            "sentiment_scores", sentiment_udf(col("text_for_sentiment"))
        )

        # Extract individual scores and add sentiment label
        processed_df = (
            processed_df.withColumn(
                "sentiment_compound", col("sentiment_scores").getItem("compound")
            )
            .withColumn("sentiment_pos", col("sentiment_scores").getItem("pos"))
            .withColumn("sentiment_neu", col("sentiment_scores").getItem("neu"))
            .withColumn("sentiment_neg", col("sentiment_scores").getItem("neg"))
            .withColumn(
                "sentiment_label", sentiment_label_udf(col("sentiment_compound"))
            )
        )

        # Add processing timestamp and source info
        processed_df = processed_df.withColumn(
            "processing_timestamp", current_timestamp()
        ).withColumn("data_source", lit("reddit"))

        # Select final columns for MongoDB sink (adjust as needed)
        # Ensure '_id' is not included if letting MongoDB generate it, or map 'id' to '_id'
        final_df = processed_df.select(
            col("id").alias("_id"),  # Use reddit ID as MongoDB document ID
            "subreddit",
            "title",
            # "selftext", # Keep or drop depending on whether you need the full text in Mongo
            "author",
            col("created_utc")
            .cast(TimestampType())
            .alias("created_at"),  # Convert epoch seconds to Timestamp
            "url",
            "permalink",
            "score",
            "upvote_ratio",
            "num_comments",
            "stickied",
            "over_18",
            "spoiler",
            col("retrieved_on")
            .cast(TimestampType())
            .alias("retrieved_at"),  # Convert epoch seconds to Timestamp
            "sentiment_compound",
            "sentiment_pos",
            "sentiment_neu",
            "sentiment_neg",
            "sentiment_label",
            "processing_timestamp",
            "data_source",
            # Drop intermediate columns if desired
            # .drop("sentiment_scores", "text_for_sentiment", "selftext")
        )

        # Before returning the final DataFrame, verify it's still streaming
        is_final_streaming = final_df.isStreaming
        logger.info(f"Final processed DataFrame is streaming: {is_final_streaming}")
        
        logger.info("Stream processing logic applied.")
        return final_df

    def _write_mongo_stream(self, df: DataFrame):
        """Writes the processed DataFrame stream to MongoDB."""
        logger.info(f"Setting up MongoDB write stream to db: {self.mongo_config['database']}, collection: {self.mongo_config['collection']}")
        
        # Test MongoDB connection before attempting streaming write
        try:
            from pymongo import MongoClient
            client = MongoClient(self.mongo_config["uri"], serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            logger.info("MongoDB connection test successful")
            db_names = client.list_database_names()
            logger.info(f"Available MongoDB databases: {db_names}")
        except Exception as e:
            logger.error(f"MongoDB connection test failed: {e}")
        
        # Log Spark and MongoDB connector version information
        try:
            # Check if the DataFrame is streaming
            is_streaming = df.isStreaming
            logger.info(f"Input DataFrame is streaming: {is_streaming}")
            if not is_streaming:
                logger.error("Error: Expected a streaming DataFrame but received a static DataFrame")
                raise ValueError("Cannot use writeStream with a non-streaming DataFrame")
            
            spark_version = self.spark.version
            logger.info(f"Using Spark version: {spark_version}")
            
            try:
                mongo_version = self.spark._jvm.com.mongodb.spark.sql.connector.version.MongoSparkConnectorVersion.VERSION
                logger.info(f"Using MongoDB Spark connector version: {mongo_version}")
            except:
                logger.warning("Unable to determine MongoDB connector version")
            
            logger.info(f"DataFrame schema: {df.schema}")
            
            # Log MongoDB connection details (redacting sensitive info)
            mongo_uri = self.mongo_config["uri"]
            safe_uri = mongo_uri.split('@')[-1].split('/')[0] if '@' in mongo_uri else "redacted"
            logger.info(f"MongoDB connection host: {safe_uri}")
            
            checkpoint_loc = self.processing_config["checkpoint_location"]
            logger.info(f"Using checkpoint location: {checkpoint_loc}")
            
            # Ensure the checkpoint location exists
            try:
                import os
                from pathlib import Path
                if checkpoint_loc.startswith("file:"):
                    local_path = checkpoint_loc[5:]
                    Path(local_path).mkdir(parents=True, exist_ok=True)
                    logger.info(f"Created local checkpoint directory: {local_path}")
            except Exception as e:
                logger.warning(f"Could not create checkpoint directory: {e}")
            
            logger.info("Setting up streaming write to MongoDB...")
            
            # Make sure we're explicitly setting the format to the full class name
            query = (
                df.writeStream
                .format("com.mongodb.spark.sql.connector.MongoTableProvider")
                .option("spark.mongodb.connection.uri", self.mongo_config["uri"])
                .option("spark.mongodb.database", self.mongo_config["database"])
                .option("spark.mongodb.collection", self.mongo_config["collection"])
                .option("checkpointLocation", checkpoint_loc)
                .outputMode(self.processing_config.get("output_mode", "append"))
                .trigger(
                    processingTime=self.processing_config.get(
                        "trigger_interval", "1 minute"
                    )
                )
                .start()
            )

            logger.info(f"MongoDB write stream started. Checkpoint location: {checkpoint_loc}")
            return query
        except Exception as e:
            logger.error(f"Failed to configure or start MongoDB write stream: {e}", exc_info=True)
            
            # If the exception message contains 'Kafka', log Kafka configuration details
            if 'kafka' in str(e).lower():
                logger.error("Kafka-related error detected. Checking Kafka configuration...")
                try:
                    logger.info(f"Kafka brokers: {self.kafka_config.get('brokers', 'Not configured')}")
                    logger.info(f"Kafka topic: {self.kafka_config.get('topic', 'Not configured')}")
                except:
                    logger.warning("Unable to retrieve Kafka configuration details")
            
            # Log available connector formats
            try:
                logger.info("Checking available formats in Spark session:")
                available_formats = self.spark.sparkContext._jvm.org.apache.spark.sql.execution.datasources.DataSource.lookupDataSource("mongodb")
                logger.info(f"MongoDB format lookup result: {available_formats}")
            except:
                logger.warning("Unable to check available formats")
            
            raise

    def run(self):
        """Main method to execute the streaming job."""
        logger.info(f"Starting {self.app_name} job...")
        mongo_write_query = None
        
        try:
            # Log system and JVM information
            logger.info(f"Running on Python version: {sys.version}")
            try:
                java_version = self.spark.sparkContext._jvm.System.getProperty("java.version")
                logger.info(f"Java version: {java_version}")
            except:
                logger.warning("Unable to determine Java version")
            
            kafka_stream_df = self._read_kafka_stream()
            
            # Add debug foreachBatch to count records after initial read
            kafka_stream_df.writeStream.foreachBatch(
                lambda df, _: logger.info(f"After Kafka read: {df.count()} records")
            ).start()
            
            processed_df = self._process_stream(kafka_stream_df)
            
            # Add debug foreachBatch to count records after processing
            processed_df.writeStream.foreachBatch(
                lambda df, _: logger.info(f"After processing: {df.count()} records")
            ).start()
            
            mongo_write_query = self._write_mongo_stream(processed_df)

            logger.info("Streaming job pipeline configured. Awaiting termination...")
            mongo_write_query.awaitTermination()

        except KeyboardInterrupt:
            logger.warning("Job interrupted by user (KeyboardInterrupt). Stopping...")
            # Stop query gracefully if possible
            if mongo_write_query and mongo_write_query.isActive:
                mongo_write_query.stop()
        except Exception as e:
            logger.error(
                f"An error occurred during the streaming job execution: {e}",
                exc_info=True,
            )
            
            # Try to extract more details about the error
            if hasattr(e, "__cause__") and e.__cause__:
                logger.error(f"Caused by: {e.__cause__}", exc_info=True)
            
            # If it's a Py4JJavaError, try to extract more details from the Java side
            if "Py4JJavaError" in str(type(e)):
                logger.error("Java-side exception detected. Trying to extract more details.")
                try:
                    java_exception = str(e)
                    logger.error(f"Java exception details: {java_exception}")
                except:
                    logger.error("Failed to extract Java exception details")
            
            # Consider adding cleanup logic here if needed
            if (
                "mongo_write_query" in locals()
                and mongo_write_query
                and mongo_write_query.isActive
            ):
                logger.info("Stopping MongoDB write query due to error.")
                mongo_write_query.stop()
            sys.exit(1)  # Exit with error code
        finally:
            # Ensure SparkSession stops even if awaitTermination completes normally or is interrupted
            if hasattr(self, "spark") and self.spark.getActiveSession():
                logger.info("Stopping Spark session.")
                self.spark.stop()
            logger.info(f"{self.app_name} job finished.")


if __name__ == "__main__":
    # Ensure the config file path is provided or discovered
    # Example: Use environment variable or command-line argument
    # Default path relative to a potential project root if script is in src/data_processing/reddit
    default_config_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "config",
        "spark",
        "reddit_sentiment_config.yaml",
    )
    config_file = os.getenv("REDDIT_SENTIMENT_CONFIG_PATH", default_config_path)
    config_file = os.path.abspath(config_file)  # Get absolute path

    if not os.path.exists(config_file):
        print(f"Error: Configuration file not found at {config_file}")
        sys.exit(1)

    print(f"Starting Reddit Sentiment Processor using config: {config_file}")
    processor = RedditSentimentProcessor(config_path=config_file)
    processor.run()
