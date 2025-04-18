from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import (
    TimestampType,
    StructType,
    StructField,
    DoubleType,
    LongType,
    StringType,
)
import logging
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Environment variables with defaults
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:29092")
MARKET_DATA_TOPIC = os.environ.get("MARKET_DATA_TOPIC", "market_data")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.environ.get("CASSANDRA_PORT", "9042")
CASSANDRA_USER = os.environ.get("CASSANDRA_USER", "cassandra")
CASSANDRA_PASSWORD = os.environ.get("CASSANDRA_PASSWORD", "cassandra")


class StockDataProcessor:
    """Processes stock market data streams and computes technical indicators."""

    def __init__(
        self,
        kafka_brokers=KAFKA_BOOTSTRAP_SERVERS,
        kafka_topic=MARKET_DATA_TOPIC,
        cassandra_host=CASSANDRA_HOST,
        cassandra_port=CASSANDRA_PORT,
        cassandra_user=CASSANDRA_USER,
        cassandra_password=CASSANDRA_PASSWORD,
        checkpoint_dir="/tmp/checkpoints",
        cassandra_keyspace="market_data",
        cassandra_table="stock_features",
    ):
        logger.warning(f"THIS IS IT: {MARKET_DATA_TOPIC}")
        """Initialize the StockDataProcessor with configuration parameters."""
        self.kafka_brokers = kafka_brokers
        self.kafka_topic = kafka_topic
        self.cassandra_host = cassandra_host
        self.cassandra_port = cassandra_port
        self.cassandra_user = cassandra_user
        self.cassandra_password = cassandra_password
        self.checkpoint_dir = checkpoint_dir
        self.cassandra_keyspace = cassandra_keyspace
        self.cassandra_table = cassandra_table
        self.spark = self._init_spark()

        # Define schema for stock data
        self.stock_schema = StructType(
            [
                StructField("date", TimestampType(), False),
                StructField("open", DoubleType(), False),
                StructField("high", DoubleType(), False),
                StructField("low", DoubleType(), False),
                StructField("close", DoubleType(), False),
                StructField("volume", LongType(), False),
                StructField("Name", StringType(), False),
            ]
        )

    def _init_spark(self):
        """Initialize the Spark session with required configurations."""
        logger.info(f"Initializing Spark with Cassandra host: {self.cassandra_host}")

        # Add required JARs for Spark to connect to Kafka and Cassandra
        spark_builder = (
            SparkSession.builder.appName("StockDataProcessing")
            .config("spark.cassandra.connection.host", self.cassandra_host)
            .config("spark.cassandra.connection.port", self.cassandra_port)
            .config("spark.streaming.kafka.consumer.cache.enabled", "false")
            .config("spark.streaming.kafka.consumer.poll.ms", "60000")
            # Add packages for Kafka and Cassandra integration
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0",
            )
            .master("local[*]")
        )

        # Add authentication if provided
        if self.cassandra_user and self.cassandra_password:
            spark_builder = spark_builder.config(
                "spark.cassandra.auth.username", self.cassandra_user
            ).config("spark.cassandra.auth.password", self.cassandra_password)

        return spark_builder.getOrCreate()

    def read_from_kafka(self):
        """Read stock data stream from Kafka."""
        logger.info(
            f"Reading from Kafka topic: {self.kafka_topic} at {self.kafka_brokers}"
        )

        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_brokers)
            .option("subscribe", self.kafka_topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

    def validate_data(self, kafka_stream):
        """Parse and validate the incoming stock data."""
        logger.info("Validating and parsing stock data")

        # Parse JSON data
        parsed_stream = kafka_stream.select(
            from_json(col("value").cast("string"), self.stock_schema).alias("data")
        ).select("data.*")

        # Apply validation rules
        validated_stream = (
            parsed_stream.filter(
                col("open").isNotNull()
                & col("high").isNotNull()
                & col("low").isNotNull()
                & col("close").isNotNull()
                & col("volume").isNotNull()
                & col("Name").isNotNull()
            )
            .filter(col("high") >= col("low"))
            .filter(col("high") >= col("open"))
            .filter(col("high") >= col("close"))
            .filter(col("low") <= col("open"))
            .filter(col("low") <= col("close"))
            .filter(col("volume") >= 0)
            .withColumnRenamed("Name", "name")
        )

        logger.info("Data validation complete")
        return validated_stream

    def write_to_cassandra(self, feature_stream):
        """
        Write the processed feature stream to Cassandra
        """
        logger.info(
            f"Writing data to Cassandra keyspace={self.cassandra_keyspace}, table={self.cassandra_table}"
        )

        # Configure Cassandra connection options
        cassandra_options = {
            "keyspace": self.cassandra_keyspace,
            "table": self.cassandra_table,
            "spark.cassandra.connection.host": self.cassandra_host,
            "spark.cassandra.connection.port": self.cassandra_port,
            "spark.cassandra.auth.username": self.cassandra_user,
            "spark.cassandra.auth.password": self.cassandra_password,
            "checkpointLocation": f"{self.checkpoint_dir}/cassandra",
        }

        # Write the streaming data to Cassandra
        query = (
            feature_stream.writeStream.foreachBatch(
                lambda batch_df, batch_id: batch_df.write.format(
                    "org.apache.spark.sql.cassandra"
                )
                .options(**cassandra_options)
                .mode("append")
                .save()
            )
            .option("checkpointLocation", f"{self.checkpoint_dir}/cassandra")
            .start()
        )

        logger.info("Started streaming write to Cassandra")
        return query

    def create_console_output(self, feature_stream):
        """
        Create a console output for debugging purposes
        """
        logger.info("Creating console output stream for debugging")

        return (
            feature_stream.writeStream.outputMode("append")
            .format("console")
            .option("truncate", "false")
            .option("numRows", 10)
            .start()
        )


def main():
    """
    Main function to run the stock data processing pipeline
    """
    logger.info("Starting stock data processing pipeline")
    logger.info(f"Using Kafka broker: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Using Cassandra host: {CASSANDRA_HOST}")

    # Initialize the processor
    processor = StockDataProcessor()

    try:
        # Read data from Kafka
        kafka_stream = processor.read_from_kafka()

        # Validate the data
        feature_stream = processor.validate_data(kafka_stream)

        # # Compute features
        # feature_stream = processor.compute_features(validated_stream)

        # # Optional: Debug output to console
        # console_query = processor.create_console_output(feature_stream)

        # Write to Cassandra
        cassandra_query = processor.write_to_cassandra(feature_stream)

        # # Wait for the streaming queries to terminate
        # console_query.awaitTermination()
        cassandra_query.awaitTermination()

    except Exception as e:
        logger.error(f"Error in processing pipeline: {str(e)}")
        import traceback

        logger.error(traceback.format_exc())
        raise
    finally:
        # Clean up resources if needed
        logger.info("Shutting down stock data processing pipeline")


if __name__ == "__main__":
    main()
