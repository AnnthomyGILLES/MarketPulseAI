from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType, StructField, DoubleType, LongType
from pyspark.sql.window import Window
import logging
from src.data_collection import (
    MARKET_DATA_TOPIC,
    CASSANDRA_HOST,
    CASSANDRA_PORT,
    CASSANDRA_USER,
    CASSANDRA_PASSWORD,
    STOCK_SYMBOLS,
    KAFKA_BOOTSTRAP_SERVERS,
)

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


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
        """Initialize the StockDataProcessor with configuration parameters.

        Args:
            kafka_brokers: Kafka broker addresses (default: from config)
            kafka_topic: Kafka topic to consume from (default: from config)
            cassandra_host: Cassandra host address (default: from config)
            cassandra_port: Cassandra port (default: from config)
            cassandra_user: Cassandra username (default: from config)
            cassandra_password: Cassandra password (default: from config)
            checkpoint_dir: Directory for Spark checkpoints
            cassandra_keyspace: Cassandra keyspace name (default: 'market_data')
            cassandra_table: Cassandra table name (default: 'stock_features')
        """
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
        self.stock_symbols = STOCK_SYMBOLS

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
        spark_builder = (
            SparkSession.builder.appName("StockDataProcessing")
            .config("spark.cassandra.connection.host", self.cassandra_host)
            .config("spark.cassandra.connection.port", self.cassandra_port)
            .config("spark.streaming.kafka.consumer.cache.enabled", "false")
            .config("spark.streaming.kafka.consumer.poll.ms", "60000")
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
        logger.info(f"Reading from Kafka topic: {self.kafka_topic}")

        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_brokers)
            .option("subscribe", self.kafka_topic)
            .option("startingOffsets", "latest")
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
        )

        logger.info("Data validation complete")
        return validated_stream

    def compute_features(self, validated_stream):
        """Compute technical indicators and feature engineering."""
        logger.info("Computing technical indicators and features")

        # Define windows for different time periods
        window_5 = Window.partitionBy("Name").orderBy("date").rowsBetween(-4, 0)
        window_20 = Window.partitionBy("Name").orderBy("date").rowsBetween(-19, 0)
        prev_row = Window.partitionBy("Name").orderBy("date").rowsBetween(-1, -1)

        # Compute technical indicators
        processed_stream = (
            validated_stream.withColumn(
                "returns",
                (
                    col("close")
                    - lag("close", 1).over(Window.partitionBy("Name").orderBy("date"))
                )
                / lag("close", 1).over(Window.partitionBy("Name").orderBy("date")),
            )
            .withColumn("sma_5", avg("close").over(window_5))
            .withColumn("sma_20", avg("close").over(window_20))
            .withColumn("volatility", stddev("returns").over(window_20))
            .withColumn(
                "upper_band", col("sma_20") + (2 * stddev("close").over(window_20))
            )
            .withColumn(
                "lower_band", col("sma_20") - (2 * stddev("close").over(window_20))
            )
            .withColumn("volume_sma_5", avg("volume").over(window_5))
            .withColumn("price_to_sma_ratio", col("close") / col("sma_20"))
            .withColumn("high_low_diff", col("high") - col("low"))
            .withColumn("daily_range_pct", col("high_low_diff") / col("low"))
            .withColumn(
                "rsi_diff",
                when(
                    col("close") > lag("close", 1).over(prev_row),
                    col("close") - lag("close", 1).over(prev_row),
                ).otherwise(0),
            )
            .withColumn(
                "rsi_down",
                when(
                    col("close") < lag("close", 1).over(prev_row),
                    lag("close", 1).over(prev_row) - col("close"),
                ).otherwise(0),
            )
        )

        # Add time components for Cassandra partitioning
        processed_stream = (
            processed_stream.withColumn("year", year(col("date")))
            .withColumn("month", month(col("date")))
            .withColumn("day", dayofmonth(col("date")))
            .withColumn("hour", hour(col("date")))
            .withColumn("minute", minute(col("date")))
            .withColumn("processing_time", current_timestamp())
        )

        logger.info("Feature computation complete")
        return processed_stream

    def write_to_cassandra(self, feature_stream):
        """
        Write the processed feature stream to Cassandra

        Args:
            feature_stream: Processed DataFrame with computed features
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


def main():
    """
    Main function to run the stock data processing pipeline
    """
    logger.info("Starting stock data processing pipeline")

    # Initialize the processor
    processor = StockDataProcessor()

    try:
        # Read data from Kafka
        kafka_stream = processor.read_from_kafka()

        # Validate the data
        validated_stream = processor.validate_data(kafka_stream)

        # Compute features
        feature_stream = processor.compute_features(validated_stream)

        # Write to Cassandra
        query = processor.write_to_cassandra(feature_stream)

        # Wait for the streaming query to terminate
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Error in processing pipeline: {str(e)}")
        raise
    finally:
        # Clean up resources if needed
        logger.info("Shutting down stock data processing pipeline")


if __name__ == "__main__":
    main()
