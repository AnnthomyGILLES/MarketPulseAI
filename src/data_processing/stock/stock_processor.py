# src/data_processing/stock/stock_processor.py
from pathlib import Path
import time
from typing import Optional

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, to_date, avg, sum, round, first
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    DoubleType,
    LongType,
    StringType,
)

from src.data_processing.common.base_processor import BaseStreamProcessor


class StockDataProcessor(BaseStreamProcessor):
    """Processes stock market data streams."""

    def __init__(self, config_path: str):
        """Initialize the stock data processor.

        Args:
            config_path: Path to the configuration file
        """
        super().__init__(config_path)
        self.stock_schema = self._define_schema()
        self.timeout_seconds = self.config.get("processing", {}).get(
            "timeout_seconds", 600
        )  # 10 minutes default
        self.retry_delay = self.config.get("processing", {}).get(
            "retry_delay_seconds", 5
        )

    def _define_schema(self) -> StructType:
        """Define the schema for stock data.

        Returns:
            Spark schema for stock data
        """
        return StructType(
            [
                StructField("timestamp", TimestampType(), False),
                StructField("open", DoubleType(), False),
                StructField("high", DoubleType(), False),
                StructField("low", DoubleType(), False),
                StructField("close", DoubleType(), False),
                StructField("volume", LongType(), False),
                StructField("symbol", StringType(), False),
                StructField("vwap", DoubleType(), True),
                StructField("collection_timestamp", TimestampType(), True),
                StructField("transactions", LongType(), True),
            ]
        )

    def validate_data(self, kafka_stream: DataFrame) -> DataFrame:
        """Parse and validate the incoming stock data.

        Args:
            kafka_stream: Raw Kafka stream DataFrame

        Returns:
            Validated stock data DataFrame
        """
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
                & col("symbol").isNotNull()
            )
            .filter(col("high") >= col("low"))
            .filter(col("high") >= col("open"))
            .filter(col("high") >= col("close"))
            .filter(col("low") <= col("open"))
            .filter(col("low") <= col("close"))
            .filter(col("volume") >= 0)
            .withColumnRenamed("symbol", "name")
            .withColumnRenamed("timestamp", "date")
        )

        logger.info("Data validation complete")
        return validated_stream

    def compute_features(self, df: DataFrame) -> DataFrame:
        """Compute technical indicators and features from stock data.

        Args:
            df: Validated stock data DataFrame

        Returns:
            DataFrame with computed features
        """
        logger.info("Computing stock features")

        # Pass through validated data as-is for the stock_features table
        stock_features = df

        # Calculate daily statistics for the daily_stock_stats table
        daily_stats = (
            df.withColumn("date_only", to_date(col("date")))
            .groupBy("name", "date_only")
            .agg(
                avg((col("high") + col("low")) / 2).alias("avg_price"),
                sum(col("volume")).alias("volume_sum"),
                (first("close") - first("open")).alias("price_change"),
                round((first("close") - first("open")) / first("open") * 100, 2).alias(
                    "percent_change"
                ),
            )
            .withColumnRenamed("date_only", "date")
        )

        # Return the original features for the main Cassandra table
        return stock_features

    def topic_exists(self, kafka_topic: str) -> bool:
        """Check if a Kafka topic exists.

        Args:
            kafka_topic: The name of the Kafka topic to check

        Returns:
            bool: True if the topic exists, False otherwise
        """
        from kafka import KafkaAdminClient
        from kafka.errors import KafkaError
        from contextlib import contextmanager

        @contextmanager
        def get_admin_client(bootstrap_servers):
            """Context manager for KafkaAdminClient to ensure proper cleanup."""
            client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            try:
                yield client
            finally:
                client.close()

        try:
            # Get Kafka bootstrap servers from config
            bootstrap_servers = self.config["kafka"]["bootstrap_servers_container"]

            # Use context manager for the admin client
            with get_admin_client(bootstrap_servers) as admin_client:
                # Get list of topics
                topics = admin_client.list_topics()

                # Check if topic exists
                return kafka_topic in topics

        except KafkaError as e:
            logger.warning(f"Error checking Kafka topics: {e}")
            return False

    def read_from_kafka_with_retry(self, kafka_topic: str) -> Optional[DataFrame]:
        """Read data from Kafka with retry logic, waiting for topic to exist.

        Args:
            kafka_topic: The Kafka topic to read from

        Returns:
            DataFrame from Kafka or None if timeout period exceeded
        """
        start_time = time.time()
        end_time = start_time + self.timeout_seconds
        attempts = 0

        logger.info(
            f"Starting to read from Kafka topic '{kafka_topic}' with {self.timeout_seconds}s timeout"
        )

        while time.time() < end_time:
            attempts += 1
            elapsed = int(time.time() - start_time)
            remaining = max(0, self.timeout_seconds - elapsed)

            # Check if topic exists
            if self.topic_exists(kafka_topic):
                logger.info(f"Kafka topic '{kafka_topic}' found, attempting to read")
                try:
                    return self.read_from_kafka(kafka_topic)
                except Exception as e:
                    logger.error(f"Error reading from Kafka topic '{kafka_topic}': {e}")
                    # If there's an error reading despite topic existing, wait before retry
                    if time.time() + self.retry_delay < end_time:
                        time.sleep(self.retry_delay)
                    else:
                        logger.error(
                            f"Timeout period of {self.timeout_seconds}s exceeded when trying to read from Kafka topic '{kafka_topic}'"
                        )
                        break
            else:
                logger.warning(
                    f"Kafka topic '{kafka_topic}' not found, waiting {self.retry_delay}s before retry (attempt #{attempts}, {elapsed}s elapsed, {remaining}s remaining)"
                )

                # Check if we have enough time for another retry
                if time.time() + self.retry_delay < end_time:
                    time.sleep(self.retry_delay)
                else:
                    logger.error(
                        f"Timeout period of {self.timeout_seconds}s exceeded when waiting for Kafka topic '{kafka_topic}'"
                    )
                    break

        logger.error(
            f"Failed to find or read from Kafka topic '{kafka_topic}' after {elapsed}s"
        )
        return None

    def run(self) -> None:
        """Run the stock data processing pipeline."""
        try:
            logger.info("Starting stock data processing pipeline")

            # Get configuration parameters
            kafka_topic = self.config["kafka"]["topics"]["market_data_validated"]
            cassandra_keyspace = self.config.get("cassandra", {}).get(
                "keyspace", "market_data"
            )
            cassandra_table = self.config.get("cassandra", {}).get(
                "table", "stock_features"
            )

            # Create a static checkpoint location for the stream to enable resumption
            checkpoint_dir = Path(
                self.config.get(
                    "checkpoint_location_base_path", "/opt/bitnami/spark/checkpoints"
                )
            )
            checkpoint_location = str(checkpoint_dir / "stock_features")

            # Ensure checkpoint directory exists
            checkpoint_dir.mkdir(parents=True, exist_ok=True)

            logger.info(f"Using checkpoint location: {checkpoint_location}")
            logger.info(
                f"Writing data to Cassandra keyspace: {cassandra_keyspace}, table: {cassandra_table}"
            )

            # Read data from Kafka with retry logic
            kafka_stream = self.read_from_kafka_with_retry(kafka_topic)
            if not kafka_stream:
                logger.error(
                    "Failed to establish connection to Kafka topic after retries"
                )
                return

            # Process the data
            validated_stream = self.validate_data(kafka_stream)
            feature_stream = self.compute_features(validated_stream)

            # Write to Cassandra
            query = self.write_to_cassandra(
                feature_stream, cassandra_keyspace, cassandra_table, checkpoint_location
            )

            logger.info(
                "Stock data processing pipeline started, waiting for termination"
            )
            query.awaitTermination()

        except Exception as e:
            logger.exception(f"Error in stock data processing pipeline: {e}")
            raise