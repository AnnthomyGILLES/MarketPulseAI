# src/data_processing/stock/stock_processor.py
from pathlib import Path

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, to_date, avg, sum, when, round, first
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

    def _define_schema(self) -> StructType:
        """Define the schema for stock data.

        Returns:
            Spark schema for stock data
        """
        return StructType(
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
                round(
                    (first("close") - first("open")) / first("open") * 100, 2
                ).alias("percent_change"),
            )
            .withColumnRenamed("date_only", "date")
        )


        # Return the original features for the main Cassandra table
        return stock_features

    def run(self) -> None:
        """Run the stock data processing pipeline."""
        try:
            logger.info("Starting stock data processing pipeline")

            # Get configuration parameters
            kafka_topic = self.config["kafka"]["topics"]["market_data_raw"]
            cassandra_keyspace = self.config.get("cassandra", {}).get("keyspace", "market_data")
            cassandra_table = self.config.get("cassandra", {}).get("table", "stock_features")

            # Create a static checkpoint location for the stream to enable resumption
            checkpoint_dir = Path(self.config.get("checkpoint_location_base_path", "/opt/bitnami/spark/checkpoints"))
            checkpoint_location = str(checkpoint_dir / "stock_features")
            
            # Ensure checkpoint directory exists
            checkpoint_dir.mkdir(parents=True, exist_ok=True)
            
            logger.info(f"Using checkpoint location: {checkpoint_location}")
            logger.info(f"Writing data to Cassandra keyspace: {cassandra_keyspace}, table: {cassandra_table}")

            # Read data from Kafka
            kafka_stream = self.read_from_kafka(kafka_topic)

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