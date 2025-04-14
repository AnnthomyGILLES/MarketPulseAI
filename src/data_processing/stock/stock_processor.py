# src/data_processing/stock/stock_processor.py
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, TimestampType, DoubleType,
    LongType, StringType
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
        return StructType([
            StructField("date", TimestampType(), False),
            StructField("open", DoubleType(), False),
            StructField("high", DoubleType(), False),
            StructField("low", DoubleType(), False),
            StructField("close", DoubleType(), False),
            StructField("volume", LongType(), False),
            StructField("Name", StringType(), False),
        ])

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
        # This is a placeholder for technical indicator calculation
        # In a real implementation, you would add moving averages, RSI, MACD, etc.
        logger.info("Computing stock features")

        # For now, just passing through the validated data
        # In a production implementation, add your feature calculations here
        return df

    def run(self) -> None:
        """Run the stock data processing pipeline."""
        try:
            logger.info("Starting stock data processing pipeline")

            # Get configuration parameters
            kafka_topic = self.config["kafka"]["topics"]["market_data_raw"]
            cassandra_keyspace = self.config["cassandra"]["keyspace"]
            cassandra_table = self.config["cassandra"]["table"]

            # Generate a unique checkpoint location
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            checkpoint_dir = Path(self.config["checkpoint_location_base_path"])
            checkpoint_location = str(checkpoint_dir / f"stock_features_{timestamp}")

            # Read data from Kafka
            kafka_stream = self.read_from_kafka(kafka_topic)

            # Process the data
            validated_stream = self.validate_data(kafka_stream)
            feature_stream = self.compute_features(validated_stream)

            # Write to Cassandra
            query = self.write_to_cassandra(
                feature_stream,
                cassandra_keyspace,
                cassandra_table,
                checkpoint_location
            )

            logger.info("Stock data processing pipeline started, waiting for termination")
            query.awaitTermination()

        except Exception as e:
            logger.exception(f"Error in stock data processing pipeline: {e}")
            raise