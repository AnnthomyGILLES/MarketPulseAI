# src/processing/market_data/utils.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, current_timestamp


def add_processing_metadata(df: DataFrame) -> DataFrame:
    """Adds processing timestamp and service name."""
    return df.withColumn("processed_at", current_timestamp()).withColumn(
        "processing_service", lit("market-data-spark")
    )
