# src/processing/market_data/schemas.py
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    LongType,
    IntegerType,
)

# Schema for raw data expected from Kafka (assuming JSON string in 'value')
# Example: {"symbol": "AAPL", "timestamp_ms": 1678886400000, "price": 175.50, "volume": 100000}
raw_market_data_schema = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("timestamp_ms", LongType(), True),  # Epoch milliseconds
        StructField("price", DoubleType(), True),
        StructField("volume", IntegerType(), True),
    ]
)

# Schema for the data to be written to Cassandra
cassandra_market_data_schema = StructType(
    [
        StructField("symbol", StringType(), False),  # Partition key
        StructField("timestamp", TimestampType(), False),  # Clustering key
        StructField("price", DoubleType(), True),
        StructField("volume", IntegerType(), True),
    ]
)

# Schema for error data sent to Kafka
error_schema = StructType(
    [
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("raw_value", StringType(), True),  # Original Kafka message value
        StructField("error_message", StringType(), True),
    ]
)
