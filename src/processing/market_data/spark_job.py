# src/processing/market_data/spark_job.py
from loguru import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StringType, DoubleType, IntegerType

# Assuming config and schemas are correctly placed
from .config import KAFKA_SETTINGS, CASSANDRA_SETTINGS
from .schemas import raw_market_data_schema, error_schema

# from .utils import add_processing_metadata # Example utility function

# Configure Loguru
logger.add("logs/market_data_processing_{time}.log", rotation="10 MB")


def create_spark_session() -> SparkSession:
    """Creates and configures the SparkSession."""
    logger.info("Creating Spark session...")
    try:
        spark = (
            SparkSession.builder.appName("MarketDataProcessing")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.cassandra.connection.host", CASSANDRA_SETTINGS.host)
            .config("spark.cassandra.connection.port", str(CASSANDRA_SETTINGS.port))
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1",
            )
            .config(
                "spark.sql.extensions",
                "com.datastax.spark.connector.CassandraSparkExtensions",
            )
            .getOrCreate()
        )
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """Reads the raw market data stream from Kafka."""
    logger.info(f"Reading from Kafka topic: {KAFKA_SETTINGS.market_data_raw_topic}")
    try:
        df = (
            spark.readStream.format("kafka")
            .option(
                "kafka.bootstrap.servers", ",".join(KAFKA_SETTINGS.bootstrap_servers)
            )
            .option("subscribe", KAFKA_SETTINGS.market_data_raw_topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .option("kafka.group.id", KAFKA_SETTINGS.consumer_group)
            .load()
        )
        logger.info("Successfully connected to Kafka stream.")
        # Select specific Kafka metadata along with the value
        return df.selectExpr(
            "CAST(key AS STRING)",
            "CAST(value AS STRING)",
            "topic",
            "partition",
            "offset",
        )
    except Exception as e:
        logger.error(f"Error reading from Kafka: {e}")
        raise


def parse_and_validate_data(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Parses JSON data, validates schema, and splits into valid/invalid streams."""
    logger.info("Parsing and validating data...")
    # Parse the JSON string value column
    parsed_df = df.withColumn("data", from_json(col("value"), raw_market_data_schema))

    # Check for parsing errors (null data column) or missing required fields
    # Basic validation: Ensure symbol and timestamp_ms are not null
    invalid_df = (
        parsed_df.filter(
            col("data").isNull()
            | col("data.symbol").isNull()
            | col("data.timestamp_ms").isNull()
        )
        .withColumn("error_message", expr("'Invalid JSON or missing required fields'"))
        .select(
            "topic",
            "partition",
            "offset",
            col("value").alias("raw_value"),
            "error_message",
        )
    )  # Keep original value for error topic

    valid_df = parsed_df.filter(
        col("data").isNotNull()
        & col("data.symbol").isNotNull()
        & col("data.timestamp_ms").isNotNull()
    ).select("data.*")  # Select fields from the parsed struct

    # Further transformations on valid data
    valid_df = valid_df.withColumn(
        "timestamp", (col("timestamp_ms") / 1000).cast("timestamp")
    ).drop("timestamp_ms")

    # Ensure final schema matches Cassandra table (select and cast if needed)
    valid_df = valid_df.select(
        col("symbol").cast(StringType()),
        col("timestamp").cast("timestamp"),
        col("price").cast(DoubleType()),
        col("volume").cast(IntegerType()),
    )

    # Filter out rows that still don't match the target schema after transformation
    # This is a safety check, ideally validation catches this earlier
    final_valid_df = valid_df.dropna(
        subset=["symbol", "timestamp"]
    )  # Based on Cassandra primary key

    # Identify records that became invalid *after* initial parsing but before final selection
    # This comparison logic might need refinement based on specific validation rules
    post_transform_invalid_condition = (
        col("data").isNotNull()
        & col("data.symbol").isNotNull()
        & col("data.timestamp_ms").isNotNull()
    ) & (
        col("symbol").isNull() | col("timestamp").isNull()
    )  # Example: Check PK became null after transform

    post_transform_invalid_df = (
        parsed_df.join(
            valid_df.select("symbol", "timestamp"), ["symbol", "timestamp"], "left_anti"
        )
        .filter(
            col("data").isNotNull()
            & col("data.symbol").isNotNull()
            & col("data.timestamp_ms").isNotNull()
        )
        .withColumn(
            "error_message", expr("'Data became invalid during transformation'")
        )
        .select(
            "topic",
            "partition",
            "offset",
            col("value").alias("raw_value"),
            "error_message",
        )
    )

    combined_invalid_df = invalid_df.unionByName(post_transform_invalid_df)

    logger.info("Data validation complete.")
    return final_valid_df, combined_invalid_df.select(
        error_schema.fieldNames()
    )  # Ensure error schema


def write_to_cassandra(df: DataFrame, epoch_id: int):
    """Micro-batch write function for Cassandra."""
    # This function executes for each micro-batch.
    # Consider adding logic here for idempotency if needed, though Spark handles some cases.
    logger.info(f"Writing batch {epoch_id} to Cassandra...")
    try:
        df.write.format("org.apache.spark.sql.cassandra").options(
            table=CASSANDRA_SETTINGS.table, keyspace=CASSANDRA_SETTINGS.keyspace
        ).mode("append").save()
        logger.info(f"Successfully wrote batch {epoch_id} to Cassandra.")
    except Exception as e:
        # Catching errors here is crucial, otherwise the stream might fail.
        # Depending on requirements, you might retry or log extensively.
        logger.error(f"Failed to write batch {epoch_id} to Cassandra: {e}")
        # Potentially write failed batch data to a recovery location/topic if needed
        # For simplicity here, we just log the error. A robust system needs more handling.
        # raise e # Re-raising will stop the stream, decide based on requirements.


def write_stream_to_cassandra(df: DataFrame):
    """Writes the valid data stream to Cassandra."""
    logger.info(
        f"Setting up Cassandra stream writer for {CASSANDRA_SETTINGS.keyspace}.{CASSANDRA_SETTINGS.table}"
    )
    query = (
        df.writeStream.foreachBatch(write_to_cassandra)
        .outputMode("update")
        .option(
            "checkpointLocation",
            f"/tmp/checkpoints/market_data_cassandra_{CASSANDRA_SETTINGS.table}",
        )
        .trigger(processingTime="30 seconds")
        .start()
    )
    logger.info("Cassandra stream writer started.")
    return query


def write_stream_to_kafka(df: DataFrame, topic: str):
    """Writes a DataFrame stream (errors) back to a Kafka topic."""
    logger.info(f"Setting up Kafka stream writer for topic: {topic}")
    query = (
        df.selectExpr("CAST(null AS STRING) AS key", "to_json(struct(*)) AS value")
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", ",".join(KAFKA_SETTINGS.bootstrap_servers))
        .option("topic", topic)
        .option("checkpointLocation", "/tmp/checkpoints/market_data_kafka_error")
        .trigger(processingTime="30 seconds")
        .start()
    )
    logger.info(f"Kafka stream writer started for topic: {topic}")
    return query


def main():
    """Main execution function."""
    logger.info("Starting Market Data Processing Spark Job...")
    spark = None
    try:
        spark = create_spark_session()
        raw_stream_df = read_kafka_stream(spark)
        valid_df, invalid_df = parse_and_validate_data(raw_stream_df)

        # Start the writers
        cassandra_query = write_stream_to_cassandra(valid_df)
        error_kafka_query = write_stream_to_kafka(
            invalid_df, KAFKA_SETTINGS.market_data_error_topic
        )

        logger.info("Streaming queries started. Awaiting termination...")
        # Await termination for all streams
        spark.streams.awaitAnyTermination()

    except Exception as e:
        logger.error(f"Critical error in main job execution: {e}", exc_info=True)
    finally:
        if spark:
            logger.info("Stopping Spark session.")
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
