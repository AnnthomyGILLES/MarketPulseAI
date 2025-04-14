# src/data_processing/common/base_processor.py
import os
from pathlib import Path
from typing import Dict, Any, Optional

import yaml
from loguru import logger
from pyspark.sql import SparkSession, DataFrame


class BaseStreamProcessor:
    """Base class for Spark streaming processors with common functionality."""

    def __init__(self, config_path: str):
        """Initialize the processor with configuration from a file.

        Args:
            config_path: Path to the YAML configuration file
        """
        self.config = self.load_config(config_path)
        self.spark = self._init_spark_session()

    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file.

        Args:
            config_path: Path to configuration file

        Returns:
            Dictionary containing configuration values
        """
        config_file = Path(config_path)
        if not config_file.exists():
            logger.error(f"Configuration file not found: {config_path}")
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_file, "r") as f:
            return yaml.safe_load(f)

    def _init_spark_session(self) -> SparkSession:
        """Initialize and configure the Spark session.

        Returns:
            Configured SparkSession
        """
        app_name = self.config.get("app_name", "StreamProcessor")
        logger.info(f"Initializing Spark session: {app_name}")

        spark_builder = (
            SparkSession.builder.appName(app_name)
            # Common configurations
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.streaming.kafka.consumer.cache.enabled", "false")
            .config("spark.streaming.kafka.consumer.poll.ms", "60000")
        )

        # Add Cassandra configurations
        cassandra_config = self.config.get("cassandra", {})
        if cassandra_config:
            host = cassandra_config.get("connection_host", "localhost")
            port = cassandra_config.get("connection_port", "9042")
            spark_builder = spark_builder.config("spark.cassandra.connection.host", host)
            spark_builder = spark_builder.config("spark.cassandra.connection.port", port)

            # Add authentication if provided
            username = cassandra_config.get("auth_username")
            password = cassandra_config.get("auth_password")
            if username and password:
                spark_builder = (
                    spark_builder.config("spark.cassandra.auth.username", username)
                    .config("spark.cassandra.auth.password", password)
                )

        # Add required packages
        packages = [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1"
        ]
        spark_builder = spark_builder.config("spark.jars.packages", ",".join(packages))

        # Set log level
        log_level = self.config.get("log_level", "INFO")
        spark = spark_builder.getOrCreate()
        spark.sparkContext.setLogLevel(log_level)

        return spark

    def read_from_kafka(self, topics: str) -> DataFrame:
        """Read data from Kafka topics.

        Args:
            topics: Comma-separated list of topics to subscribe to

        Returns:
            DataFrame with raw Kafka data
        """
        kafka_config = self.config.get("kafka", {})
        bootstrap_servers = ",".join(kafka_config.get("bootstrap_servers", ["localhost:9092"]))

        logger.info(f"Reading from Kafka topics: {topics}")
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topics)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

    def write_to_cassandra(self, df: DataFrame, keyspace: str, table: str,
                           checkpoint_location: str) -> None:
        """Write streaming DataFrame to Cassandra.

        Args:
            df: DataFrame to write
            keyspace: Cassandra keyspace
            table: Cassandra table
            checkpoint_location: Checkpoint directory path
        """
        logger.info(f"Writing data to Cassandra {keyspace}.{table}")

        cassandra_config = self.config.get("cassandra", {})
        cassandra_options = {
            "keyspace": keyspace,
            "table": table,
            "checkpointLocation": checkpoint_location,
        }

        return (
            df.writeStream
            .foreachBatch(lambda batch_df, batch_id:
                          batch_df.write
                          .format("org.apache.spark.sql.cassandra")
                          .options(**cassandra_options)
                          .mode("append")
                          .save()
                          )
            .option("checkpointLocation", checkpoint_location)
            .start()
        )

    def run(self) -> None:
        """Template method to be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement 'run' method")