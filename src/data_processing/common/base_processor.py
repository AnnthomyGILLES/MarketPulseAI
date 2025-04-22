from pathlib import Path
from typing import Dict, Any

import yaml
from loguru import logger
from pyspark.sql import SparkSession, DataFrame
import redis


class BaseStreamProcessor:
    """Base class for Spark streaming processors with common functionality."""

    def __init__(self, config_path: str):
        """Initialize the processor with configuration from a file.

        Args:
            config_path: Path to the YAML configuration file
        """
        self.config = self.load_config(config_path)
        self.spark = self._init_spark_session()
        self.redis_client = self._init_redis_client() if self.config.get("redis", {}).get("enabled", False) else None

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
            config = yaml.safe_load(f)

        # Load Kafka config if a path is specified
        if "kafka_config_path" in config:
            kafka_config_path = config["kafka_config_path"]
            kafka_config_file = Path(kafka_config_path)

            if not kafka_config_file.exists():
                logger.error(f"Kafka configuration file not found: {kafka_config_path}")
                raise FileNotFoundError(f"Kafka configuration file not found: {kafka_config_path}")

            with open(kafka_config_file, "r") as f:
                kafka_config = yaml.safe_load(f)

            # Add Kafka config to main config
            config["kafka"] = kafka_config
            
        # Load sentiment analysis config if a path is specified
        if "sentiment_config_path" in config:
            sentiment_config_path = config["sentiment_config_path"]
            sentiment_config_file = Path(sentiment_config_path)

            if not sentiment_config_file.exists():
                logger.error(f"Sentiment configuration file not found: {sentiment_config_path}")
                raise FileNotFoundError(f"Sentiment configuration file not found: {sentiment_config_path}")
                
            with open(sentiment_config_file, "r") as f:
                sentiment_config = yaml.safe_load(f)
                
            # Add sentiment config to main config
            config["sentiment_analysis"] = sentiment_config

        return config

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

        # Add MongoDB configurations if present
        mongodb_config = self.config.get("mongodb", {})
        if mongodb_config:
            host = mongodb_config.get("connection_host")
            port = mongodb_config.get("connection_port")
            database = mongodb_config.get("database")
            username = mongodb_config.get("auth_username")
            password = mongodb_config.get("auth_password")

            mongo_uri = f"mongodb://{host}:{port}/{database}"
            if username and password:
                mongo_uri = f"mongodb://{username}:{password}@{host}:{port}/{database}?authSource=admin"
            
            # Add MongoDB related Spark configurations if necessary
            spark_builder = spark_builder.config("spark.mongodb.output.uri", mongo_uri)
            spark_builder = spark_builder.config("spark.mongodb.input.uri", mongo_uri)

        # Add required packages
        packages = [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1"
        ]
        spark_builder = spark_builder.config("spark.jars.packages", ",".join(packages))

        # Set log level
        log_level = self.config.get("log_level", "INFO")
        spark = spark_builder.getOrCreate()
        spark.sparkContext.setLogLevel(log_level)

        return spark
        
    def _init_redis_client(self) -> redis.Redis:
        """Initialize Redis client for caching if enabled.
        
        Returns:
            Redis client instance or None if disabled
        """
        redis_config = self.config.get("redis", {})
        if not redis_config.get("enabled", False):
            logger.info("Redis caching disabled")
            return None
            
        logger.info("Initializing Redis client for caching")
        try:
            host = redis_config.get("host", "localhost")
            port = redis_config.get("port", 6379)
            db = redis_config.get("db", 0)
            password = redis_config.get("password")
            
            client = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                socket_timeout=redis_config.get("socket_timeout", 5)
            )
            
            # Test connection
            client.ping()
            logger.info(f"Redis client connected to {host}:{port}/{db}")
            return client
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {str(e)}. Caching will be disabled.")
            return None
            
    def get_cached_or_compute(self, key, compute_func, ttl=3600):
        """Get value from cache or compute it.
        
        Args:
            key: Cache key
            compute_func: Function to compute value if not in cache
            ttl: Cache TTL in seconds
            
        Returns:
            Cached or computed value
        """
        if not self.redis_client:
            return compute_func()
            
        cached_value = self.redis_client.get(key)
        if cached_value:
            logger.debug(f"Cache hit for key: {key}")
            return cached_value
            
        logger.debug(f"Cache miss for key: {key}")
        value = compute_func()
        self.redis_client.setex(key, ttl, value)
        return value

    def read_from_kafka(self, topics: str) -> DataFrame:
        """Read data from Kafka topics.

        Args:
            topics: Comma-separated list of topics to subscribe to

        Returns:
            DataFrame with raw Kafka data
        """
        kafka_config = self.config.get("kafka", {})
        # Use the bootstrap_servers from Kafka config
        bootstrap_servers = ",".join(kafka_config.get("bootstrap_servers_container"))

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

    def write_to_mongodb(self, df: DataFrame, database: str, collection: str,
                           checkpoint_location: str, output_mode: str = "append") -> None:
        """Write streaming DataFrame to MongoDB.

        Args:
            df: DataFrame to write
            database: MongoDB database name
            collection: MongoDB collection name
            checkpoint_location: Checkpoint directory path
            output_mode: Spark Structured Streaming output mode (e.g., "append", "complete", "update")
        """
        logger.info(f"Writing data to MongoDB {database}.{collection}")

        mongodb_config = self.config.get("mongodb", {})
        logger.info(f"MongoDB config: {mongodb_config}")
        host = mongodb_config.get("connection_host")
        port = mongodb_config.get("connection_port")
        username = mongodb_config.get("auth_username")
        password = mongodb_config.get("auth_password")

        logger.info(f"Using MongoDB host: {host}, port: {port}, database: {database}, collection: {collection}, username: {username}, password: {password}")
                    
        # Construct MongoDB connection URI
        mongo_uri = f"mongodb://{host}:{port}/{database}"
        if username and password:
            mongo_uri = f"mongodb://{username}:{password}@{host}:{port}/{database}?authSource=admin"

        # Writing using foreachBatch for more control and compatibility
        def write_batch_to_mongo(batch_df, batch_id):
            logger.debug(f"Writing batch {batch_id} to MongoDB {database}.{collection}")
            (
                batch_df.write
                .format("mongodb")
                .option("connection.uri", mongo_uri)
                .option("database", database)
                .option("collection", collection)
                .mode(output_mode)  # Use the specified output mode
                .save()
            )

        # Start the streaming query
        query = (
            df.writeStream
            .foreachBatch(write_batch_to_mongo)
            .option("checkpointLocation", checkpoint_location)
            .outputMode(output_mode) # Ensure output mode is set for the stream
            .start()
        )
        
        # Returning the query object allows the caller to manage its lifecycle (e.g., awaitTermination)
        # If you prefer the original fire-and-forget style, remove the return statement.
        return query

    def run(self) -> None:
        """Template method to be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement 'run' method")