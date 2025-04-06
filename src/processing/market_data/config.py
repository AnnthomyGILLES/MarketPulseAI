# src/processing/market_data/config.py
import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from loguru import logger

# Define paths relative to the project root or use environment variables
CONFIG_PATH = (
    Path(__file__).parents[3] / "config"
)  # Assumes src is one level down from root
KAFKA_CONFIG_FILE = CONFIG_PATH / "kafka" / "kafka_config.yaml"


@dataclass
class KafkaConfig:
    bootstrap_servers: list[str]
    market_data_raw_topic: str
    market_data_error_topic: str
    consumer_group: str


@dataclass
class CassandraConfig:
    host: str
    port: int
    keyspace: str
    table: str


def load_kafka_config() -> KafkaConfig:
    """Loads Kafka configuration from the YAML file."""
    try:
        with open(KAFKA_CONFIG_FILE, "r") as f:
            config = yaml.safe_load(f)

        # Prioritize production servers if available, else use default
        bootstrap_servers = config.get("bootstrap_servers_prod") or config.get(
            "bootstrap_servers", ["localhost:9092"]
        )

        return KafkaConfig(
            bootstrap_servers=bootstrap_servers,
            market_data_raw_topic=config["topics"]["market_data_raw"],
            market_data_error_topic=config["topics"]["market_data_error"],
            consumer_group=config["consumer_groups"]["market_data_validation"],
        )
    except FileNotFoundError:
        logger.error(f"Kafka configuration file not found at {KAFKA_CONFIG_FILE}")
        raise
    except KeyError as e:
        logger.error(f"Missing key in Kafka configuration: {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading Kafka configuration: {e}")
        raise


def load_cassandra_config() -> CassandraConfig:
    """Loads Cassandra configuration from environment variables."""
    try:
        host = os.environ.get("CASSANDRA_HOST", "localhost")
        port = int(os.environ.get("CASSANDRA_PORT", "9042"))
        keyspace = os.environ.get("CASSANDRA_KEYSPACE", "market_pulse")
        table = os.environ.get("CASSANDRA_TABLE", "market_data")
        return CassandraConfig(host=host, port=port, keyspace=keyspace, table=table)
    except ValueError as e:
        logger.error(f"Invalid Cassandra port format in environment variable: {e}")
        raise
    except Exception as e:
        logger.error(f"Error loading Cassandra configuration: {e}")
        raise


# Load configurations globally for the module
KAFKA_SETTINGS = load_kafka_config()
CASSANDRA_SETTINGS = load_cassandra_config()

logger.info("Kafka configuration loaded.")
logger.info("Cassandra configuration loaded.")
