#!/usr/bin/env python
"""
Script to set up Cassandra keyspace and tables for storing stock data
"""
import os
import sys
import time
from pathlib import Path
import yaml
from loguru import logger

# Try to import Cassandra with fallback options
try:
    from cassandra.cluster import Cluster, NoHostAvailable
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.policies import DCAwareRoundRobinPolicy, RetryPolicy
    # Configure Cassandra to use the pure Python implementation
    os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = '1'
except ImportError as e:
    logger.error(f"Failed to import Cassandra driver: {e}")
    logger.error("Please install the Cassandra driver: pip install cassandra-driver")
    sys.exit(1)
# For Python 3.12, set this environment variable to avoid asyncore issues
if sys.version_info >= (3, 12):
    os.environ['CASS_DRIVER_NO_EXTENSIONS'] = '1'

def load_config(config_path):
    """Load configuration from a YAML file."""
    config_file = Path(config_path)
    if not config_file.exists():
        logger.error(f"Configuration file not found: {config_path}")
        sys.exit(1)

    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    return config

def setup_cassandra_schema(config):
    """Set up the Cassandra keyspace and tables for stock data."""
    # Get Cassandra configuration
    cassandra_config = config.get("cassandra", {})
    
    # Use environment variables if available, otherwise use config values
    host = os.environ.get("CASSANDRA_HOST", cassandra_config.get("connection_host", "localhost"))
    port = int(os.environ.get("CASSANDRA_PORT", cassandra_config.get("connection_port", 9042)))
    username = os.environ.get("CASSANDRA_USER", cassandra_config.get("auth_username", "cassandra"))
    password = os.environ.get("CASSANDRA_PASSWORD", cassandra_config.get("auth_password", "cassandra"))
    keyspace = cassandra_config.get("keyspace", "market_data")
    table = cassandra_config.get("table", "stock_features")

    logger.info(f"Connecting to Cassandra at {host}:{port}")
    
    # Set up authentication if provided
    auth_provider = None
    if username and password:
        auth_provider = PlainTextAuthProvider(username=username, password=password)
        logger.info(f"Using authentication with username: {username}")
    
    # Initialize cluster and session
    session = None
    cluster = None
    
    # Add retry logic for connection (Docker container might not be ready immediately)
    max_retries = 5
    retry_wait = 10  # seconds
    
    for retry_count in range(max_retries):
        try:
            # Handle connection differently based on Python version
            if sys.version_info >= (3, 12):
                # Force using the pure Python implementation
                os.environ['CASS_DRIVER_NO_EXTENSIONS'] = '1'
                # Disable asyncore-based event loop
                os.environ['CASS_DRIVER_NO_ASYNCORE'] = '1'
            
            # Try connecting with different configuration options
            try:
                logger.info(f"Attempt {retry_count + 1} to connect to Cassandra")
                cluster = Cluster(
                    [host], 
                    port=port, 
                    auth_provider=auth_provider,
                    control_connection_timeout=10.0,
                    connect_timeout=10.0
                )
                session = cluster.connect()
                logger.info("Connected to Cassandra")
                break
            except Exception as e:
                logger.warning(f"Connection attempt failed: {e}, trying with alternative configuration")
                # Try with pure Python driver if the C extension fails
                os.environ['CASS_DRIVER_NO_EXTENSIONS'] = '1'
                cluster = Cluster(
                    [host], 
                    port=port, 
                    auth_provider=auth_provider,
                    protocol_version=4,
                    control_connection_timeout=10.0,
                    connect_timeout=10.0
                )
                session = cluster.connect()
                logger.info("Connected to Cassandra with alternative configuration")
                break
                
        except NoHostAvailable as e:
            if retry_count < max_retries - 1:
                logger.warning(f"Cassandra not available yet, retrying in {retry_wait} seconds... ({retry_count + 1}/{max_retries})")
                time.sleep(retry_wait)
            else:
                logger.error(f"Failed to connect to Cassandra after {max_retries} attempts: {e}")
                logger.error("Please make sure Cassandra is running and accessible.")
                sys.exit(1)
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            logger.error("Please make sure Cassandra is running and accessible.")
            sys.exit(1)
    
    if session is None:
        logger.error("Failed to establish Cassandra session")
        sys.exit(1)
    
    # Create keyspace if it doesn't exist
    logger.info(f"Creating keyspace {keyspace} if it doesn't exist")
    create_keyspace_query = f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace}
    WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }};
    """
    
    try:
        session.execute(create_keyspace_query)
        logger.info(f"Keyspace {keyspace} created or already exists")
    except Exception as e:
        logger.error(f"Failed to create keyspace: {e}")
        cluster.shutdown()
        sys.exit(1)
    
    # Use the keyspace
    session.set_keyspace(keyspace)
    
    # Create table if it doesn't exist
    logger.info(f"Creating table {table} if it doesn't exist")
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        name TEXT,
        date TIMESTAMP,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        PRIMARY KEY ((name), date)
    ) WITH CLUSTERING ORDER BY (date DESC);
    """
    
    try:
        session.execute(create_table_query)
        logger.info(f"Table {table} created or already exists")
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        cluster.shutdown()
        sys.exit(1)
    
    # Close the connection
    cluster.shutdown()
    logger.info("Cassandra schema setup complete")

def main():
    """Main function to set up Cassandra schema."""
    if len(sys.argv) < 2:
        logger.error("Usage: python cassandra_setup.py <config_file_path>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    config = load_config(config_path)
    setup_cassandra_schema(config)

if __name__ == "__main__":
    main() 