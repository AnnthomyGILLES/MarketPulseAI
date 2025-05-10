#!/bin/bash
# run-stock-processor.sh - Run the stock processor with proper environment setup

set -e

# Set environment variables indicating we're in Docker
export IN_DOCKER=true

# Default config path
CONFIG_PATH=${1:-"/opt/bitnami/spark/config/spark/stock_processor_config.yaml"}

# Get Cassandra connection info from environment or use defaults
CASSANDRA_HOST=${CASSANDRA_HOST:-"cassandra"}
CASSANDRA_PORT=${CASSANDRA_PORT:-"9042"}

echo "==== Starting Stock Data Processor ===="
echo "Config path: $CONFIG_PATH"
echo "Cassandra host: $CASSANDRA_HOST"
echo "Cassandra port: $CASSANDRA_PORT"

# First, wait for Cassandra to be ready
/opt/bitnami/spark/scripts/wait-for-cassandra.sh "$CASSANDRA_HOST" "$CASSANDRA_PORT" || {
  echo "Failed to connect to Cassandra after multiple attempts"
  echo "Will try to run the processor anyway..."
}

# Run the stock data processor
cd /opt/bitnami/spark
python -m src.data_processing.stock.run_stock_processor "$CONFIG_PATH" 