#!/usr/bin/env python
"""
Run script for Stock Data Processor with Cassandra schema setup
"""
import os
import sys
import time
from pathlib import Path
import importlib
from loguru import logger

# Import the required modules
from src.data_processing.stock.cassandra_setup import setup_cassandra_schema
from src.data_processing.stock.stock_processor import StockDataProcessor


def main():
    """Main function to run the stock data processor."""
    if len(sys.argv) < 2:
        logger.error("Usage: python run_stock_processor.py <config_file_path>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    config_file = Path(config_path)
    
    if not config_file.exists():
        logger.error(f"Configuration file not found: {config_path}")
        sys.exit(1)
    
    try:
        # Load the configuration
        from src.data_processing.common.base_processor import BaseStreamProcessor
        config = BaseStreamProcessor.load_config(BaseStreamProcessor, config_path)
        
        # Check if we're running in Docker environment
        in_docker = os.environ.get("IN_DOCKER", "false").lower() == "true"
        logger.info(f"Running in Docker environment: {in_docker}")
        
        # Log environment variables related to Cassandra
        cassandra_host = os.environ.get("CASSANDRA_HOST", config.get("cassandra", {}).get("connection_host", "localhost"))
        cassandra_port = os.environ.get("CASSANDRA_PORT", config.get("cassandra", {}).get("connection_port", "9042"))
        logger.info(f"Cassandra connection: {cassandra_host}:{cassandra_port}")
        
        # Wait for a moment to ensure Cassandra is ready (helpful in Docker environment)
        if in_docker:
            logger.info("Waiting for services to initialize...")
            time.sleep(10)
        
        # Set up the Cassandra schema
        logger.info("Setting up Cassandra schema...")
        try:
            setup_cassandra_schema(config)
        except Exception as e:
            logger.error(f"Error setting up Cassandra schema: {e}")
            logger.error("Will attempt to continue with stock processor anyway...")
        
        # Initialize and run the stock data processor
        logger.info("Initializing stock data processor...")
        processor = StockDataProcessor(config_path)
        
        logger.info("Running stock data processor...")
        processor.run()
    
    except ImportError as e:
        logger.error(f"Failed to import required modules: {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Error running stock data processor: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 