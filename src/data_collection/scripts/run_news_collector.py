#!/usr/bin/env python
"""
Script to run the NewsAPI collector.

This script loads the NewsAPI configuration and runs the collector
to fetch financial news data from NewsAPI.org.
"""

import os
import sys
import time
from pathlib import Path
import argparse
import yaml

from loguru import logger
from dotenv import load_dotenv

from src.data_collection.social_media.news import NewsApiCollector

# Add src directory to Python path
current_dir = Path(__file__).resolve().parent
src_dir = current_dir.parent.parent
sys.path.append(str(src_dir))


def load_config(config_path):
    """Load the configuration from a YAML file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def setup_logging(log_level):
    """Configure logging with loguru."""
    logger.remove()  # Remove default handler
    logger.add(sys.stderr, level=log_level)
    logger.add(
        "logs/news_collector_{time}.log",
        rotation="10 MB",
        retention="7 days",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    )


def run_collector(config_path, run_once=False):
    """
    Run the NewsAPI collector with specified configuration.

    Args:
        config_path: Path to configuration file
        run_once: If True, collect once and exit; otherwise run on a schedule
    """
    # Load environment variables from .env file
    load_dotenv()

    # Load configuration
    config = load_config(config_path)

    # Validate API key
    api_key = os.getenv("NEWSAPI_KEY")
    if not api_key:
        logger.error("NEWSAPI_KEY environment variable is not set")
        sys.exit(1)

    # Create persistence directory if it doesn't exist
    persistence_dir = config["collection"]["persistence_dir"]
    Path(persistence_dir).mkdir(parents=True, exist_ok=True)

    # Create directories for entity extraction files
    for path_key in [
        "stock_symbols_path",
        "company_names_path",
        "financial_terms_path",
    ]:
        path = config["entity_extraction"].get(path_key)
        if path:
            Path(path).parent.mkdir(parents=True, exist_ok=True)

    # Create collector instance
    collector = NewsApiCollector(
        api_key=api_key,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093"),
        topic_name=config["kafka"]["news_topic"],
        error_topic_name=config["kafka"]["error_topic"],
        queries=config["filters"]["queries"],
        sources=config["filters"]["sources"],
        categories=config["filters"]["categories"],
        languages=config["filters"]["languages"],
        countries=config["filters"]["countries"],
        page_size=config["collection"]["page_size"],
        days_to_fetch=config["collection"]["days_to_fetch"],
        persistence_dir=persistence_dir,
        stock_symbols_path=config["entity_extraction"]["stock_symbols_path"],
        collector_name="NewsApiCollector",
    )

    try:
        if run_once:
            # Run collection once
            logger.info("Running news collection (one-time mode)")
            collector.collect()
            logger.info("News collection completed")
        else:
            # Run on schedule
            frequency_minutes = config["collection"]["frequency"]
            logger.info(
                f"Starting news collection scheduler (every {frequency_minutes} minutes)"
            )

            while True:
                try:
                    collector.collect()
                    logger.info(
                        f"Waiting {frequency_minutes} minutes until next collection"
                    )
                    time.sleep(frequency_minutes * 60)
                except KeyboardInterrupt:
                    logger.info("Keyboard interrupt received, stopping collector")
                    collector.stop()
                    break
                except Exception as e:
                    logger.exception(f"Error during collection cycle: {e}")
                    logger.info(
                        f"Waiting {frequency_minutes} minutes until next attempt"
                    )
                    time.sleep(frequency_minutes * 60)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    finally:
        collector.cleanup()
        logger.info("News collector stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the NewsAPI collector")
    parser.add_argument(
        "--config",
        default="config/news_api_config.yaml",
        help="Path to the configuration file",
    )
    parser.add_argument(
        "--once", action="store_true", help="Run once and exit, don't run on a schedule"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level",
    )

    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    # Run collector
    run_collector(args.config, args.once) 