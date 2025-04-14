# src/data_processing/main.py
from pathlib import Path
import typer
from loguru import logger

from src.data_processing.stock.stock_processor import StockDataProcessor
from src.data_processing.reddit.sentiment_processor import RedditSentimentProcessor

app = typer.Typer()


@app.command()
def process_stocks(config_path: str = "config/spark/stock_processor_config.yaml"):
    """Run the stock data processing pipeline."""
    logger.info(f"Starting stock data processor with config: {config_path}")
    processor = StockDataProcessor(config_path)
    processor.run()


@app.command()
def process_reddit(config_path: str = "config/spark/reddit_sentiment_config.yaml"):
    """Run the Reddit sentiment analysis pipeline."""
    logger.info(f"Starting Reddit sentiment processor with config: {config_path}")
    processor = RedditSentimentProcessor(config_path)
    processor.run()


if __name__ == "__main__":
    # Configure logging
    logger.remove()
    logger.add(
        sink=lambda msg: print(msg, end=""),
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}"
    )

    app()