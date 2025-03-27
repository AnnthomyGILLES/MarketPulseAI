"""
Stock market data collector for retrieving real-time market data.
"""

import time
from typing import Dict, Any, List, Optional
import asyncio
from datetime import datetime
import requests
from requests.exceptions import RequestException
from pathlib import Path

from src.data_collection.base_collector import BaseCollector


class StockMarketCollector(BaseCollector):
    """
    Collector for real-time stock market data from various APIs.

    This collector can pull data from multiple market data sources
    and send it to Kafka for further processing.
    """

    def __init__(self, config_path: str = None):
        """
        Initialize the stock market collector.

        Args:
            config_path: Path to the configuration file
        """
        if config_path is None:
            # Get the absolute path to the config file relative to the project root
            base_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
            config_path = str(base_dir / "config" / "kafka" / "kafka_config.yaml")

        super().__init__(config_path, "market_data_collector")
        self.symbols = self._load_symbols()
        self.api_keys = self._load_api_keys()
        self.running = False
        self.collection_interval = 1  # seconds

    def _load_symbols(self) -> List[str]:
        """
        Load the list of stock symbols to track.

        Returns:
            List of stock symbols
        """
        # In a real application, this might come from a database or config file
        # For now, we'll use a hardcoded list of example symbols
        return ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA"]

    def _load_api_keys(self) -> Dict[str, str]:
        """
        Load API keys for data sources.

        Returns:
            Dictionary of API keys by provider
        """
        # Only load the Polygon API key
        return {
            "polygon": self._get_env_var("POLYGON_API_KEY", "demo"),
        }

    def _get_env_var(self, name: str, default: Optional[str] = None) -> str:
        """
        Get environment variable with fallback to default.

        Args:
            name: Name of the environment variable
            default: Default value if not found

        Returns:
            Value of the environment variable or default
        """
        import os

        return os.environ.get(name, default)

    def _fetch_polygon_data(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch data from Polygon API.

        Args:
            symbol: Stock symbol to fetch data for

        Returns:
            Market data dictionary

        Raises:
            RequestException: If the API request fails
        """
        api_key = self.api_keys["polygon"]
        today = datetime.now().strftime("%Y-%m-%d")
        url = f"https://api.polygon.io/v3/reference/dividends?apiKey={api_key}"
        # url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{today}/{today}?apiKey={api_key}"

        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()

        # Transform API response to our standard format
        if "results" in data and data["results"]:
            latest = data["results"][-1]  # Get the latest result
            return {
                "source": "polygon",
                "symbol": symbol,
                "price": float(latest.get("c", 0)),  # Close price
                "volume": int(latest.get("v", 0)),  # Volume
                "timestamp": datetime.now().isoformat(),
                "change_percent": 0,  # Would need to calculate this based on previous close
                "raw_data": latest,
            }
        else:
            raise ValueError(f"No results found in Polygon response: {data}")

    def _fetch_data_for_symbol(self, symbol: str, source: str) -> Dict[str, Any]:
        """
        Fetch data for a specific symbol from a specific source.

        Args:
            symbol: Stock symbol to fetch data for
            source: Data source to use

        Returns:
            Market data dictionary

        Raises:
            ValueError: If the source is not supported
            RequestException: If the API request fails
        """
        try:
            if source == "polygon":
                return self._fetch_polygon_data(symbol)
            else:
                raise ValueError(f"Unsupported data source: {source}")
        except RequestException as e:
            self.logger.error(
                f"API request failed for {symbol} from {source}: {str(e)}"
            )
            raise
        except Exception as e:
            self.logger.error(
                f"Failed to fetch data for {symbol} from {source}: {str(e)}"
            )
            raise

    async def _collect_symbol_data(self, symbol: str) -> None:
        """
        Collect data for a specific symbol from all sources.

        Args:
            symbol: Stock symbol to collect data for
        """
        # Use Polygon as the default source
        source = "polygon"

        try:
            # Fetch the data
            data = self._fetch_data_for_symbol(symbol, source)

            # Add some metadata
            data["collection_timestamp"] = datetime.now().isoformat()
            data["collector_id"] = id(self)

            # Send to Kafka
            self.send_to_kafka(
                self.config["kafka"]["topics"]["market_data_raw"], data, key=symbol
            )

            self.logger.debug(f"Collected data for {symbol} from {source}")
        except Exception as e:
            self.logger.error(f"Failed to collect data for {symbol}: {str(e)}")

    async def _collect_all_symbols(self) -> None:
        """Collect data for all symbols concurrently."""
        tasks = []
        for symbol in self.symbols:
            tasks.append(self._collect_symbol_data(symbol))

        await asyncio.gather(*tasks)

    def collect(self) -> None:
        """
        Start the data collection process.

        This will continuously collect data for all symbols at the
        specified interval until stopped.
        """
        self.running = True
        self.logger.info(
            f"Starting market data collection for symbols: {', '.join(self.symbols)}"
        )

        try:
            while self.running:
                start_time = time.time()

                # Use asyncio to collect data concurrently
                asyncio.run(self._collect_all_symbols())

                # Calculate sleep time to maintain consistent interval
                elapsed = time.time() - start_time
                sleep_time = max(0, self.collection_interval - elapsed)

                if sleep_time > 0:
                    time.sleep(sleep_time)
        except KeyboardInterrupt:
            self.logger.info("Market data collection stopped by user")
        except Exception as e:
            self.logger.error(f"Market data collection failed: {str(e)}")
        finally:
            self.running = False
            self.cleanup()  # Clean up resources properly

    def stop(self) -> None:
        """Stop the data collection process."""
        self.running = False
        self.logger.info("Stopping market data collection")
        self.cleanup()  # Ensure resources are cleaned up

if __name__ == "__main__":
    # Create an instance of the collector
    collector = StockMarketCollector()
    
    try:
        # Start data collection process
        print("Starting stock market data collection...")
        collector.collect()
    except KeyboardInterrupt:
        # Handle Ctrl+C gracefully
        print("\nData collection interrupted by user")
    finally:
        # Ensure resources are properly cleaned up
        collector.stop()
        print("Data collection stopped")
