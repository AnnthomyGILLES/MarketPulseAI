import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

from polygon import RESTClient

from src.data_collection.base_collector import BaseCollector


class StockMarketCollector(BaseCollector):
    def __init__(self, config_path: str = None):
        if config_path is None:
            base_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
            config_path = str(base_dir / "config" / "kafka" / "kafka_config.yaml")

        super().__init__(config_path, "market_data_collector")
        self.symbols = self._load_symbols()
        self.api_keys = self._load_api_keys()
        self.client = RESTClient(self.api_keys["polygon"])
        self.running = False
        self.collection_interval = 30  # seconds

    def _load_symbols(self) -> List[str]:
        """
        Load the list of stock symbols to track.

        Returns:
            List of stock symbols
        """

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
        try:
            trade = self.client.get_last_trade(symbol)
            return {
                "source": "polygon",
                "symbol": symbol,
                "price": trade.price,
                "size": trade.size,
                "exchange": trade.exchange,
                "timestamp": datetime.fromtimestamp(trade.timestamp / 1e9).isoformat(),
                "raw_data": trade._raw,
            }
        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {str(e)}")
            raise

    def _fetch_data_for_symbol(self, symbol: str, source: str) -> Dict[str, Any]:
        if source == "polygon":
            return self._fetch_polygon_data(symbol)
        raise ValueError(f"Unsupported data source: {source}")

    def _collect_symbol_data(self, symbol: str) -> None:
        source = "polygon"
        try:
            data = self._fetch_data_for_symbol(symbol, source)
            data["collection_timestamp"] = datetime.now().isoformat()
            data["collector_id"] = id(self)
            self.send_to_kafka(
                self.config["kafka"]["topics"]["market_data_raw"], data, key=symbol
            )
            self.logger.debug(f"Collected data for {symbol} from {source}")
        except Exception as e:
            self.logger.error(f"Failed to collect data for {symbol}: {str(e)}")

    def _collect_all_symbols(self) -> None:
        for symbol in self.symbols:
            self._collect_symbol_data(symbol)

    def collect(self) -> None:
        self.running = True
        self.logger.info(
            f"Starting market data collection for symbols: {', '.join(self.symbols)}"
        )

        try:
            while self.running:
                start_time = time.time()

                # Collect data sequentially instead of using asyncio
                self._collect_all_symbols()

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
            self.cleanup()

    def stop(self) -> None:
        self.running = False
        self.logger.info("Stopping market data collection")
        self.cleanup()


if __name__ == "__main__":
    collector = StockMarketCollector()

    ticker = "AAPL"

    # List Aggregates (Bars)
    aggs = []
    for a in collector.client.list_aggs(
        ticker=ticker,
        multiplier=1,
        timespan="minute",
        from_="2023-01-01",
        to="2023-06-13",
        limit=50000,
    ):
        aggs.append(a)

    print(aggs)

    # Get Last Trade
    trade = collector.client.get_last_trade(ticker=ticker)
    print(trade)

    # List Trades
    trades = collector.client.list_trades(ticker=ticker, timestamp="2022-01-04")
    for trade in trades:
        print(trade)

    # Get Last Quote
    quote = collector.client.get_last_quote(ticker=ticker)
    print(quote)

    # # List Quotes
    # quotes = collector.client.list_quotes(ticker=ticker, timestamp="2022-01-04")
    # for quote in quotes:
    #     print(quote)
    # try:
    #     print("Starting stock market data collection...")
    #     collector.collect()
    # except KeyboardInterrupt:
    #     print("\nData collection interrupted by user")
    # finally:
    #     collector.stop()
    #     print("Data collection stopped")
