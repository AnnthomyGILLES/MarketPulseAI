import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List

from polygon import RESTClient

from src.data_collection.base_collector import BaseCollector


class StockMarketCollector(BaseCollector):
    def __init__(self, config_path: str = None):
        if config_path is None:
            base_dir = Path(__file__).resolve().parent.parent.parent.parent.parent
            config_path = str(base_dir / "config" / "kafka" / "kafka_config.yaml")

        super().__init__(config_path, "market_data_collector")
        self.symbols = [
            "AAPL",
            "MSFT",
            "GOOGL",
            "AMZN",
            "META",
            "TSLA",
        ]  # Default symbols
        self.client = RESTClient(self._get_polygon_api_key())
        self.running = False
        self.collection_interval = 30  # seconds

    def _get_polygon_api_key(self) -> str:
        """Get the Polygon API key from environment variables"""
        import os

        return os.environ.get("POLYGON_API_KEY", "demo")

    def get_agg_bars(
        self,
        symbol: str,
        multiplier: int = 1,
        timespan: str = "day",
        from_date: str = None,
        to_date: str = None,
        adjusted: bool = True,
        sort: str = "asc",
        limit: int = 120,
    ) -> List[Dict[str, Any]]:
        """
        Get aggregated bars (OHLC) data for a given symbol.

        Args:
            symbol: The stock ticker symbol
            multiplier: The size of the timespan multiplier
            timespan: The timespan unit (minute, hour, day, week, month, quarter, year)
            from_date: Start date in format YYYY-MM-DD (defaults to 30 days ago)
            to_date: End date in format YYYY-MM-DD (defaults to today)
            adjusted: Whether results are adjusted for splits
            sort: Sort direction ('asc' or 'desc')
            limit: Maximum number of results (max 50000)

        Returns:
            List of dictionaries containing the aggregated bar data
        """
        # Set default dates if not provided
        if not from_date:
            from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%d")

        self.logger.info(
            f"Fetching {timespan} bars for {symbol} from {from_date} to {to_date}"
        )

        try:
            # Fetch the aggregated bars
            aggs = []
            for agg in self.client.list_aggs(
                symbol,
                multiplier,
                timespan,
                from_date,
                to_date,
                adjusted=adjusted,
                sort=sort,
                limit=limit,
            ):
                # Convert polygon object to dictionary
                agg_dict = {
                    "symbol": symbol,
                    "open": agg.open,
                    "high": agg.high,
                    "low": agg.low,
                    "close": agg.close,
                    "volume": agg.volume,
                    "vwap": getattr(agg, "vwap", None),
                    "timestamp": agg.timestamp,
                    "transactions": getattr(agg, "transactions", None),
                    "collection_timestamp": datetime.now().isoformat(),
                }
                aggs.append(agg_dict)

                # Optionally, send each bar to Kafka
                self.send_to_kafka(
                    self.config["kafka"]["topics"]["market_data_agg"],
                    agg_dict,
                    key=f"{symbol}_{agg.timestamp}",
                )

            self.logger.info(f"Collected {len(aggs)} {timespan} bars for {symbol}")
            return aggs

        except Exception as e:
            self.logger.error(
                f"Failed to collect aggregated bars for {symbol}: {str(e)}"
            )
            return []

    def _collect_symbol_data(self, symbol: str) -> None:
        """Collect and send data for a single symbol"""
        try:
            trade = self.client.get_last_trade(symbol)
            data = {
                "symbol": symbol,
                "price": trade.price,
                "size": trade.size,
                "timestamp": datetime.fromtimestamp(trade.timestamp / 1e9).isoformat(),
                "collection_timestamp": datetime.now().isoformat(),
            }
            self.send_to_kafka(
                self.config["kafka"]["topics"]["market_data_raw"], data, key=symbol
            )
            self.logger.debug(f"Collected data for {symbol}")
        except Exception as e:
            self.logger.error(f"Failed to collect data for {symbol}: {str(e)}")

    def collect(self) -> None:
        """Run the collection process for all symbols"""
        self.running = True
        self.logger.info(
            f"Starting market data collection for symbols: {', '.join(self.symbols)}"
        )

        try:
            while self.running:
                start_time = time.time()

                # Collect data for all symbols
                for symbol in self.symbols:
                    self._collect_symbol_data(symbol)

                # Sleep to maintain collection interval
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
        """Stop the data collection process"""
        self.running = False
        self.logger.info("Stopping market data collection")
        self.cleanup()


if __name__ == "__main__":
    collector = StockMarketCollector()

    # Example of using the new get_agg_bars method
    if collector.client:
        aggs = collector.get_agg_bars(
            symbol="AAPL",
            multiplier=1,
            timespan="day",
            from_date="2023-01-09",
            to_date="2023-02-10",
            adjusted=True,
            sort="asc",
            limit=120,
        )
        print(f"Collected {len(aggs)} aggregated bars for AAPL")

    try:
        print("Starting stock market data collection...")
        collector.collect()
    except KeyboardInterrupt:
        print("\nData collection interrupted by user")
    finally:
        collector.stop()
        print("Data collection stopped")
