import time
from datetime import datetime, timedelta
from pathlib import Path

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
        ]
        self.client = RESTClient(self._get_polygon_api_key())
        self.running = False
        self.collection_interval = 30  # seconds

    def _get_polygon_api_key(self) -> str:
        """Get the Polygon API key from environment variables"""
        import os

        return os.environ.get("POLYGON_API_KEY", "demo")

    def get_agg_bars(
        self,
        symbols=None,
        multiplier: int = 1,
        timespan: str = "day",
        from_date: str = None,
        to_date: str = None,
        adjusted: bool = True,
        sort: str = "asc",
        limit: int = 120,
    ) -> None:
        """
        Get aggregated bars (OHLC) data for given symbols and send directly to Kafka.

        Args:
            symbols: List of stock ticker symbols (defaults to self.symbols)
            multiplier: The size of the timespan multiplier
            timespan: The timespan unit (minute, hour, day, week, month, quarter, year)
            from_date: Start date in format YYYY-MM-DD (defaults to 30 days ago)
            to_date: End date in format YYYY-MM-DD (defaults to today)
            adjusted: Whether results are adjusted for splits
            sort: Sort direction ('asc' or 'desc')
            limit: Maximum number of results (max 50000)
        """
        # Use default symbols list if not provided
        if symbols is None:
            symbols = self.symbols

        # Set default dates if not provided
        if not from_date:
            from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%d")

        self.logger.info(
            f"Fetching and streaming {timespan} bars for {len(symbols)} symbols from {from_date} to {to_date}"
        )

        for symbol in symbols:
            try:
                # Fetch and immediately stream the aggregated bars
                bar_count = 0
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

                    # Send each bar directly to Kafka without storing
                    self.send_to_kafka(
                        self.config["kafka"]["topics"]["market_data_raw"],
                        agg_dict,
                        key=f"{symbol}_{agg.timestamp}",
                    )
                    bar_count += 1

                self.logger.info(
                    f"Streamed {bar_count} {timespan} bars for {symbol} to Kafka"
                )

            except Exception as e:
                self.logger.error(
                    f"Failed to stream aggregated bars for {symbol}: {str(e)}"
                )

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
        """
        Run the collection process for all symbols.

        Currently collects historical aggregated bar data.
        In production, this will be replaced with websocket Aggregates (Per Minute).
        """
        self.running = True
        self.logger.info(
            f"Starting market data collection for symbols: {', '.join(self.symbols)}"
        )

        try:
            # For now, we're collecting historical aggregated data for all symbols at once
            # In production, this will be replaced with websocket Aggregates (Per Minute)
            self.get_agg_bars(
                symbols=self.symbols,
                multiplier=1,
                timespan="minute",
                from_date=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
                to_date=datetime.now().strftime("%Y-%m-%d"),
                adjusted=True,
                sort="asc",
                limit=50000,
            )

            self.logger.info("Completed initial data collection for all symbols")

            # Future implementation will use websocket for real-time updates
            # Placeholder for now - just wait until stopped
            while self.running:
                time.sleep(60)  # Sleep for a minute

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
    # Create an instance of the collector
    collector = StockMarketCollector()

    try:
        # Start the collection process
        collector.collect()
    except KeyboardInterrupt:
        # Handle graceful shutdown on Ctrl+C
        print("Collection interrupted. Shutting down...")
    finally:
        # Ensure resources are cleaned up
        collector.stop()
