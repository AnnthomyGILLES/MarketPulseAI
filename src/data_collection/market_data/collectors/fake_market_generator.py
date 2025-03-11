import numpy as np
from datetime import datetime, timedelta
import random
import argparse

from src.common.messaging.kafka_producer import KafkaProducerWrapper


def generate_and_stream_fake_stock_data(
    ticker_symbol,
    start_date,
    num_days,
    kafka_producer,
    trading_hours=(9, 16),
    initial_price=15.0,
    intraday_volatility=0.001,
    daily_drift=0.005,
):
    """
    Generate fake stock data with minute-by-minute updates and stream to Kafka.

    Parameters:
    -----------
    ticker_symbol : str
        The stock ticker symbol
    start_date : datetime
        Starting date for the data
    num_days : int
        Number of trading days to generate
    kafka_producer : KafkaProducerWrapper
        Kafka producer to send messages
    trading_hours : tuple
        Trading session hours (start_hour, end_hour)
    initial_price : float
        Starting price for the stock
    intraday_volatility : float
        Per-minute volatility factor
    daily_drift : float
        Daily price drift factor
    """
    # Create a list of trading dates (excluding weekends)
    trading_dates = []
    current_date = start_date

    while len(trading_dates) < num_days:
        # Skip weekends (5 = Saturday, 6 = Sunday)
        if current_date.weekday() < 5:
            trading_dates.append(current_date)
        current_date += timedelta(days=1)

    # Initialize price data
    price = initial_price
    total_records = 0

    # For each trading day
    for date in trading_dates:
        # Daily price drift
        daily_factor = 1 + np.random.normal(0, daily_drift)
        day_open_price = price

        # Track intraday high and low
        day_high = day_open_price
        day_low = day_open_price

        # Generate minute-by-minute data for the trading session
        start_hour, end_hour = trading_hours
        minutes_per_day = (end_hour - start_hour) * 60

        for minute in range(minutes_per_day):
            # Calculate current time
            current_hour = start_hour + minute // 60
            current_minute = minute % 60
            timestamp = datetime.combine(
                date.date(),
                datetime.min.time().replace(hour=current_hour, minute=current_minute),
            )

            # Generate minute-by-minute price movement
            minute_volatility = np.random.normal(0, intraday_volatility)
            price = max(0.01, price * (1 + minute_volatility))

            # Update day high and low
            day_high = max(day_high, price)
            day_low = min(day_low, price)

            # Generate volume for this minute (lower for individual minutes)
            minute_volume = int(random.randrange(1000, 100000, 100))

            # Round prices to 2 decimal places
            open_price = round(day_open_price if minute == 0 else price, 2)
            high_price = round(day_high, 2)
            low_price = round(day_low, 2)
            close_price = round(price, 2)

            # Create message for Kafka
            message = {
                "date": timestamp.isoformat(),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": minute_volume,
                "Name": ticker_symbol,
            }

            # Send to Kafka
            kafka_producer.send_message(message)
            total_records += 1
        # Apply daily factor at end of day
        price = price * daily_factor

    return total_records


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Generate fake stock data and send to Kafka"
    )
    parser.add_argument("--ticker", type=str, default="AAL", help="Stock ticker symbol")
    parser.add_argument(
        "--days", type=int, default=5, help="Number of trading days to generate"
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:9093",
        help="Kafka bootstrap servers (comma-separated)",
    )
    parser.add_argument("--topic", type=str, default="market_data", help="Kafka topic")

    args = parser.parse_args()

    # Parameters
    ticker = args.ticker
    start = datetime(2023, 3, 1, 9, 30)  # Starting at 9:30 AM
    days = args.days

    # Set up Kafka producer
    bootstrap_servers = args.bootstrap_servers.split(",")

    # Generate and stream data
    with KafkaProducerWrapper(bootstrap_servers, args.topic) as producer:
        total_records = generate_and_stream_fake_stock_data(
            ticker, start, days, producer, trading_hours=(9, 16)
        )
        print(
            f"Generated and sent {total_records} records to Kafka topic '{args.topic}'"
        )
