import time

import pandas as pd
from kafka import KafkaProducer
from json import dumps
from src.data_collection import logger, KAFKA_BOOTSTRAP_SERVERS, MARKET_DATA_TOPIC


class MarketShareCollector:
    """Collects market share data from a CSV file and sends it to Kafka."""

    def __init__(self, csv_file: str):
        """Initialize the collector with the CSV file path."""
        self.csv_file = csv_file
        print(KAFKA_BOOTSTRAP_SERVERS)
        self.producer = KafkaProducer(
            bootstrap_servers="localhost:9093",
            value_serializer=lambda x: dumps(x).encode("utf-8"),
        )
        logger.info(f"MarketShareCollector initialized with CSV file: {self.csv_file}")

    def produce(self):
        """Produce market share data to Kafka in a loop."""
        df = pd.read_csv(self.csv_file)
        while True:
            market_share_record = df.sample(1).to_dict(orient="records")[0]
            self.producer.send(MARKET_DATA_TOPIC, value=market_share_record)
            logger.info(f"Sent market share data: {market_share_record}")
            time.sleep(1)

    def close(self):
        """Close the Kafka producer."""
        self.producer.flush()
        self.producer.close()
        logger.info("MarketShareCollector closed.")


if __name__ == "__main__":
    collector = MarketShareCollector("../../../../data/raw/all_stocks_5yr.csv")
    try:
        collector.produce()
    except Exception as e:
        logger.error(f"Error in market share data collection: {e}")
