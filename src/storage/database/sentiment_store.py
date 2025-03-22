import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, BatchStatement
from cassandra.policies import RoundRobinPolicy
import uuid

from loguru import logger


class SentimentStore:
    """
    Database interface for storing and retrieving sentiment analysis data.

    This class handles all database interactions for sentiment data, using
    Cassandra as the backend for efficient time-series storage and retrieval.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the sentiment store with database configuration.

        Args:
            config: Database configuration parameters
        """
        self.config = config
        self.cluster = None
        self.session = None
        self.keyspace = config.get("keyspace", "market_pulse")

        # Initialize database connection
        self._init_connection()

        # Initialize schema if needed
        if config.get("create_schema", False):
            self._init_schema()

        logger.info("SentimentStore initialized successfully")

    def _init_connection(self):
        """Initialize connection to Cassandra cluster."""
        try:
            auth_provider = None

            # Configure authentication if provided
            if "username" in self.config and "password" in self.config:
                auth_provider = PlainTextAuthProvider(
                    username=self.config["username"], password=self.config["password"]
                )

            # Connect to cluster
            self.cluster = Cluster(
                contact_points=self.config.get("hosts", ["localhost"]),
                port=self.config.get("port", 9042),
                auth_provider=auth_provider,
                load_balancing_policy=RoundRobinPolicy(),
                protocol_version=self.config.get("protocol_version", 4),
                connect_timeout=self.config.get("timeout", 10),
            )

            # Create session
            self.session = self.cluster.connect()

            # Create keyspace if it doesn't exist
            self._create_keyspace_if_not_exists()

            # Set to use the keyspace
            self.session.set_keyspace(self.keyspace)

            logger.info(
                f"Connected to Cassandra cluster, using keyspace {self.keyspace}"
            )

        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}", exc_info=True)
            raise

    def _create_keyspace_if_not_exists(self):
        """Create the keyspace if it doesn't already exist."""
        replication_factor = self.config.get("replication_factor", 3)
        create_keyspace_query = f"""
        CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {replication_factor}}};
        """
        self.session.execute(create_keyspace_query)

    def _init_schema(self):
        """Initialize database schema if it doesn't exist."""
        try:
            # Create sentiment data table
            self.session.execute("""
            CREATE TABLE IF NOT EXISTS sentiment_data (
                ticker text,
                timestamp timestamp,
                source text,
                source_id text,
                sentiment text,
                score float,
                confidence float,
                influence_score float,
                model_type text,
                metadata map<text, text>,
                id uuid,
                PRIMARY KEY ((ticker), timestamp, id)
            ) WITH CLUSTERING ORDER BY (timestamp DESC, id ASC);
            """)

            # Create sentiment by source table for fast source-based queries
            self.session.execute("""
            CREATE TABLE IF NOT EXISTS sentiment_by_source (
                source text,
                ticker text,
                timestamp timestamp,
                sentiment text,
                score float,
                confidence float,
                influence_score float,
                source_id text,
                id uuid,
                PRIMARY KEY ((source, ticker), timestamp, id)
            ) WITH CLUSTERING ORDER BY (timestamp DESC, id ASC);
            """)

            # Create market sentiment table for aggregated market sentiment
            self.session.execute("""
            CREATE TABLE IF NOT EXISTS market_sentiment (
                timestamp timestamp,
                market_sentiment_score float,
                market_sentiment_label text,
                confidence float,
                total_data_points int,
                ticker_count int,
                timeframe_hours int,
                id uuid,
                PRIMARY KEY (timestamp, id)
            ) WITH CLUSTERING ORDER BY (id ASC);
            """)

            # Create ticker sentiment table for aggregated ticker sentiment
            self.session.execute("""
            CREATE TABLE IF NOT EXISTS ticker_sentiment (
                ticker text,
                timestamp timestamp,
                sentiment_score float,
                sentiment_label text,
                confidence float,
                data_points int,
                timeframe_hours int,
                id uuid,
                PRIMARY KEY ((ticker), timestamp, id)
            ) WITH CLUSTERING ORDER BY (timestamp DESC, id ASC);
            """)

            # Create sentiment shift table for tracking significant changes
            self.session.execute("""
            CREATE TABLE IF NOT EXISTS sentiment_shifts (
                ticker text,
                timestamp timestamp,
                current_score float,
                previous_score float,
                change float,
                direction text,
                current_data_points int,
                previous_data_points int,
                id uuid,
                PRIMARY KEY ((ticker), timestamp, id)
            ) WITH CLUSTERING ORDER BY (timestamp DESC, id ASC);
            """)

            # Create indexes for common query patterns
            self.session.execute("""
            CREATE INDEX IF NOT EXISTS sentiment_data_sentiment_idx ON sentiment_data (sentiment);
            """)

            self.session.execute("""
            CREATE INDEX IF NOT EXISTS sentiment_shifts_direction_idx ON sentiment_shifts (direction);
            """)

            logger.info("Cassandra schema initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Cassandra schema: {e}", exc_info=True)
            raise

    def save_sentiment(self, sentiment_data: Dict[str, Any]) -> bool:
        """
        Save a single sentiment analysis result.

        Args:
            sentiment_data: Dictionary containing sentiment analysis result

        Returns:
            True if successful, False otherwise
        """
        try:
            ticker = sentiment_data.get("ticker")
            if not ticker:
                logger.warning("Attempted to save sentiment data without a ticker")
                return False

            # Generate a unique ID if one doesn't exist
            if "id" not in sentiment_data:
                sentiment_data["id"] = uuid.uuid4()

            # Ensure timestamp is present
            if "timestamp" not in sentiment_data:
                sentiment_data["timestamp"] = datetime.now()
            elif isinstance(sentiment_data["timestamp"], str):
                # Convert ISO format string to datetime
                sentiment_data["timestamp"] = datetime.fromisoformat(
                    sentiment_data["timestamp"].replace("Z", "+00:00")
                )

            # Extract core fields
            source = sentiment_data.get("source", "unknown")
            source_id = sentiment_data.get("source_id", "")
            sentiment = sentiment_data.get("sentiment", "neutral")
            score = float(sentiment_data.get("score", 0.5))
            confidence = float(sentiment_data.get("confidence", 0.0))
            influence_score = float(sentiment_data.get("influence_score", 1.0))
            model_type = sentiment_data.get("model_type", "unknown")

            # Prepare metadata for other fields
            metadata = {}
            for key, value in sentiment_data.items():
                if key not in [
                    "ticker",
                    "timestamp",
                    "source",
                    "source_id",
                    "sentiment",
                    "score",
                    "confidence",
                    "influence_score",
                    "model_type",
                    "id",
                ]:
                    if isinstance(value, (dict, list)):
                        metadata[key] = json.dumps(value)
                    else:
                        metadata[key] = str(value)

            # Insert into main sentiment data table
            self.session.execute(
                """
                INSERT INTO sentiment_data (
                    ticker, timestamp, source, source_id, sentiment, score, 
                    confidence, influence_score, model_type, metadata, id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    ticker,
                    sentiment_data["timestamp"],
                    source,
                    source_id,
                    sentiment,
                    score,
                    confidence,
                    influence_score,
                    model_type,
                    metadata,
                    sentiment_data["id"],
                ),
            )

            # Insert into sentiment by source table
            self.session.execute(
                """
                INSERT INTO sentiment_by_source (
                    source, ticker, timestamp, sentiment, score, 
                    confidence, influence_score, source_id, id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    source,
                    ticker,
                    sentiment_data["timestamp"],
                    sentiment,
                    score,
                    confidence,
                    influence_score,
                    source_id,
                    sentiment_data["id"],
                ),
            )

            logger.debug(f"Saved sentiment data for ticker {ticker} from {source}")
            return True

        except Exception as e:
            logger.error(f"Failed to save sentiment data: {e}", exc_info=True)
            return False

    def get_ticker_sentiment(
        self, ticker: str, start_time: datetime, limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Get sentiment data for a specific ticker since a start time.

        Args:
            ticker: Stock symbol to get sentiment for
            start_time: Starting timestamp for the query
            limit: Maximum number of records to return

        Returns:
            List of sentiment data dictionaries
        """
        try:
            # Prepare query with parameters
            query = """
            SELECT * FROM sentiment_data 
            WHERE ticker = %s AND timestamp >= %s
            LIMIT %s
            """

            # Execute query
            rows = self.session.execute(query, (ticker, start_time, limit))

            # Convert rows to dictionaries
            results = []
            for row in rows:
                result = {key: getattr(row, key) for key in row._fields}

                # Parse metadata JSON values
                if "metadata" in result and result["metadata"]:
                    for key, value in result["metadata"].items():
                        try:
                            result[key] = json.loads(value)
                        except (json.JSONDecodeError, TypeError):
                            result[key] = value

                results.append(result)

            logger.debug(
                f"Retrieved {len(results)} sentiment records for {ticker} since {start_time}"
            )
            return results

        except Exception as e:
            logger.error(f"Failed to get ticker sentiment: {e}", exc_info=True)
            return []

    def get_ticker_sentiment_for_period(
        self, ticker: str, start_time: datetime, end_time: datetime
    ) -> List[Dict[str, Any]]:
        """
        Get sentiment data for a specific ticker within a time range.

        Args:
            ticker: Stock symbol to get sentiment for
            start_time: Starting timestamp for the query
            end_time: Ending timestamp for the query

        Returns:
            List of sentiment data dictionaries
        """
        try:
            # Prepare query with parameters
            query = """
            SELECT * FROM sentiment_data 
            WHERE ticker = %s AND timestamp >= %s AND timestamp <= %s
            """

            # Execute query
            rows = self.session.execute(query, (ticker, start_time, end_time))

            # Convert rows to dictionaries
            results = []
            for row in rows:
                result = {key: getattr(row, key) for key in row._fields}

                # Parse metadata JSON values
                if "metadata" in result and result["metadata"]:
                    for key, value in result["metadata"].items():
                        try:
                            result[key] = json.loads(value)
                        except (json.JSONDecodeError, TypeError):
                            result[key] = value

                results.append(result)

            logger.debug(
                f"Retrieved {len(results)} sentiment records for {ticker} between {start_time} and {end_time}"
            )
            return results

        except Exception as e:
            logger.error(
                f"Failed to get ticker sentiment for period: {e}", exc_info=True
            )
            return []

    def get_sentiment_by_source(
        self, source: str, ticker: str, start_time: datetime, limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Get sentiment data for a specific source and ticker.

        Args:
            source: Source of the sentiment (twitter, reddit, news)
            ticker: Stock symbol to get sentiment for
            start_time: Starting timestamp for the query
            limit: Maximum number of records to return

        Returns:
            List of sentiment data dictionaries
        """
        try:
            # Prepare query with parameters
            query = """
            SELECT * FROM sentiment_by_source 
            WHERE source = %s AND ticker = %s AND timestamp >= %s
            LIMIT %s
            """

            # Execute query
            rows = self.session.execute(query, (source, ticker, start_time, limit))

            # Convert rows to dictionaries
            results = []
            for row in rows:
                result = {key: getattr(row, key) for key in row._fields}
                results.append(result)

            logger.debug(
                f"Retrieved {len(results)} {source} sentiment records for {ticker}"
            )
            return results

        except Exception as e:
            logger.error(f"Failed to get sentiment by source: {e}", exc_info=True)
            return []

    def save_market_sentiment(self, market_sentiment: Dict[str, Any]) -> bool:
        """
        Save aggregated market sentiment data.

        Args:
            market_sentiment: Dictionary containing market sentiment data

        Returns:
            True if successful, False otherwise
        """
        try:
            # Generate a unique ID if one doesn't exist
            if "id" not in market_sentiment:
                market_sentiment["id"] = uuid.uuid4()

            # Ensure timestamp is present
            if "timestamp" not in market_sentiment:
                market_sentiment["timestamp"] = datetime.now()
            elif isinstance(market_sentiment["timestamp"], str):
                # Convert ISO format string to datetime
                market_sentiment["timestamp"] = datetime.fromisoformat(
                    market_sentiment["timestamp"].replace("Z", "+00:00")
                )

            # Extract core fields
            market_score = float(market_sentiment.get("market_sentiment_score", 0.5))
            market_label = market_sentiment.get("market_sentiment_label", "neutral")
            confidence = float(market_sentiment.get("confidence", 0.0))
            total_data_points = int(market_sentiment.get("total_data_points", 0))
            ticker_count = int(market_sentiment.get("ticker_count", 0))
            timeframe_hours = int(market_sentiment.get("timeframe_hours", 24))

            # Insert into market sentiment table
            self.session.execute(
                """
                INSERT INTO market_sentiment (
                    timestamp, market_sentiment_score, market_sentiment_label, 
                    confidence, total_data_points, ticker_count, timeframe_hours, id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    market_sentiment["timestamp"],
                    market_score,
                    market_label,
                    confidence,
                    total_data_points,
                    ticker_count,
                    timeframe_hours,
                    market_sentiment["id"],
                ),
            )

            # Also save individual ticker sentiments if available
            if "ticker_sentiments" in market_sentiment and isinstance(
                market_sentiment["ticker_sentiments"], dict
            ):
                batch = BatchStatement()
                for ticker, ticker_sentiment in market_sentiment[
                    "ticker_sentiments"
                ].items():
                    ticker_score = float(ticker_sentiment.get("sentiment_score", 0.5))
                    ticker_label = ticker_sentiment.get("sentiment_label", "neutral")
                    ticker_confidence = float(ticker_sentiment.get("confidence", 0.0))
                    ticker_data_points = int(ticker_sentiment.get("data_points", 0))

                    # Insert into ticker sentiment table
                    batch.add(
                        SimpleStatement(
                            """
                            INSERT INTO ticker_sentiment (
                                ticker, timestamp, sentiment_score, sentiment_label, 
                                confidence, data_points, timeframe_hours, id
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """
                        ),
                        (
                            ticker,
                            market_sentiment["timestamp"],
                            ticker_score,
                            ticker_label,
                            ticker_confidence,
                            ticker_data_points,
                            timeframe_hours,
                            uuid.uuid4(),
                        ),
                    )

                if batch:
                    self.session.execute(batch)

            logger.debug(
                f"Saved market sentiment data with score {market_score} ({market_label})"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to save market sentiment: {e}", exc_info=True)
            return False

    def save_ticker_sentiment(self, ticker_sentiment: Dict[str, Any]) -> bool:
        """
        Save aggregated ticker sentiment data.

        Args:
            ticker_sentiment: Dictionary containing ticker sentiment data

        Returns:
            True if successful, False otherwise
        """
        try:
            ticker = ticker_sentiment.get("ticker")
            if not ticker:
                logger.warning("Attempted to save ticker sentiment without a ticker")
                return False

            # Generate a unique ID if one doesn't exist
            if "id" not in ticker_sentiment:
                ticker_sentiment["id"] = uuid.uuid4()

            # Ensure timestamp is present
            if "timestamp" not in ticker_sentiment:
                ticker_sentiment["timestamp"] = datetime.now()
            elif isinstance(ticker_sentiment["timestamp"], str):
                # Convert ISO format string to datetime
                ticker_sentiment["timestamp"] = datetime.fromisoformat(
                    ticker_sentiment["timestamp"].replace("Z", "+00:00")
                )

            # Extract core fields
            sentiment_score = float(ticker_sentiment.get("sentiment_score", 0.5))
            sentiment_label = ticker_sentiment.get("sentiment_label", "neutral")
            confidence = float(ticker_sentiment.get("confidence", 0.0))
            data_points = int(ticker_sentiment.get("data_points", 0))
            timeframe_hours = int(ticker_sentiment.get("timeframe_hours", 24))

            # Insert into ticker sentiment table
            self.session.execute(
                """
                INSERT INTO ticker_sentiment (
                    ticker, timestamp, sentiment_score, sentiment_label, 
                    confidence, data_points, timeframe_hours, id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    ticker,
                    ticker_sentiment["timestamp"],
                    sentiment_score,
                    sentiment_label,
                    confidence,
                    data_points,
                    timeframe_hours,
                    ticker_sentiment["id"],
                ),
            )

            logger.debug(
                f"Saved ticker sentiment for {ticker} with score {sentiment_score} ({sentiment_label})"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to save ticker sentiment: {e}", exc_info=True)
            return False

    def save_sentiment_shift(self, shift_data: Dict[str, Any]) -> bool:
        """
        Save a detected sentiment shift.

        Args:
            shift_data: Dictionary containing shift detection data

        Returns:
            True if successful, False otherwise
        """
        try:
            ticker = shift_data.get("ticker")
            if not ticker:
                logger.warning("Attempted to save sentiment shift without a ticker")
                return False

            # Generate a unique ID if one doesn't exist
            if "id" not in shift_data:
                shift_data["id"] = uuid.uuid4()

            # Ensure timestamp is present
            if "timestamp" not in shift_data:
                shift_data["timestamp"] = datetime.now()
            elif isinstance(shift_data["timestamp"], str):
                # Convert ISO format string to datetime
                shift_data["timestamp"] = datetime.fromisoformat(
                    shift_data["timestamp"].replace("Z", "+00:00")
                )

            # Extract core fields
            current_score = float(shift_data.get("current_score", 0.5))
            previous_score = float(shift_data.get("previous_score", 0.5))
            change = float(shift_data.get("change", 0.0))
            direction = shift_data.get("direction", "neutral")
            current_data_points = int(shift_data.get("current_data_points", 0))
            previous_data_points = int(shift_data.get("previous_data_points", 0))

            # Insert into sentiment shifts table
            self.session.execute(
                """
                INSERT INTO sentiment_shifts (
                    ticker, timestamp, current_score, previous_score, 
                    change, direction, current_data_points, previous_data_points, id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    ticker,
                    shift_data["timestamp"],
                    current_score,
                    previous_score,
                    change,
                    direction,
                    current_data_points,
                    previous_data_points,
                    shift_data["id"],
                ),
            )

            logger.debug(
                f"Saved sentiment shift for {ticker} with change {change} ({direction})"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to save sentiment shift: {e}", exc_info=True)
            return False

    def get_recent_market_sentiment(
        self, timeframe_hours: int = 24
    ) -> Optional[Dict[str, Any]]:
        """
        Get the most recent market sentiment data.

        Args:
            timeframe_hours: Filter for a specific timeframe (hours)

        Returns:
            Market sentiment data dictionary or None if not found
        """
        try:
            # Prepare query with parameters
            query = """
            SELECT * FROM market_sentiment 
            WHERE timeframe_hours = %s 
            ORDER BY timestamp DESC
            LIMIT 1
            """

            # Execute query
            rows = self.session.execute(query, (timeframe_hours,))

            # Get the first row (most recent)
            for row in rows:
                result = {key: getattr(row, key) for key in row._fields}
                logger.debug(
                    f"Retrieved recent market sentiment with score {result.get('market_sentiment_score')}"
                )
                return result

            logger.debug(
                f"No recent market sentiment found for timeframe {timeframe_hours}"
            )
            return None

        except Exception as e:
            logger.error(f"Failed to get recent market sentiment: {e}", exc_info=True)
            return None

    def get_recent_sentiment_shifts(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get the most recent significant sentiment shifts.

        Args:
            limit: Maximum number of shifts to return

        Returns:
            List of sentiment shift dictionaries
        """
        try:
            # Prepare query to get the most recent shifts across all tickers
            query = """
            SELECT * FROM sentiment_shifts 
            ORDER BY timestamp DESC
            LIMIT %s
            """

            # Execute query
            rows = self.session.execute(query, (limit,))

            # Convert rows to dictionaries
            results = []
            for row in rows:
                result = {key: getattr(row, key) for key in row._fields}
                results.append(result)

            logger.debug(f"Retrieved {len(results)} recent sentiment shifts")
            return results

        except Exception as e:
            logger.error(f"Failed to get recent sentiment shifts: {e}", exc_info=True)
            return []

    def close(self):
        """Close database connection."""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Closed Cassandra connection")
