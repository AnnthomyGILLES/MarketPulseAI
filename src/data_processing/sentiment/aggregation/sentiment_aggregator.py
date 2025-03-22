# src/data_processing/sentiment/aggregation/sentiment_aggregator.py

import logging
import numpy as np
from typing import Dict, List, Optional, Union
from datetime import datetime, timedelta
from collections import defaultdict

from src.utils.metrics import timer
from src.storage.database.sentiment_store import SentimentStore
from src.utils.config import AggregationConfig

logger = logging.getLogger(__name__)


class SentimentAggregator:
    """
    Aggregates sentiment analysis results from multiple sources
    to provide unified market sentiment scores.

    This class:
    1. Combines sentiment from different sources (Twitter, Reddit, news)
    2. Applies time decay to prioritize recent sentiment
    3. Tracks sentiment trends over time
    4. Detects significant sentiment shifts
    """

    def __init__(self, config: AggregationConfig, sentiment_store: SentimentStore):
        """
        Initialize the sentiment aggregator with configuration.

        Args:
            config: Configuration for sentiment aggregation parameters
            sentiment_store: Database interface for storing/retrieving sentiment data
        """
        self.config = config
        self.sentiment_store = sentiment_store

        # Initialize source weights with defaults and normalize
        self._initialize_source_weights()

        # Extract time decay settings
        self.time_decay_factor = config.time_decay_factor
        self.max_age_hours = config.max_age_hours

        logger.info(
            f"SentimentAggregator initialized with source weights: {self.source_weights}"
        )

    def _initialize_source_weights(self):
        """Initialize and normalize source weights from configuration."""
        # Get source weights with defaults
        self.source_weights = {
            "twitter": self.config.source_weights.get("twitter", 0.3),
            "reddit": self.config.source_weights.get("reddit", 0.3),
            "news": self.config.source_weights.get("news", 0.4),
            "other": self.config.source_weights.get("other", 0.1),
        }

        # Normalize weights to ensure they sum to 1.0
        weight_sum = sum(self.source_weights.values())
        if weight_sum > 0:
            self.source_weights = {
                k: v / weight_sum for k, v in self.source_weights.items()
            }

    @timer
    def aggregate_ticker_sentiment(
        self, ticker: str, timeframe_hours: int = 24
    ) -> Dict[str, Union[float, Dict]]:
        """
        Aggregate sentiment for a specific ticker over the specified timeframe.

        Args:
            ticker: Stock symbol to aggregate sentiment for
            timeframe_hours: Number of hours to look back for sentiment data

        Returns:
            Dictionary with aggregated sentiment score and metadata
        """
        # Calculate start time for the query
        start_time = datetime.now() - timedelta(hours=timeframe_hours)

        # Retrieve sentiment data from the database
        sentiment_data = self.sentiment_store.get_ticker_sentiment(
            ticker, start_time, limit=self.config.max_records
        )

        if not sentiment_data:
            logger.warning(
                f"No sentiment data found for ticker {ticker} in the last {timeframe_hours} hours"
            )
            return self._create_empty_sentiment_response(ticker, timeframe_hours)

        # Group and process sentiment data by source
        source_sentiment = self._group_by_source(sentiment_data)
        source_scores, aggregated_score, total_weight, total_data_points = (
            self._compute_source_scores(source_sentiment)
        )

        # Determine sentiment label based on score
        sentiment_label = self._score_to_label(aggregated_score)

        # Calculate overall confidence based on number of data points
        confidence_factor = min(
            1.0, total_data_points / self.config.confidence_data_points
        )

        return {
            "ticker": ticker,
            "sentiment_score": float(aggregated_score),
            "sentiment_label": sentiment_label,
            "confidence": float(confidence_factor),
            "data_points": total_data_points,
            "sources": source_scores,
            "timeframe_hours": timeframe_hours,
            "timestamp": datetime.now().isoformat(),
        }

    def _create_empty_sentiment_response(
        self, ticker: str, timeframe_hours: int
    ) -> Dict:
        """Create a default response for when no sentiment data is available."""
        return {
            "ticker": ticker,
            "sentiment_score": 0.5,  # Neutral score
            "sentiment_label": "neutral",
            "confidence": 0.0,
            "data_points": 0,
            "sources": {},
            "timeframe_hours": timeframe_hours,
            "timestamp": datetime.now().isoformat(),
        }

    def _group_by_source(self, sentiment_data: List[Dict]) -> Dict[str, List]:
        """Group sentiment data by source."""
        source_sentiment = defaultdict(list)
        for item in sentiment_data:
            source = item.get("source", "unknown")
            source_sentiment[source].append(item)
        return source_sentiment

    def _compute_source_scores(self, source_sentiment: Dict[str, List]) -> tuple:
        """
        Compute aggregated scores for each source and overall.

        Args:
            source_sentiment: Dictionary of sentiment data grouped by source

        Returns:
            Tuple of (source_scores, total_weighted_score, total_weight, total_data_points)
        """
        source_scores = {}
        total_weighted_score = 0.0
        total_weight = 0.0
        total_data_points = 0

        current_time = datetime.now()

        for source, items in source_sentiment.items():
            # Skip sources with no data
            if not items:
                continue

            source_weight = self.source_weights.get(
                source, self.source_weights.get("other", 0.1)
            )
            source_result = self._process_source_items(items, current_time)

            if source_result["total_time_weight"] > 0:
                source_avg_score = (
                    source_result["total_weighted_score"]
                    / source_result["total_time_weight"]
                )
                source_avg_confidence = sum(source_result["confidences"]) / len(
                    source_result["confidences"]
                )
            else:
                source_avg_score = 0.5  # Neutral if all items too old
                source_avg_confidence = 0.0

            # Store source statistics
            source_scores[source] = {
                "score": float(source_avg_score),
                "confidence": float(source_avg_confidence),
                "data_points": len(items),
            }

            # Add to total weighted score
            source_contribution = (
                source_avg_score * source_weight * source_avg_confidence
            )
            total_weighted_score += source_contribution
            total_weight += source_weight * source_avg_confidence
            total_data_points += len(items)

        # Calculate final aggregated score
        if total_weight > 0:
            aggregated_score = total_weighted_score / total_weight
        else:
            aggregated_score = 0.5  # Neutral if no confident data

        return source_scores, aggregated_score, total_weight, total_data_points

    def _process_source_items(self, items: List[Dict], current_time: datetime) -> Dict:
        """
        Process items from a single source with time decay.

        Args:
            items: List of sentiment items from a source
            current_time: Current time for time decay calculation

        Returns:
            Dictionary with processed results
        """
        result = {
            "weighted_scores": [],
            "confidences": [],
            "time_weights": [],
            "total_weighted_score": 0.0,
            "total_time_weight": 0.0,
        }

        for item in items:
            # Calculate time decay weight
            try:
                item_time = datetime.fromisoformat(item.get("timestamp"))
            except (ValueError, TypeError):
                # Use current time if timestamp is invalid
                item_time = current_time

            # Calculate hours since the sentiment was recorded
            hours_ago = (current_time - item_time).total_seconds() / 3600

            # Apply exponential time decay
            time_weight = (
                np.exp(-self.time_decay_factor * hours_ago)
                if hours_ago <= self.max_age_hours
                else 0
            )

            # Apply influence score if available
            influence = item.get("influence_score", 1.0)
            effective_weight = time_weight * influence

            # Get sentiment score and confidence
            score = item.get("score", 0.5)
            confidence = item.get("confidence", 0.5)

            weighted_score = score * confidence * effective_weight

            result["weighted_scores"].append(weighted_score)
            result["confidences"].append(confidence)
            result["time_weights"].append(effective_weight)

            result["total_weighted_score"] += weighted_score
            result["total_time_weight"] += effective_weight

        return result

    @timer
    def aggregate_market_sentiment(
        self, tickers: Optional[List[str]] = None, timeframe_hours: int = 24
    ) -> Dict[str, Union[float, Dict]]:
        """
        Aggregate overall market sentiment across multiple tickers.

        Args:
            tickers: Optional list of tickers to include (default: top tickers by volume)
            timeframe_hours: Number of hours to look back for sentiment data

        Returns:
            Dictionary with market sentiment score and per-ticker breakdown
        """
        # If no tickers provided, get top tickers by trading volume
        if not tickers:
            tickers = self._get_top_tickers(self.config.default_ticker_count)

        # Calculate sentiment for each ticker
        ticker_sentiments = {}
        for ticker in tickers:
            ticker_sentiments[ticker] = self.aggregate_ticker_sentiment(
                ticker, timeframe_hours
            )

        # Calculate market-wide sentiment using ticker weights
        market_sentiment = self._calculate_market_sentiment(
            ticker_sentiments, tickers, timeframe_hours
        )

        return market_sentiment

    def _calculate_market_sentiment(
        self, ticker_sentiments: Dict, tickers: List[str], timeframe_hours: int
    ) -> Dict:
        """Calculate overall market sentiment from individual ticker sentiments."""
        # Get ticker weights based on market cap, volume, etc.
        ticker_weights = self._calculate_ticker_weights(tickers)

        weighted_scores = []
        weights = []

        for ticker, sentiment in ticker_sentiments.items():
            score = sentiment.get("sentiment_score", 0.5)
            confidence = sentiment.get("confidence", 0.0)
            ticker_weight = ticker_weights.get(ticker, 1.0)
            data_points = sentiment.get("data_points", 0)

            # Skip tickers with no data
            if data_points == 0:
                continue

            # Weight by ticker importance, confidence, and data points
            combined_weight = ticker_weight * confidence
            weighted_scores.append(score * combined_weight)
            weights.append(combined_weight)

        # Calculate weighted average market sentiment
        if sum(weights) > 0:
            market_score = sum(weighted_scores) / sum(weights)
        else:
            market_score = 0.5  # Neutral if no data

        # Determine market sentiment label
        market_label = self._score_to_label(market_score)

        # Confidence based on data coverage and recency
        total_data_points = sum(
            s.get("data_points", 0) for s in ticker_sentiments.values()
        )
        expected_data_points = self.config.confidence_data_points * len(tickers)
        confidence_factor = min(
            1.0,
            total_data_points
            / (expected_data_points if expected_data_points > 0 else 1),
        )

        return {
            "market_sentiment_score": float(market_score),
            "market_sentiment_label": market_label,
            "confidence": float(confidence_factor),
            "total_data_points": total_data_points,
            "ticker_count": len(tickers),
            "ticker_sentiments": ticker_sentiments,
            "timeframe_hours": timeframe_hours,
            "timestamp": datetime.now().isoformat(),
        }

    def track_sentiment_trends(
        self, ticker: str, timeframes: List[int] = [24, 72, 168]
    ) -> Dict[str, Union[Dict, List]]:
        """
        Track sentiment trends for a ticker across multiple timeframes.

        Args:
            ticker: Stock symbol to track trends for
            timeframes: List of timeframes in hours to analyze

        Returns:
            Dictionary with sentiment trends and change metrics
        """
        trend_data = {
            "ticker": ticker,
            "timestamp": datetime.now().isoformat(),
            "timeframes": {},
            "changes": {},
        }

        # Get sentiment for each timeframe
        previous_sentiment = None
        previous_timeframe = None

        for timeframe in sorted(timeframes):
            sentiment = self.aggregate_ticker_sentiment(ticker, timeframe)
            trend_data["timeframes"][f"{timeframe}h"] = sentiment

            # Calculate change from previous timeframe
            if previous_sentiment:
                score_change = (
                    sentiment["sentiment_score"] - previous_sentiment["sentiment_score"]
                )
                confidence_change = (
                    sentiment["confidence"] - previous_sentiment["confidence"]
                )

                trend_data["changes"][f"{previous_timeframe}h_to_{timeframe}h"] = {
                    "score_change": float(score_change),
                    "confidence_change": float(confidence_change),
                    "direction": "improving"
                    if score_change > 0.05
                    else "worsening"
                    if score_change < -0.05
                    else "stable",
                }

            previous_sentiment = sentiment
            previous_timeframe = timeframe

        # Calculate sentiment momentum (rate of change)
        if len(timeframes) >= 2:
            shortest = min(timeframes)
            longest = max(timeframes)

            short_score = trend_data["timeframes"][f"{shortest}h"]["sentiment_score"]
            long_score = trend_data["timeframes"][f"{longest}h"]["sentiment_score"]

            # Normalize by time difference to get rate of change per hour
            time_diff = longest - shortest
            if time_diff > 0:
                momentum = (short_score - long_score) / time_diff
            else:
                momentum = 0

            trend_data["momentum"] = float(momentum)
            trend_data["momentum_direction"] = (
                "improving"
                if momentum > 0.001
                else "worsening"
                if momentum < -0.001
                else "stable"
            )

        return trend_data

    def detect_sentiment_shifts(
        self, tickers: List[str], threshold: float = 0.15, timeframe_hours: int = 24
    ) -> List[Dict]:
        """
        Detect significant sentiment shifts for the given tickers.

        Args:
            tickers: List of stock symbols to monitor
            threshold: Minimum change in sentiment score to be considered significant
            timeframe_hours: Number of hours to look back for sentiment data

        Returns:
            List of tickers with significant sentiment shifts
        """
        # Calculate periods for comparison
        current_period_end = datetime.now()
        current_period_start = current_period_end - timedelta(hours=timeframe_hours)
        previous_period_end = current_period_start
        previous_period_start = previous_period_end - timedelta(hours=timeframe_hours)

        shifts = []

        for ticker in tickers:
            # Get current period sentiment
            current_sentiment = self.sentiment_store.get_ticker_sentiment_for_period(
                ticker, current_period_start, current_period_end
            )

            # Get previous period sentiment
            previous_sentiment = self.sentiment_store.get_ticker_sentiment_for_period(
                ticker, previous_period_start, previous_period_end
            )

            # Skip if not enough data
            if not current_sentiment or not previous_sentiment:
                continue

            # Calculate sentiment scores for both periods
            current_score = self._calculate_period_sentiment(current_sentiment)
            previous_score = self._calculate_period_sentiment(previous_sentiment)

            # Calculate absolute change
            change = current_score - previous_score

            # Check if change exceeds threshold
            if abs(change) >= threshold:
                shifts.append(
                    {
                        "ticker": ticker,
                        "current_score": float(current_score),
                        "previous_score": float(previous_score),
                        "change": float(change),
                        "direction": "positive" if change > 0 else "negative",
                        "timestamp": datetime.now().isoformat(),
                        "current_data_points": len(current_sentiment),
                        "previous_data_points": len(previous_sentiment),
                    }
                )

        # Sort by magnitude of change
        shifts.sort(key=lambda x: abs(x["change"]), reverse=True)

        return shifts

    def _score_to_label(self, score: float) -> str:
        """Convert a sentiment score to a text label."""
        if score >= 0.6:
            return "positive"
        elif score <= 0.4:
            return "negative"
        else:
            return "neutral"

    def _get_top_tickers(self, count: int = 10) -> List[str]:
        """
        Get the top tickers by trading volume.

        In a real implementation, this would query a market data service.
        For simplicity, this returns a static list of major tickers.
        """
        # In production, this would query market data or a configuration
        default_tickers = [
            "AAPL",
            "MSFT",
            "AMZN",
            "GOOGL",
            "META",
            "TSLA",
            "NVDA",
            "JPM",
            "V",
            "PG",
            "BRK.B",
            "JNJ",
            "UNH",
            "HD",
            "XOM",
            "BAC",
            "PFE",
            "CSCO",
            "VZ",
            "DIS",
        ]
        return default_tickers[:count]

    def _calculate_ticker_weights(self, tickers: List[str]) -> Dict[str, float]:
        """
        Calculate weights for tickers based on market cap, trading volume, or other metrics.

        In a real implementation, this would query market data and calculate
        appropriate weights. For simplicity, this uses a uniform distribution.
        """
        # In production, this would incorporate market cap, trading volume, etc.
        weight = 1.0 / len(tickers) if tickers else 0
        return {ticker: weight for ticker in tickers}

    def _calculate_period_sentiment(self, sentiment_data: List[Dict]) -> float:
        """
        Calculate the average sentiment score for a period, considering confidence.

        Args:
            sentiment_data: List of sentiment items for the period

        Returns:
            Average sentiment score weighted by confidence
        """
        if not sentiment_data:
            return 0.5  # Neutral score for no data

        total_weighted_score = 0.0
        total_weight = 0.0

        for item in sentiment_data:
            score = item.get("score", 0.5)
            confidence = item.get("confidence", 0.1)
            influence = item.get("influence_score", 1.0)

            weight = confidence * influence
            total_weighted_score += score * weight
            total_weight += weight

        if total_weight > 0:
            return total_weighted_score / total_weight
        else:
            return 0.5  # Neutral score if no confident data
