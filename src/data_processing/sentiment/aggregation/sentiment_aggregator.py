# src/data_processing/sentiment/aggregation/sentiment_aggregator.py

import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple, Union
from datetime import datetime, timedelta
import time
from collections import defaultdict

from src.utils.metrics import timer
from src.storage.database.sentiment_store import SentimentStore
from src.utils.config import AggregationConfig

logger = logging.getLogger(__name__)

class SentimentAggregator:
    """
    Aggregates sentiment analysis results from multiple sources
    to provide unified market sentiment scores.
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

        # Configure source weights (importance of different sources)
        self.source_weights = {
            "twitter": config.source_weights.get("twitter", 0.3),
            "reddit": config.source_weights.get("reddit", 0.3),
            "news": config.source_weights.get("news", 0.4),
            # Add other sources as needed
        }

        # Normalize weights to ensure they sum to 1.0
        weight_sum = sum(self.source_weights.values())
        if weight_sum > 0:
            self.source_weights = {k: v/weight_sum for k, v in self.source_weights.items()}

        # Time decay parameters for weighting recent vs older sentiment
        self.time_decay_factor = config.time_decay_factor
        self.max_age_hours = config.max_age_hours

        logger.info("SentimentAggregator initialized with source weights: %s", self.source_weights)

    @timer
    def aggregate_ticker_sentiment(self, ticker: str,
                                   timeframe_hours: int = 24) -> Dict[str, Union[float, Dict]]:
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
            logger.warning(f"No sentiment data found for ticker {ticker} in the last {timeframe_hours} hours")
            return {
                "ticker": ticker,
                "sentiment_score": 0.5,  # Neutral score
                "sentiment_label": "neutral",
                "confidence": 0.0,
                "data_points": 0,
                "sources": {},
                "timeframe_hours": timeframe_hours,
                "timestamp": datetime.now().isoformat()
            }

        # Group sentiment data by source
        source_sentiment = defaultdict(list)
        for item in sentiment_data:
            source = item.get("source", "unknown")
            source_sentiment[source].append(item)

        # Calculate aggregated sentiment by source with time decay
        source_scores = {}
        total_weighted_score = 0.0
        total_weight = 0.0
        total_data_points = 0

        current_time = datetime.now()

        for source, items in source_sentiment.items():
            # Skip sources with no data
            if not items:
                continue

            source_weight = self.source_weights.get(source, 0.1)
            weighted_scores = []
            confidences = []
            time_weights = []

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
                time_weight = np.exp(-self.time_decay_factor * hours_ago) if hours_ago <= self.max_age_hours else 0

                # Get sentiment score and confidence
                score = item.get("score", 0.5)
                confidence = item.get("confidence", 0.5)

                weighted_scores.append(score * confidence * time_weight)
                confidences.append(confidence)
                time_weights.append(time_weight)

            # Calculate weighted average for the source
            if sum(time_weights) > 0:
                source_avg_score = sum(weighted_scores) / sum(time_weights)
                source_avg_confidence = sum(confidences) / len(confidences)
            else:
                source_avg_score = 0.5  # Neutral if all items too old
                source_avg_confidence = 0.0

            # Store source statistics
            source_scores[source] = {
                "score": float(source_avg_score),
                "confidence": float(source_avg_confidence),
                "data_points": len(items)
            }

            # Add to total weighted score
            source_contribution = source_avg_score * source_weight * source_avg_confidence
            total_weighted_score += source_contribution
            total_weight += source_weight * source_avg_confidence
            total_data_points += len(items)

        # Calculate final aggregated score
        if total_weight > 0:
            aggregated_score = total_weighted_score / total_weight
        else:
            aggregated_score = 0.5  # Neutral if no confident data

        # Determine sentiment label
        sentiment_label = self._score_to_label(aggregated_score)

        # Calculate overall confidence based on number of data points and their recency
        confidence_factor = min(1.0, total_data_points / self.config.confidence_data_points)

        return {
            "ticker": ticker,
            "sentiment_score": float(aggregated_score),
            "sentiment_label": sentiment_label,
            "confidence": float(confidence_factor),
            "data_points": total_data_points,
            "sources": source_scores,
            "timeframe_hours": timeframe_hours,
            "timestamp": datetime.now().isoformat()
        }

    @timer
    def aggregate_market_sentiment(self, tickers: Optional[List[str]] = None,
                                   timeframe_hours: int = 24) -> Dict[str, Union[float, Dict]]:
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

        # Calculate market-wide sentiment
        ticker_weights = self._calculate_ticker_weights(tickers)

        weighted_scores = []
        weights = []

        for ticker, sentiment in ticker_sentiments.items():
            score = sentiment.get("sentiment_score", 0.5)
            confidence = sentiment.get("confidence", 0.0)
            ticker_weight = ticker_weights.get(ticker, 1.0)

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
        total_data_points = sum(s.get("data_points", 0) for s in ticker_sentiments.values())
        confidence_factor = min(1.0, total_data_points / (self.config.confidence_data_points * len(tickers)))

        return {
            "market_sentiment_score": float(market_score),
            "market_sentiment_label": market_label,
            "confidence": float(confidence_factor),
            "total_data_points": total_data_points,
            "ticker_count": len(tickers),
            "ticker_sentiments": ticker_sentiments,
            "timeframe_hours": timeframe_hours,
            "timestamp": datetime.now().isoformat()
        }

    def track_sentiment_trends(self, ticker: str,
                               timeframes: List[int] = [24, 72, 168]) -> Dict[str, Union[Dict, List]]:
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
            "changes": {}
        }

        # Get sentiment for each timeframe
        previous_sentiment = None
        previous_timeframe = None

        for timeframe in sorted(timeframes):
            sentiment = self.aggregate_ticker_sentiment(ticker, timeframe)
            trend_data["timeframes"][f"{timeframe}h"] = sentiment

            # Calculate change from previous timeframe
            if previous_sentiment:
                change = sentiment["sentiment_score"] -