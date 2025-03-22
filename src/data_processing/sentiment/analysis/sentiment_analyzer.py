# src/data_processing/sentiment/analysis/sentiment_analyzer.py

import logging
from typing import Dict, List, Optional, Union
import numpy as np
from transformers import AutoModelForSequenceClassification, AutoTokenizer
import torch
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from src.utils.config import SentimentConfig
from src.utils.metrics import timer
from src.storage.cache.redis_client import RedisClient
from src.data_processing.sentiment.preprocessing.text_preprocessor import (
    TextPreprocessor,
)

logger = logging.getLogger(__name__)


class SentimentAnalyzer:
    """
    Analyzes text content to determine sentiment using transformer models.

    This class handles the core sentiment analysis functionality, supporting
    both financial-specific and general sentiment analysis models.
    """

    def __init__(
        self, config: SentimentConfig, redis_client: Optional[RedisClient] = None
    ):
        """
        Initialize the SentimentAnalyzer with configuration.

        Args:
            config: Configuration for sentiment analysis models and parameters
            redis_client: Optional Redis client for caching results
        """
        self.config = config
        self.redis_client = redis_client
        self.preprocessor = TextPreprocessor(config.preprocessing)

        # Load the models
        logger.info("Loading sentiment analysis models...")
        self._load_models()

        # Configure multi-threading for batch processing
        self.executor = ThreadPoolExecutor(max_workers=config.max_workers)
        logger.info("SentimentAnalyzer initialized successfully")

    @timer
    def _load_models(self) -> None:
        """Load and prepare transformer models for inference."""
        # Financial sentiment model (FinBERT or similar)
        self.financial_model = AutoModelForSequenceClassification.from_pretrained(
            self.config.financial_model_path
        )
        self.financial_tokenizer = AutoTokenizer.from_pretrained(
            self.config.financial_model_path
        )

        # General sentiment model (RoBERTa or similar)
        self.general_model = AutoModelForSequenceClassification.from_pretrained(
            self.config.general_model_path
        )
        self.general_tokenizer = AutoTokenizer.from_pretrained(
            self.config.general_model_path
        )

        # Move models to GPU if available
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.financial_model.to(self.device)
        self.general_model.to(self.device)

        # Set models to evaluation mode
        self.financial_model.eval()
        self.general_model.eval()

    def analyze_text(
        self, text: str, ticker: Optional[str] = None, model_type: str = "financial"
    ) -> Dict[str, Union[float, str]]:
        """
        Analyze a single text item for sentiment.

        Args:
            text: The text content to analyze
            ticker: Optional stock ticker symbol related to the text
            model_type: Which model to use ("financial", "general", or "ensemble")

        Returns:
            Dictionary containing sentiment scores and metadata
        """
        # Check cache if Redis client is available and caching is enabled
        if self.redis_client and self.config.enable_caching:
            cache_key = f"sentiment:{model_type}:{hash(text)}"
            cached_result = self.redis_client.get(cache_key)
            if cached_result:
                logger.debug(f"Cache hit for text: {text[:30]}...")
                return cached_result

        # Preprocess the text
        cleaned_text = self.preprocessor.preprocess(text)
        if not cleaned_text:
            logger.warning(f"Text was empty after preprocessing: {text[:50]}...")
            return {
                "sentiment": "neutral",
                "score": 0.5,
                "confidence": 0.0,
                "timestamp": datetime.now().isoformat(),
                "model_type": model_type,
                "ticker": ticker,
            }

        # Choose the model based on type
        if model_type == "financial":
            sentiment_result = self._analyze_with_financial_model(cleaned_text)
        elif model_type == "general":
            sentiment_result = self._analyze_with_general_model(cleaned_text)
        elif model_type == "ensemble":
            sentiment_result = self._analyze_with_ensemble(cleaned_text)
        else:
            logger.error(f"Unknown model type: {model_type}, using financial model")
            sentiment_result = self._analyze_with_financial_model(cleaned_text)

        # Add metadata
        result = {
            **sentiment_result,
            "timestamp": datetime.now().isoformat(),
            "model_type": model_type,
            "ticker": ticker,
        }

        # Cache result if Redis client is available
        if self.redis_client and self.config.enable_caching:
            self.redis_client.set(cache_key, result, expire=self.config.cache_ttl)

        return result

    @timer
    def analyze_batch(
        self, texts: List[Dict[str, str]], model_type: str = "financial"
    ) -> List[Dict[str, Union[float, str]]]:
        """
        Analyze a batch of text items in parallel.

        Args:
            texts: List of dictionaries containing text and metadata
            model_type: Which model to use

        Returns:
            List of dictionaries with sentiment analysis results
        """
        # Process in parallel using ThreadPoolExecutor
        futures = []
        for item in texts:
            text = item.get("text", "")
            ticker = item.get("ticker")
            futures.append(
                self.executor.submit(self.analyze_text, text, ticker, model_type)
            )

        # Collect results
        results = []
        for future in futures:
            try:
                results.append(future.result())
            except Exception as e:
                logger.error(f"Error in sentiment analysis batch processing: {e}")

        return results

    def _analyze_with_financial_model(self, text: str) -> Dict[str, Union[float, str]]:
        """Analyze text using the financial sentiment model."""
        with torch.no_grad():
            inputs = self.financial_tokenizer(
                text,
                return_tensors="pt",
                truncation=True,
                max_length=self.config.max_sequence_length,
                padding=True,
            ).to(self.device)

            outputs = self.financial_model(**inputs)
            scores = torch.nn.functional.softmax(outputs.logits, dim=1).cpu().numpy()[0]

        # Financial models typically have classes like [negative, neutral, positive]
        sentiment_label = ["negative", "neutral", "positive"][np.argmax(scores)]
        confidence = float(np.max(scores))

        # Normalize to a single score between 0 and 1 (0 = negative, 1 = positive)
        # This assumes indices are [negative, neutral, positive]
        normalized_score = float(scores[2] - scores[0])  # positive - negative
        # Scale from [-1, 1] to [0, 1]
        sentiment_score = (normalized_score + 1) / 2

        return {
            "sentiment": sentiment_label,
            "score": sentiment_score,
            "confidence": confidence,
            "scores": {
                "negative": float(scores[0]),
                "neutral": float(scores[1]),
                "positive": float(scores[2]),
            },
        }

    def _analyze_with_general_model(self, text: str) -> Dict[str, Union[float, str]]:
        """Analyze text using the general sentiment model."""
        with torch.no_grad():
            inputs = self.general_tokenizer(
                text,
                return_tensors="pt",
                truncation=True,
                max_length=self.config.max_sequence_length,
                padding=True,
            ).to(self.device)

            outputs = self.general_model(**inputs)
            scores = torch.nn.functional.softmax(outputs.logits, dim=1).cpu().numpy()[0]

        # General models might have different class structures
        # Assuming RoBERTa with [negative, positive]
        if len(scores) == 2:
            sentiment_label = "negative" if scores[0] > scores[1] else "positive"
            confidence = float(np.max(scores))
            sentiment_score = float(scores[1])  # Positive score

            return {
                "sentiment": sentiment_label,
                "score": sentiment_score,
                "confidence": confidence,
                "scores": {"negative": float(scores[0]), "positive": float(scores[1])},
            }

        # If it's a 3-class model like [negative, neutral, positive]
        else:
            sentiment_label = ["negative", "neutral", "positive"][np.argmax(scores)]
            confidence = float(np.max(scores))
            normalized_score = float(scores[2] - scores[0])  # positive - negative
            sentiment_score = (normalized_score + 1) / 2

            return {
                "sentiment": sentiment_label,
                "score": sentiment_score,
                "confidence": confidence,
                "scores": {
                    "negative": float(scores[0]),
                    "neutral": float(scores[1]),
                    "positive": float(scores[2]),
                },
            }

    def _analyze_with_ensemble(self, text: str) -> Dict[str, Union[float, str]]:
        """
        Analyze text using both models and combine the results.
        This ensemble approach can improve accuracy by leveraging both
        financial-specific and general sentiment understanding.
        """
        financial_result = self._analyze_with_financial_model(text)
        general_result = self._analyze_with_general_model(text)

        # Weighted ensemble based on confidence
        fin_weight = self.config.financial_weight * financial_result["confidence"]
        gen_weight = (1 - self.config.financial_weight) * general_result["confidence"]

        # Normalize weights
        total_weight = fin_weight + gen_weight
        if total_weight > 0:
            fin_weight /= total_weight
            gen_weight /= total_weight
        else:
            fin_weight = gen_weight = 0.5

        # Combine scores
        ensemble_score = (
            fin_weight * financial_result["score"]
            + gen_weight * general_result["score"]
        )

        # Determine ensemble sentiment label
        if ensemble_score > 0.6:
            sentiment_label = "positive"
        elif ensemble_score < 0.4:
            sentiment_label = "negative"
        else:
            sentiment_label = "neutral"

        # Confidence is the weighted average of individual confidences
        ensemble_confidence = (
            fin_weight * financial_result["confidence"]
            + gen_weight * general_result["confidence"]
        )

        return {
            "sentiment": sentiment_label,
            "score": float(ensemble_score),
            "confidence": float(ensemble_confidence),
            "model_weights": {
                "financial": float(fin_weight),
                "general": float(gen_weight),
            },
            "component_scores": {
                "financial": financial_result["score"],
                "general": general_result["score"],
            },
        }
