# src/data_processing/sentiment/analysis/sentiment_analyzer.py

import logging
from typing import Dict, List, Optional, Union
import numpy as np
from transformers import AutoModelForSequenceClassification, AutoTokenizer
import torch
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from src.utils.config import SentimentConfig
from src.utils.metrics import timer
from src.storage.cache.redis_client import RedisClient

from src.data_processing.sentiment.preprocessing.text_processor import TextPreprocessor

logger = logging.getLogger(__name__)


class SentimentAnalyzer:
    """
    Analyzes text content to determine financial sentiment using FinBERT and VADER.

    This class handles the core sentiment analysis functionality, supporting
    both FinBERT (transformer-based) and VADER (lexicon-based) approaches
    for financial text sentiment analysis.
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
        """Load and prepare sentiment analysis models for inference."""
        # Initialize model based on configuration
        self.model_type = self.config.model_type.lower()

        if self.model_type == "finbert":
            # Load FinBERT model - specialized for financial text
            self.finbert_model = AutoModelForSequenceClassification.from_pretrained(
                self.config.finbert_model_path
            )
            self.finbert_tokenizer = AutoTokenizer.from_pretrained(
                self.config.finbert_model_path
            )

            # Move model to GPU if available
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            self.finbert_model.to(self.device)

            # Set model to evaluation mode
            self.finbert_model.eval()
            logger.info(f"FinBERT model loaded on {self.device}")

        elif self.model_type == "vader":
            # Initialize NLTK's VADER sentiment analyzer
            try:
                import nltk

                nltk.data.find("sentiment/vader_lexicon.zip")
            except LookupError:
                logger.info("Downloading VADER lexicon...")
                nltk.download("vader_lexicon")

            self.vader_analyzer = SentimentIntensityAnalyzer()
            logger.info("VADER sentiment analyzer initialized")

            # Load finance-specific lexicon additions if available
            if (
                hasattr(self.config, "finance_lexicon_path")
                and self.config.finance_lexicon_path
            ):
                self._load_finance_lexicon(self.config.finance_lexicon_path)

        elif self.model_type == "ensemble":
            # Load both models for ensemble approach
            # FinBERT
            self.finbert_model = AutoModelForSequenceClassification.from_pretrained(
                self.config.finbert_model_path
            )
            self.finbert_tokenizer = AutoTokenizer.from_pretrained(
                self.config.finbert_model_path
            )

            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            self.finbert_model.to(self.device)
            self.finbert_model.eval()

            # VADER
            try:
                import nltk

                nltk.data.find("sentiment/vader_lexicon.zip")
            except LookupError:
                nltk.download("vader_lexicon")

            self.vader_analyzer = SentimentIntensityAnalyzer()

            # Load finance-specific lexicon additions if available
            if (
                hasattr(self.config, "finance_lexicon_path")
                and self.config.finance_lexicon_path
            ):
                self._load_finance_lexicon(self.config.finance_lexicon_path)

            logger.info("Ensemble model (FinBERT + VADER) initialized")
        else:
            raise ValueError(f"Unsupported model type: {self.model_type}")

    def _load_finance_lexicon(self, lexicon_path: str) -> None:
        """
        Load finance-specific lexicon to enhance VADER for financial text.

        Args:
            lexicon_path: Path to the finance lexicon file
        """
        try:
            with open(lexicon_path, "r") as f:
                for line in f:
                    if line.strip():
                        word, score = line.strip().split("\t")
                        self.vader_analyzer.lexicon[word] = float(score)
            logger.info(f"Loaded {lexicon_path} with finance-specific terms for VADER")
        except Exception as e:
            logger.error(f"Failed to load finance lexicon: {e}")

    def analyze_text(
        self, text: str, ticker: Optional[str] = None
    ) -> Dict[str, Union[float, str]]:
        """
        Analyze a single text item for sentiment.

        Args:
            text: The text content to analyze
            ticker: Optional stock ticker symbol related to the text

        Returns:
            Dictionary containing sentiment scores and metadata
        """
        # Check cache if Redis client is available and caching is enabled
        if self.redis_client and self.config.enable_caching:
            cache_key = f"sentiment:{self.model_type}:{hash(text)}"
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
                "model_type": self.model_type,
                "ticker": ticker,
            }

        # Choose the analysis method based on model type
        if self.model_type == "finbert":
            sentiment_result = self._analyze_with_finbert(cleaned_text)
        elif self.model_type == "vader":
            sentiment_result = self._analyze_with_vader(cleaned_text)
        elif self.model_type == "ensemble":
            sentiment_result = self._analyze_with_ensemble(cleaned_text)
        else:
            logger.error(
                f"Unknown model type: {self.model_type}, using VADER as fallback"
            )
            sentiment_result = self._analyze_with_vader(cleaned_text)

        # Add metadata
        result = {
            **sentiment_result,
            "timestamp": datetime.now().isoformat(),
            "model_type": self.model_type,
            "ticker": ticker,
        }

        # Cache result if Redis client is available
        if self.redis_client and self.config.enable_caching:
            self.redis_client.set(cache_key, result, expire=self.config.cache_ttl)

        return result

    @timer
    def analyze_batch(
        self, texts: List[Dict[str, str]]
    ) -> List[Dict[str, Union[float, str]]]:
        """
        Analyze a batch of text items in parallel.

        Args:
            texts: List of dictionaries containing text and metadata

        Returns:
            List of dictionaries with sentiment analysis results
        """
        # Process in parallel using ThreadPoolExecutor
        futures = []
        for item in texts:
            text = item.get("text", "")
            ticker = item.get("ticker")
            futures.append(self.executor.submit(self.analyze_text, text, ticker))

        # Collect results
        results = []
        for future in futures:
            try:
                results.append(future.result())
            except Exception as e:
                logger.error(f"Error in sentiment analysis batch processing: {e}")

        return results

    def _analyze_with_finbert(self, text: str) -> Dict[str, Union[float, str]]:
        """
        Analyze text using the FinBERT model.

        FinBERT is specifically fine-tuned for financial sentiment analysis.
        """
        with torch.no_grad():
            inputs = self.finbert_tokenizer(
                text,
                return_tensors="pt",
                truncation=True,
                max_length=self.config.max_sequence_length,
                padding=True,
            ).to(self.device)

            outputs = self.finbert_model(**inputs)
            scores = torch.nn.functional.softmax(outputs.logits, dim=1).cpu().numpy()[0]

        # FinBERT typically uses [negative, neutral, positive] labels
        sentiment_label = ["negative", "neutral", "positive"][np.argmax(scores)]
        confidence = float(np.max(scores))

        # Create a normalized score between 0 and 1 where:
        # 0 = most negative, 0.5 = neutral, 1 = most positive
        normalized_score = float(scores[2] - scores[0])  # positive - negative
        sentiment_score = (normalized_score + 1) / 2  # Scale from [-1, 1] to [0, 1]

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

    def _analyze_with_vader(self, text: str) -> Dict[str, Union[float, str]]:
        """
        Analyze text using VADER sentiment analyzer.

        VADER is a lexicon and rule-based sentiment analysis tool specifically
        attuned to sentiments expressed in social media and works well on
        financial text when enhanced with a finance-specific lexicon.
        """
        # Get VADER sentiment scores
        vader_scores = self.vader_analyzer.polarity_scores(text)

        # VADER provides compound score between -1 (most negative) and +1 (most positive)
        compound_score = vader_scores["compound"]

        # Convert compound score to normalized score (0 to 1)
        normalized_score = (compound_score + 1) / 2

        # Determine sentiment label based on compound score
        if compound_score >= 0.05:
            sentiment_label = "positive"
        elif compound_score <= -0.05:
            sentiment_label = "negative"
        else:
            sentiment_label = "neutral"

        # Estimate confidence based on the absolute value of the compound score
        # The further from zero, the more confident
        confidence = min(abs(compound_score) * 1.5, 1.0)

        return {
            "sentiment": sentiment_label,
            "score": normalized_score,
            "confidence": confidence,
            "scores": {
                "negative": vader_scores["neg"],
                "neutral": vader_scores["neu"],
                "positive": vader_scores["pos"],
                "compound": vader_scores["compound"],
            },
        }

    def _analyze_with_ensemble(self, text: str) -> Dict[str, Union[float, str]]:
        """
        Analyze text using both FinBERT and VADER and combine the results.

        This ensemble approach improves accuracy by leveraging both deep learning
        and lexicon-based approaches, which is particularly effective for
        financial text that may contain specialized terminology.
        """
        finbert_result = self._analyze_with_finbert(text)
        vader_result = self._analyze_with_vader(text)

        # Get weights from config or use default (equal weighting)
        finbert_weight = self.config.ensemble_weights.get("finbert", 0.6)
        vader_weight = self.config.ensemble_weights.get("vader", 0.4)

        # Weight by confidence
        finbert_weighted = finbert_weight * finbert_result["confidence"]
        vader_weighted = vader_weight * vader_result["confidence"]

        # Normalize weights
        total_weight = finbert_weighted + vader_weighted
        if total_weight > 0:
            finbert_weighted /= total_weight
            vader_weighted /= total_weight
        else:
            finbert_weighted = vader_weighted = 0.5

        # Calculate weighted ensemble score
        ensemble_score = (
            finbert_weighted * finbert_result["score"]
            + vader_weighted * vader_result["score"]
        )

        # Determine sentiment label based on ensemble score
        if ensemble_score > 0.6:
            sentiment_label = "positive"
        elif ensemble_score < 0.4:
            sentiment_label = "negative"
        else:
            sentiment_label = "neutral"

        # Calculate ensemble confidence
        ensemble_confidence = (
            finbert_weighted * finbert_result["confidence"]
            + vader_weighted * vader_result["confidence"]
        )

        return {
            "sentiment": sentiment_label,
            "score": float(ensemble_score),
            "confidence": float(ensemble_confidence),
            "model_weights": {
                "finbert": float(finbert_weighted),
                "vader": float(vader_weighted),
            },
            "component_scores": {
                "finbert": finbert_result["score"],
                "vader": vader_result["score"],
            },
        }
