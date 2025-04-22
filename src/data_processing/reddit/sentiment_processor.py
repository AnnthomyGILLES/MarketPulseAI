import os
import sys
import re
import torch
from pathlib import Path
from typing import Dict, List, Set, Tuple, Any, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    lit,
    current_timestamp,
    when,
    udf,
    explode,
    abs as spark_abs,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    FloatType,
    ArrayType,
    MapType,
)
from loguru import logger
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from src.data_processing.common.base_processor import BaseStreamProcessor

# Set Python interpreter for Spark
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


class TextPreprocessor:
    """Handle text preprocessing for Reddit posts."""

    def __init__(self, valid_tickers: Set[str], config: Dict[str, Any]):
        """Initialize the text preprocessor.

        Args:
            valid_tickers: Set of valid stock ticker symbols
            config: Configuration dictionary for preprocessing
        """
        self.valid_tickers = valid_tickers
        self.preprocessing_config = config.get("preprocessing", {})
        self.ticker_extraction_config = config.get("reddit", {}).get(
            "ticker_extraction", {}
        )

    def create_preprocess_function(self):
        """Create a function to preprocess Reddit posts."""

        def preprocess_reddit_post(
            title: Optional[str],
            selftext: Optional[str],
            subreddit: Optional[str],
            score: Optional[int],
            upvote_ratio: Optional[float],
            num_comments: Optional[int],
        ) -> Dict[str, str]:
            """Preprocess Reddit post for sentiment analysis."""
            # Combine title and selftext
            title = title or ""
            selftext = selftext or ""
            full_text = f"{title} {selftext}"

            # Apply configured preprocessing steps
            text = full_text

            # Remove URLs if configured
            if self.preprocessing_config.get("remove_urls", True):
                text = re.sub(r"https?://\S+", "", text)

            # Remove HTML if configured
            if self.preprocessing_config.get("remove_html", True):
                text = re.sub(r"<[^>]+>", "", text)

            # Remove mentions if configured
            if self.preprocessing_config.get("remove_mentions", True):
                text = re.sub(r"@\w+", "", text)

            # Lowercase if configured
            if self.preprocessing_config.get("lowercase", True):
                text = text.lower()

            # Extract potential stock symbols
            min_length = self.ticker_extraction_config.get("min_length", 1)
            max_length = self.ticker_extraction_config.get("max_length", 5)
            require_dollar = self.ticker_extraction_config.get(
                "require_dollar_sign", True
            )

            if require_dollar:
                # Look for $TICKER format
                pattern = r"\$([A-Z]{" + str(min_length) + "," + str(max_length) + "})"
                potential_symbols = re.findall(pattern, full_text)
            else:
                # Look for standalone uppercase words that could be tickers
                pattern = (
                    r"\b([A-Z]{" + str(min_length) + "," + str(max_length) + "})\b"
                )
                potential_symbols = re.findall(pattern, full_text)

            # Filter to valid tickers
            ticker_mentions = [
                tick for tick in potential_symbols if tick in self.valid_tickers
            ]

            return {
                "cleaned_text": text,
                "tickers": ",".join(ticker_mentions),
                "subreddit": subreddit.lower() if subreddit else "",
                "score": str(score) if score is not None else "0",
                "upvote_ratio": str(upvote_ratio)
                if upvote_ratio is not None
                else "0.5",
                "num_comments": str(num_comments) if num_comments is not None else "0",
            }

        return preprocess_reddit_post


class SentimentAnalyzer:
    """Handle sentiment analysis for Reddit posts."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize the sentiment analyzer.

        Args:
            config: Configuration dictionary for sentiment analysis
        """
        self.config = config
        self.vader_analyzer = SentimentIntensityAnalyzer()
        self.ensemble_weights = (
            config.get("model", {})
            .get("ensemble", {})
            .get("weights", {"vader": 0.3, "finbert": 0.7})
        )
        self.finbert_model, self.tokenizer = self._load_sentiment_models()

    def _load_sentiment_models(self) -> Tuple[Optional[Any], Optional[Any]]:
        """Load optimized FinBERT model for Reddit finance posts."""
        try:
            # Get model configuration
            model_config = self.config.get("model", {}).get("finbert", {})
            model_path = model_config.get("model_path", "ProsusAI/finbert")
            use_quantization = model_config.get("quantization", True)

            logger.info(f"Loading FinBERT model from {model_path}")

            # Check if model should be cached locally for faster loading
            if model_path.startswith("models/"):
                # Using local model
                if not Path(model_path).exists():
                    logger.warning(
                        f"Local model not found at {model_path}, falling back to HuggingFace"
                    )
                    model_path = "ProsusAI/finbert"

            # Load model and tokenizer
            model = AutoModelForSequenceClassification.from_pretrained(model_path)
            tokenizer = AutoTokenizer.from_pretrained(model_path)

            # Apply quantization if configured
            if use_quantization:
                logger.info(
                    "Applying quantization to FinBERT model for faster inference"
                )
                model = torch.quantization.quantize_dynamic(
                    model, {torch.nn.Linear}, dtype=torch.qint8
                )

            logger.info("Sentiment models loaded successfully")
            return model, tokenizer
        except Exception as e:
            logger.error(f"Error loading sentiment models: {str(e)}")
            # Return None and handle gracefully in processing
            return None, None

    def create_fast_sentiment_function(self):
        """Create fast sentiment analysis function using VADER."""

        def fast_sentiment_analysis(cleaned_text: str) -> Dict[str, Union[str, float]]:
            """Fast sentiment analysis using VADER."""
            try:
                if not cleaned_text:
                    return {
                        "sentiment": "Neutral",
                        "compound": 0.0,
                        "pos": 0.0,
                        "neg": 0.0,
                        "neu": 1.0,
                    }

                # Get sentiment scores
                scores = self.vader_analyzer.polarity_scores(cleaned_text)

                # Determine sentiment label
                if scores["compound"] >= 0.05:
                    sentiment = "Bullish"
                elif scores["compound"] <= -0.05:
                    sentiment = "Bearish"
                else:
                    sentiment = "Neutral"

                return {
                    "sentiment": sentiment,
                    "compound": scores["compound"],
                    "pos": scores["pos"],
                    "neg": scores["neg"],
                    "neu": scores["neu"],
                }
            except Exception as e:
                logger.error(f"Error in fast sentiment analysis: {str(e)}")
                return {
                    "sentiment": "Neutral",
                    "compound": 0.0,
                    "pos": 0.0,
                    "neg": 0.0,
                    "neu": 1.0,
                }

        return fast_sentiment_analysis

    def create_deep_sentiment_function(self):
        """Create deep sentiment analysis function using FinBERT."""

        def deep_sentiment_analysis(
            cleaned_text: str, subreddit: str
        ) -> Dict[str, Union[str, float]]:
            """Deep sentiment analysis using FinBERT."""
            try:
                if not cleaned_text or not self.finbert_model or not self.tokenizer:
                    return {"sentiment": "Neutral", "score": 0.5}

                # Truncate text if too long
                max_length = 512
                if len(cleaned_text) > max_length * 4:  # Approximate character limit
                    cleaned_text = cleaned_text[: max_length * 4]

                # Tokenize the text
                inputs = self.tokenizer(
                    cleaned_text,
                    return_tensors="pt",
                    padding=True,
                    truncation=True,
                    max_length=max_length,
                )

                # Get model prediction
                with torch.no_grad():
                    outputs = self.finbert_model(**inputs)
                    scores = (
                        torch.nn.functional.softmax(outputs.logits, dim=1)
                        .squeeze()
                        .tolist()
                    )

                # FinBERT labels: negative (bearish), neutral, positive (bullish)
                # Map to our sentiment labels
                sentiment_labels = ["Bearish", "Neutral", "Bullish"]
                result = {}

                # Add scores for each label
                for i, label in enumerate(sentiment_labels):
                    result[label] = float(scores[i])

                # Map to 5-class scale
                if result["Bullish"] > 0.7:
                    sentiment = "Very Bullish"
                    score = 0.75
                elif result["Bullish"] > 0.5:
                    sentiment = "Bullish"
                    score = 0.25
                elif result["Bearish"] > 0.7:
                    sentiment = "Very Bearish"
                    score = -0.75
                elif result["Bearish"] > 0.5:
                    sentiment = "Bearish"
                    score = -0.25
                else:
                    sentiment = "Neutral"
                    score = 0.0

                # Add primary sentiment and score
                result["sentiment"] = sentiment
                result["score"] = score

                # Add additional labels for the 5-class scale
                result["Very Bearish"] = result["Bearish"] * 0.7
                result["Very Bullish"] = result["Bullish"] * 0.7

                return result

            except Exception as e:
                logger.error(f"Error in deep sentiment analysis: {str(e)}")
                return {"sentiment": "Neutral", "score": 0.5}

        return deep_sentiment_analysis

    def create_weighted_sentiment_function(self):
        """Create weighted sentiment function combining VADER and FinBERT."""

        def weighted_sentiment(
            fast_sentiment: Dict[str, Any],
            deep_sentiment: Dict[str, Any],
            score: str,
            upvote_ratio: str,
        ) -> Dict[str, Union[str, float]]:
            """Combine fast and deep sentiment with Reddit metrics weighting."""
            try:
                # Convert score and upvote ratio to influence factors
                score_factor = min(1.0, 0.5 + (float(score) / 1000))  # Scale up to 1.0
                upvote_factor = float(upvote_ratio) if upvote_ratio else 0.5

                # Get sentiment values
                fast_label = fast_sentiment.get("sentiment", "Neutral")
                fast_score = fast_sentiment.get("compound", 0.0)

                deep_label = deep_sentiment.get("sentiment", "Neutral")
                deep_score = deep_sentiment.get("score", 0.5)

                # Weight fast vs deep sentiment using configured weights
                vader_weight = self.ensemble_weights.get("vader", 0.3)
                finbert_weight = self.ensemble_weights.get("finbert", 0.7)

                weighted_score = (vader_weight * fast_score) + (
                    finbert_weight * deep_score
                )

                # Apply Reddit metrics influence
                final_score = weighted_score * score_factor * upvote_factor

                # Determine final sentiment label
                if final_score >= 0.4:
                    final_label = "Very Bullish"
                elif final_score >= 0.1:
                    final_label = "Bullish"
                elif final_score <= -0.4:
                    final_label = "Very Bearish"
                elif final_score <= -0.1:
                    final_label = "Bearish"
                else:
                    final_label = "Neutral"

                return {
                    "label": final_label,
                    "score": final_score,
                    "confidence": min(0.5 + abs(final_score), 0.99),
                }
            except Exception as e:
                logger.error(f"Error in weighted sentiment: {str(e)}")
                return {"label": "Neutral", "score": 0.0, "confidence": 0.5}

        return weighted_sentiment

    def create_ticker_sentiment_function(self):
        """Create ticker-specific sentiment function."""

        def ticker_specific_sentiment(
            tickers_str: str, sentiment: Dict[str, Any]
        ) -> List[Dict[str, str]]:
            """Create ticker-specific sentiment entries."""
            try:
                result = []
                if not tickers_str:
                    return result

                tickers = tickers_str.split(",")
                for ticker in tickers:
                    if ticker:
                        result.append(
                            {
                                "ticker": ticker,
                                "sentiment": sentiment.get("label", "Neutral"),
                                "score": str(sentiment.get("score", 0.0)),
                                "confidence": str(sentiment.get("confidence", 0.5)),
                            }
                        )
                return result
            except Exception as e:
                logger.error(f"Error in ticker sentiment extraction: {str(e)}")
                return []

        return ticker_specific_sentiment


class RedditSentimentProcessor(BaseStreamProcessor):
    """Processor for Reddit posts with sentiment analysis streamed from Kafka to MongoDB."""

    def __init__(self, config_path: str):
        """Initialize the Reddit sentiment processor.

        Args:
            config_path: Path to the configuration file
        """
        super().__init__(config_path)
        self.reddit_schema = self._create_schema()
        self.valid_tickers = self._load_valid_tickers()
        self.subreddit_calibrations = self._load_subreddit_calibrations()

        # Initialize specialized components
        self.preprocessor = TextPreprocessor(
            self.valid_tickers, self.config.get("sentiment_analysis", {})
        )
        self.sentiment_analyzer = SentimentAnalyzer(
            self.config.get("sentiment_analysis", {})
        )

        # Register UDFs
        self._register_udfs()

    def _load_valid_tickers(self) -> Set[str]:
        """Load valid ticker symbols from configured file.

        Returns:
            Set of valid ticker symbols
        """
        valid_tickers = set()

        # Get ticker file path from config
        tickers_file = (
            self.config.get("sentiment_analysis", {})
            .get("paths", {})
            .get("tickers_file")
        )

        if not tickers_file:
            logger.warning(
                "No tickers file specified in config. Using empty ticker set."
            )
            return valid_tickers

        try:
            tickers_path = Path(tickers_file)
            if tickers_path.exists():
                with open(tickers_path, "r") as f:
                    valid_tickers = {line.strip() for line in f if line.strip()}
                logger.info(f"Loaded {len(valid_tickers)} valid ticker symbols")
            else:
                logger.warning(f"Tickers file not found: {tickers_file}")
        except Exception as e:
            logger.error(f"Error loading ticker symbols: {str(e)}")

        return valid_tickers

    def _load_subreddit_calibrations(self) -> Dict[str, Dict[str, float]]:
        """Load subreddit calibration values from configuration.

        Returns:
            Dictionary mapping subreddit names to calibration parameters
        """
        # Get calibrations from config
        subreddit_config = (
            self.config.get("sentiment_analysis", {})
            .get("reddit", {})
            .get("subreddit_calibration", {})
        )

        if subreddit_config:
            logger.info(f"Loaded calibrations for {len(subreddit_config)} subreddits")
            return subreddit_config

        # Fallback to defaults if not configured
        default_calibrations = {
            "wallstreetbets": {"bias": 0.1, "scale": 1.2},
            "stocks": {"bias": 0.0, "scale": 1.0},
            "investing": {"bias": -0.05, "scale": 0.9},
        }
        logger.warning("Using default subreddit calibrations (not from config)")
        return default_calibrations

    def _create_schema(self) -> StructType:
        """Create the schema for Reddit post data.

        Returns:
            StructType schema for Reddit posts
        """
        return StructType(
            [
                StructField("id", StringType(), True),
                StructField("source", StringType(), True),
                StructField("content_type", StringType(), True),
                StructField("collection_timestamp", StringType(), True),
                StructField("created_utc", IntegerType(), True),
                StructField("author", StringType(), True),
                StructField("score", IntegerType(), True),
                StructField("subreddit", StringType(), True),
                StructField("permalink", StringType(), True),
                StructField("detected_symbols", ArrayType(StringType()), True),
                StructField("created_datetime", StringType(), True),
                StructField("title", StringType(), True),
                StructField("selftext", StringType(), True),
                StructField("url", StringType(), True),
                StructField("is_self", BooleanType(), True),
                StructField("upvote_ratio", FloatType(), True),
                StructField("num_comments", IntegerType(), True),
                StructField("collection_method", StringType(), True),
            ]
        )

    def _register_udfs(self) -> None:
        """Register UDFs for sentiment analysis."""
        # Create UDF functions
        preprocess_func = self.preprocessor.create_preprocess_function()
        fast_sentiment_func = self.sentiment_analyzer.create_fast_sentiment_function()
        deep_sentiment_func = self.sentiment_analyzer.create_deep_sentiment_function()
        weighted_sentiment_func = (
            self.sentiment_analyzer.create_weighted_sentiment_function()
        )
        ticker_sentiment_func = (
            self.sentiment_analyzer.create_ticker_sentiment_function()
        )

        # Register UDFs with appropriate return types
        self.preprocess_udf = udf(preprocess_func, MapType(StringType(), StringType()))
        self.fast_sentiment_udf = udf(
            fast_sentiment_func, MapType(StringType(), FloatType())
        )
        self.deep_sentiment_udf = udf(
            deep_sentiment_func, MapType(StringType(), FloatType())
        )
        self.weighted_sentiment_udf = udf(
            weighted_sentiment_func, MapType(StringType(), FloatType())
        )
        self.explode_ticker_sentiment_udf = udf(
            ticker_sentiment_func, ArrayType(MapType(StringType(), StringType()))
        )

        logger.info("UDFs registered successfully")

    def process_reddit_posts(self, kafka_df: DataFrame) -> DataFrame:
        """Process Reddit posts from Kafka with sentiment analysis.

        Args:
            kafka_df: DataFrame containing raw Kafka messages

        Returns:
            Processed DataFrame with sentiment analysis results
        """
        logger.info("Processing Reddit posts with sentiment analysis")

        # Register UDFs with Spark
        spark = kafka_df.sparkSession
        spark.udf.register("preprocess_udf", self.preprocess_udf)
        spark.udf.register("fast_sentiment_udf", self.fast_sentiment_udf)
        spark.udf.register("deep_sentiment_udf", self.deep_sentiment_udf)
        spark.udf.register("weighted_sentiment_udf", self.weighted_sentiment_udf)
        spark.udf.register(
            "explode_ticker_sentiment_udf", self.explode_ticker_sentiment_udf
        )

        # Parse JSON from Kafka value field
        reddit_df = (
            kafka_df.selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), self.reddit_schema).alias("data"))
            .select("data.*")
        )

        # Preprocess posts
        processed_df = (
            reddit_df.withColumn("processing_timestamp", current_timestamp())
            .withColumn(
                "detected_symbols",
                when(col("detected_symbols").isNull(), lit([])).otherwise(
                    col("detected_symbols")
                ),
            )
            .withColumn(
                "preprocessed",
                self.preprocess_udf(
                    col("title"),
                    col("selftext"),
                    col("subreddit"),
                    col("score"),
                    col("upvote_ratio"),
                    col("num_comments"),
                ),
            )
        )

        # Apply sentiment analysis
        sentiment_df = (
            processed_df.withColumn(
                "fast_sentiment",
                self.fast_sentiment_udf(col("preprocessed.cleaned_text")),
            )
            .withColumn(
                "deep_sentiment",
                self.deep_sentiment_udf(
                    col("preprocessed.cleaned_text"), col("preprocessed.subreddit")
                ),
            )
            .withColumn(
                "sentiment",
                self.weighted_sentiment_udf(
                    col("fast_sentiment"),
                    col("deep_sentiment"),
                    col("preprocessed.score"),
                    col("preprocessed.upvote_ratio"),
                ),
            )
        )

        # Generate ticker-specific sentiment
        ticker_sentiment = sentiment_df.withColumn(
            "ticker_sentiments",
            self.explode_ticker_sentiment_udf(
                col("preprocessed.tickers"), col("sentiment")
            ),
        )

        # Final output: both full post sentiment and exploded ticker sentiments
        output_df = ticker_sentiment.select(
            "*",
            col("sentiment.label").alias("sentiment_label"),
            col("sentiment.score").alias("sentiment_score"),
            col("sentiment.confidence").alias("sentiment_confidence"),
        )

        # Add topics if they exist in the original data
        if "topics" in reddit_df.columns:
            output_df = output_df.withColumn("topics", col("topics"))

        # Add market_impact_estimate based on confidence and score
        output_df = output_df.withColumn(
            "market_impact_estimate",
            when(
                col("sentiment_confidence") > 0.8,
                when(spark_abs(col("sentiment_score")) > 0.5, "high").otherwise(
                    "moderate"
                ),
            ).otherwise(
                when(spark_abs(col("sentiment_score")) > 0.3, "moderate").otherwise(
                    "low"
                )
            ),
        )

        return output_df

    def run(self) -> None:
        """Main execution method to process Reddit posts with sentiment from Kafka to MongoDB."""
        logger.info("Starting Reddit sentiment processing pipeline")

        try:
            # Get configuration values for Kafka topics
            kafka_config = self.config.get("kafka", {}).get("topics", {})
            reddit_posts_topic = kafka_config.get(
                "social_media_reddit_validated", "social-media-reddit-posts-validated"
            )
            reddit_comments_topic = kafka_config.get(
                "social_media_reddit_comments_validated",
                "social-media-reddit-comments-validated",
            )
            kafka_topics = f"{reddit_posts_topic}, {reddit_comments_topic}"

            # Get checkpoint location
            checkpoint_location = (
                self.config.get("sentiment_analysis", {})
                .get("paths", {})
                .get("checkpoint_location", "/tmp/reddit_sentiment_checkpoint")
            )

            # Get MongoDB configuration
            mongodb_database = self.config.get("mongodb", {}).get(
                "database", "social_media"
            )
            mongodb_collection = self.config.get("mongodb", {}).get(
                "collection", "reddit_sentiment"
            )
            ticker_collection = self.config.get("mongodb", {}).get(
                "ticker_collection", "ticker_sentiment"
            )

            # Create checkpoint directory if it doesn't exist
            Path(checkpoint_location).mkdir(parents=True, exist_ok=True)

            # Get performance config for window duration
            perf_config = self.config.get("sentiment_analysis", {}).get(
                "performance", {}
            )
            window_duration_ms = perf_config.get("window_duration_ms", 500)

            # Read from Kafka
            kafka_df = self.read_from_kafka(kafka_topics)

            # Process data with sentiment analysis
            processed_df = self.process_reddit_posts(kafka_df)

            # Create two output streams:
            # 1. Main posts with sentiment
            logger.info(
                f"Starting MongoDB output stream for reddit posts to {mongodb_database}.{mongodb_collection}"
            )
            post_query = self.write_to_mongodb(
                processed_df,
                database=mongodb_database,
                collection=mongodb_collection,
                checkpoint_location=f"{checkpoint_location}/posts",
                output_mode="append",
            )

            # 2. Exploded ticker sentiment
            logger.info(
                f"Starting MongoDB output stream for ticker sentiment to {mongodb_database}.{ticker_collection}"
            )
            ticker_df = (
                processed_df.select("id", "processing_timestamp", "ticker_sentiments")
                .withColumn("exploded", explode(col("ticker_sentiments")))
                .select(
                    col("id"),
                    col("processing_timestamp"),
                    col("exploded.ticker").alias("ticker"),
                    col("exploded.sentiment").alias("sentiment"),
                    col("exploded.score").alias("score"),
                    col("exploded.confidence").alias("confidence"),
                )
            )

            ticker_query = self.write_to_mongodb(
                ticker_df,
                database=mongodb_database,
                collection=ticker_collection,
                checkpoint_location=f"{checkpoint_location}/tickers",
                output_mode="append",
            )

            # Wait for termination
            logger.info(
                f"Streaming queries started with {window_duration_ms}ms window duration"
            )
            spark = SparkSession.getActiveSession()
            spark.streams.awaitAnyTermination()

        except Exception as e:
            logger.error(f"Error in Reddit sentiment processing: {str(e)}")
            raise


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python sentiment_processor.py <config_path>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    processor = RedditSentimentProcessor(config_path)
    processor.run()