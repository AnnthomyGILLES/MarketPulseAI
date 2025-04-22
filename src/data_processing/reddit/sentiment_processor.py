import os
import sys
import re
import torch
import numpy as np
import hashlib
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import from_json, col, lit, current_timestamp, when, udf, explode
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType, 
    FloatType, ArrayType, MapType, DoubleType
)
from loguru import logger
from pathlib import Path
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from src.data_processing.common.base_processor import BaseStreamProcessor

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

class RedditSentimentProcessor(BaseStreamProcessor):
    """Processor for Reddit posts with sentiment analysis streamed from Kafka to MongoDB."""

    def __init__(self, config_path: str):
        """Initialize the Reddit sentiment processor.

        Args:
            config_path: Path to the configuration file
        """
        super().__init__(config_path)
        self.reddit_schema = self._create_schema()
        self._load_valid_tickers()
        self._load_subreddit_calibrations()
        self.vader_analyzer = SentimentIntensityAnalyzer()
        self.finbert_model, self.tokenizer = self._load_sentiment_models()
        self._register_udfs()
        
    def _load_valid_tickers(self):
        """Load valid ticker symbols from configured file."""
        self.valid_tickers = set()
        
        # Get ticker file path from config
        tickers_file = self.config.get("sentiment_analysis", {}).get("paths", {}).get("tickers_file")
        
        if not tickers_file:
            logger.warning("No tickers file specified in config. Using empty ticker set.")
            return
            
        try:
            tickers_path = Path(tickers_file)
            if tickers_path.exists():
                with open(tickers_path, 'r') as f:
                    self.valid_tickers = {line.strip() for line in f if line.strip()}
                logger.info(f"Loaded {len(self.valid_tickers)} valid ticker symbols")
            else:
                logger.warning(f"Tickers file not found: {tickers_file}")
        except Exception as e:
            logger.error(f"Error loading ticker symbols: {str(e)}")
            
    def _load_subreddit_calibrations(self):
        """Load subreddit calibration values from configuration."""
        self.subreddit_calibrations = {}
        
        # Get calibrations from config
        subreddit_config = self.config.get("sentiment_analysis", {}).get("reddit", {}).get("subreddit_calibration", {})
        
        if subreddit_config:
            self.subreddit_calibrations = subreddit_config
            logger.info(f"Loaded calibrations for {len(self.subreddit_calibrations)} subreddits")
        else:
            # Fallback to defaults if not configured
            self.subreddit_calibrations = {
                "wallstreetbets": {"bias": 0.1, "scale": 1.2},
                "stocks": {"bias": 0.0, "scale": 1.0},
                "investing": {"bias": -0.05, "scale": 0.9},
            }
            logger.warning("Using default subreddit calibrations (not from config)")

    def _create_schema(self) -> StructType:
        """Create the schema for Reddit post data.

        Returns:
            StructType schema for Reddit posts
        """
        return StructType([
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
        ])

    def _load_sentiment_models(self):
        """Load optimized FinBERT model for Reddit finance posts."""
        try:
            # Get model configuration
            model_config = self.config.get("sentiment_analysis", {}).get("model", {}).get("finbert", {})
            model_path = model_config.get("model_path", "ProsusAI/finbert")
            use_quantization = model_config.get("quantization", True)
            
            logger.info(f"Loading FinBERT model from {model_path}")
            
            # Check if model should be cached locally for faster loading
            if model_path.startswith("models/"):
                # Using local model
                if not Path(model_path).exists():
                    logger.warning(f"Local model not found at {model_path}, falling back to HuggingFace")
                    model_path = "ProsusAI/finbert"
            
            # Load model and tokenizer
            model = AutoModelForSequenceClassification.from_pretrained(model_path)
            tokenizer = AutoTokenizer.from_pretrained(model_path)
            
            # Apply quantization if configured
            if use_quantization:
                logger.info("Applying quantization to FinBERT model for faster inference")
                model = torch.quantization.quantize_dynamic(
                    model, {torch.nn.Linear}, dtype=torch.qint8
                )
            
            logger.info("Sentiment models loaded successfully")
            return model, tokenizer
        except Exception as e:
            logger.error(f"Error loading sentiment models: {str(e)}")
            # Return None and handle gracefully in processing
            return None, None

    def _register_udfs(self):
        """Register UDFs for sentiment analysis"""
        # Preprocess UDF
        self.preprocess_udf = udf(self._preprocess_reddit_post, 
                                 returnType=MapType(StringType(), StringType()))
        
        # Fast sentiment UDF (VADER)
        self.fast_sentiment_udf = udf(self._fast_sentiment_analysis, 
                                     returnType=MapType(StringType(), FloatType()))
        
        # Deep sentiment UDF (FinBERT)
        self.deep_sentiment_udf = udf(self._deep_sentiment_analysis, 
                                     returnType=MapType(StringType(), FloatType()))
        
        # Combined sentiment UDF
        self.weighted_sentiment_udf = udf(self._weighted_sentiment, 
                                         returnType=MapType(StringType(), FloatType()))
        
        # Ticker sentiment exploder
        self.explode_ticker_sentiment_udf = udf(self._ticker_specific_sentiment, 
                                               returnType=ArrayType(MapType(StringType(), StringType())))

    def _preprocess_reddit_post(self, title, selftext, subreddit, score, upvote_ratio, num_comments):
        """Preprocess Reddit post for sentiment analysis."""
        # Combine title and selftext
        title = title or ""
        selftext = selftext or ""
        full_text = f"{title} {selftext}"
        
        # Get preprocessing configuration
        preproc_config = self.config.get("sentiment_analysis", {}).get("preprocessing", {})
        
        # Apply configured preprocessing steps
        text = full_text
        
        # Remove URLs if configured
        if preproc_config.get("remove_urls", True):
            text = re.sub(r'https?://\S+', '', text)
        
        # Remove HTML if configured
        if preproc_config.get("remove_html", True):
            text = re.sub(r'<[^>]+>', '', text)
            
        # Remove mentions if configured
        if preproc_config.get("remove_mentions", True):
            text = re.sub(r'@\w+', '', text)
            
        # Lowercase if configured
        if preproc_config.get("lowercase", True):
            text = text.lower()
        
        # Extract potential stock symbols using ticker extraction config
        ticker_config = self.config.get("sentiment_analysis", {}).get("reddit", {}).get("ticker_extraction", {})
        min_length = ticker_config.get("min_length", 1)
        max_length = ticker_config.get("max_length", 5)
        require_dollar = ticker_config.get("require_dollar_sign", True)
        
        if require_dollar:
            # Look for $TICKER format
            pattern = r'\$([A-Z]{' + str(min_length) + ',' + str(max_length) + '})'
            potential_symbols = re.findall(pattern, full_text)
        else:
            # Look for standalone uppercase words that could be tickers
            pattern = r'\b([A-Z]{' + str(min_length) + ',' + str(max_length) + '})\b'
            potential_symbols = re.findall(pattern, full_text)
            
        # Filter to valid tickers
        ticker_mentions = [tick for tick in potential_symbols if tick in self.valid_tickers]
        
        return {
            "cleaned_text": text,
            "tickers": ",".join(ticker_mentions),
            "subreddit": subreddit.lower() if subreddit else "",
            "score": str(score) if score is not None else "0",
            "upvote_ratio": str(upvote_ratio) if upvote_ratio is not None else "0.5",
            "num_comments": str(num_comments) if num_comments is not None else "0"
        }

    def _fast_sentiment_analysis(self, cleaned_text):
        """Fast sentiment analysis using VADER."""
        try:
            if not cleaned_text:
                return {"sentiment": "Neutral", "compound": 0.0, "pos": 0.0, "neg": 0.0, "neu": 1.0}
            
            # Create cache key for this text
            if self.redis_client:
                cache_key = f"vader_sentiment:{hashlib.md5(cleaned_text.encode()).hexdigest()}"
                cached_result = self.redis_client.get(cache_key)
                if cached_result:
                    return eval(cached_result.decode())
            
            # Get sentiment scores
            scores = self.vader_analyzer.polarity_scores(cleaned_text)
            
            # Determine sentiment label
            if scores['compound'] >= 0.05:
                sentiment = "Bullish"
            elif scores['compound'] <= -0.05:
                sentiment = "Bearish"
            else:
                sentiment = "Neutral"
                
            result = {
                "sentiment": sentiment,
                "compound": scores['compound'],
                "pos": scores['pos'],
                "neg": scores['neg'],
                "neu": scores['neu']
            }
            
            # Cache result if redis is enabled
            if self.redis_client:
                ttl = self.config.get("sentiment_analysis", {}).get("performance", {}).get("cache_ttl", 3600)
                self.redis_client.setex(cache_key, ttl, str(result))
                
            return result
        except Exception as e:
            logger.error(f"Error in fast sentiment analysis: {str(e)}")
            return {"sentiment": "Neutral", "compound": 0.0, "pos": 0.0, "neg": 0.0, "neu": 1.0}

    def _deep_sentiment_analysis(self, cleaned_text, subreddit):
        """Deep sentiment analysis using FinBERT."""
        try:
            if not cleaned_text or self.finbert_model is None:
                return {"sentiment": "Neutral", "score": 0.5}
            
            # Create cache key for this text + subreddit combination
            if self.redis_client:
                cache_key = f"finbert_sentiment:{subreddit}:{hashlib.md5(cleaned_text.encode()).hexdigest()}"
                cached_result = self.redis_client.get(cache_key)
                if cached_result:
                    return eval(cached_result.decode())
            
            # Get subreddit calibration from loaded values
            subreddit_calibration = self.subreddit_calibrations.get(subreddit, {"bias": 0.0, "scale": 1.0})
            
            # Get model config
            model_config = self.config.get("sentiment_analysis", {}).get("model", {}).get("finbert", {})
            max_length = model_config.get("max_sequence_length", 512)
            
            # Tokenize text (with truncation for long posts)
            inputs = self.tokenizer(cleaned_text, return_tensors="pt", truncation=True, max_length=max_length)
            
            # Run inference
            with torch.no_grad():
                outputs = self.finbert_model(**inputs)
                scores = outputs.logits.softmax(dim=1).numpy()[0]
            
            # Apply calibration
            bias = subreddit_calibration["bias"]
            scale = subreddit_calibration["scale"]
            
            # Simple calibration method
            adjusted_scores = scores * scale
            adjusted_scores[1] += bias  # Adjust neutral score
            calibrated_scores = adjusted_scores / adjusted_scores.sum()  # Renormalize
            
            sentiment_labels = ["Very Bearish", "Bearish", "Neutral", "Bullish", "Very Bullish"]
            sentiment_index = np.argmax(calibrated_scores)
            
            result = {"sentiment": sentiment_labels[sentiment_index], "score": float(calibrated_scores[sentiment_index])}
            
            # Add individual scores
            for i, label in enumerate(sentiment_labels):
                result[label] = float(calibrated_scores[i])
            
            # Cache result if redis is enabled
            if self.redis_client:
                ttl = self.config.get("sentiment_analysis", {}).get("performance", {}).get("cache_ttl", 3600)
                self.redis_client.setex(cache_key, ttl, str(result))
                
            return result
        except Exception as e:
            logger.error(f"Error in deep sentiment analysis: {str(e)}")
            return {"sentiment": "Neutral", "score": 0.5}

    def _weighted_sentiment(self, fast_sentiment, deep_sentiment, score, upvote_ratio):
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
            
            # Get ensemble weights from config
            ensemble_config = self.config.get("sentiment_analysis", {}).get("model", {}).get("ensemble", {})
            weights = ensemble_config.get("weights", {})
            vader_weight = weights.get("vader", 0.3)
            finbert_weight = weights.get("finbert", 0.7)
            
            # Weight fast vs deep sentiment using configured weights
            weighted_score = (vader_weight * fast_score) + (finbert_weight * deep_score)
            
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
                "confidence": min(0.5 + abs(final_score), 0.99)
            }
        except Exception as e:
            logger.error(f"Error in weighted sentiment: {str(e)}")
            return {"label": "Neutral", "score": 0.0, "confidence": 0.5}

    def _ticker_specific_sentiment(self, tickers_str, sentiment):
        """Create ticker-specific sentiment entries."""
        try:
            result = []
            if not tickers_str:
                return result
                
            tickers = tickers_str.split(",")
            for ticker in tickers:
                if ticker:
                    result.append({
                        "ticker": ticker,
                        "sentiment": sentiment.get("label", "Neutral"),
                        "score": str(sentiment.get("score", 0.0)),
                        "confidence": str(sentiment.get("confidence", 0.5))
                    })
            return result
        except Exception as e:
            logger.error(f"Error in ticker sentiment extraction: {str(e)}")
            return []

    def process_reddit_posts(self, kafka_df: DataFrame) -> DataFrame:
        """Process Reddit posts from Kafka with sentiment analysis.

        Args:
            kafka_df: Raw DataFrame from Kafka

        Returns:
            Processed DataFrame with sentiment analysis
        """
        logger.info("Processing Reddit posts with sentiment analysis")

        # Register UDFs with Spark
        spark = kafka_df.sparkSession
        spark.udf.register("preprocess_udf", self.preprocess_udf)
        spark.udf.register("fast_sentiment_udf", self.fast_sentiment_udf)
        spark.udf.register("deep_sentiment_udf", self.deep_sentiment_udf)
        spark.udf.register("weighted_sentiment_udf", self.weighted_sentiment_udf)
        spark.udf.register("explode_ticker_sentiment_udf", self.explode_ticker_sentiment_udf)

        # Parse JSON from Kafka value field
        reddit_df = (
            kafka_df.selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), self.reddit_schema).alias("data"))
            .select("data.*")
        )

        # Preprocess posts
        processed_df = reddit_df.withColumn(
            "processing_timestamp", current_timestamp()
        ).withColumn(
            "detected_symbols",
            when(col("detected_symbols").isNull(), lit([])).otherwise(col("detected_symbols"))
        ).withColumn(
            "preprocessed", 
            self.preprocess_udf(
                col("title"), col("selftext"), col("subreddit"), 
                col("score"), col("upvote_ratio"), col("num_comments")
            )
        )

        # Apply sentiment analysis
        sentiment_df = processed_df.withColumn(
            "fast_sentiment", 
            self.fast_sentiment_udf(col("preprocessed.cleaned_text"))
        ).withColumn(
            "deep_sentiment",
            self.deep_sentiment_udf(col("preprocessed.cleaned_text"), col("preprocessed.subreddit"))
        ).withColumn(
            "sentiment",
            self.weighted_sentiment_udf(
                col("fast_sentiment"), 
                col("deep_sentiment"),
                col("preprocessed.score"),
                col("preprocessed.upvote_ratio")
            )
        )

        # Generate ticker-specific sentiment
        ticker_sentiment = sentiment_df.withColumn(
            "ticker_sentiments",
            self.explode_ticker_sentiment_udf(col("preprocessed.tickers"), col("sentiment"))
        )

        # Add quality score based on post metadata
        quality_config = self.config.get("sentiment_analysis", {}).get("reddit", {}).get("quality_scoring", {})
        min_length = quality_config.get("min_text_length", 50)
        ideal_length = quality_config.get("ideal_text_length", 500)
        
        # Final output: both full post sentiment and exploded ticker sentiments
        output_df = ticker_sentiment.select(
            "*",
            col("sentiment.label").alias("sentiment_label"),
            col("sentiment.score").alias("sentiment_score"),
            col("sentiment.confidence").alias("sentiment_confidence")
        )

        # Add topics if they exist in the original data
        if "topics" in reddit_df.columns:
            output_df = output_df.withColumn("topics", col("topics"))
        
        # Add market_impact_estimate based on confidence and score
        output_df = output_df.withColumn(
            "market_impact_estimate", 
            when(col("sentiment_confidence") > 0.8, 
                 when(abs(col("sentiment_score")) > 0.5, "high").otherwise("moderate")
            ).otherwise(
                when(abs(col("sentiment_score")) > 0.3, "moderate").otherwise("low")
            )
        )

        return output_df

    def run(self) -> None:
        """Main execution method to process Reddit posts with sentiment from Kafka to MongoDB."""
        logger.info("Starting Reddit sentiment processing pipeline")

        try:
            # Get configuration values for Kafka topics
            kafka_config = self.config.get("kafka", {}).get("topics", {})
            reddit_posts_topic = kafka_config.get("social_media_reddit_validated", "social-media-reddit-posts-validated")
            reddit_comments_topic = kafka_config.get("social_media_reddit_comments_validated", "social-media-reddit-comments-validated")
            kafka_topics = f"{reddit_posts_topic}, {reddit_comments_topic}"
            
            # Get checkpoint location
            checkpoint_location = self.config.get("sentiment_analysis", {}).get("paths", {}).get(
                "checkpoint_location", "/tmp/reddit_sentiment_checkpoint"
            )

            # Get MongoDB configuration
            mongodb_database = self.config.get("mongodb", {}).get("database", "social_media")
            mongodb_collection = self.config.get("mongodb", {}).get("collection", "reddit_sentiment")
            ticker_collection = self.config.get("mongodb", {}).get("ticker_collection", "ticker_sentiment")

            # Create checkpoint directory if it doesn't exist
            Path(checkpoint_location).mkdir(parents=True, exist_ok=True)

            # Get performance config for window duration
            perf_config = self.config.get("sentiment_analysis", {}).get("performance", {})
            window_duration_ms = perf_config.get("window_duration_ms", 500)

            # Read from Kafka
            kafka_df = self.read_from_kafka(kafka_topics)

            # Process data with sentiment analysis
            processed_df = self.process_reddit_posts(kafka_df)

            # Create two output streams:
            # 1. Main posts with sentiment
            logger.info(f"Starting MongoDB output stream for reddit posts to {mongodb_database}.{mongodb_collection}")
            post_query = self.write_to_mongodb(
                processed_df,
                database=mongodb_database,
                collection=mongodb_collection,
                checkpoint_location=f"{checkpoint_location}/posts",
                output_mode="append",
            )

            # 2. Exploded ticker sentiment (if needed)
            logger.info(f"Starting MongoDB output stream for ticker sentiment to {mongodb_database}.{ticker_collection}")
            ticker_df = processed_df.select("id", "processing_timestamp", "explode(ticker_sentiments)").select(
                "id", "processing_timestamp", "col.*"
            )
            
            ticker_query = self.write_to_mongodb(
                ticker_df,
                database=mongodb_database,
                collection=ticker_collection,
                checkpoint_location=f"{checkpoint_location}/tickers",
                output_mode="append",
            )

            # Wait for termination
            logger.info(f"Streaming queries started with {window_duration_ms}ms window duration")
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