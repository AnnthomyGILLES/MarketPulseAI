import os
import sys
import re
import torch
import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import from_json, col, lit, current_timestamp, when, udf, explode
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    FloatType,
    ArrayType,
    MapType,
    DoubleType
)
from loguru import logger
from pathlib import Path
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from src.data_processing.common.base_processor import BaseStreamProcessor

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Load valid ticker symbols
VALID_TICKERS = set()  # This should be loaded from a proper source

# Subreddit-specific calibration values
SUBREDDIT_CALIBRATIONS = {
    "wallstreetbets": {"bias": 0.1, "scale": 1.2},
    "stocks": {"bias": 0.0, "scale": 1.0},
    "investing": {"bias": -0.05, "scale": 0.9},
    # Add more subreddits as needed
}

class RedditSentimentProcessor(BaseStreamProcessor):
    """Processor for Reddit posts with sentiment analysis streamed from Kafka to MongoDB."""

    def __init__(self, config_path: str):
        """Initialize the Reddit sentiment processor.

        Args:
            config_path: Path to the configuration file
        """
        super().__init__(config_path)
        self.reddit_schema = self._create_schema()
        self.vader_analyzer = SentimentIntensityAnalyzer()
        self.finbert_model, self.tokenizer = self._load_sentiment_models()
        self._register_udfs()

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

    def _load_sentiment_models(self):
        """Load optimized FinBERT model for Reddit finance posts."""
        try:
            # Load FinBERT model
            model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
            tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
            
            # Apply quantization for faster inference
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
        
        # Basic cleaning
        text = re.sub(r'https?://\S+', '', full_text)
        
        # Extract potential stock symbols
        potential_symbols = re.findall(r'\$([A-Z]{1,5})', full_text)
        ticker_mentions = [tick for tick in potential_symbols if tick in VALID_TICKERS]
        
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
            
            # Get sentiment scores
            scores = self.vader_analyzer.polarity_scores(cleaned_text)
            
            # Determine sentiment label
            if scores['compound'] >= 0.05:
                sentiment = "Bullish"
            elif scores['compound'] <= -0.05:
                sentiment = "Bearish"
            else:
                sentiment = "Neutral"
                
            return {
                "sentiment": sentiment,
                "compound": scores['compound'],
                "pos": scores['pos'],
                "neg": scores['neg'],
                "neu": scores['neu']
            }
        except Exception as e:
            logger.error(f"Error in fast sentiment analysis: {str(e)}")
            return {"sentiment": "Neutral", "compound": 0.0, "pos": 0.0, "neg": 0.0, "neu": 1.0}

    def _deep_sentiment_analysis(self, cleaned_text, subreddit):
        """Deep sentiment analysis using FinBERT."""
        try:
            if not cleaned_text or self.finbert_model is None:
                return {"sentiment": "Neutral", "score": 0.5}
            
            # Get subreddit calibration
            subreddit_calibration = SUBREDDIT_CALIBRATIONS.get(subreddit, {"bias": 0.0, "scale": 1.0})
            
            # Tokenize text (with truncation for long posts)
            inputs = self.tokenizer(cleaned_text, return_tensors="pt", truncation=True, max_length=512)
            
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
            
            # Weight fast vs deep sentiment (give more weight to deep sentiment)
            weighted_score = (0.3 * fast_score) + (0.7 * deep_score)
            
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

        # Final output: both full post sentiment and exploded ticker sentiments
        output_df = ticker_sentiment.select(
            "*",
            col("sentiment.label").alias("sentiment_label"),
            col("sentiment.score").alias("sentiment_score"),
            col("sentiment.confidence").alias("sentiment_confidence")
        )

        return output_df

    def run(self) -> None:
        """Main execution method to process Reddit posts with sentiment from Kafka to MongoDB."""
        logger.info("Starting Reddit sentiment processing pipeline")

        try:
            # Get configuration values
            kafka_topics = f"{self.config['kafka']['topics']['social_media_reddit_validated']}, {self.config['kafka']['topics']['social_media_reddit_comments_validated']}"
            checkpoint_location = self.config.get("checkpoint_location", "/tmp/reddit_sentiment_checkpoint")

            # Get MongoDB configuration
            mongodb_database = self.config.get("mongodb", {}).get("database", "social_media")
            mongodb_collection = self.config.get("mongodb", {}).get("collection", "reddit_sentiment")
            ticker_collection = self.config.get("mongodb", {}).get("ticker_collection", "ticker_sentiment")

            # Create checkpoint directory if it doesn't exist
            Path(checkpoint_location).mkdir(parents=True, exist_ok=True)

            # Read from Kafka
            kafka_df = self.read_from_kafka(kafka_topics)

            # Process data with sentiment analysis
            processed_df = self.process_reddit_posts(kafka_df)

            # Create two output streams:
            # 1. Main posts with sentiment
            post_query = self.write_to_mongodb(
                processed_df,
                database=mongodb_database,
                collection=mongodb_collection,
                checkpoint_location=f"{checkpoint_location}/posts",
                output_mode="append",
            )

            # 2. Exploded ticker sentiment (if needed)
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
            logger.info(f"Streaming queries started, writing to MongoDB {mongodb_database}")
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