# src/utils/config.py
from dataclasses import dataclass
from typing import Dict


@dataclass
class SentimentConfig:
    model_type: str
    finbert_model_path: str
    max_sequence_length: int
    finance_lexicon_path: str
    ensemble_weights: Dict[str, float]
    max_workers: int
    enable_caching: bool
    cache_ttl: int


@dataclass
class AggregationConfig:
    source_weights: Dict[str, float]
    time_decay_factor: float
    max_age_hours: int
    confidence_data_points: int
    default_ticker_count: int
    max_records: int
    min_change_threshold: float
    shift_detection_threshold: float


# Add other config classes...


@dataclass
class PipelineConfig:
    sentiment: SentimentConfig
    aggregation: AggregationConfig
    # kafka: KafkaConfig
    # redis: RedisConfig
    # database: DatabaseConfig
    # preprocessing: PreprocessingConfig
    # batch_processing: BatchProcessingConfig


def load_config(config_dict: Dict) -> PipelineConfig:
    """Load and validate configuration from dictionary."""
    sentiment_dict = config_dict.get("sentiment_analysis", {})

    # Extract model config
    model_dict = sentiment_dict.get("model", {})
    model_type = model_dict.get("type", "ensemble")

    # Create SentimentConfig
    sentiment_config = SentimentConfig(
        model_type=model_type,
        finbert_model_path=model_dict.get("finbert", {}).get(
            "model_path", "models/finbert-sentiment"
        ),
        max_sequence_length=model_dict.get("finbert", {}).get(
            "max_sequence_length", 512
        ),
        finance_lexicon_path=model_dict.get("vader", {}).get(
            "finance_lexicon_path", ""
        ),
        ensemble_weights=model_dict.get("ensemble", {}).get(
            "weights", {"finbert": 0.6, "vader": 0.4}
        ),
        max_workers=sentiment_dict.get("performance", {}).get("max_workers", 4),
        enable_caching=sentiment_dict.get("performance", {}).get("cache_enabled", True),
        cache_ttl=sentiment_dict.get("performance", {}).get("cache_ttl", 3600),
    )

    # Create AggregationConfig
    aggregation_dict = config_dict.get("aggregation", {})
    aggregation_config = AggregationConfig(
        source_weights=aggregation_dict.get(
            "source_weights",
            {"twitter": 0.30, "reddit": 0.25, "news": 0.40, "other": 0.05},
        ),
        time_decay_factor=aggregation_dict.get("time_decay_factor", 0.05),
        max_age_hours=aggregation_dict.get("max_age_hours", 72),
        confidence_data_points=aggregation_dict.get("confidence_data_points", 20),
        default_ticker_count=aggregation_dict.get("default_ticker_count", 20),
        max_records=aggregation_dict.get("max_records", 1000),
        min_change_threshold=aggregation_dict.get("trend", {}).get(
            "min_change_threshold", 0.05
        ),
        shift_detection_threshold=aggregation_dict.get("trend", {}).get(
            "shift_detection_threshold", 0.15
        ),
    )

    # Return complete pipeline config
    return PipelineConfig(
        sentiment=sentiment_config,
        aggregation=aggregation_config,
        # kafka=kafka_config,
        # redis=redis_config,
        # database=database_config,
        # preprocessing=preprocessing_config,
        # batch_processing=batch_processing_config,
    )
