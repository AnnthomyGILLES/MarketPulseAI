import re
import logging
import unicodedata
import emoji
from typing import Dict, Set
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

logger = logging.getLogger(__name__)


class TextPreprocessor:
    """
    Preprocesses text for sentiment analysis by cleaning, normalizing,
    and preparing it for model input.
    """

    def __init__(self, config: Dict):
        """
        Initialize the text preprocessor with configuration.

        Args:
            config: Configuration dictionary with preprocessing parameters
        """
        self.config = config

        # Download NLTK resources if needed
        try:
            nltk.data.find("tokenizers/punkt")
            nltk.data.find("corpora/stopwords")
            nltk.data.find("corpora/wordnet")
        except LookupError:
            logger.info("Downloading required NLTK resources...")
            nltk.download("punkt")
            nltk.download("stopwords")
            nltk.download("wordnet")

        # Initialize components
        self.lemmatizer = (
            WordNetLemmatizer() if config.get("use_lemmatization", True) else None
        )

        # Get English stopwords and add financial/trading specific stopwords
        self.stopwords = (
            set(stopwords.words("english"))
            if config.get("remove_stopwords", True)
            else set()
        )
        if config.get("remove_stopwords", True) and config.get(
            "use_finance_stopwords", True
        ):
            self.stopwords.update(self._get_finance_stopwords())

        # Compile regex patterns for efficiency
        self.url_pattern = re.compile(r"https?://\S+|www\.\S+")
        self.html_pattern = re.compile(r"<.*?>")
        self.mention_pattern = re.compile(r"@\w+")
        self.hashtag_pattern = re.compile(r"#(\w+)")
        self.extra_spaces_pattern = re.compile(r"\s+")
        self.special_chars_pattern = re.compile(r"[^\w\s]")
        self.number_pattern = re.compile(r"\d+")

        logger.info("TextPreprocessor initialized successfully")

    def preprocess(self, text: str) -> str:
        """
        Apply all preprocessing steps to the input text.

        Args:
            text: Raw input text

        Returns:
            Preprocessed text ready for sentiment analysis
        """
        if not text or not isinstance(text, str):
            logger.warning(f"Invalid text input: {type(text)}")
            return ""

        # Apply preprocessing steps sequentially
        processed_text = text

        # Convert to lowercase if configured
        if self.config.get("lowercase", True):
            processed_text = processed_text.lower()

        # Remove URLs
        if self.config.get("remove_urls", True):
            processed_text = self.url_pattern.sub(" ", processed_text)

        # Remove HTML tags
        if self.config.get("remove_html", True):
            processed_text = self.html_pattern.sub(" ", processed_text)

        # Handle mentions (@ symbols)
        if self.config.get("remove_mentions", True):
            processed_text = self.mention_pattern.sub(" ", processed_text)
        elif self.config.get("anonymize_mentions", False):
            processed_text = self.mention_pattern.sub(" @user ", processed_text)

        # Handle hashtags
        if self.config.get("extract_hashtags", True):
            # Replace hashtags with the word (e.g., #Bitcoin -> Bitcoin)
            processed_text = self.hashtag_pattern.sub(r" \1 ", processed_text)
        elif self.config.get("remove_hashtags", False):
            processed_text = self.hashtag_pattern.sub(" ", processed_text)

        # Convert emojis to text if configured
        if self.config.get("convert_emojis", True):
            processed_text = self._convert_emojis(processed_text)

        # Normalize Unicode characters
        if self.config.get("normalize_unicode", True):
            processed_text = unicodedata.normalize("NFKD", processed_text)
            processed_text = "".join(
                [c for c in processed_text if not unicodedata.combining(c)]
            )

        # Remove special characters
        if self.config.get("remove_special_chars", True):
            processed_text = self.special_chars_pattern.sub(" ", processed_text)

        # Handle numbers
        if self.config.get("remove_numbers", False):
            processed_text = self.number_pattern.sub(" ", processed_text)

        # Tokenize, remove stopwords, and lemmatize if configured
        if any(
            [
                self.config.get("remove_stopwords", True),
                self.config.get("use_lemmatization", True),
            ]
        ):
            tokens = word_tokenize(processed_text)

            # Remove stopwords if configured
            if self.config.get("remove_stopwords", True):
                tokens = [
                    token for token in tokens if token.lower() not in self.stopwords
                ]

            # Lemmatize if configured
            if self.config.get("use_lemmatization", True) and self.lemmatizer:
                tokens = [self.lemmatizer.lemmatize(token) for token in tokens]

            processed_text = " ".join(tokens)

        # Remove extra whitespace
        processed_text = self.extra_spaces_pattern.sub(" ", processed_text).strip()

        # Log preprocessing stats if debug is enabled
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"Preprocessing reduced text from {len(text)} to {len(processed_text)} chars"
            )

        return processed_text

    def _convert_emojis(self, text: str) -> str:
        """Convert emojis to text representations for better sentiment capture."""
        return emoji.demojize(text).replace(":", " ").replace("_", " ")

    def _get_finance_stopwords(self) -> Set[str]:
        """Return a set of finance-specific stopwords to filter out."""
        # Common finance-specific terms that don't carry sentiment
        finance_stopwords = {
            "stock",
            "stocks",
            "market",
            "markets",
            "trade",
            "trading",
            "invest",
            "investor",
            "investors",
            "investment",
            "investments",
            "share",
            "shares",
            "price",
            "prices",
            "value",
            "values",
            "profit",
            "profits",
            "loss",
            "losses",
            "revenue",
            "revenues",
            "dividend",
            "dividends",
            "earning",
            "earnings",
            "quarter",
            "quarterly",
            "annual",
            "annually",
            "fiscal",
            "financial",
            "finance",
            "company",
            "companies",
            "corporation",
            "corporations",
            "inc",
            "llc",
            "ltd",
            "exchange",
            "nasdaq",
            "nyse",
            "dow",
            "jones",
            "sp500",
            "s&p",
            "etf",
            "etfs",
            "fund",
            "funds",
            "index",
            "indices",
            "bond",
            "bonds",
            "treasury",
            "treasuries",
            "yield",
            "yields",
            "rate",
            "rates",
            "fed",
            "federal",
            "reserve",
            "sec",
            "dollar",
            "euro",
            "yen",
            "currency",
            "currencies",
            "forex",
            "broker",
            "brokers",
            "portfolio",
            "portfolios",
            "asset",
            "assets",
            "liability",
            "liabilities",
            "balance",
            "sheet",
            "cash",
            "flow",
            "eps",
            "pe",
            "ratio",
            "ratios",
            "technical",
            "fundamental",
            "analysis",
            "analyst",
            "analysts",
            "recommendation",
            "target",
            "outlook",
            "guidance",
            "forecast",
            "forecasts",
            "projection",
            "projections",
            "estimate",
            "estimates",
            "volatility",
        }
        return finance_stopwords
