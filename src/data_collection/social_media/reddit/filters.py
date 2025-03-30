import json
import logging
import re

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("reddit_filter")


class RedditContentFilter:
    """
    Filters Reddit content to identify financial relevance and stock symbols.
    """

    def __init__(self, config=None):
        """
        Initialize the content filter.

        Args:
            config (dict, optional): Configuration dictionary.
        """
        self.config = config or {}

        # Download required NLTK data
        try:
            nltk.download("punkt", quiet=True)
            nltk.download("stopwords", quiet=True)
        except Exception as e:
            logger.warning(f"Failed to download NLTK data: {str(e)}")

        # Load financial terms and stock symbols
        self._load_financial_terms()
        self._load_stock_symbols()

        # Compile regex patterns
        self._compile_patterns()

        logger.info("Content filter initialized")

    def _load_financial_terms(self):
        """Load financial terms for relevance filtering."""
        # This could be loaded from a file, database, or API in production
        self.financial_terms = [
            "stock",
            "market",
            "invest",
            "trading",
            "shares",
            "bull",
            "bear",
            "options",
            "calls",
            "puts",
            "earnings",
            "price",
            "rally",
            "crash",
            "dividend",
            "portfolio",
            "broker",
            "etf",
            "fund",
            "analyst",
            "short",
            "long",
            "buy",
            "sell",
            "hold",
            "resistance",
            "support",
            "volatility",
            "volume",
            "trend",
            "bullish",
            "bearish",
            "moon",
            "tendies",
            "yolo",
            "gains",
            "loss",
            "nio",
            "tsla",
            "amc",
            "gme",
            "squeeze",
            "margin",
            "ipo",
            "spac",
            "merger",
            "acquisition",
        ]
        logger.info(f"Loaded {len(self.financial_terms)} financial terms")

    def _load_stock_symbols(self):
        """Load stock symbols for identification."""
        # In production, this should load from a file or database with all symbols
        # For now, we'll use a small sample of popular symbols
        self.stock_symbols = [
            "AAPL",
            "MSFT",
            "AMZN",
            "GOOGL",
            "GOOG",
            "FB",
            "TSLA",
            "BRK.A",
            "BRK.B",
            "JPM",
            "V",
            "JNJ",
            "WMT",
            "MA",
            "PG",
            "UNH",
            "DIS",
            "HD",
            "BAC",
            "XOM",
            "NVDA",
            "PYPL",
            "INTC",
            "CMCSA",
            "VZ",
            "ADBE",
            "NFLX",
            "CRM",
            "KO",
            "PEP",
            "GME",
            "AMC",
            "BB",
            "NOK",
            "PLTR",
            "NIO",
            "COIN",
            "HOOD",
            "SPY",
            "QQQ",
        ]
        # Create a set for faster lookups
        self.stock_symbol_set = set(self.stock_symbols)
        logger.info(f"Loaded {len(self.stock_symbols)} stock symbols")

    def _compile_patterns(self):
        """Compile regex patterns for symbol extraction."""
        # Pattern for $TICKER mentions
        self.ticker_pattern = re.compile(r"\$([A-Z]{1,5}(?:\.[A-Z])?)(?=[^A-Z]|$)")

        # Pattern for plain TICKER mentions (must be all caps and 1-5 letters)
        self.plain_ticker_pattern = re.compile(
            r"\b([A-Z]{1,5}(?:\.[A-Z])?)(?=[^A-Z]|$)"
        )

    def is_financially_relevant(self, text, title=None, threshold=0.05):
        """
        Determine if the content is financially relevant.

        Args:
            text (str): Main text content
            title (str, optional): Title text
            threshold (float): Minimum ratio of financial terms to consider relevant

        Returns:
            bool: Whether the content is financially relevant
        """
        if not text and not title:
            return False

        combined_text = f"{title} {text}" if title else text

        # Tokenize and lowercase
        try:
            tokens = word_tokenize(combined_text.lower())
            # Remove stopwords
            stop_words = set(stopwords.words("english"))
            tokens = [token for token in tokens if token not in stop_words]

            # Count financial terms
            financial_term_count = sum(
                1
                for token in tokens
                if any(term in token for term in self.financial_terms)
            )

            # Calculate ratio
            if len(tokens) > 0:
                financial_ratio = financial_term_count / len(tokens)
                return financial_ratio >= threshold
            return False
        except Exception as e:
            logger.error(f"Error checking financial relevance: {str(e)}")
            # Fallback to simple substring check
            combined_text = combined_text.lower()
            return any(term in combined_text for term in self.financial_terms)

    def extract_stock_symbols(self, text, title=None):
        """
        Extract stock symbols from content.

        Args:
            text (str): Main text content
            title (str, optional): Title text

        Returns:
            list: List of extracted stock symbols
        """
        if not text and not title:
            return []

        combined_text = f"{title} {text}" if title else text
        symbols = set()

        # Extract $TICKER mentions
        ticker_mentions = re.findall(self.ticker_pattern, combined_text)
        for ticker in ticker_mentions:
            # Add to set (automatically de-duplicates)
            symbols.add(ticker.upper())

        # Extract plain TICKER mentions (all caps)
        # This is more prone to false positives, so we check against known symbols
        plain_tickers = re.findall(self.plain_ticker_pattern, combined_text)
        for ticker in plain_tickers:
            if ticker in self.stock_symbol_set:
                symbols.add(ticker)

        return list(symbols)

    def filter_content(self, content):
        """
        Filter Reddit content and enrich with financial metadata.

        Args:
            content (dict): Reddit content (post or comment)

        Returns:
            dict: Filtered and enriched content, or None if not relevant
        """
        try:
            # Determine text fields based on content type
            if content.get("content_type") == "post":
                title = content.get("title", "")
                text = content.get("selftext", "")
            else:  # comment
                title = None
                text = content.get("body", "")

            # Check if content is financially relevant
            is_relevant = self.is_financially_relevant(text, title)

            # Skip if not relevant
            if not is_relevant:
                return None

            # Extract stock symbols
            symbols = self.extract_stock_symbols(text, title)

            # Enrich content with financial metadata
            enriched_content = content.copy()
            enriched_content.update(
                {
                    "is_financially_relevant": is_relevant,
                    "stock_symbols": symbols,
                    "symbol_count": len(symbols),
                }
            )

            return enriched_content

        except Exception as e:
            logger.error(f"Error filtering content: {str(e)}")
            return None


# Example usage
if __name__ == "__main__":
    # Test the filter
    filter = RedditContentFilter()

    test_post = {
        "content_type": "post",
        "title": "AAPL is going to the moon! $TSLA is next!",
        "selftext": "I think Apple stock is undervalued. Tesla might also rally soon.",
        "id": "test123",
    }

    result = filter.filter_content(test_post)
    print(json.dumps(result, indent=2))
