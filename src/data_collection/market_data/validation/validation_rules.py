from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, Optional


class ValidationRules:
    """Business rules for validating market data beyond schema validation."""

    @staticmethod
    def check_price_consistency(data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Check if OHLC prices are consistent with each other."""
        # Skip if not all required fields present
        if not all(key in data for key in ["open", "high", "low", "close"]):
            return True, None

        # Skip if any value is None
        if any(data[key] is None for key in ["open", "high", "low", "close"]):
            return True, None

        errors = []
        # Check if high is highest
        if (
            data["high"] < data["open"]
            or data["high"] < data["close"]
            or data["high"] < data["low"]
        ):
            errors.append("High price is not the highest value")

        # Check if low is lowest
        if (
            data["low"] > data["open"]
            or data["low"] > data["close"]
            or data["low"] > data["high"]
        ):
            errors.append("Low price is not the lowest value")

        if errors:
            return False, "; ".join(errors)

        return True, None

    @staticmethod
    def check_timestamp_recency(
        data: Dict[str, Any], max_age_hours: int = 24
    ) -> Tuple[bool, Optional[str]]:
        """Check if the data is recent enough."""
        try:
            timestamp = datetime.fromisoformat(data["timestamp"])
            current_time = datetime.now()
            max_age = timedelta(hours=max_age_hours)

            if current_time - timestamp > max_age:
                return False, f"Data is too old (older than {max_age_hours} hours)"

            # Check if timestamp is in the future
            if timestamp > current_time + timedelta(
                minutes=5
            ):  # 5-minute buffer for clock skew
                return False, "Data timestamp is in the future"

            return True, None
        except (KeyError, ValueError):
            return False, "Invalid timestamp format"

    @staticmethod
    def check_price_range(data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Check if prices are within realistic ranges for the symbol."""
        # This would ideally use historical data for the symbol to determine realistic ranges
        # For now, using simplified rules based on price level

        if "close" not in data or data["close"] is None:
            return True, None

        close_price = data["close"]

        # Very primitive price range check - would be much more sophisticated in production
        if close_price > 0:
            # Check for extreme outliers based on price level
            if close_price < 1 and close_price > 0.001:
                return True, None
            elif close_price < 1000 and close_price >= 1:
                return True, None
            elif close_price < 10000 and close_price >= 1000:
                return True, None
            else:
                return (
                    False,
                    f"Price {close_price} is outside expected range for most stocks",
                )
        else:
            return False, "Price must be positive"

    @staticmethod
    def check_for_sudden_change(
        data: Dict[str, Any], previous_data: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, Optional[str]]:
        """Check for suspicious sudden changes in price."""
        if previous_data is None or "close" not in data or "close" not in previous_data:
            return True, None

        if data["close"] is None or previous_data["close"] is None:
            return True, None

        # Calculate percentage change
        price_change_pct = (
            abs(data["close"] - previous_data["close"]) / previous_data["close"] * 100
        )

        # Alert on changes above 20% (configure based on symbol volatility in production)
        if price_change_pct > 20:
            return False, f"Suspicious price change of {price_change_pct:.2f}%"

        return True, None
