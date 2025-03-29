from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator, ConfigDict


class MarketDataSchema(BaseModel):
    """Schema for validating market data records."""

    symbol: str = Field(..., description="Stock ticker symbol")
    open: Optional[float] = Field(None, description="Opening price")
    high: Optional[float] = Field(None, description="Highest price")
    low: Optional[float] = Field(None, description="Lowest price")
    close: float = Field(..., description="Closing price or current price")
    volume: Optional[int] = Field(None, description="Trading volume")
    vwap: Optional[float] = Field(None, description="Volume weighted average price")
    timestamp: str = Field(..., description="Data timestamp")
    collection_timestamp: str = Field(..., description="When data was collected")
    transactions: Optional[int] = Field(None, description="Number of transactions")

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        if not v or len(v) > 10:
            raise ValueError("Symbol must be a non-empty string of max 10 characters")
        return v.upper()

    @field_validator("timestamp", "collection_timestamp")
    @classmethod
    def validate_timestamp(cls, v: str | int | float) -> str:
        try:
            if isinstance(v, str):
                datetime.fromisoformat(v)
                return v
            if isinstance(v, (int, float)):
                return datetime.fromtimestamp(v / 1000).isoformat()
        except (ValueError, TypeError):
            pass
        raise ValueError("Invalid timestamp format")

    @field_validator("open", "high", "low", "close", "vwap")
    @classmethod
    def validate_prices(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and (v <= 0 or v > 1_000_000):
            raise ValueError("Price values must be positive and reasonable")
        return v

    @field_validator("volume", "transactions")
    @classmethod
    def validate_counts(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and (v < 0 or v > 10_000_000_000):
            raise ValueError("Count values must be non-negative and reasonable")
        return v

    model_config = ConfigDict(
        extra="forbid"
    )  # Forbid extra fields not defined in the schema
