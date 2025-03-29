# src/data_collection/market_data/validation/validation_schema.py

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, validator


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

    @validator("symbol")
    def validate_symbol(cls, v):
        if not v or not isinstance(v, str) or len(v) > 10:
            raise ValueError("Symbol must be a non-empty string of max 10 characters")
        return v.upper()

    @validator("timestamp", "collection_timestamp")
    def validate_timestamp(cls, v):
        try:
            # Validate ISO format
            datetime.fromisoformat(v)
            return v
        except ValueError:
            try:
                # Try parsing as milliseconds timestamp
                if isinstance(v, (int, float)):
                    datetime.fromtimestamp(v / 1000)
                    return datetime.fromtimestamp(v / 1000).isoformat()
                raise ValueError("Invalid timestamp format")
            except (ValueError, TypeError):
                raise ValueError("Invalid timestamp format")

    @validator("open", "high", "low", "close", "vwap")
    def validate_prices(cls, v):
        if v is not None and (v <= 0 or v > 1000000):
            raise ValueError("Price values must be positive and reasonable")
        return v

    @validator("volume", "transactions")
    def validate_counts(cls, v):
        if v is not None and (v < 0 or v > 10000000000):
            raise ValueError("Count values must be non-negative and reasonable")
        return v

    class Config:
        extra = "forbid"  # Forbid extra fields not defined in the schema
