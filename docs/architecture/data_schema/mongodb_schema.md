# MongoDB Schema Documentation for MarketPulseAI

This document outlines the MongoDB schema used within the MarketPulseAI system, focusing on data storage for social media content and sentiment analysis results.

## Overview

MongoDB is used as the document storage solution within MarketPulseAI for handling unstructured social media content, sentiment analysis results, and entity relationships. Its flexible schema capabilities are ideal for storing varied content types while enabling efficient querying and aggregation operations.

## Database: `social_media`

The primary database used for social content storage and analysis.

### Collection: `reddit_sentiment`

This collection stores processed Reddit posts with comprehensive sentiment analysis results.

#### Schema Structure

| Field | Type | Description |
|-------|------|-------------|
| `_id` | ObjectId | MongoDB's internal document identifier |
| `id` | String | Original Reddit post ID |
| `source` | String | Content source platform ("reddit") |
| `content_type` | String | Type of content ("post" vs "comment") |
| `collection_timestamp` | String (ISO 8601) | When the data was collected by the system |
| `created_utc` | Integer | Original creation time of the post (Unix timestamp) |
| `created_datetime` | String (ISO 8601) | Original creation time in readable format |
| `processing_timestamp` | Date | When sentiment analysis was performed |
| `author` | String | Username who created the post |
| `score` | Integer | Number of upvotes (minus downvotes) |
| `subreddit` | String | Community where the post was published |
| `permalink` | String | Reddit URL path to the post |
| `detected_symbols` | Array[String] | Stock symbols detected in the text |
| `title` | String | Post title |
| `selftext` | String | Post body content |
| `url` | String | Full URL to the post |
| `is_self` | Boolean | Whether this is a text post (true) vs link post (false) |
| `upvote_ratio` | Float | Percentage of upvotes vs total votes (0.0-1.0) |
| `num_comments` | Integer | Total comments on the post |
| `collection_method` | String | How the post was collected (e.g., "top/week") |
| `preprocessed` | Object | Cleaned and normalized data for analysis |
| `fast_sentiment` | Object | Results from VADER sentiment analysis |
| `deep_sentiment` | Object | Results from FinBERT model |
| `sentiment` | Object | Combined sentiment results |
| `ticker_sentiments` | Array[Object] | Array of ticker-specific sentiment objects |
| `sentiment_label` | String | Overall sentiment classification |
| `sentiment_score` | Float | Final sentiment score value |
| `sentiment_confidence` | Float | Confidence in the sentiment prediction |
| `market_impact_estimate` | String | Estimated market impact ("low", "moderate", "high") |
| `topics` | Array[String] | Topics identified in the content (optional) |

#### Nested Object: `preprocessed`

| Field | Type | Description |
|-------|------|-------------|
| `subreddit` | String | Lowercase subreddit name |
| `cleaned_text` | String | Text after preprocessing (URLs removed, etc.) |
| `score` | String | Post score as string |
| `upvote_ratio` | String | Upvote ratio as string |
| `num_comments` | String | Number of comments as string |
| `tickers` | String | Comma-separated ticker symbols extracted from text |

#### Nested Object: `fast_sentiment` (VADER)

| Field | Type | Description |
|-------|------|-------------|
| `pos` | Float | Proportion of text with positive sentiment (0.0-1.0) |
| `neg` | Float | Proportion of text with negative sentiment (0.0-1.0) |
| `neu` | Float | Proportion of text with neutral sentiment (0.0-1.0) |
| `compound` | Float | Compound sentiment score (-1.0 to 1.0) |
| `sentiment` | String | VADER's sentiment classification |

#### Nested Object: `deep_sentiment` (FinBERT)

| Field | Type | Description |
|-------|------|-------------|
| `Very Bearish` | Float | Probability of very bearish sentiment |
| `Bearish` | Float | Probability of bearish sentiment |
| `Neutral` | Float | Probability of neutral sentiment |
| `Bullish` | Float | Probability of bullish sentiment |
| `Very Bullish` | Float | Probability of very bullish sentiment |
| `score` | Float | Normalized sentiment score |
| `sentiment` | String | FinBERT's sentiment classification |

#### Nested Object: `sentiment` (Ensemble)

| Field | Type | Description |
|-------|------|-------------|
| `score` | Float | Combined sentiment score from all models |
| `label` | String | Final sentiment classification |
| `confidence` | Float | Confidence level in the sentiment determination |

#### Example Document

```json
{
  "_id": {
    "$oid": "6807e5260398a43e09593344"
  },
  "id": "1jygokx",
  "source": "reddit",
  "content_type": "post",
  "collection_timestamp": "2025-04-19T01:11:03.891446Z",
  "created_utc": 1744574736,
  "created_datetime": "2025-04-13T20:05:36Z",
  "processing_timestamp": {
    "$date": "2025-04-22T18:39:40.348Z"
  },
  "author": "postpartum-blues",
  "score": 4759,
  "subreddit": "investing",
  "permalink": "https://www.reddit.com/r/investing/comments/1jygokx/there_was_no_tariff_exception_announced_on_friday/",
  "detected_symbols": [],
  "title": "\"There was no tariff 'exception' announced on Friday.\" Donald Trump",
  "selftext": "What the actual fuck? How is anyone supposed to do business under this administration? Literally in under 3 days we went from exceptions announced for smartphones, laptop computers, hard drives and computer processors to having that pulled back because of one schizophrenic TruthSocial post? \n\nhttps://truthsocial.com/@realDonaldTrump/posts/114332337028519855",
  "url": "https://www.reddit.com/r/investing/comments/1jygokx/there_was_no_tariff_exception_announced_on_friday/",
  "is_self": true,
  "upvote_ratio": 0.9700000286102295,
  "num_comments": 532,
  "collection_method": "top/week",
  "preprocessed": {
    "subreddit": "investing",
    "cleaned_text": "\"there was no tariff 'exception' announced on friday.\" donald trump what the actual fuck? how is anyone supposed to do business under this administration? literally in under 3 days we went from exceptions announced for smartphones, laptop computers, hard drives and computer processors to having that pulled back because of one schizophrenic truthsocial post? \n\n",
    "score": "4759",
    "upvote_ratio": "0.9700000286102295",
    "num_comments": "532",
    "tickers": ""
  },
  "fast_sentiment": {
    "neu": 0.8700000047683716,
    "compound": -0.7677000164985657,
    "sentiment": null,
    "pos": 0,
    "neg": 0.12999999523162842
  },
  "deep_sentiment": {
    "Very Bearish": 0.20000000298023224,
    "Very Bullish": 0.20000000298023224,
    "Neutral": 0.20000000298023224,
    "Bearish": 0.20000000298023224,
    "sentiment": null,
    "score": 0.5,
    "Bullish": 0.20000000298023224
  },
  "sentiment": {
    "score": 0.1160992980003357,
    "label": null,
    "confidence": 0.6160992980003357
  },
  "ticker_sentiments": [],
  "sentiment_label": null,
  "sentiment_score": 0.1160992980003357,
  "sentiment_confidence": 0.6160992980003357,
  "market_impact_estimate": "low"
}
```

### Collection: `ticker_sentiment`

This collection stores ticker-specific sentiment extracted from Reddit posts, providing a more focused view for analysis by stock symbol.

#### Schema Structure

| Field | Type | Description |
|-------|------|-------------|
| `_id` | ObjectId | MongoDB's internal document identifier |
| `id` | String | Original Reddit post ID |
| `processing_timestamp` | Date | When sentiment analysis was performed |
| `ticker` | String | Stock symbol/ticker |
| `sentiment` | String | Sentiment classification for this ticker |
| `score` | String | Sentiment score specific to this ticker |
| `confidence` | String | Confidence level in the ticker sentiment |

#### Example Document

```json
{
  "_id": {
    "$oid": "6807e5270398a43e09593347"
  },
  "id": "1jygokx",
  "processing_timestamp": {
    "$date": "2025-04-22T18:39:40.348Z"
  },
  "ticker": "AAPL",
  "sentiment": "Bearish",
  "score": "-0.42",
  "confidence": "0.76"
}
```

## Indices

The following indices are recommended for optimal performance:

### `reddit_sentiment` Collection

- `{id: 1}` - For looking up specific posts
- `{created_utc: 1}` - For time-based queries
- `{subreddit: 1, created_utc: -1}` - For subreddit-specific time-based browsing
- `{sentiment_label: 1, sentiment_confidence: -1}` - For finding high-confidence sentiment
- `{processing_timestamp: 1}` - For tracking processing pipeline

### `ticker_sentiment` Collection

- `{ticker: 1, processing_timestamp: -1}` - For ticker-specific recent sentiment
- `{sentiment: 1, confidence: -1}` - For finding strong sentiment signals
- `{id: 1}` - For linking back to original posts

## Usage Patterns

The MongoDB collections are designed for these primary access patterns:

1. Retrieving recent sentiment for specific stocks
2. Analyzing sentiment trends over time
3. Finding posts with strong sentiment signals
4. Aggregating sentiment by subreddit or time period
5. Correlating sentiment with market movements

## Schema Evolution Strategy

The schema is designed to be flexible to accommodate future improvements:

1. New fields can be added without disrupting existing queries
2. Sentiment model improvements will add fields rather than change existing ones
3. Versioning can be handled through collection naming (e.g., `reddit_sentiment_v2`)

## Configuration

The MongoDB connection is configured in the sentiment processor with these parameters:

```yaml
# MongoDB configuration
mongodb:
  connection_host: "mongodb"
  connection_port: 27017
  database: "social_media"
  collection: "reddit_sentiment"
  ticker_collection: "ticker_sentiment"
  auth_username: "mongodb_user"
  auth_password: "mongodb_password"
``` 