# NewsAPI Integration for MarketPulseAI

This document provides an overview of the NewsAPI integration with MarketPulseAI, explaining the architecture, data flow, and how to set up and use the integration.

## Overview

The NewsAPI integration enables MarketPulseAI to collect financial news articles from various sources through the [NewsAPI.org](https://newsapi.org/) service. This adds a valuable data source for sentiment analysis alongside social media data, providing a more comprehensive view of market sentiment.

## Architecture

The NewsAPI integration follows a streaming architecture pattern:

1. **Collection**: The `NewsApiCollector` fetches articles from NewsAPI.org
2. **Validation**: Articles are validated and enriched with metadata
3. **Streaming**: Valid articles are published to Kafka for real-time processing
4. **Storage**: Articles are consumed from Kafka and stored in MongoDB
5. **Analysis**: Sentiment analysis processes the stored articles
6. **Visualization**: Insights are displayed in dashboards

![Architecture Diagram](../docs/architecture/images/news_api_flow.png)

## Data Flow

1. **NewsAPI.org → NewsApiCollector**: 
   - Financial news articles are fetched based on configured queries
   - Rate limits are respected through intelligent throttling

2. **NewsApiCollector → Kafka**:
   - Articles are validated using Pydantic models
   - Stock symbols and categories are extracted from content
   - Relevance scores are calculated
   - Validated articles are published to the `news-articles` Kafka topic

3. **Kafka → MongoDB**:
   - The `news-mongodb-consumer` consumes from the `news-articles` topic
   - Articles are stored in the `news_articles` collection in MongoDB
   - Duplicate articles are automatically detected and skipped

4. **MongoDB → Sentiment Analysis**:
   - Unprocessed articles are retrieved for sentiment analysis
   - Sentiment scores are calculated and stored back in MongoDB
   - Aggregated sentiment data is stored in `news_sentiment_timeseries`

5. **Sentiment Data → Visualization**:
   - Dashboards query MongoDB for news sentiment
   - Sentiment is displayed alongside market data for correlation analysis

## Setup Instructions

### Prerequisites

- MarketPulseAI system up and running
- NewsAPI.org API key - [Get one here](https://newsapi.org/register)
- Docker and Docker Compose

### Configuration

1. **Set Environment Variables**:
   Create or update your `.env` file with your NewsAPI key:
   ```
   NEWSAPI_KEY=your_api_key_here
   ```

2. **Configure Collection Parameters**:
   Edit `config/news_api_config.yaml` to customize:
   - Search queries
   - News sources
   - Collection frequency
   - Validation rules

3. **MongoDB Schema**:
   The schema for news articles is automatically initialized with MongoDB startup using the script in `config/mongodb/news_schema.js`.

### Running the Integration

1. **Start the Services**:
   ```bash
   docker-compose up -d news-collector news-mongodb-consumer
   ```

2. **Monitor Collection**:
   ```bash
   docker-compose logs -f news-collector
   ```

3. **Monitor Storage**:
   ```bash
   docker-compose logs -f news-mongodb-consumer
   ```

## Data Model

### News Article Schema

The news articles are stored in MongoDB with the following structure:

```json
{
  "article_id": "unique_hash_id",
  "source_id": "bloomberg",
  "source_name": "Bloomberg",
  "author": "John Smith",
  "title": "Market Outlook for Q3 2023",
  "description": "Analysis of market trends for the coming quarter",
  "url": "https://example.com/article",
  "image_url": "https://example.com/image.jpg",
  "published_at": "2023-06-15T14:30:00Z",
  "content": "Article content...",
  "symbols": ["AAPL", "MSFT", "GOOGL"],
  "categories": ["market", "tech", "earnings"],
  "relevance_score": 0.85,
  "collection_timestamp": "2023-06-15T15:00:00Z",
  "processed": true,
  "sentiment": {
    "score": 0.75,
    "magnitude": 0.6,
    "label": "positive",
    "processed_at": "2023-06-15T15:10:00Z"
  }
}
```

### Sentiment Timeseries Schema

Aggregated sentiment data is stored with this structure:

```json
{
  "timestamp": "2023-06-15T16:00:00Z",
  "symbol": "AAPL",
  "sentiment_score": 0.65,
  "sentiment_magnitude": 0.8,
  "article_count": 5,
  "sources": ["Bloomberg", "Reuters", "CNBC"],
  "article_ids": ["id1", "id2", "id3", "id4", "id5"]
}
```

## Managing the System

### Rate Limiting

The NewsAPI free tier has a limit of 100 requests per day. The collector is configured to:
- Wait at least 60 seconds between requests
- Schedule full collection runs at configurable intervals
- Maintain a persistence store to avoid processing duplicates

### Scaling Considerations

- **Vertical Scaling**: Increase memory limits in docker-compose.yml
- **Horizontal Scaling**: For production, implement Kafka partitioning
- **API Limits**: For higher volume, upgrade to a paid NewsAPI plan

### Troubleshooting

Common issues and solutions:

1. **No Data Collected**:
   - Verify your NewsAPI key is valid and properly set
   - Check the collector logs for rate limit messages
   - Ensure your search queries match available news

2. **Duplicate Articles**:
   - This is normal - the system detects and avoids duplicates
   - Check the MongoDB collection to confirm data is being stored

3. **High Error Rate**:
   - Review the validation settings in config
   - Check error logs to identify patterns in rejected articles

## Extending the System

The NewsAPI integration is designed to be extensible:

1. **Add New Sources**:
   - Implement new collector classes following the same pattern
   - Reuse the validation and entity extraction components

2. **Custom Entity Extraction**:
   - Update the stock symbols list in `data/symbols/stock_symbols.txt`
   - Enhance the entity extractor with more advanced NLP

3. **Advanced Sentiment Analysis**:
   - Integrate with specialized financial sentiment models
   - Implement fine-grained entity-level sentiment analysis

## API Reference

### NewsApiCollector

```python
collector = NewsApiCollector(
    api_key="your_api_key",
    bootstrap_servers="kafka:9092",
    topic_name="news-articles",
    queries=["stock market", "financial news"],
    days_to_fetch=7
)
collector.collect()
```

### NewsArticleRepository

```python
repo = NewsArticleRepository(
    connection_string="mongodb://user:pass@host:port/",
    database_name="social_media",
    collection_name="news_articles"
)
articles = repo.get_articles_by_symbol("AAPL", days=7, limit=10)
```

## Further Reading

- [NewsAPI Documentation](https://newsapi.org/docs)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [MongoDB Documentation](https://docs.mongodb.com/)
- [Sentiment Analysis in MarketPulseAI](./sentiment_analysis.md) 