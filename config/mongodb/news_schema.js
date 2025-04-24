// MongoDB schema for news articles in MarketPulseAI
db = db.getSiblingDB('social_media');

// Create collection with validation
db.createCollection('news_articles', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['article_id', 'source_name', 'title', 'url', 'published_at', 'collection_timestamp'],
      properties: {
        article_id: {
          bsonType: 'string',
          description: 'Unique identifier for the article'
        },
        source_id: {
          bsonType: ['string', 'null'],
          description: 'ID of the news source'
        },
        source_name: {
          bsonType: 'string',
          description: 'Name of the news source'
        },
        author: {
          bsonType: ['string', 'null'],
          description: 'Author of the article'
        },
        title: {
          bsonType: 'string',
          description: 'Article title'
        },
        description: {
          bsonType: ['string', 'null'],
          description: 'Brief description or summary of the article'
        },
        url: {
          bsonType: 'string',
          description: 'URL to the full article'
        },
        image_url: {
          bsonType: ['string', 'null'],
          description: 'URL to the article image'
        },
        published_at: {
          bsonType: 'date',
          description: 'Publication date and time'
        },
        content: {
          bsonType: ['string', 'null'],
          description: 'Article content or snippet'
        },
        symbols: {
          bsonType: 'array',
          description: 'Stock symbols mentioned in the article',
          items: {
            bsonType: 'string'
          }
        },
        categories: {
          bsonType: 'array',
          description: 'News categories',
          items: {
            bsonType: 'string'
          }
        },
        relevance_score: {
          bsonType: 'double',
          minimum: 0,
          maximum: 1,
          description: 'Relevance score between 0 and 1'
        },
        collection_timestamp: {
          bsonType: 'date',
          description: 'When the article was collected'
        },
        processed: {
          bsonType: 'bool',
          description: 'Whether sentiment analysis has been performed'
        },
        sentiment: {
          bsonType: ['object', 'null'],
          description: 'Sentiment analysis results',
          properties: {
            score: {
              bsonType: 'double',
              description: 'Overall sentiment score'
            },
            magnitude: {
              bsonType: 'double',
              description: 'Sentiment magnitude/strength'
            },
            label: {
              bsonType: 'string',
              enum: ['positive', 'negative', 'neutral', 'mixed'],
              description: 'Sentiment classification label'
            },
            processed_at: {
              bsonType: 'date',
              description: 'When sentiment analysis was performed'
            }
          }
        }
      }
    }
  }
});

// Create indices
db.news_articles.createIndex({ article_id: 1 }, { unique: true });
db.news_articles.createIndex({ url: 1 }, { unique: true });
db.news_articles.createIndex({ published_at: -1 });
db.news_articles.createIndex({ source_name: 1 });
db.news_articles.createIndex({ symbols: 1 });
db.news_articles.createIndex({ categories: 1 });
db.news_articles.createIndex({ relevance_score: -1 });
db.news_articles.createIndex({ 'sentiment.label': 1 });
db.news_articles.createIndex({ 'sentiment.score': -1 });
db.news_articles.createIndex({ processed: 1 });

// Create a text index for searching article content
db.news_articles.createIndex(
  { title: "text", description: "text", content: "text" },
  { 
    name: "news_text_search",
    weights: {
      title: 10,
      description: 5,
      content: 1
    },
    default_language: "english"
  }
);

// Create aggregated news sentiment collection for timeseries data
db.createCollection('news_sentiment_timeseries', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['timestamp', 'symbol', 'sentiment_score', 'article_count'],
      properties: {
        timestamp: {
          bsonType: 'date',
          description: 'Timestamp for the aggregated data point'
        },
        symbol: {
          bsonType: 'string',
          description: 'Stock symbol the sentiment applies to'
        },
        sentiment_score: {
          bsonType: 'double',
          description: 'Aggregated sentiment score'
        },
        sentiment_magnitude: {
          bsonType: 'double',
          description: 'Aggregated sentiment magnitude'
        },
        article_count: {
          bsonType: 'int',
          description: 'Number of articles included in aggregation'
        },
        sources: {
          bsonType: 'array',
          description: 'News sources included in the aggregation',
          items: {
            bsonType: 'string'
          }
        },
        article_ids: {
          bsonType: 'array',
          description: 'IDs of articles included in aggregation',
          items: {
            bsonType: 'string'
          }
        }
      }
    }
  }
});

// Create indices for the sentiment timeseries collection
db.news_sentiment_timeseries.createIndex({ timestamp: -1, symbol: 1 });
db.news_sentiment_timeseries.createIndex({ symbol: 1, timestamp: -1 });
db.news_sentiment_timeseries.createIndex({ sentiment_score: -1 }); 