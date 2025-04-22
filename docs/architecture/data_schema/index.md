# MarketPulseAI Data Schema Documentation

This directory contains detailed documentation on the data schemas used in the MarketPulseAI system. These documents describe the structure, relationships, and access patterns for the various data stores used throughout the platform.

## Available Schema Documentation

| Schema | Description | Link |
|--------|-------------|------|
| MongoDB - Social Media | Documentation for MongoDB collections storing social media content and sentiment analysis | [MongoDB Schema](./mongodb_schema.md) |

## Schema Organization

The MarketPulseAI system uses a combination of specialized data stores optimized for different types of data:

1. **MongoDB** - Document store for unstructured social media content, sentiment analysis, and entity relationships
2. **TimescaleDB** _(planned)_ - Time-series database for market data and technical indicators
3. **Redis** _(planned)_ - In-memory store for real-time feature serving and caching

## Schema Design Principles

Our data schemas follow these core principles:

1. **Purpose-Specific Storage** - Using the right database for each data type
2. **Flexible Evolution** - Schemas can evolve without disrupting existing functionality
3. **Performance Optimization** - Indices and structure designed for common access patterns
4. **Minimal Duplication** - References used where appropriate to avoid data duplication
5. **Comprehensive Documentation** - All schemas are thoroughly documented

## How to Use This Documentation

Each schema document contains:

1. Complete field listings with types and descriptions
2. Examples of typical documents
3. Information about indices and performance optimizations
4. Common usage patterns
5. Configuration details 