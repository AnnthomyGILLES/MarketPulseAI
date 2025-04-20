# src/visualization/streamlit_dashboard.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
import redis
from loguru import logger
from pathlib import Path
import yaml
from datetime import datetime, timedelta
import time
import json
import numpy as np


class MarketPulseDashboard:
    """Streamlit dashboard for MongoDB and Redis data visualization."""

    def __init__(self, config_path: str):
        """Initialize dashboard with configuration."""
        self.config = self._load_config(config_path)
        self.mongo_client = self._connect_mongodb()
        self.redis_client = self._connect_redis()
        self.setup_page_config()

    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file."""
        config_file = Path(config_path)
        if not config_file.exists():
            logger.error(f"Configuration file not found: {config_path}")
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_file, "r") as f:
            return yaml.safe_load(f)

    def _connect_mongodb(self):
        """Connect to MongoDB using configuration."""
        mongodb_config = self.config.get("mongodb", {})
        host = mongodb_config.get("connection_host", "mongodb")
        port = mongodb_config.get("connection_port", "27017")
        username = mongodb_config.get("auth_username", "mongodb_user")
        password = mongodb_config.get("auth_password", "mongodb_password")

        connection_string = (
            f"mongodb://{username}:{password}@{host}:{port}/?authSource=admin"
        )
        logger.info(f"Connecting to MongoDB at {host}:{port}")
        return MongoClient(connection_string)

    def _connect_redis(self):
        """Connect to Redis using configuration."""
        redis_config = self.config.get("redis", {})
        host = redis_config.get("host", "redis")
        port = redis_config.get("port", 6379)
        db = redis_config.get("db", 0)
        password = redis_config.get("password", None)

        try:
            logger.info(f"Connecting to Redis at {host}:{port}")
            return redis.Redis(
                host=host, port=port, db=db, password=password, decode_responses=True
            )
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {str(e)}")
            return None

    def setup_page_config(self):
        """Set up Streamlit page configuration."""
        st.set_page_config(
            page_title="MarketPulseAI Dashboard",
            page_icon="📊",
            layout="wide",
            initial_sidebar_state="expanded",
        )

    def get_mongodb_data(
        self,
        collection_name: str,
        database_name: str = None,
        query: dict = None,
        limit: int = 10000,
        sort_field: str = "created_timestamp",
        sort_direction: int = -1,
    ):
        """Fetch data from MongoDB collection."""
        if database_name is None:
            database_name = self.config.get("mongodb", {}).get(
                "database", "social_media"
            )

        db = self.mongo_client[database_name]
        collection = db[collection_name]

        query = query or {}
        logger.info(
            f"Fetching data from {database_name}.{collection_name} with query: {query}"
        )

        cursor = collection.find(query).sort(sort_field, sort_direction).limit(limit)
        return pd.DataFrame(list(cursor))

    def get_redis_data(self, pattern: str, as_dataframe=True):
        """Fetch data from Redis matching pattern."""
        if not self.redis_client:
            logger.warning("Redis client not available")
            return pd.DataFrame() if as_dataframe else []

        try:
            keys = self.redis_client.keys(pattern)
            if not keys:
                return pd.DataFrame() if as_dataframe else []

            data = []
            for key in keys:
                value = self.redis_client.get(key)
                if value:
                    try:
                        item = json.loads(value)
                        item["key"] = key
                        data.append(item)
                    except json.JSONDecodeError:
                        # Handle non-JSON values
                        data.append({"key": key, "value": value})

            return pd.DataFrame(data) if as_dataframe else data
        except Exception as e:
            logger.error(f"Error fetching data from Redis: {str(e)}")
            return pd.DataFrame() if as_dataframe else []

    def get_timeseries_data(self, key_pattern: str):
        """Get time series data from Redis."""
        if not self.redis_client:
            return pd.DataFrame()

        try:
            keys = self.redis_client.keys(key_pattern)
            if not keys:
                return pd.DataFrame()

            all_data = []
            for key in keys:
                # Get all items in the list
                items = self.redis_client.lrange(key, 0, -1)
                for item in items:
                    try:
                        data = json.loads(item)
                        # Extract entity from key (e.g., 'symbol' or 'subreddit')
                        entity_type = key_pattern.split(":")[2]
                        entity = key.split(":")[-1]
                        data[entity_type] = entity
                        all_data.append(data)
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON data in Redis key {key}")

            return pd.DataFrame(all_data)
        except Exception as e:
            logger.error(f"Error fetching time series data from Redis: {str(e)}")
            return pd.DataFrame()

    def render_metrics_section(self):
        """Render metrics section with real-time data."""
        st.header("Real-Time Metrics")

        # Get overall metrics from Redis
        total_posts_today = self.redis_client.get("stats:total_posts:today") or "0"
        total_posts_today = int(total_posts_today)

        overall_sentiment = self.redis_client.get("sentiment:overall:latest") or "0"
        if isinstance(overall_sentiment, str):
            try:
                overall_sentiment = float(overall_sentiment)
            except ValueError:
                overall_sentiment = 0.0

        # Get total posts from MongoDB for comparison
        db_name = self.config.get("mongodb", {}).get("database", "social_media")
        collection_name = "reddit_sentiment"
        
        # Get today's date at midnight
        today_midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_midnight = today_midnight - timedelta(days=1)
        
        try:
            db = self.mongo_client[db_name]
            collection = db[collection_name]
            
            # Count documents from the last 24 hours
            query_24h = {
                "created_timestamp": {
                    "$gte": yesterday_midnight, 
                    "$lt": today_midnight + timedelta(days=1)
                }
            }
            total_posts_24h = collection.count_documents(query_24h)
            
            # Count total documents
            total_posts_all = collection.count_documents({})
        except Exception as e:
            logger.error(f"Error querying MongoDB: {str(e)}")
            total_posts_24h = 0
            total_posts_all = 0

        # Display metrics in columns
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Total Posts Today",
                total_posts_today,
                delta=None,
                delta_color="normal",
            )

        with col2:
            sentiment_label = "Neutral"
            if overall_sentiment > 0.05:
                sentiment_label = "Positive"
            elif overall_sentiment < -0.05:
                sentiment_label = "Negative"
                
            st.metric(
                "Current Market Sentiment",
                f"{sentiment_label} ({overall_sentiment:.2f})",
                delta=None,
            )

        with col3:
            st.metric(
                "Posts in Last 24 Hours", 
                total_posts_24h,
                delta=None
            )

        with col4:
            st.metric("Total Historical Posts", total_posts_all, delta=None)

        # Create sentiment gauge
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=overall_sentiment,
            title={'text': "Market Sentiment"},
            gauge={
                'axis': {'range': [-1, 1], 'tickwidth': 1, 'tickcolor': "darkblue"},
                'bar': {'color': "darkblue"},
                'bgcolor': "white",
                'borderwidth': 2,
                'bordercolor': "gray",
                'steps': [
                    {'range': [-1, -0.05], 'color': 'red'},
                    {'range': [-0.05, 0.05], 'color': 'gray'},
                    {'range': [0.05, 1], 'color': 'green'},
                ],
            }
        ))
        
        fig.update_layout(height=250)
        st.plotly_chart(fig, use_container_width=True)

    def render_subreddit_analysis(self):
        """Render subreddit analysis section."""
        st.subheader("Subreddit Analysis")
        
        # Get real-time subreddit sentiment data from Redis
        subreddit_data = self.get_redis_data("sentiment:subreddit:*")
        
        # If no real-time data, fallback to MongoDB
        if subreddit_data.empty:
            db_name = self.config.get("mongodb", {}).get("database", "social_media")
            subreddit_data = self.get_mongodb_data("reddit_sentiment", db_name)
            
            if not subreddit_data.empty and "subreddit" in subreddit_data.columns:
                # Aggregate data
                subreddit_stats = (
                    subreddit_data.groupby("subreddit")
                    .agg({
                        "sentiment_score": "mean",
                        "id": "count",
                        "score": ["mean", "sum"],
                        "num_comments": ["mean", "sum"]
                    })
                    .reset_index()
                )
                subreddit_stats.columns = [
                    "subreddit", "avg_sentiment", "post_count", 
                    "avg_score", "total_score", "avg_comments", "total_comments"
                ]
            else:
                subreddit_stats = pd.DataFrame()
        else:
            # Process real-time data
            subreddit_stats = (
                subreddit_data.groupby("subreddit")
                .agg({
                    "sentiment_score": "mean",
                    "post_count": "sum"
                })
                .reset_index()
            )
            subreddit_stats = subreddit_stats.rename(
                columns={"sentiment_score": "avg_sentiment"}
            )
        
        if not subreddit_stats.empty:
            # Sort by post count for relevance
            top_subreddits = subreddit_stats.sort_values("post_count", ascending=False).head(10)
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(
                    top_subreddits,
                    x="subreddit",
                    y="post_count",
                    title="Top Subreddits by Post Count",
                    color="avg_sentiment",
                    color_continuous_scale="RdBu",
                    range_color=[-1, 1],
                )
                st.plotly_chart(fig, use_container_width=True)
                
            with col2:
                fig = px.bar(
                    top_subreddits,
                    x="subreddit",
                    y="avg_sentiment",
                    title="Subreddit Sentiment Analysis",
                    color="avg_sentiment",
                    color_continuous_scale="RdBu",
                    range_color=[-1, 1],
                )
                fig.update_layout(yaxis_range=[-1, 1])
                st.plotly_chart(fig, use_container_width=True)
                
            # Get time series data for subreddits
            timeseries_data = self.get_timeseries_data("timeseries:sentiment:subreddit:*")
            
            if not timeseries_data.empty and "timestamp" in timeseries_data.columns:
                timeseries_data["timestamp"] = pd.to_datetime(timeseries_data["timestamp"])
                
                # Filter to top 5 subreddits by post volume
                top5_subreddits = top_subreddits["subreddit"].head(5).tolist()
                filtered_data = timeseries_data[timeseries_data["subreddit"].isin(top5_subreddits)]
                
                if not filtered_data.empty:
                    # Plot time series
                    fig = px.line(
                        filtered_data, 
                        x="timestamp", 
                        y="sentiment_score", 
                        color="subreddit",
                        title="Sentiment Trends by Subreddit (Last Hour)",
                        markers=True
                    )
                    fig.update_layout(yaxis_range=[-1, 1])
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No subreddit data available")

    def render_symbol_analysis(self):
        """Render symbol analysis section."""
        st.subheader("Symbol Analysis")
        
        # Get real-time symbol sentiment data from Redis
        timeseries_data = self.get_timeseries_data("timeseries:sentiment:symbol:*")
        
        if not timeseries_data.empty:
            # Process data
            timeseries_data["timestamp"] = pd.to_datetime(timeseries_data["timestamp"])
            
            # Get symbol counts and average sentiment
            symbol_stats = (
                timeseries_data.groupby("symbol")
                .agg({
                    "sentiment_score": "mean",
                    "symbol": "count"
                })
                .reset_index()
            )
            symbol_stats.columns = ["symbol", "avg_sentiment", "mention_count"]
            
            # Sort by mention count
            top_symbols = symbol_stats.sort_values("mention_count", ascending=False).head(10)
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(
                    top_symbols,
                    x="symbol",
                    y="mention_count",
                    title="Top Symbols by Mention Count",
                    color="avg_sentiment",
                    color_continuous_scale="RdBu",
                    range_color=[-1, 1],
                )
                st.plotly_chart(fig, use_container_width=True)
                
            with col2:
                fig = px.bar(
                    top_symbols,
                    x="symbol",
                    y="avg_sentiment",
                    title="Symbol Sentiment Analysis",
                    color="avg_sentiment",
                    color_continuous_scale="RdBu",
                    range_color=[-1, 1],
                )
                fig.update_layout(yaxis_range=[-1, 1])
                st.plotly_chart(fig, use_container_width=True)
            
            # Create symbol selector
            top_symbols_list = top_symbols["symbol"].tolist()
            selected_symbol = st.selectbox(
                "Select a symbol for detailed analysis",
                options=["All"] + top_symbols_list,
            )
            
            if selected_symbol != "All":
                # Filter data for selected symbol
                symbol_data = timeseries_data[timeseries_data["symbol"] == selected_symbol]
                
                if not symbol_data.empty:
                    # Time series chart
                    fig = px.line(
                        symbol_data.sort_values("timestamp"),
                        x="timestamp",
                        y="sentiment_score",
                        title=f"Sentiment Trend for {selected_symbol}",
                        markers=True,
                    )
                    fig.update_layout(yaxis_range=[-1, 1])
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Show statistics
                    avg_sentiment = symbol_data["sentiment_score"].mean()
                    sentiment_trend = "Neutral"
                    if avg_sentiment > 0.1:
                        sentiment_trend = "Positive"
                    elif avg_sentiment < -0.1:
                        sentiment_trend = "Negative"
                        
                    st.metric(
                        f"{selected_symbol} Overall Sentiment",
                        f"{sentiment_trend} ({avg_sentiment:.2f})"
                    )
                    
                    # Get historical data from MongoDB
                    historical_data = self.get_mongodb_data(
                        "reddit_sentiment",
                        query={"detected_symbols": {"$elemMatch": {"$eq": selected_symbol}}},
                        limit=500,
                    )
                    
                    if not historical_data.empty:
                        st.subheader(f"Recent Posts Mentioning {selected_symbol}")
                        
                        # Display recent posts
                        for idx, row in historical_data.head(5).iterrows():
                            sentiment = row.get("sentiment_score", 0)
                            sentiment_color = "gray"
                            if sentiment > 0.05:
                                sentiment_color = "green"
                            elif sentiment < -0.05:
                                sentiment_color = "red"
                                
                            st.markdown(f"**Title**: {row.get('title', 'N/A')}")
                            st.markdown(f"**Subreddit**: r/{row.get('subreddit', 'N/A')}")
                            st.markdown(f"**Sentiment**: <span style='color:{sentiment_color}'>{sentiment:.2f}</span>", unsafe_allow_html=True)
                            st.markdown(f"**Created**: {row.get('created_datetime', 'N/A')}")
                            st.markdown("---")
        else:
            # Fallback to MongoDB data
            db_name = self.config.get("mongodb", {}).get("database", "social_media")
            mongodb_data = self.get_mongodb_data("reddit_sentiment", db_name)
            
            if not mongodb_data.empty and "detected_symbols" in mongodb_data.columns:
                # Extract symbols from the detected_symbols array
                all_symbols = []
                for symbols in mongodb_data["detected_symbols"]:
                    if isinstance(symbols, list) and symbols:
                        all_symbols.extend(symbols)
                
                if all_symbols:
                    symbol_counts = pd.Series(all_symbols).value_counts().reset_index()
                    symbol_counts.columns = ["symbol", "count"]
                    
                    # Get top symbols
                    top_symbols = symbol_counts.head(10)
                    
                    fig = px.bar(
                        top_symbols,
                        x="symbol",
                        y="count",
                        title="Most Mentioned Symbols",
                        color="count",
                        color_continuous_scale="Viridis",
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Symbol selection for detailed analysis
                    selected_symbol = st.selectbox(
                        "Select a symbol for detailed analysis",
                        options=["All"] + list(symbol_counts.head(20)["symbol"]),
                    )
                    
                    if selected_symbol != "All":
                        symbol_posts = mongodb_data[
                            mongodb_data["detected_symbols"].apply(
                                lambda x: isinstance(x, list) and selected_symbol in x
                            )
                        ]
                        
                        if not symbol_posts.empty:
                            avg_sentiment = symbol_posts["sentiment_score"].mean() if "sentiment_score" in symbol_posts.columns else 0
                            
                            st.write(f"Analysis for symbol: **{selected_symbol}**")
                            st.write(f"Total mentions: **{len(symbol_posts)}**")
                            st.write(f"Average sentiment: **{avg_sentiment:.2f}**")
                            
                            # Display recent posts
                            st.subheader(f"Recent Posts Mentioning {selected_symbol}")
                            for idx, row in symbol_posts.head(5).iterrows():
                                sentiment = row.get("sentiment_score", 0)
                                sentiment_color = "gray"
                                if sentiment > 0.05:
                                    sentiment_color = "green"
                                elif sentiment < -0.05:
                                    sentiment_color = "red"
                                    
                                st.markdown(f"**Title**: {row.get('title', 'N/A')}")
                                st.markdown(f"**Subreddit**: r/{row.get('subreddit', 'N/A')}")
                                st.markdown(f"**Sentiment**: <span style='color:{sentiment_color}'>{sentiment:.2f}</span>", unsafe_allow_html=True)
                                st.markdown(f"**Created**: {row.get('created_datetime', 'N/A')}")
                                st.markdown("---")
                else:
                    st.info("No symbols detected in the dataset")
            else:
                st.info("No symbol data available")

    def render_sentiment_trends(self):
        """Render sentiment trends over time."""
        st.subheader("Sentiment Analysis Trends")
        
        # Get time-based data
        db_name = self.config.get("mongodb", {}).get("database", "social_media")
        collection_name = "reddit_sentiment"
        
        # Set default time range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        # Date range selector
        date_range = st.date_input(
            "Select Date Range for Historical Analysis",
            value=(start_date.date(), end_date.date()),
        )
        
        if len(date_range) == 2:
            start_date, end_date = date_range
            end_date = datetime.combine(end_date, datetime.max.time())  # End of selected day
            start_date = datetime.combine(start_date, datetime.min.time())  # Start of selected day
            
            # Query MongoDB for data in date range
            query = {
                "created_timestamp": {
                    "$gte": start_date,
                    "$lte": end_date,
                }
            }
            
            historical_data = self.get_mongodb_data(
                collection_name, db_name, query=query, limit=100000
            )
            
            if not historical_data.empty and "created_timestamp" in historical_data.columns:
                # Convert to datetime if needed
                if not pd.api.types.is_datetime64_any_dtype(historical_data["created_timestamp"]):
                    historical_data["created_timestamp"] = pd.to_datetime(historical_data["created_timestamp"])
                
                # Extract date for aggregation
                historical_data["date"] = historical_data["created_timestamp"].dt.date
                
                # Aggregate by date
                daily_data = (
                    historical_data.groupby("date")
                    .agg({
                        "id": "count",
                        "sentiment_score": "mean",
                    })
                    .reset_index()
                )
                daily_data.columns = ["date", "post_count", "avg_sentiment"]
                
                # Plot time series data
                col1, col2 = st.columns(2)
                
                with col1:
                    fig = px.line(
                        daily_data,
                        x="date",
                        y="post_count",
                        title="Post Volume by Date",
                        markers=True,
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    fig = px.line(
                        daily_data,
                        x="date",
                        y="avg_sentiment",
                        title="Average Sentiment by Date",
                        markers=True,
                    )
                    fig.update_layout(yaxis_range=[-1, 1])
                    fig.add_hline(y=0, line_dash="dash", line_color="gray")
                    st.plotly_chart(fig, use_container_width=True)
                
                # Hourly analysis for the most recent day
                if (end_date - start_date).days <= 3:  # Only show hourly for short ranges
                    historical_data["hour"] = historical_data["created_timestamp"].dt.hour
                    historical_data["date_hour"] = historical_data["created_timestamp"].dt.floor("H")
                    
                    hourly_data = (
                        historical_data.groupby("date_hour")
                        .agg({
                            "id": "count",
                            "sentiment_score": "mean",
                        })
                        .reset_index()
                    )
                    hourly_data.columns = ["date_hour", "post_count", "avg_sentiment"]
                    
                    st.subheader("Hourly Sentiment Analysis")
                    
                    fig = px.line(
                        hourly_data,
                        x="date_hour",
                        y="avg_sentiment",
                        title="Hourly Sentiment Trends",
                        markers=True,
                    )
                    fig.update_layout(yaxis_range=[-1, 1])
                    fig.add_hline(y=0, line_dash="dash", line_color="gray")
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Show hourly post volume
                    fig = px.bar(
                        hourly_data,
                        x="date_hour",
                        y="post_count",
                        title="Hourly Post Volume",
                        color="avg_sentiment",
                        color_continuous_scale="RdBu",
                        range_color=[-1, 1],
                    )
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No data available for the selected date range")

    def run_dashboard(self):
        """Run the Streamlit dashboard."""
        st.title("MarketPulseAI - Social Media Analytics")

        # Add refresh rate selection in sidebar
        st.sidebar.header("Dashboard Settings")
        refresh_interval = st.sidebar.slider(
            "Auto-refresh interval (seconds)", 
            min_value=5, 
            max_value=300, 
            value=60,
            step=5
        )
        
        auto_refresh = st.sidebar.checkbox("Enable auto-refresh", value=True)

        # Display last update time
        last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        update_info = st.empty()
        update_info.info(f"Last updated: {last_updated}")
        
        # Render metrics dashboard section
        self.render_metrics_section()
        
        # Create tabs for different analyses
        tab1, tab2, tab3 = st.tabs(
            ["Subreddit Analysis", "Symbol Analysis", "Sentiment Trends"]
        )
        
        with tab1:
            self.render_subreddit_analysis()
            
        with tab2:
            self.render_symbol_analysis()
            
        with tab3:
            self.render_sentiment_trends()
        
        # Add auto-refresh functionality
        if auto_refresh:
            time.sleep(refresh_interval)
            st.rerun()


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: streamlit run src/visualization/streamlit_dashboard.py -- <config_path>")
        config_path = "/opt/bitnami/spark/config/spark/reddit_sentiment_config.yaml"
    else:
        config_path = sys.argv[1]

    dashboard = MarketPulseDashboard(config_path)
    dashboard.run_dashboard()