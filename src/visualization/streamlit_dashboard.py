# src/visualization/streamlit_dashboard.py
import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
from loguru import logger
from pathlib import Path
import yaml
from datetime import datetime, timedelta
import time


class MongoDashboard:
    """Streamlit dashboard for MongoDB data visualization."""

    def __init__(self, config_path: str):
        """Initialize dashboard with configuration."""
        self.config = self._load_config(config_path)
        self.client = self._connect_mongodb()

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
        username = mongodb_config.get("auth_username")
        password = mongodb_config.get("auth_password")

        connection_string = (
            f"mongodb://{username}:{password}@{host}:{port}/?authSource=admin"
        )
        logger.info(f"Connecting to MongoDB at {host}:{port}")
        return MongoClient(connection_string)

    def get_data(
        self,
        collection_name: str,
        database_name: str = None,
        query: dict = None,
        limit: int = 10000,
    ):
        """Fetch data from MongoDB collection."""
        if database_name is None:
            database_name = self.config.get("mongodb", {}).get(
                "database", "social_media"
            )

        db = self.client[database_name]
        collection = db[collection_name]

        query = query or {}
        logger.info(
            f"Fetching data from {database_name}.{collection_name} with query: {query}"
        )

        cursor = collection.find(query).limit(limit)
        return pd.DataFrame(list(cursor))

    def run_dashboard(self):
        """Run the Streamlit dashboard."""
        st.set_page_config(
            page_title="MarketPulseAI Dashboard", page_icon="📊", layout="wide"
        )

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
        
        # Sidebar filters
        st.sidebar.header("Filters")

        # Database and collection selection
        db_name = st.sidebar.selectbox(
            "Database",
            options=[self.config.get("mongodb", {}).get("database", "social_media")],
        )

        collection_name = st.sidebar.selectbox(
            "Collection", options=["reddit_sentiment"]
        )

        # Date range filter
        with st.sidebar.expander("Date Range", expanded=True):
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            date_range = st.date_input(
                "Select Date Range", value=(start_date, end_date), key="date_range"
            )

            if len(date_range) == 2:
                start_date, end_date = date_range
                date_filter = {
                    "created_datetime": {
                        "$gte": start_date.strftime("%Y-%m-%d"),
                        "$lte": (end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
                    }
                }
            else:
                date_filter = {}

        # Main dashboard
        df = self.get_data(collection_name, db_name, query=date_filter)

        if df.empty:
            st.warning("No data found for the selected filters.")
            return

        # Display basic stats
        st.header("Overview")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Posts", len(df))

        with col2:
            if "score" in df.columns:
                avg_score = round(df["score"].mean(), 2)
                st.metric("Average Score", avg_score)

        with col3:
            if "num_comments" in df.columns:
                total_comments = df["num_comments"].sum()
                st.metric("Total Comments", total_comments)

        with col4:
            if "subreddit" in df.columns:
                unique_subreddits = df["subreddit"].nunique()
                st.metric("Unique Subreddits", unique_subreddits)

        # Dashboard tabs
        tab1, tab2, tab3 = st.tabs(
            ["Subreddit Analysis", "Symbol Analysis", "Sentiment Trends"]
        )

        with tab1:
            st.subheader("Subreddit Analysis")

            if "subreddit" in df.columns:
                # Top subreddits by post count
                top_subreddits = df["subreddit"].value_counts().head(10).reset_index()
                top_subreddits.columns = ["subreddit", "count"]

                fig = px.bar(
                    top_subreddits,
                    x="subreddit",
                    y="count",
                    title="Top Subreddits by Post Count",
                    color="count",
                    color_continuous_scale="Viridis",
                )
                st.plotly_chart(fig, use_container_width=True)

                # Subreddit engagement metrics
                subreddit_metrics = (
                    df.groupby("subreddit")
                    .agg({"score": ["mean", "sum"], "num_comments": ["mean", "sum"]})
                    .reset_index()
                )
                subreddit_metrics.columns = [
                    "subreddit",
                    "avg_score",
                    "total_score",
                    "avg_comments",
                    "total_comments",
                ]
                subreddit_metrics = subreddit_metrics.sort_values(
                    "total_score", ascending=False
                ).head(10)

                col1, col2 = st.columns(2)

                with col1:
                    fig = px.bar(
                        subreddit_metrics,
                        x="subreddit",
                        y="avg_score",
                        title="Average Score by Subreddit",
                        color="avg_score",
                        color_continuous_scale="Bluered",
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    fig = px.bar(
                        subreddit_metrics,
                        x="subreddit",
                        y="avg_comments",
                        title="Average Comments by Subreddit",
                        color="avg_comments",
                        color_continuous_scale="Sunsetdark",
                    )
                    st.plotly_chart(fig, use_container_width=True)

        with tab2:
            st.subheader("Symbol Analysis")

            if "detected_symbols" in df.columns:
                # Extract all symbols
                all_symbols = []
                for symbols in df["detected_symbols"]:
                    if isinstance(symbols, list) and symbols:
                        all_symbols.extend(symbols)

                if all_symbols:
                    symbol_counts = (
                        pd.Series(all_symbols).value_counts().head(15).reset_index()
                    )
                    symbol_counts.columns = ["symbol", "count"]

                    fig = px.bar(
                        symbol_counts,
                        x="symbol",
                        y="count",
                        title="Most Mentioned Symbols",
                        color="count",
                        color_continuous_scale="Plasma",
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    # Create symbol filter for detailed analysis
                    selected_symbol = st.selectbox(
                        "Select a symbol for detailed analysis",
                        options=["All"] + list(symbol_counts["symbol"]),
                        key="symbol_select",
                    )

                    if selected_symbol != "All":
                        symbol_posts = df[
                            df["detected_symbols"].apply(
                                lambda x: isinstance(x, list) and selected_symbol in x
                            )
                        ]

                        st.write(f"Analysis for symbol: **{selected_symbol}**")
                        st.write(f"Total mentions: **{len(symbol_posts)}**")

                        if "score" in symbol_posts.columns:
                            avg_symbol_score = round(symbol_posts["score"].mean(), 2)
                            st.write(f"Average score: **{avg_symbol_score}**")

                        if "created_datetime" in symbol_posts.columns:
                            symbol_posts["date"] = pd.to_datetime(
                                symbol_posts["created_datetime"]
                            ).dt.date
                            mentions_by_date = (
                                symbol_posts.groupby("date").size().reset_index()
                            )
                            mentions_by_date.columns = ["date", "mentions"]

                            fig = px.line(
                                mentions_by_date,
                                x="date",
                                y="mentions",
                                title=f"{selected_symbol} Mentions Over Time",
                                markers=True,
                            )
                            st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No symbols detected in the current dataset.")
            else:
                st.info("Symbol data not available.")

        with tab3:
            st.subheader("Sentiment Analysis Trends")

            # Time-based analysis
            if "created_datetime" in df.columns:
                df["date"] = pd.to_datetime(df["created_datetime"]).dt.date
                posts_by_date = df.groupby("date").size().reset_index()
                posts_by_date.columns = ["date", "posts"]

                fig = px.line(
                    posts_by_date,
                    x="date",
                    y="posts",
                    title="Posts Volume Over Time",
                    markers=True,
                )
                st.plotly_chart(fig, use_container_width=True)

                # If sentiment data is available
                if "sentiment_score" in df.columns:
                    sentiment_by_date = (
                        df.groupby("date")["sentiment_score"].mean().reset_index()
                    )

                    fig = px.line(
                        sentiment_by_date,
                        x="date",
                        y="sentiment_score",
                        title="Average Sentiment Score Over Time",
                        markers=True,
                    )
                    fig.add_hline(y=0, line_dash="dash", line_color="gray")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Sentiment score data not available.")

        # Add auto-refresh functionality at the end
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

    dashboard = MongoDashboard(config_path)
    dashboard.run_dashboard()