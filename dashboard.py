import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from utils import normalize_db_url

# Page config
st.set_page_config(page_title="Scout Stock Dashboard", layout="wide")

# Database connection
@st.cache_resource
def get_engine():
    return create_engine(normalize_db_url())

engine = get_engine()

# Helper functions
@st.cache_data(ttl=3600)  # cache for 1 hour
def load_latest_scores():
    query = """
    SELECT ticker, score, action, signals, date
    FROM scout_scores
    WHERE date = (SELECT MAX(date) FROM scout_scores)
    ORDER BY score DESC
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=3600)
def load_ticker_history(ticker):
    query = f"""
    SELECT date, score, action, signals
    FROM scout_scores
    WHERE ticker = '{ticker}'
    ORDER BY date
    """
    return pd.read_sql(query, engine)

@st.cache_data(ttl=3600)
def load_score_distribution_history():
    query = """
    SELECT date, AVG(score) as avg_score, COUNT(*) as count
    FROM scout_scores
    GROUP BY date
    ORDER BY date
    """
    return pd.read_sql(query, engine)

# Title
st.title("📈 Scout Stock Dashboard")
st.markdown("Automated daily stock ratings based on analyst trends, price momentum, volume, and short interest.")

# Load data
df_latest = load_latest_scores()
if df_latest.empty:
    st.warning("No scout scores found. Run `python main.py` first to populate the database.")
    st.stop()

# Sidebar filters
st.sidebar.header("Filters")
min_score = st.sidebar.slider("Minimum Score", 0, 100, 40)
action_filter = st.sidebar.multiselect(
    "Action",
    options=df_latest['action'].unique(),
    default=df_latest['action'].unique()
)

# Filter data
filtered = df_latest[(df_latest['score'] >= min_score) & (df_latest['action'].isin(action_filter))]

# Main area - top picks
col1, col2 = st.columns(2)

with col1:
    st.subheader("🔥 Strong Buy (score ≥ 80)")
    strong_buy = filtered[filtered['score'] >= 80][['ticker', 'score', 'action', 'signals']]
    if not strong_buy.empty:
        st.dataframe(strong_buy, use_container_width=True)
    else:
        st.info("No strong buys today")

with col2:
    st.subheader("✅ Buy (score 65–79)")
    buy = filtered[(filtered['score'] >= 65) & (filtered['score'] < 80)][['ticker', 'score', 'action', 'signals']]
    if not buy.empty:
        st.dataframe(buy, use_container_width=True)
    else:
        st.info("No buys today")

# Full table
st.subheader(f"All Stocks (Score ≥ {min_score})")
st.dataframe(filtered[['ticker', 'score', 'action', 'signals', 'date']], use_container_width=True)

# Score distribution histogram
st.subheader("Score Distribution (Latest Day)")
fig = px.histogram(filtered, x='score', nbins=20, title='Scout Score Distribution', color='action')
st.plotly_chart(fig, use_container_width=True)

# Historical average score trend
st.subheader("Historical Average Scout Score")
df_history = load_score_distribution_history()
if not df_history.empty:
    fig2 = px.line(df_history, x='date', y='avg_score', title='Average Score Over Time')
    st.plotly_chart(fig2, use_container_width=True)

# Individual ticker trend
st.subheader("Individual Ticker History")
ticker_list = df_latest['ticker'].unique()
selected_ticker = st.selectbox("Select Ticker", sorted(ticker_list))
if selected_ticker:
    hist = load_ticker_history(selected_ticker)
    if not hist.empty:
        fig3 = px.line(hist, x='date', y='score', title=f'{selected_ticker} Scout Score History')
        # Highlight action thresholds
        fig3.add_hline(y=80, line_dash="dash", line_color="red", annotation_text="Strong Buy")
        fig3.add_hline(y=65, line_dash="dash", line_color="orange", annotation_text="Buy")
        fig3.add_hline(y=40, line_dash="dash", line_color="yellow", annotation_text="Watch")
        st.plotly_chart(fig3, use_container_width=True)
        
        # Show signals over time
        st.subheader("Signals Over Time")
        st.dataframe(hist[['date', 'score', 'action', 'signals']], use_container_width=True)
    else:
        st.info(f"No historical data for {selected_ticker}")

# Footer
st.markdown("---")
st.caption(f"Data last updated: {df_latest['date'].max()}")