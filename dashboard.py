import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine
from utils import normalize_db_url
from main import SessionLocal, analyze_rating_trend, DailyMetric

st.set_page_config(page_title="Scout Stock Dashboard", layout="wide")

@st.cache_resource
def get_engine():
    return create_engine(normalize_db_url())

engine = get_engine()

# ---------- Helper functions ----------
def get_component_scores(ticker, session):
    curr = session.query(DailyMetric).filter_by(ticker=ticker).order_by(DailyMetric.date.desc()).first()
    if not curr:
        return None
    rating_points, _ = analyze_rating_trend(ticker, session)
    rs_points = 0
    if curr.rs_slope_5d and curr.rs_slope_5d > 0:
        rs_points = 25
    elif curr.rs_slope_5d and curr.rs_slope_5d < -0.5:
        rs_points = -20
    rvol = 0
    rvol_points = 0
    if curr.average_volume_30d and curr.average_volume_30d > 0:
        rvol = curr.volume / curr.average_volume_30d
        if rvol > 2.0:
            rvol_points = 40
        elif rvol > 1.5:
            rvol_points = 25
    insider_points = curr.insider_score if curr.insider_score else 0
    raw_score = rating_points + rs_points + rvol_points + insider_points
    multiplier = 1.2 if (curr.short_float_pct and curr.short_float_pct > 15) else 1.0
    final_score = raw_score * multiplier
    return {
        "analyst_points": rating_points,
        "rs_points": rs_points,
        "rvol_points": rvol_points,
        "insider_points": insider_points,
        "raw_score": raw_score,
        "multiplier": multiplier,
        "final_score": round(final_score, 2),
        "rvol": round(rvol, 2),
        "short_float": curr.short_float_pct,
        "rs_slope": curr.rs_slope_5d,
    }

@st.cache_data(ttl=3600)
def load_ticker_timeseries(ticker):
    query = f"""
    SELECT 
        d.date,
        d.rs_slope_5d,
        d.volume,
        d.average_volume_30d,
        d.short_float_pct,
        d.insider_score,
        d.insider_alert_flag,
        COALESCE(s.score, 0) as scout_score,
        s.signals as scout_signals
    FROM daily_metrics d
    LEFT JOIN scout_scores s ON d.ticker = s.ticker AND d.date = s.date
    WHERE d.ticker = '{ticker}'
    ORDER BY d.date
    """
    df = pd.read_sql(query, engine)
    if not df.empty:
        df['rvol'] = df.apply(lambda row: row['volume'] / row['average_volume_30d'] if row['average_volume_30d'] > 0 else 0, axis=1)
        df['rvol'] = df['rvol'].round(2)
    return df

def load_analyst_events(ticker):
    query = f"""
    SELECT date, event
    FROM stock_ratings
    WHERE ticker = '{ticker}' AND event != '-'
    ORDER BY date
    """
    return pd.read_sql(query, engine)

def load_analyst_history(ticker):
    query = f"""
    SELECT date, score, sb, b, h, s, ss, (sb+b+h+s+ss) as total_analysts
    FROM stock_ratings
    WHERE ticker = '{ticker}'
    ORDER BY date
    """
    return pd.read_sql(query, engine)

# ---------- Main UI ----------
st.title("📈 Scout Stock Dashboard")
tab1, tab2 = st.tabs(["Overview", "Signal Breakdown"])

# ---------- Tab 1: Overview ----------
with tab1:
    @st.cache_data(ttl=3600)
    def load_latest_data():
        query = """
        WITH latest_date AS (SELECT MAX(date) as max_date FROM daily_metrics)
        SELECT 
            d.ticker,
            COALESCE(s.score, 0) as score,
            CASE 
                WHEN s.score >= 80 THEN '🔥 STRONG BUY'
                WHEN s.score >= 65 THEN '✅ BUY'
                WHEN s.score >= 40 THEN '👀 WATCH'
                ELSE '❄️ HOLD'
            END as action,
            COALESCE(s.signals, '') as signals,
            d.date,
            d.short_float_pct,
            d.insider_alert_flag,
            d.rs_slope_5d,
            d.volume,
            d.average_volume_30d,
            CASE WHEN d.average_volume_30d > 0 THEN ROUND(d.volume::numeric / d.average_volume_30d, 2) ELSE 0 END as rvol
        FROM daily_metrics d
        LEFT JOIN scout_scores s ON d.ticker = s.ticker AND d.date = s.date
        WHERE d.date = (SELECT max_date FROM latest_date)
        ORDER BY s.score DESC NULLS LAST
        """
        df = pd.read_sql(query, engine)
        df['insider_alert_flag'] = df['insider_alert_flag'].fillna('-')
        df['rs_slope_5d'] = df['rs_slope_5d'].fillna(0).round(2)
        return df

    df_latest = load_latest_data()
    if df_latest.empty:
        st.warning("No daily metrics found. Run your sync scripts first.")
        st.stop()

    st.sidebar.header("Filters")
    min_score = st.sidebar.slider("Minimum Score", 0, 100, 40)
    action_filter = st.sidebar.multiselect(
        "Action",
        options=df_latest['action'].unique(),
        default=df_latest['action'].unique()
    )
    show_only_insider = st.sidebar.checkbox("Show only stocks with insider activity")
    
    df_filtered = df_latest.copy()
    if show_only_insider:
        df_filtered = df_filtered[df_filtered['insider_alert_flag'] != '-']
    df_filtered = df_filtered[(df_filtered['score'] >= min_score) & (df_filtered['action'].isin(action_filter))]

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("🔥 Strong Buy (score ≥ 80)")
        strong = df_filtered[df_filtered['score'] >= 80][['ticker', 'score', 'rvol', 'short_float_pct', 'rs_slope_5d', 'insider_alert_flag']]
        if not strong.empty:
            st.dataframe(strong, width='stretch')
        else:
            st.info("None")
    with col2:
        st.subheader("✅ Buy (score 65–79)")
        buy = df_filtered[(df_filtered['score'] >= 65) & (df_filtered['score'] < 80)][['ticker', 'score', 'rvol', 'short_float_pct', 'rs_slope_5d', 'insider_alert_flag']]
        if not buy.empty:
            st.dataframe(buy, width='stretch')
        else:
            st.info("None")

    st.subheader(f"All Stocks (Score ≥ {min_score})")
    display_cols = ['ticker', 'score', 'action', 'rvol', 'short_float_pct', 'rs_slope_5d', 'insider_alert_flag', 'signals']
    st.dataframe(df_filtered[display_cols], width='stretch')
    st.caption("**Legend:** `rvol` = volume/30d avg (>1.5 high), `short_float_pct` = % short, `rs_slope_5d` = price trend (positive = bullish).")

    fig_hist = px.histogram(df_filtered, x='score', nbins=20, title='Score Distribution', color='action')
    st.plotly_chart(fig_hist, width='stretch')

    @st.cache_data(ttl=3600)
    def load_avg_history():
        df = pd.read_sql("SELECT date, AVG(score) as avg_score FROM scout_scores WHERE score IS NOT NULL GROUP BY date ORDER BY date", engine)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date']).dt.date
        return df
    
    df_hist_avg = load_avg_history()
    if not df_hist_avg.empty:
        fig_avg = px.line(df_hist_avg, x='date', y='avg_score', title='Average Score Over Time', markers=True)
        st.plotly_chart(fig_avg, width='stretch')

    st.subheader("Ticker History")
    ticker_list = df_latest['ticker'].unique()
    selected = st.selectbox("Select Ticker", sorted(ticker_list))
    if selected:
        hist = load_ticker_timeseries(selected)
        if not hist.empty:
            fig_ind = px.line(hist, x='date', y='scout_score', title=f'{selected} Score History')
            fig_ind.add_hline(y=80, line_dash="dash", line_color="red", annotation_text="Strong Buy")
            fig_ind.add_hline(y=65, line_dash="dash", line_color="orange", annotation_text="Buy")
            fig_ind.add_hline(y=40, line_dash="dash", line_color="yellow", annotation_text="Watch")
            st.plotly_chart(fig_ind, width='stretch')
            st.dataframe(hist[['date', 'scout_score', 'insider_alert_flag', 'rs_slope_5d', 'short_float_pct', 'rvol', 'scout_signals']], width='stretch')
        else:
            st.info("No historical data for this ticker.")

# ---------- Tab 2: Signal Breakdown ----------
with tab2:
    st.subheader("🔍 Deconstruct the Scout Score")
    ticker_list2 = df_latest['ticker'].unique() if 'df_latest' in locals() else []
    if not ticker_list2:
        st.warning("No data loaded. Please ensure daily_metrics exists.")
        st.stop()
    selected2 = st.selectbox("Select Ticker for Breakdown", sorted(ticker_list2), key="breakdown_ticker")
    
    session = SessionLocal()
    comp = get_component_scores(selected2, session)
    if comp:
        st.markdown("### 📊 Current Component Contributions")
        components = {
            "Analyst Trend": comp['analyst_points'],
            "RS Slope (5d)": comp['rs_points'],
            "RVOL": comp['rvol_points'],
            "Insider": comp['insider_points']
        }
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Raw Score (before multiplier)", comp['raw_score'])
        with col2:
            st.metric("Short Squeeze Multiplier", f"{comp['multiplier']:.1f}x")
        with col3:
            st.metric("Final Scout Score", comp['final_score'])
        
        fig_bar = go.Figure([go.Bar(x=list(components.keys()), y=list(components.values()), marker_color=['#1f77b4','#ff7f0e','#2ca02c','#d62728'])])
        fig_bar.update_layout(title="Component Points", yaxis_title="Points")
        st.plotly_chart(fig_bar, width='stretch')
        st.caption("Analyst Trend: max 60; RS Slope: +25 (positive) / -20 (negative); RVOL: +40 (>2.0) / +25 (1.5-2.0); Insider: variable.")

        # --- Combined Analyst Graph (dual axis) with larger event labels ---
        st.markdown("### 📈 Analyst Rating Evolution")
        analyst_hist = load_analyst_history(selected2)
        if not analyst_hist.empty:
            fig_combined = make_subplots(specs=[[{"secondary_y": True}]])

            # Analyst score (left axis)
            fig_combined.add_trace(
                go.Scatter(
                    x=analyst_hist['date'],
                    y=analyst_hist['score'],
                    mode='lines+markers',
                    name='Analyst Score (1-5)',
                    line=dict(color='purple', width=2),
                    marker=dict(size=6, color='purple')
                ),
                secondary_y=False
            )

            # Number of analysts (right axis)
            fig_combined.add_trace(
                go.Scatter(
                    x=analyst_hist['date'],
                    y=analyst_hist['total_analysts'],
                    mode='lines+markers',
                    name='Number of Analysts',
                    line=dict(color='blue', width=2, dash='dot'),
                    marker=dict(size=5, color='blue')
                ),
                secondary_y=True
            )

            # Event vertical lines & larger horizontal annotations
            events_df = load_analyst_events(selected2)
            for _, row in events_df.iterrows():
                fig_combined.add_vline(
                    x=row['date'], line_dash="dot", line_color="red", line_width=1,
                    secondary_y=False
                )
                fig_combined.add_annotation(
                    x=row['date'], y=5.4,
                    text=row['event'][:35], showarrow=False,
                    textangle=0, font=dict(size=12, color='red'),
                    bgcolor='rgba(255,255,255,0.85)'
                )

            fig_combined.update_layout(
                title=f"{selected2} Analyst Rating & Coverage",
                xaxis_title="Date",
                yaxis_title="Analyst Score (1=Strong Buy → 5=Strong Sell)",
                yaxis2_title="Number of Analysts Reporting",
                yaxis=dict(tickmode='linear', tick0=1, dtick=0.5, range=[1,5.8], tickfont=dict(size=11)),
                yaxis2=dict(tickfont=dict(size=11), showgrid=False),
                xaxis=dict(tickangle=-45, tickfont=dict(size=10)),
                height=500,
                legend=dict(x=0.02, y=0.98, bgcolor='rgba(255,255,255,0.8)')
            )

            st.plotly_chart(fig_combined, use_container_width=True)
            st.caption("Red vertical lines mark upgrade/downgrade events. Analyst score is weighted average (1-5).")
        else:
            st.info("No analyst rating history found for this ticker.")

        # --- Scout Score History ---
        st.markdown("### 🎯 Scout Score History")
        ts_df = load_ticker_timeseries(selected2)
        if not ts_df.empty:
            fig_scout = px.line(ts_df, x='date', y='scout_score', title='Scout Score Over Time')
            fig_scout.add_hline(y=80, line_dash="dash", line_color="red", annotation_text="Strong Buy")
            fig_scout.add_hline(y=65, line_dash="dash", line_color="orange", annotation_text="Buy")
            fig_scout.add_hline(y=40, line_dash="dash", line_color="yellow", annotation_text="Watch")
            fig_scout.update_layout(xaxis=dict(tickangle=-45), height=400)
            st.plotly_chart(fig_scout, width='stretch')
        else:
            st.info("No scout score history available.")

        # --- Other Historical Metrics ---
        st.markdown("### 📉 Other Historical Metrics")
        if not ts_df.empty:
            fig_dual = go.Figure()
            fig_dual.add_trace(go.Scatter(x=ts_df['date'], y=ts_df['rs_slope_5d'], name='RS Slope (5d)', yaxis='y1', line=dict(color='blue')))
            fig_dual.add_trace(go.Scatter(x=ts_df['date'], y=ts_df['rvol'], name='RVOL', yaxis='y2', line=dict(color='green')))
            fig_dual.update_layout(title='RS Slope (left) & RVOL (right)', yaxis=dict(title='RS Slope'), yaxis2=dict(title='RVOL', overlaying='y', side='right'), xaxis=dict(tickangle=-45), height=400)
            st.plotly_chart(fig_dual, width='stretch')
            
            fig_short = px.line(ts_df, x='date', y='short_float_pct', title='Short Float %')
            fig_short.update_layout(xaxis=dict(tickangle=-45), height=400)
            st.plotly_chart(fig_short, width='stretch')
            
            if (ts_df['insider_score'] > 0).any():
                fig_insider = px.bar(ts_df, x='date', y='insider_score', title='Insider Score (when >0)')
                fig_insider.update_layout(xaxis=dict(tickangle=-45), height=400)
                st.plotly_chart(fig_insider, width='stretch')
            else:
                st.info("No insider activity recorded for this ticker.")
        else:
            st.info("No historical data available.")
    else:
        st.error("Could not compute component scores – missing daily metrics for this ticker.")
    session.close()