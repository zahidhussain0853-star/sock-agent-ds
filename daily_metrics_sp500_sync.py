import os
import yfinance as yf
import pandas as pd
import numpy as np
import time
import feedparser
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from main import DailyMetric, engine
from dotenv import load_dotenv
from utils import get_sp500_tickers

load_dotenv()
Session = sessionmaker(bind=engine)

analyzer = SentimentIntensityAnalyzer()

def get_sentiment_from_rss(ticker):
    url = f"https://news.google.com/rss/search?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en"
    try:
        feed = feedparser.parse(url)
        if not feed.entries:
            return 0.0
        scores = []
        for entry in feed.entries[:10]:
            score = analyzer.polarity_scores(entry.title)['compound']
            scores.append(score)
        return sum(scores) / len(scores) if scores else 0.0
    except Exception:
        return 0.0

def get_30d_avg_volume(ticker, current_date, session):
    start_date = current_date - timedelta(days=30)
    result = session.query(DailyMetric.volume).filter(
        DailyMetric.ticker == ticker,
        DailyMetric.date >= start_date,
        DailyMetric.date < current_date
    ).all()
    volumes = [r[0] for r in result if r[0] is not None]
    if volumes:
        return int(sum(volumes) / len(volumes))
    else:
        t_obj = yf.Ticker(ticker)
        info = t_obj.info
        return int(info.get('averageVolume', 1000000))

def compute_rsi(prices, period=14):
    """prices: list of close prices (oldest to newest). Returns RSI for the last day."""
    if len(prices) < period + 1:
        return 50.0
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return float(round(rsi, 2))

def compute_macd(prices, fast=12, slow=26, signal=9):
    """prices: list of close prices. Returns histogram for the last day."""
    if len(prices) < slow:
        return 0.0
    series = pd.Series(prices)
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return float(round(histogram.iloc[-1], 4))

def forward_fill_sp500():
    session = Session()
    tickers = get_sp500_tickers()
    if not tickers:
        print("No tickers found. Sync aborted.")
        return

    print(f"--- SYNCING {len(tickers)} S&P 500 TICKERS: {datetime.now()} ---")

    try:
        # Fetch 20 days of data for RSI & MACD (also need 5-day slope)
        data = yf.download(tickers, period="20d", group_by='ticker', threads=True, progress=False)
        updated_count = 0
        for ticker in tickers:
            if ticker not in data.columns.levels[0]:
                continue
            ticker_df = data[ticker].dropna(subset=['Close'])
            if ticker_df.empty:
                continue
            last_row = ticker_df.iloc[-1]
            current_date = last_row.name.date()

            # 5‑day slope (using last 5 days)
            if len(ticker_df) >= 5:
                closes_5 = ticker_df['Close'].values[-5:]
                slope_5d = float(np.polyfit(range(5), closes_5, 1)[0])
            else:
                slope_5d = 0.0

            # Compute RSI and MACD using all available prices (at least 20 days)
            closes_all = ticker_df['Close'].values
            rsi = compute_rsi(closes_all)
            macd_hist = compute_macd(closes_all)

            # Metadata
            t_obj = yf.Ticker(ticker)
            info = t_obj.info
            short_float = float((info.get('shortPercentOfFloat', 0) or 0) * 100)
            avg_vol = get_30d_avg_volume(ticker, current_date, session)
            sentiment = get_sentiment_from_rss(ticker)

            # Upsert
            stmt = insert(DailyMetric).values(
                ticker=ticker, date=current_date,
                price=float(last_row['Close']),
                volume=int(last_row['Volume']),
                average_volume_30d=avg_vol,
                short_float_pct=short_float,
                rs_slope_5d=slope_5d,
                sentiment_score=sentiment,
                rsi_14d=rsi,
                macd_histogram=macd_hist,
                analyst_rating=0.0,
                insider_score=0.0
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['ticker', 'date'],
                set_={
                    'price': stmt.excluded.price,
                    'volume': stmt.excluded.volume,
                    'average_volume_30d': stmt.excluded.average_volume_30d,
                    'short_float_pct': stmt.excluded.short_float_pct,
                    'rs_slope_5d': stmt.excluded.rs_slope_5d,
                    'sentiment_score': stmt.excluded.sentiment_score,
                    'rsi_14d': stmt.excluded.rsi_14d,
                    'macd_histogram': stmt.excluded.macd_histogram
                }
            )
            session.execute(stmt)
            updated_count += 1

            if updated_count % 20 == 0:
                session.commit()
                print(f"✅ Progress: {updated_count}/{len(tickers)} synced...")

            time.sleep(0.05)

        session.commit()
        print(f"🏁 Sync Complete. Processed {updated_count} tickers.")

    except Exception as e:
        print(f"❌ Bulk Sync Error: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    forward_fill_sp500()