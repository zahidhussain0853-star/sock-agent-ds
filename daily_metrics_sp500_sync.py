import os
import yfinance as yf
import pandas as pd
import numpy as np
import time
import feedparser
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from main import DailyMetric, engine
from dotenv import load_dotenv
from utils import get_sp500_tickers

load_dotenv()
Session = sessionmaker(bind=engine)

# Initialize VADER sentiment analyzer (once, globally)
analyzer = SentimentIntensityAnalyzer()

def get_sentiment_from_rss(ticker):
    """
    Fetch Google News RSS for ticker, analyze headlines with VADER,
    return average sentiment score (-1 to 1). Returns 0.0 if no news.
    """
    url = f"https://news.google.com/rss/search?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en"
    try:
        feed = feedparser.parse(url)
        if not feed.entries:
            return 0.0
        scores = []
        for entry in feed.entries[:10]:  # latest 10 headlines
            score = analyzer.polarity_scores(entry.title)['compound']
            scores.append(score)
        return sum(scores) / len(scores) if scores else 0.0
    except Exception as e:
        # Silently fail (print only if debugging)
        # print(f"RSS error for {ticker}: {e}")
        return 0.0

def forward_fill_sp500():
    session = Session()
    tickers = get_sp500_tickers()

    if not tickers:
        print("No tickers found. Sync aborted.")
        return

    print(f"--- SYNCING {len(tickers)} S&P 500 TICKERS: {datetime.now()} ---")

    try:
        # Fetch 5 days of market data (for slope calculation)
        data = yf.download(tickers, period="5d", group_by='ticker', threads=True, progress=False)

        updated_count = 0
        for idx, ticker in enumerate(tickers):
            try:
                if ticker not in data.columns.levels[0]:
                    continue

                ticker_df = data[ticker].dropna(subset=['Close'])
                if ticker_df.empty:
                    continue

                # Most recent trading day
                last_row = ticker_df.iloc[-1]

                # Compute 5-day slope (convert numpy float to Python float)
                if len(ticker_df) >= 5:
                    closes = ticker_df['Close'].values[-5:]
                    slope = float(np.polyfit(range(5), closes, 1)[0])
                else:
                    slope = 0.0

                # Fetch metadata from yfinance
                t_obj = yf.Ticker(ticker)
                info = t_obj.info
                short_float = float((info.get('shortPercentOfFloat', 0) or 0) * 100)
                avg_vol = int(info.get('averageVolume', 1))

                # Get sentiment from RSS (free, unlimited)
                sentiment = get_sentiment_from_rss(ticker)

                # Upsert into daily_metrics
                stmt = insert(DailyMetric).values(
                    ticker=ticker,
                    date=last_row.name.date(),
                    price=float(last_row['Close']),
                    volume=int(last_row['Volume']),
                    average_volume_30d=avg_vol,
                    short_float_pct=short_float,
                    rs_slope_5d=slope,
                    sentiment_score=sentiment,
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
                        'sentiment_score': stmt.excluded.sentiment_score
                    }
                )

                session.execute(stmt)
                updated_count += 1

                if updated_count % 20 == 0:
                    session.commit()
                    print(f"✅ Progress: {updated_count}/{len(tickers)} synced...")

                # Small delay to avoid overwhelming network/CPU (optional)
                time.sleep(0.05)

            except Exception as e:
                print(f"Skipping {ticker}: {e}")
                continue

        session.commit()
        print(f"🏁 Sync Complete. Successfully processed {updated_count} tickers.")

    except Exception as e:
        print(f"❌ Bulk Sync Error: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    forward_fill_sp500()