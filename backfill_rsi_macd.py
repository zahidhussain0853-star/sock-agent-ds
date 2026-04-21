# backfill_rsi_macd.py
from main import SessionLocal, DailyMetric
import numpy as np
import pandas as pd
import time

def compute_rsi(prices, period=14):
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
    if len(prices) < slow:
        return 0.0
    series = pd.Series(prices)
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return float(round(histogram.iloc[-1], 4))

def backfill():
    session = SessionLocal()
    tickers = [t[0] for t in session.query(DailyMetric.ticker).distinct().all()]
    session.close()

    for ticker in tickers:
        retries = 3
        for attempt in range(retries):
            try:
                session = SessionLocal()
                print(f"Processing {ticker}...")
                rows = session.query(DailyMetric).filter(DailyMetric.ticker == ticker).order_by(DailyMetric.date).all()
                if len(rows) < 15:
                    session.close()
                    break
                prices = [row.price for row in rows]
                # Update each row's RSI and MACD
                for i, row in enumerate(rows):
                    window = prices[:i+1]
                    if len(window) >= 14:
                        rsi = compute_rsi(window)
                        macd = compute_macd(window)
                    else:
                        rsi = 50.0
                        macd = 0.0
                    row.rsi_14d = rsi
                    row.macd_histogram = macd
                session.commit()
                print(f"  Updated {len(rows)} rows for {ticker}")
                session.close()
                break  # success, exit retry loop
            except Exception as e:
                print(f"  Error on {ticker} (attempt {attempt+1}/{retries}): {e}")
                if session:
                    session.rollback()
                    session.close()
                if attempt < retries - 1:
                    time.sleep(5)
                else:
                    print(f"  Failed to process {ticker} after {retries} attempts. Skipping.")
    print("✅ Backfill complete.")

if __name__ == "__main__":
    backfill()