import yfinance as yf
import pandas as pd
import numpy as np
import time
from sqlalchemy.dialects.postgresql import insert
from main import SessionLocal, DailyMetric
from utils import get_sp500_tickers

def run_60day_backfill():
    tickers = get_sp500_tickers()
    data = yf.download(tickers, period="70d", group_by='ticker', threads=True, progress=False)
    session = SessionLocal()

    for symbol in tickers:
        try:
            if symbol not in data.columns.levels[0]:
                continue
            ticker_data = data[symbol].dropna(subset=['Close'])

            # Calculate 5‑day slope on closing prices
            ticker_data['rs_slope'] = ticker_data['Close'].rolling(window=5).apply(
                lambda x: np.polyfit(np.arange(5), x, 1)[0] if len(x) == 5 else np.nan
            )
            # Convert numpy float to Python float and fill NaN
            ticker_data['rs_slope'] = ticker_data['rs_slope'].apply(lambda x: float(x) if not np.isnan(x) else 0.0)

            info = yf.Ticker(symbol).info
            short_f = float((info.get('shortPercentOfFloat', 0) or 0) * 100)
            avg_v = int(info.get('averageVolume', 1))

            for ts, row in ticker_data.tail(60).iterrows():
                stmt = insert(DailyMetric).values(
                    ticker=symbol, date=ts.date(), price=float(row['Close']),
                    volume=int(row['Volume']), average_volume_30d=avg_v,
                    short_float_pct=short_f, rs_slope_5d=float(row['rs_slope']),
                    sentiment_score=0.0, analyst_rating=0.0, insider_score=0.0
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=['ticker', 'date'],
                    set_={'price': stmt.excluded.price, 'volume': stmt.excluded.volume,
                          'rs_slope_5d': stmt.excluded.rs_slope_5d}
                )
                session.execute(stmt)
            session.commit()
            print(f"✅ Backfilled {symbol}")
        except Exception as e:
            session.rollback()
            print(f"Error on {symbol}: {e}")
    session.close()

if __name__ == "__main__":
    run_60day_backfill()