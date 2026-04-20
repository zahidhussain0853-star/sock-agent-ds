import os
import yfinance as yf
import pandas as pd
import numpy as np          # added for slope calculation
import time
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from main import DailyMetric, engine
from dotenv import load_dotenv          # fixed import
from utils import get_sp500_tickers     # use shared function

load_dotenv()
Session = sessionmaker(bind=engine)

def forward_fill_sp500():
    session = Session()
    tickers = get_sp500_tickers()

    if not tickers:
        print("No tickers found. Sync aborted.")
        return

    print(f"--- SYNCING {len(tickers)} S&P 500 TICKERS: {datetime.now()} ---")

    try:
        # Fetch 5 days of data to compute 5-day slope
        data = yf.download(tickers, period="5d", group_by='ticker', threads=True, progress=False)

        updated_count = 0
        for index, ticker in enumerate(tickers):
            try:
                if ticker not in data.columns.levels[0]:
                    continue

                ticker_df = data[ticker].dropna(subset=['Close'])
                if ticker_df.empty:
                    continue

                # Get the most recent trading day
                last_row = ticker_df.iloc[-1]

                # Compute real 5-day slope if enough data
                if len(ticker_df) >= 5:
                    closes = ticker_df['Close'].values[-5:]
                    slope = np.polyfit(range(5), closes, 1)[0]
                else:
                    slope = 0.0

                # Fetch metadata snapshot
                t_obj = yf.Ticker(ticker)
                info = t_obj.info
                short_float = float((info.get('shortPercentOfFloat', 0) or 0) * 100)
                avg_vol = int(info.get('averageVolume', 1))

                # Prepare upsert
                stmt = insert(DailyMetric).values(
                    ticker=ticker,
                    date=last_row.name.date(),
                    price=float(last_row['Close']),
                    volume=int(last_row['Volume']),
                    average_volume_30d=avg_vol,
                    short_float_pct=short_float,
                    rs_slope_5d=slope,                     # now uses real slope
                    sentiment_score=0.0,
                    analyst_rating=0.0
                )

                stmt = stmt.on_conflict_do_update(
                    index_elements=['ticker', 'date'],
                    set_={
                        'price': stmt.excluded.price,
                        'volume': stmt.excluded.volume,
                        'average_volume_30d': stmt.excluded.average_volume_30d,
                        'short_float_pct': stmt.excluded.short_float_pct,
                        'rs_slope_5d': stmt.excluded.rs_slope_5d    # added
                    }
                )

                session.execute(stmt)
                updated_count += 1

                if updated_count % 20 == 0:
                    session.commit()
                    print(f"✅ Progress: {updated_count}/{len(tickers)} synced...")

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