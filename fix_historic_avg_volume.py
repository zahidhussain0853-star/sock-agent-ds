# fix_historic_avg_volume.py
from main import SessionLocal, DailyMetric
from datetime import timedelta
import os
import time

PROCESSED_FILE = "processed_tickers.txt"

def load_processed():
    if os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE, "r") as f:
            return set(line.strip() for line in f)
    return set()

def save_processed(ticker):
    with open(PROCESSED_FILE, "a") as f:
        f.write(ticker + "\n")

def fix_historical_avg_volume():
    processed = load_processed()
    session = SessionLocal()
    tickers = [t[0] for t in session.query(DailyMetric.ticker).distinct().all()]
    session.close()

    total_updated = 0
    for ticker in tickers:
        if ticker in processed:
            print(f"Skipping {ticker} (already processed)")
            continue

        print(f"Processing {ticker}...")
        session = SessionLocal()
        try:
            rows = session.query(DailyMetric).filter(DailyMetric.ticker == ticker).order_by(DailyMetric.date).all()
            updated_for_ticker = 0
            for i, row in enumerate(rows):
                start_date = row.date - timedelta(days=30)
                vols = [r.volume for r in rows if start_date <= r.date < row.date]
                if vols:
                    avg_vol = int(sum(vols) / len(vols))
                else:
                    avg_vol = row.volume if row.volume else 1000000
                if row.average_volume_30d != avg_vol:
                    row.average_volume_30d = avg_vol
                    updated_for_ticker += 1
            session.commit()
            total_updated += updated_for_ticker
            print(f"  Updated {updated_for_ticker} rows for {ticker}. Total: {total_updated}")
            save_processed(ticker)
        except Exception as e:
            print(f"❌ Error on {ticker}: {e}. Stopping. Re-run to continue from next ticker.")
            session.rollback()
            return  # stop on error; next run will skip processed tickers
        finally:
            session.close()

    print(f"✅ Done. Updated {total_updated} rows in total.")
    # Clean up processed file after success
    if os.path.exists(PROCESSED_FILE):
        os.remove(PROCESSED_FILE)

if __name__ == "__main__":
    fix_historical_avg_volume()