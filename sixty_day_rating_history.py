import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time
from utils import get_sp500_tickers, normalize_db_url
from main import init_db   # ensure table exists

load_dotenv()
DB_URL = normalize_db_url()

def calc_score(sb, b, h, s, ss):
    sum_vals = sb + b + h + s + ss
    if sum_vals == 0:
        return 0
    return round(((sb*1) + (b*2) + (h*3) + (s*4) + (ss*5)) / sum_vals, 2)

def run_production_sync():
    # Ensure stock_ratings table exists
    init_db()

    sp500_tickers = get_sp500_tickers()
    today = datetime.now().date()

    print(f"\n{'='*135}")
    print(f" PRODUCTION S&P 500 SYNC | DATABASE: railway | TABLE: stock_ratings")
    print(f" TOTAL TICKERS: {len(sp500_tickers)}")
    print(f"{'='*135}")

    # Open ONE database connection for all tickers
    conn = psycopg2.connect(DB_URL, sslmode='require')
    cursor = conn.cursor()

    for symbol in sp500_tickers:
        # symbol already hyphenated from get_sp500_tickers()
        try:
            ticker_obj = yf.Ticker(symbol)
            summ = ticker_obj.recommendations_summary

            try:
                events = ticker_obj.get_upgrades_downgrades()
            except:
                events = None

            if summ is None or summ.empty:
                continue

            # Initialize ledger
            row = summ.iloc[0]
            curr = {
                'sb': int(row['strongBuy']), 'b': int(row['buy']),
                'h': int(row['hold']), 's': int(row['sell']), 'ss': int(row['strongSell'])
            }

            # Map event feed
            event_map = {}
            if events is not None and not events.empty:
                if not isinstance(events.index, pd.RangeIndex):
                    events = events.reset_index()
                date_col = next((c for c in events.columns if any(x in c.lower() for x in ['date', 'period', 'grade'])), events.columns[0])
                events[date_col] = pd.to_datetime(events[date_col], format='mixed', errors='coerce', utc=True).dt.date
                for _, e_row in events.iterrows():
                    d = e_row[date_col]
                    if pd.notnull(d):
                        if d not in event_map:
                            event_map[d] = []
                        event_map[d].append(e_row)

            # 60-Day walkback
            batch_data = []
            for i in range(60):
                target_date = today - timedelta(days=i)
                day_event_str = "-"

                if target_date in event_map:
                    actions = event_map[target_date]
                    day_event_str = f"★ {actions[0].get('Firm', 'Analyst')}: {actions[0].get('ToGrade', 'Update')}"

                if i == 30 and len(summ) > 1:
                    row = summ.iloc[1]
                    curr = {
                        'sb': int(row['strongBuy']), 'b': int(row['buy']),
                        'h': int(row['hold']), 's': int(row['sell']), 'ss': int(row['strongSell'])
                    }

                score = calc_score(curr['sb'], curr['b'], curr['h'], curr['s'], curr['ss'])
                total_sum = sum(curr.values())

                batch_data.append((
                    symbol, target_date, float(score),
                    curr['sb'], curr['b'], curr['h'], curr['s'], curr['ss'],
                    total_sum, day_event_str
                ))

                # Reverse ledger logic
                if target_date in event_map:
                    for action in event_map[target_date]:
                        tg = str(action.get('ToGrade', '')).lower()
                        if 'buy' in tg and 'strong' not in tg:
                            curr['b'] = max(0, curr['b'] - 1)
                            curr['h'] += 1
                        elif 'strong buy' in tg:
                            curr['sb'] = max(0, curr['sb'] - 1)
                            curr['b'] += 1
                        elif any(x in tg for x in ['hold', 'neutral']):
                            curr['h'] = max(0, curr['h'] - 1)
                            curr['b'] += 1

            # UPSERT
            upsert_query = """
                INSERT INTO stock_ratings (ticker, date, score, sb, b, h, s, ss, total, event)
                VALUES %s
                ON CONFLICT (ticker, date) DO UPDATE SET
                    score = EXCLUDED.score, sb = EXCLUDED.sb, b = EXCLUDED.b,
                    h = EXCLUDED.h, s = EXCLUDED.s, ss = EXCLUDED.ss,
                    total = EXCLUDED.total, event = EXCLUDED.event;
            """
            execute_values(cursor, upsert_query, batch_data)
            conn.commit()

            print(f"Synced {symbol:<6} | Data Points: {len(batch_data)}")
            time.sleep(0.4)

        except Exception as e:
            print(f"Error on {symbol}: {e}")

    cursor.close()
    conn.close()

    print(f"\nLocked Production Sync Complete.")
    print(f"{'='*135}\n")

if __name__ == "__main__":
    run_production_sync()