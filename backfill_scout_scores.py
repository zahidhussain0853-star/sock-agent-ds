# backfill_scout_scores.py (30-day backfill)
from main import SessionLocal, ScoutScore, calculate_scout_score_for_date
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text

def backfill():
    session = SessionLocal()
    
    # Get dates from the last 30 days only
    dates = session.execute(text("""
        SELECT DISTINCT date FROM daily_metrics 
        WHERE date >= (SELECT MAX(date) - interval '30 days' FROM daily_metrics)
        ORDER BY date
    """)).fetchall()
    dates = [d[0] for d in dates]
    
    tickers = session.execute(text("SELECT DISTINCT ticker FROM daily_metrics")).fetchall()
    tickers = [t[0] for t in tickers]
    
    total = 0
    for target_date in dates:
        print(f"Backfilling {target_date}...")
        for ticker in tickers:
            res = calculate_scout_score_for_date(ticker, target_date, session)
            if res:
                stmt = insert(ScoutScore).values(
                    ticker=ticker,
                    date=target_date,
                    score=res['score'],
                    action=res['action'],
                    signals=','.join(res['signals'])
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=['ticker', 'date'],
                    set_={
                        'score': stmt.excluded.score,
                        'action': stmt.excluded.action,
                        'signals': stmt.excluded.signals
                    }
                )
                session.execute(stmt)
                total += 1
        session.commit()
        print(f"  Stored {total} rows so far")
    
    print(f"✅ Backfill complete. Total rows stored: {total}")
    session.close()

if __name__ == "__main__":
    backfill()