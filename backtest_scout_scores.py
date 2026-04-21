# backtest_scout_scores.py (store ALL scout scores, no threshold)
from main import SessionLocal, TradeSignal, ScoutScore, DailyMetric
from datetime import timedelta

def backtest_and_store():
    session = SessionLocal()
    
    # Optional: clear existing data (already cleared manually, but safe to keep)
    # session.query(TradeSignal).delete()
    
    # Get ALL scout scores (no filter)
    signals = session.query(ScoutScore.ticker, ScoutScore.date, ScoutScore.score).all()
    
    count = 0
    for ticker, signal_date, score in signals:
        # Get price on signal date
        price_t = session.query(DailyMetric.price).filter(
            DailyMetric.ticker == ticker,
            DailyMetric.date == signal_date
        ).first()
        if not price_t:
            continue
        
        # 5-day forward
        date_5 = signal_date + timedelta(days=5)
        price_5 = session.query(DailyMetric.price).filter(
            DailyMetric.ticker == ticker,
            DailyMetric.date == date_5
        ).first()
        ret_5 = (price_5[0] / price_t[0] - 1) if price_5 else None
        
        # 10-day forward
        date_10 = signal_date + timedelta(days=10)
        price_10 = session.query(DailyMetric.price).filter(
            DailyMetric.ticker == ticker,
            DailyMetric.date == date_10
        ).first()
        ret_10 = (price_10[0] / price_t[0] - 1) if price_10 else None
        
        ts = TradeSignal(
            ticker=ticker,
            signal_date=signal_date,
            signal_type='BUY',
            score_at_signal=score,
            price_at_signal=price_t[0],
            forward_5d_return=ret_5,
            forward_10d_return=ret_10,
            status='CLOSED' if (price_5 or price_10) else 'OPEN'
        )
        session.add(ts)
        count += 1
        if count % 100 == 0:
            session.commit()
            print(f"Processed {count} signals...")
    
    session.commit()
    print(f"✅ Stored {count} buy signals (all scores) in trade_signals.")
    session.close()

if __name__ == "__main__":
    backtest_and_store()