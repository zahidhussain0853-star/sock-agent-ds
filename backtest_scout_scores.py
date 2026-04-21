# backtest_scout_scores.py (fast – only scores ≥40)
from main import SessionLocal, TradeSignal, ScoutScore, DailyMetric
from datetime import timedelta

def backtest_and_store(score_threshold=40):
    session = SessionLocal()
    
    # Clear old data (optional – if you want fresh)
    session.query(TradeSignal).filter(TradeSignal.signal_type == 'BUY').delete()
    
    # Only scores ≥ threshold
    signals = session.query(ScoutScore).filter(ScoutScore.score >= score_threshold).all()
    
    count = 0
    for s in signals:
        price_t = session.query(DailyMetric.price).filter(
            DailyMetric.ticker == s.ticker,
            DailyMetric.date == s.date
        ).first()
        if not price_t:
            continue
        
        date_5 = s.date + timedelta(days=5)
        price_5 = session.query(DailyMetric.price).filter(
            DailyMetric.ticker == s.ticker,
            DailyMetric.date == date_5
        ).first()
        ret_5 = (price_5[0] / price_t[0] - 1) if price_5 else None
        
        date_10 = s.date + timedelta(days=10)
        price_10 = session.query(DailyMetric.price).filter(
            DailyMetric.ticker == s.ticker,
            DailyMetric.date == date_10
        ).first()
        ret_10 = (price_10[0] / price_t[0] - 1) if price_10 else None
        
        ts = TradeSignal(
            ticker=s.ticker,
            signal_date=s.date,
            signal_type='BUY',
            score_at_signal=s.score,
            price_at_signal=price_t[0],
            forward_5d_return=ret_5,
            forward_10d_return=ret_10,
            entry_date=s.date,
            status='CLOSED' if (price_5 or price_10) else 'OPEN'
        )
        session.add(ts)
        count += 1
        if count % 100 == 0:
            session.commit()
            print(f"Processed {count} signals...")
    
    session.commit()
    print(f"✅ Stored {count} historical buy signals (score ≥{score_threshold}).")
    session.close()

if __name__ == "__main__":
    backtest_and_store()