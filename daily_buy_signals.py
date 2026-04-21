# daily_buy_signals.py
from main import SessionLocal, ScoutScore, TradeSignal, DailyMetric
from datetime import date

BUY_THRESHOLD = 40   # store all signals from WATCH upwards

def daily_buy_signals():
    session = SessionLocal()
    today = date.today()
    
    signals = session.query(ScoutScore).filter(
        ScoutScore.date == today,
        ScoutScore.score >= BUY_THRESHOLD
    ).all()
    
    new_signals = 0
    for s in signals:
        existing = session.query(TradeSignal).filter(
            TradeSignal.ticker == s.ticker,
            TradeSignal.signal_date == today,
            TradeSignal.signal_type == 'BUY'
        ).first()
        if existing:
            continue
        
        price = session.query(DailyMetric.price).filter(
            DailyMetric.ticker == s.ticker,
            DailyMetric.date == today
        ).first()
        if not price:
            continue
        
        ts = TradeSignal(
            ticker=s.ticker,
            signal_date=today,
            signal_type='BUY',
            score_at_signal=s.score,
            price_at_signal=price[0],
            entry_date=today,
            status='OPEN'
        )
        session.add(ts)
        new_signals += 1
    
    session.commit()
    print(f"✅ {new_signals} new buy signals recorded for {today} (score ≥{BUY_THRESHOLD}).")
    session.close()

if __name__ == "__main__":
    daily_buy_signals()