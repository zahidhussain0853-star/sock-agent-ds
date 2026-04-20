# backfill_insider_scores.py
from main import SessionLocal, DailyMetric, InsiderSignal
from datetime import timedelta

def get_insider_score(ticker, date, session):
    # Look at insider transactions 7 days prior to `date`
    start_date = date - timedelta(days=7)
    trades = session.query(InsiderSignal).filter(
        InsiderSignal.ticker == ticker,
        InsiderSignal.date >= start_date,
        InsiderSignal.date <= date
    ).all()
    
    if not trades:
        return 0.0, None
    
    total_value = sum(t.value_num for t in trades)
    avg_change = sum(t.change_pct for t in trades) / len(trades)
    
    score = 0
    flags = []
    
    if len(trades) >= 3:
        score += 20
        flags.append("INSIDER_CLUSTER")
    if total_value > 1_000_000:
        score += 15
        flags.append("LARGE_INSIDER_BUY")
    if avg_change > 20:
        score += 10
        flags.append("HIGH_INSIDER_CHANGE")
    
    flag_str = ", ".join(flags) if flags else None
    return float(score), flag_str

def backfill():
    session = SessionLocal()
    metrics = session.query(DailyMetric).all()
    updated = 0
    for metric in metrics:
        score, flag = get_insider_score(metric.ticker, metric.date, session)
        metric.insider_score = score
        metric.insider_alert_flag = flag
        updated += 1
        if updated % 1000 == 0:
            session.commit()
            print(f"Updated {updated} records...")
    session.commit()
    print(f"✅ Done. Updated {updated} daily metrics with insider scores.")

if __name__ == "__main__":
    backfill()