# update_analyst_slope_stats.py
from main import SessionLocal, StockRating, AnalystSlopeStat
import numpy as np
from datetime import date

def compute_slopes(scores):
    """Compute 60-day rolling slopes from a list of scores (chronological)."""
    slopes = []
    for i in range(len(scores) - 60):
        y = scores[i:i+60]
        x = np.arange(60)
        slope = np.polyfit(x, y, 1)[0]
        slopes.append(slope)
    return slopes

def update_stats():
    session = SessionLocal()
    tickers = [t[0] for t in session.query(StockRating.ticker).distinct().all()]
    
    for ticker in tickers:
        # Get all scores for this ticker, ordered by date
        rows = session.query(StockRating.score).filter(StockRating.ticker == ticker).order_by(StockRating.date).all()
        scores = [r[0] for r in rows]
        
        if len(scores) < 61:
            # Not enough data to compute 60-day slopes
            mean_slope = 0.0
            std_slope = 0.0001  # small default to avoid division by zero
        else:
            slopes = compute_slopes(scores)
            mean_slope = float(np.mean(slopes))  # Convert to Python float
            std_slope = float(np.std(slopes))    # Convert to Python float
            if std_slope == 0:
                std_slope = 0.0001  # avoid division by zero
        
        # Upsert using a more robust approach to avoid numpy types
        stat = session.query(AnalystSlopeStat).filter(AnalystSlopeStat.ticker == ticker).first()
        if stat:
            stat.mean_slope = mean_slope
            stat.std_slope = std_slope
            stat.last_updated = date.today()
        else:
            stat = AnalystSlopeStat(
                ticker=ticker,
                mean_slope=mean_slope,
                std_slope=std_slope,
                last_updated=date.today()
            )
            session.add(stat)
        
        print(f"Updated {ticker}: mean={mean_slope:.6f}, std={std_slope:.6f}")
    
    session.commit()
    session.close()
    print("✅ Analyst slope statistics updated.")

if __name__ == "__main__":
    update_stats()