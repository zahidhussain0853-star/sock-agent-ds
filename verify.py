from main import SessionLocal, DailyMetric
from datetime import date

session = SessionLocal()

# 1. Check today's rows with non-zero sentiment
today = date.today()
rows_today = session.query(DailyMetric).filter(
    DailyMetric.date == today,
    DailyMetric.sentiment_score != 0
).count()
print(f"Rows with sentiment != 0 for {today}: {rows_today}")

# 2. Check any row with non-zero sentiment (any date)
any_rows = session.query(DailyMetric).filter(DailyMetric.sentiment_score != 0).count()
print(f"Total rows with sentiment != 0 in entire table: {any_rows}")

# 3. Show a few sample sentiment values (including zeros) for today
sample = session.query(DailyMetric.ticker, DailyMetric.sentiment_score).filter(DailyMetric.date == today).limit(10).all()
print(f"\nSample sentiment scores for {today}:")
for ticker, score in sample:
    print(f"  {ticker}: {score}")

# 4. Check the latest date in the table
latest = session.query(DailyMetric.date).order_by(DailyMetric.date.desc()).first()
print(f"\nLatest date in daily_metrics: {latest[0] if latest else None}")

session.close()