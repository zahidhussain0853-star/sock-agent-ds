# add_columns.py
from main import engine
from sqlalchemy import text

with engine.connect() as conn:
    conn.execute(text("ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS rsi_14d FLOAT DEFAULT 50.0"))
    conn.execute(text("ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS macd_histogram FLOAT DEFAULT 0.0"))
    conn.commit()
    print("✅ Columns added successfully.")