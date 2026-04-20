from main import engine
from sqlalchemy import text

with engine.connect() as conn:
    # Check if constraint already exists
    result = conn.execute(text("""
        SELECT constraint_name 
        FROM information_schema.table_constraints 
        WHERE table_name = 'daily_metrics' 
        AND constraint_type = 'UNIQUE'
    """))
    constraints = result.fetchall()
    
    if constraints:
        print("Existing unique constraints:", [c[0] for c in constraints])
    else:
        print("No unique constraint found. Adding one...")
        try:
            conn.execute(text("""
                ALTER TABLE daily_metrics 
                ADD CONSTRAINT daily_metrics_ticker_date_key 
                UNIQUE (ticker, date)
            """))
            conn.commit()
            print("✅ Unique constraint added successfully.")
        except Exception as e:
            print(f"❌ Error: {e}")