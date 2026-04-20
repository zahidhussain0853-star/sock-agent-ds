import os
import numpy as np
from sqlalchemy import create_engine, Column, Integer, String, Date, Float, Boolean, BigInteger
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
from dotenv import load_dotenv
from utils import normalize_db_url

load_dotenv()

# --- DATABASE SETUP ---
DATABASE_URL = normalize_db_url()
engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_recycle=300,
    pool_pre_ping=True,
    connect_args={"connect_timeout": 60}
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
Base = declarative_base()

# --- EXISTING MODELS ---
class DailyMetric(Base):
    __tablename__ = "daily_metrics"
    id = Column(Integer, primary_key=True)
    ticker = Column(String)
    date = Column(Date)
    analyst_rating = Column(Float)
    sentiment_score = Column(Float)
    volume = Column(BigInteger)
    average_volume_30d = Column(Integer)
    call_put_ratio = Column(Float, default=0.0)
    short_float_pct = Column(Float)
    bb_width_30d_low = Column(Boolean, default=False)
    rs_slope_5d = Column(Float)
    price = Column(Float)
    insider_score = Column(Float, default=0.0)
    insider_alert_flag = Column(String, nullable=True)

class StockRating(Base):
    __tablename__ = "stock_ratings"
    ticker = Column(String, primary_key=True)
    date = Column(Date, primary_key=True)
    score = Column(Float)
    sb = Column(Integer)
    b = Column(Integer)
    h = Column(Integer)
    s = Column(Integer)
    ss = Column(Integer)
    total = Column(Integer)
    event = Column(String)

class InsiderSignal(Base):
    __tablename__ = "insider_signals"
    ticker = Column(String, primary_key=True)
    date = Column(Date, primary_key=True)
    insider_name = Column(String)
    title = Column(String)
    change_pct = Column(Float)
    value_num = Column(Float)
    is_cluster = Column(Boolean, default=False)

class ScoutScore(Base):
    __tablename__ = "scout_scores"
    ticker = Column(String, primary_key=True)
    date = Column(Date, primary_key=True)
    score = Column(Float)
    action = Column(String)
    signals = Column(String)

# --- TREND QUANTIZATION (unchanged) ---
def analyze_rating_trend(ticker, session):
    ratings = session.query(StockRating.score, StockRating.event)\
        .filter(StockRating.ticker == ticker)\
        .order_by(StockRating.date.desc())\
        .limit(60).all()
    if len(ratings) < 20:
        return 0, []
    scores = [r.score for r in reversed(ratings)]
    recent_event = next((r.event for r in ratings if r.event and r.event != "-"), None)
    x = np.arange(len(scores))
    y = np.array(scores)
    slope, _ = np.polyfit(x, y, 1)
    first_half_slope = np.polyfit(x[:45], y[:45], 1)[0] if len(x) > 45 else slope
    recent_slope = np.polyfit(x[-15:], y[-15:], 1)[0] if len(x) >= 15 else slope
    acceleration = recent_slope - first_half_slope
    points = 0
    signals = []
    if slope > 0.001:
        if acceleration > 0.0005:
            points = 50
            signals.append("RATING_ACCELERATING")
        else:
            points = 30
            signals.append("RATING_IMPROVING")
        if recent_event:
            points += 10
            signals.append(f"EVENT: {recent_event}")
    return points, signals

# --- CORE ALGORITHM (updated with sentiment) ---
def calculate_scout_score(ticker, session):
    curr = session.query(DailyMetric).filter_by(ticker=ticker).order_by(DailyMetric.date.desc()).first()
    if not curr:
        return None

    raw_score = 0
    signals = []

    # ENGINE 1: Analyst Velocity
    rating_points, rating_signals = analyze_rating_trend(ticker, session)
    raw_score += rating_points
    signals.extend(rating_signals)

    # ENGINE 2: RS Slope
    if curr.rs_slope_5d and curr.rs_slope_5d > 0:
        raw_score += 25
        signals.append("POSITIVE_TREND_CONFIRMED")
    elif curr.rs_slope_5d and curr.rs_slope_5d < -0.5:
        raw_score -= 20
        signals.append("BEARISH_DIVERGENCE")

    # ENGINE 3: RVOL Multiplier
    if curr.average_volume_30d and curr.average_volume_30d > 0:
        rvol = curr.volume / curr.average_volume_30d
        if rvol > 2.0:
            raw_score += 40
            signals.append("INSTITUTIONAL_ACCUMULATION")
        elif rvol > 1.5:
            raw_score += 25
            signals.append("VOLUME_MOMENTUM")

    # ENGINE 4: Insider Conviction
    if curr.insider_score and curr.insider_score > 0:
        raw_score += curr.insider_score
        if curr.insider_alert_flag:
            signals.append(curr.insider_alert_flag)

    # ENGINE 5: News Sentiment (new)
    if curr.sentiment_score is not None:
        s = curr.sentiment_score
        if s > 0.3:
            raw_score += 15
            signals.append("POSITIVE_NEWS")
        elif s < -0.3:
            raw_score -= 10
            signals.append("NEGATIVE_NEWS")
        elif s > 0.1:
            raw_score += 5
            signals.append("SLIGHTLY_POSITIVE_NEWS")
        elif s < -0.1:
            raw_score -= 3
            signals.append("SLIGHTLY_NEGATIVE_NEWS")

    # FINAL: Squeeze multiplier
    final_score = raw_score
    if curr.short_float_pct and curr.short_float_pct > 15:
        final_score *= 1.2
        signals.append("SQUEEZE_BOOST")

    # Threshold mapping
    if final_score >= 80:
        action = "🔥 STRONG BUY"
    elif final_score >= 65:
        action = "✅ BUY"
    elif final_score >= 40:
        action = "👀 WATCH"
    else:
        action = "❄️ HOLD"

    return {
        "ticker": ticker,
        "score": round(final_score, 2),
        "signals": signals,
        "action": action
    }

def init_db():
    Base.metadata.create_all(engine)

# --- REPORT GENERATOR + STORAGE ---
if __name__ == "__main__":
    init_db()
    print(f"--- SCOUT REPORT (TRI-FACTOR REFINED): {datetime.now().strftime('%Y-%m-%d')} ---")
    session = SessionLocal()
    try:
        tickers = [t.ticker for t in session.query(DailyMetric.ticker).distinct().all()]
        today = datetime.now().date()
        for t in tickers:
            try:
                res = calculate_scout_score(t, session)
                if res:
                    if res['score'] >= 40:
                        print(f"{res['action']} | {res['ticker']} | Score: {res['score']} | Signals: {res['signals']}")
                    # Store ALL scores (including below 40) for accurate averages
                    from sqlalchemy.dialects.postgresql import insert
                    stmt = insert(ScoutScore).values(
                        ticker=t,
                        date=today,
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
            except Exception as e:
                print(f"Error processing {t}: {e}")
        session.commit()
        print(f"\n✅ Scout scores stored for {today}.")
    finally:
        session.close()