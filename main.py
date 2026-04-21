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

# --- MODELS (with new columns) ---
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
    # NEW COLUMNS
    rsi_14d = Column(Float, default=50.0)
    macd_histogram = Column(Float, default=0.0)

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

class AnalystSlopeStat(Base):
    __tablename__ = "analyst_slope_stats"
    ticker = Column(String, primary_key=True)
    mean_slope = Column(Float, default=0.0)
    std_slope = Column(Float, default=0.0)
    last_updated = Column(Date, default=datetime.now().date)

class TradeSignal(Base):
    __tablename__ = "trade_signals"
    id = Column(Integer, primary_key=True)
    ticker = Column(String)
    signal_date = Column(Date)
    signal_type = Column(String)   # 'BUY' or 'SELL'
    score_at_signal = Column(Float, nullable=True)
    price_at_signal = Column(Float)
    buy_signal_id = Column(Integer, nullable=True)
    exit_reason = Column(String, nullable=True)
    entry_date = Column(Date, nullable=True)
    exit_date = Column(Date, nullable=True)
    exit_price = Column(Float, nullable=True)
    status = Column(String, default='OPEN')
    forward_5d_return = Column(Float, nullable=True)
    forward_10d_return = Column(Float, nullable=True)
    peak_price_since_buy = Column(Float, nullable=True)
    
# --- ANALYST TREND FUNCTION (unchanged, uses pre‑computed stats) ---
def analyze_rating_trend(ticker, session):
    ratings = session.query(StockRating.score, StockRating.event)\
        .filter(StockRating.ticker == ticker)\
        .order_by(StockRating.date.desc())\
        .limit(60).all()
    if len(ratings) < 20:
        return 0, []
    scores = [r.score for r in reversed(ratings)]
    x = np.arange(len(scores))
    y = np.array(scores)
    current_slope = np.polyfit(x, y, 1)[0]
    stat = session.query(AnalystSlopeStat).filter(AnalystSlopeStat.ticker == ticker).first()
    if stat and stat.std_slope > 0:
        z_score = (current_slope - stat.mean_slope) / stat.std_slope
    else:
        if current_slope > 0.001:
            return 30, ["RATING_IMPROVING"]
        else:
            return 0, []
    points = 0
    signals = []
    if len(scores) >= 60:
        first_half_slope = np.polyfit(x[:45], y[:45], 1)[0]
        recent_slope = np.polyfit(x[-15:], y[-15:], 1)[0]
        acceleration = recent_slope - first_half_slope
    else:
        acceleration = 0
    if z_score > 2.0:
        points = 50
        signals.append("RATING_ACCELERATING")
    elif z_score > 1.0:
        points = 30
        signals.append("RATING_IMPROVING")
    elif z_score > 0:
        points = 15
        signals.append("RATING_SLIGHTLY_IMPROVING")
    if acceleration > 0.0005 and z_score > 1.0:
        points += 10
        signals.append("RATING_ACCELERATION_BOOST")
    recent_event = next((r.event for r in ratings if r.event and r.event != "-"), None)
    if recent_event:
        points += 10
        signals.append(f"EVENT: {recent_event}")
    return points, signals

# --- CORE SCORING FUNCTIONS (with RSI and MACD) ---
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

    # ENGINE 2a: 5‑day RS Slope
    if curr.rs_slope_5d and curr.rs_slope_5d > 0:
        raw_score += 25
        signals.append("POSITIVE_TREND_CONFIRMED")
    elif curr.rs_slope_5d and curr.rs_slope_5d < -0.5:
        raw_score -= 20
        signals.append("BEARISH_DIVERGENCE")

    # ENGINE 2b: 20‑day RS Slope
    price_rows = session.query(DailyMetric.price).filter(
        DailyMetric.ticker == ticker,
        DailyMetric.date <= curr.date
    ).order_by(DailyMetric.date.desc()).limit(20).all()
    prices = [p[0] for p in reversed(price_rows)]
    if len(prices) >= 10:
        x = np.arange(len(prices))
        twenty_day_slope = np.polyfit(x, prices, 1)[0]
        if twenty_day_slope > 0:
            raw_score += 15
            signals.append("LONG_TERM_MOMENTUM")

    # ENGINE 3: RVOL
    if curr.average_volume_30d and curr.average_volume_30d > 0:
        rvol = curr.volume / curr.average_volume_30d
        if rvol > 2.0:
            raw_score += 40
            signals.append("INSTITUTIONAL_ACCUMULATION")
        elif rvol > 1.5:
            raw_score += 25
            signals.append("VOLUME_MOMENTUM")

    # ENGINE 4: Insider
    if curr.insider_score and curr.insider_score > 0:
        raw_score += curr.insider_score
        if curr.insider_alert_flag:
            signals.append(curr.insider_alert_flag)

    # ENGINE 5: News Sentiment
    if curr.sentiment_score is not None:
        s = curr.sentiment_score
        if s > 0.3:
            raw_score += 15
            signals.append("POSITIVE_NEWS")
        elif s > 0.1:
            raw_score += 5
            signals.append("SLIGHTLY_POSITIVE_NEWS")
        elif s < -0.3:
            raw_score -= 10
            signals.append("NEGATIVE_NEWS")
        elif s < -0.1:
            raw_score -= 3
            signals.append("SLIGHTLY_NEGATIVE_NEWS")

    # ENGINE 6: RSI (NEW)
    if curr.rsi_14d is not None:
        if curr.rsi_14d < 30:
            raw_score += 15
            signals.append("RSI_OVERSOLD")
        elif curr.rsi_14d > 70:
            raw_score -= 10
            signals.append("RSI_OVERBOUGHT")

    # ENGINE 7: MACD Histogram (NEW)
    if curr.macd_histogram is not None:
        if curr.macd_histogram > 0:
            raw_score += 20
            signals.append("MACD_BULLISH")
        else:
            raw_score -= 10
            signals.append("MACD_BEARISH")

    # SQUEEZE MULTIPLIER
    final_score = raw_score
    if curr.short_float_pct and curr.short_float_pct > 15:
        final_score *= 1.2
        signals.append("SQUEEZE_BOOST")

    # Optional cap at 100
    # final_score = min(final_score, 100)

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

def calculate_scout_score_for_date(ticker, target_date, session):
    """Same as above but for a specific historical date (for backfill)."""
    curr = session.query(DailyMetric).filter(
        DailyMetric.ticker == ticker,
        DailyMetric.date == target_date
    ).first()
    if not curr:
        return None

    raw_score = 0
    signals = []

    # Analyst trend (simplified for backfill; use pre‑computed stats if available)
    ratings = session.query(StockRating.score, StockRating.event)\
        .filter(StockRating.ticker == ticker, StockRating.date <= target_date)\
        .order_by(StockRating.date.desc())\
        .limit(60).all()
    if len(ratings) >= 20:
        scores = [r.score for r in reversed(ratings)]
        x = np.arange(len(scores))
        y = np.array(scores)
        current_slope = np.polyfit(x, y, 1)[0]
        stat = session.query(AnalystSlopeStat).filter(AnalystSlopeStat.ticker == ticker).first()
        if stat and stat.std_slope > 0:
            z_score = (current_slope - stat.mean_slope) / stat.std_slope
        else:
            z_score = None
        if stat and z_score is not None:
            if z_score > 2.0:
                raw_score += 50
                signals.append("RATING_ACCELERATING")
            elif z_score > 1.0:
                raw_score += 30
                signals.append("RATING_IMPROVING")
            elif z_score > 0:
                raw_score += 15
                signals.append("RATING_SLIGHTLY_IMPROVING")
        else:
            if current_slope > 0.001:
                raw_score += 30
                signals.append("RATING_IMPROVING")
        # acceleration
        if len(scores) >= 60:
            first_half_slope = np.polyfit(x[:45], y[:45], 1)[0]
            recent_slope = np.polyfit(x[-15:], y[-15:], 1)[0]
            if recent_slope - first_half_slope > 0.0005:
                raw_score += 10
                signals.append("RATING_ACCELERATION_BOOST")
        recent_event = next((r.event for r in ratings if r.event and r.event != "-"), None)
        if recent_event:
            raw_score += 10
            signals.append(f"EVENT: {recent_event}")

    # 5-day slope
    if curr.rs_slope_5d and curr.rs_slope_5d > 0:
        raw_score += 25
        signals.append("POSITIVE_TREND_CONFIRMED")
    elif curr.rs_slope_5d and curr.rs_slope_5d < -0.5:
        raw_score -= 20
        signals.append("BEARISH_DIVERGENCE")

    # 20-day slope
    price_rows = session.query(DailyMetric.price).filter(
        DailyMetric.ticker == ticker,
        DailyMetric.date <= target_date
    ).order_by(DailyMetric.date.desc()).limit(20).all()
    prices = [p[0] for p in reversed(price_rows)]
    if len(prices) >= 10:
        x = np.arange(len(prices))
        twenty_day_slope = np.polyfit(x, prices, 1)[0]
        if twenty_day_slope > 0:
            raw_score += 15
            signals.append("LONG_TERM_MOMENTUM")

    # RVOL
    if curr.average_volume_30d and curr.average_volume_30d > 0:
        rvol = curr.volume / curr.average_volume_30d
        if rvol > 2.0:
            raw_score += 40
            signals.append("INSTITUTIONAL_ACCUMULATION")
        elif rvol > 1.5:
            raw_score += 25
            signals.append("VOLUME_MOMENTUM")

    # Insider
    if curr.insider_score and curr.insider_score > 0:
        raw_score += curr.insider_score
        if curr.insider_alert_flag:
            signals.append(curr.insider_alert_flag)

    # Sentiment
    if curr.sentiment_score is not None:
        s = curr.sentiment_score
        if s > 0.3:
            raw_score += 15
            signals.append("POSITIVE_NEWS")
        elif s > 0.1:
            raw_score += 5
            signals.append("SLIGHTLY_POSITIVE_NEWS")
        elif s < -0.3:
            raw_score -= 10
            signals.append("NEGATIVE_NEWS")
        elif s < -0.1:
            raw_score -= 3
            signals.append("SLIGHTLY_NEGATIVE_NEWS")

    # RSI (NEW)
    if curr.rsi_14d is not None:
        if curr.rsi_14d < 30:
            raw_score += 15
            signals.append("RSI_OVERSOLD")
        elif curr.rsi_14d > 70:
            raw_score -= 10
            signals.append("RSI_OVERBOUGHT")

    # MACD (NEW)
    if curr.macd_histogram is not None:
        if curr.macd_histogram > 0:
            raw_score += 20
            signals.append("MACD_BULLISH")
        else:
            raw_score -= 10
            signals.append("MACD_BEARISH")

    # Squeeze multiplier
    final_score = raw_score
    if curr.short_float_pct and curr.short_float_pct > 15:
        final_score *= 1.2
        signals.append("SQUEEZE_BOOST")

    # Optional cap
    # final_score = min(final_score, 100)

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

# --- MAIN REPORT GENERATOR ---
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
                    # Store ALL scores
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