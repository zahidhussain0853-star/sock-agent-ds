# daily_sell_signals.py
from main import SessionLocal, TradeSignal, ScoutScore, DailyMetric
from datetime import date, timedelta
import numpy as np

# Sell conditions
TRAILING_STOP_PCT = 0.05        # 5% below peak
SLOPE_THRESHOLD = -0.1          # exit if 5-day slope of scout score < this value
SLOPE_DAYS = 5                  # number of days to compute slope

def get_score_slope(ticker, end_date, session, window=SLOPE_DAYS):
    """Return the slope of scout score over the last `window` days (including end_date)."""
    start_date = end_date - timedelta(days=window-1)
    scores = session.query(ScoutScore.score).filter(
        ScoutScore.ticker == ticker,
        ScoutScore.date >= start_date,
        ScoutScore.date <= end_date
    ).order_by(ScoutScore.date).all()
    if len(scores) < window:
        return 0.0
    y = [s[0] for s in scores]
    x = np.arange(len(y))
    slope = np.polyfit(x, y, 1)[0]
    return slope

def update_peak_prices():
    """Update peak_price_since_buy for all open positions using daily prices."""
    session = SessionLocal()
    open_positions = session.query(TradeSignal).filter(
        TradeSignal.signal_type == 'BUY',
        TradeSignal.status == 'OPEN'
    ).all()
    for pos in open_positions:
        prices = session.query(DailyMetric.price).filter(
            DailyMetric.ticker == pos.ticker,
            DailyMetric.date >= pos.entry_date
        ).order_by(DailyMetric.date).all()
        if prices:
            peak = max(p[0] for p in prices)
            pos.peak_price_since_buy = peak
    session.commit()
    session.close()

def daily_sell_signals():
    session = SessionLocal()
    today = date.today()
    
    # Update peak prices for open positions
    update_peak_prices()
    
    # Get all open buy positions
    open_positions = session.query(TradeSignal).filter(
        TradeSignal.signal_type == 'BUY',
        TradeSignal.status == 'OPEN'
    ).all()
    
    sells = 0
    for pos in open_positions:
        # Get current price
        current_price_row = session.query(DailyMetric.price).filter(
            DailyMetric.ticker == pos.ticker,
            DailyMetric.date == today
        ).first()
        if not current_price_row:
            continue
        current_price = current_price_row[0]
        peak = pos.peak_price_since_buy or current_price
        
        # Check trailing stop loss
        stop_price = peak * (1 - TRAILING_STOP_PCT)
        trailing_hit = current_price <= stop_price
        
        # Check downward trend using slope of scout score
        slope = get_score_slope(pos.ticker, today, session)
        slope_hit = slope < SLOPE_THRESHOLD
        
        if trailing_hit or slope_hit:
            # Determine exit reason
            if trailing_hit and slope_hit:
                reason = 'STOP_LOSS_AND_DOWNTREND'
            elif trailing_hit:
                reason = 'TRAILING_STOP_LOSS'
            else:
                reason = f'DOWNWARD_TREND (slope={slope:.2f})'
            
            # Create sell signal
            sell = TradeSignal(
                ticker=pos.ticker,
                signal_date=today,
                signal_type='SELL',
                price_at_signal=current_price,
                buy_signal_id=pos.id,
                exit_reason=reason,
                exit_date=today,
                exit_price=current_price,
                status='CLOSED'
            )
            session.add(sell)
            
            # Close the buy position
            pos.status = 'CLOSED'
            pos.exit_date = today
            pos.exit_price = current_price
            pos.exit_reason = reason
            
            sells += 1
            print(f"SELL {pos.ticker} | Entry: {pos.entry_date} @ {pos.price_at_signal:.2f} | Exit: {today} @ {current_price:.2f} | Reason: {reason}")
    
    session.commit()
    print(f"✅ {sells} sell signals recorded for {today}.")
    session.close()

if __name__ == "__main__":
    daily_sell_signals()