# test_thresholds.py
from main import SessionLocal, TradeSignal
import pandas as pd

def test_thresholds(thresholds=[65, 70, 75, 80, 85, 90, 95, 100]):
    session = SessionLocal()
    
    # Get all buy signals with forward returns (both 5d and 10d)
    signals = session.query(TradeSignal).filter(
        TradeSignal.signal_type == 'BUY',
        TradeSignal.forward_5d_return.isnot(None)
    ).all()
    
    if not signals:
        print("No buy signals with forward returns found.")
        session.close()
        return
    
    data = []
    for s in signals:
        data.append({
            'ticker': s.ticker,
            'signal_date': s.signal_date,
            'score': s.score_at_signal,
            'return_5d': s.forward_5d_return,
            'return_10d': s.forward_10d_return
        })
    df = pd.DataFrame(data)
    
    print(f"Total signals with forward data: {len(df)}")
    print(f"Date range: {df['signal_date'].min()} to {df['signal_date'].max()}\n")
    
    results = []
    for thresh in thresholds:
        subset = df[df['score'] >= thresh]
        if len(subset) == 0:
            continue
        
        ret5 = subset['return_5d'].dropna()
        ret10 = subset['return_10d'].dropna()
        
        results.append({
            'threshold': thresh,
            'signals': len(subset),
            'win_rate_5d': (ret5 > 0).mean(),
            'mean_return_5d': ret5.mean(),
            'median_return_5d': ret5.median(),
            'win_rate_10d': (ret10 > 0).mean(),
            'mean_return_10d': ret10.mean(),
            'median_return_10d': ret10.median()
        })
    
    # Display results
    results_df = pd.DataFrame(results)
    print("=== PERFORMANCE BY SCORE THRESHOLD ===")
    print(results_df.to_string(index=False, formatters={
        'win_rate_5d': '{:.1%}'.format,
        'mean_return_5d': '{:.2%}'.format,
        'median_return_5d': '{:.2%}'.format,
        'win_rate_10d': '{:.1%}'.format,
        'mean_return_10d': '{:.2%}'.format,
        'median_return_10d': '{:.2%}'.format
    }))
    
    session.close()

if __name__ == "__main__":
    test_thresholds()