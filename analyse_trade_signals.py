# analyse_trade_signals.py
from main import SessionLocal, TradeSignal
import pandas as pd
import numpy as np

def analyse():
    session = SessionLocal()
    
    # Query all buy signals with forward returns
    signals = session.query(TradeSignal).filter(
        TradeSignal.signal_type == 'BUY',
        TradeSignal.forward_5d_return.isnot(None)
    ).all()
    
    if not signals:
        print("No buy signals with forward returns found in trade_signals.")
        session.close()
        return
    
    data = []
    for s in signals:
        data.append({
            'ticker': s.ticker,
            'signal_date': s.signal_date,
            'score': s.score_at_signal,
            'price_at_signal': s.price_at_signal,
            'return_5d': s.forward_5d_return,
            'return_10d': s.forward_10d_return
        })
    
    df = pd.DataFrame(data)
    
    print("\n" + "="*60)
    print("TRADE SIGNALS PERFORMANCE ANALYSIS")
    print("="*60)
    print(f"Total buy signals: {len(df)}")
    print(f"Date range: {df['signal_date'].min()} to {df['signal_date'].max()}")
    
    # 5-day returns
    ret5 = df['return_5d'].dropna()
    print("\n--- 5-DAY FORWARD RETURNS ---")
    print(f"Mean: {ret5.mean():.2%}")
    print(f"Median: {ret5.median():.2%}")
    print(f"Std Dev: {ret5.std():.2%}")
    print(f"Positive: {(ret5 > 0).sum()} / {len(ret5)} ({((ret5 > 0).sum()/len(ret5))*100:.1f}%)")
    print(f"Negative: {(ret5 < 0).sum()} / {len(ret5)} ({((ret5 < 0).sum()/len(ret5))*100:.1f}%)")
    
    # 10-day returns
    ret10 = df['return_10d'].dropna()
    print("\n--- 10-DAY FORWARD RETURNS ---")
    print(f"Mean: {ret10.mean():.2%}")
    print(f"Median: {ret10.median():.2%}")
    print(f"Std Dev: {ret10.std():.2%}")
    print(f"Positive: {(ret10 > 0).sum()} / {len(ret10)} ({((ret10 > 0).sum()/len(ret10))*100:.1f}%)")
    print(f"Negative: {(ret10 < 0).sum()} / {len(ret10)} ({((ret10 < 0).sum()/len(ret10))*100:.1f}%)")
    
    # Top 10 best performers (10-day)
    print("\n--- TOP 10 BEST PERFORMERS (10-DAY RETURN) ---")
    top10 = df.nlargest(10, 'return_10d')[['ticker', 'signal_date', 'score', 'return_10d']]
    for _, row in top10.iterrows():
        print(f"{row['ticker']} on {row['signal_date']}: score={row['score']:.1f}, return={row['return_10d']:.2%}")
    
    # Bottom 10 worst performers
    print("\n--- BOTTOM 10 WORST PERFORMERS (10-DAY RETURN) ---")
    bottom10 = df.nsmallest(10, 'return_10d')[['ticker', 'signal_date', 'score', 'return_10d']]
    for _, row in bottom10.iterrows():
        print(f"{row['ticker']} on {row['signal_date']}: score={row['score']:.1f}, return={row['return_10d']:.2%}")
    
    # Optional: histogram bins
    print("\n--- RETURN DISTRIBUTION (10-DAY) ---")
    bins = [-1, -0.1, -0.05, -0.02, 0, 0.02, 0.05, 0.1, 1]
    labels = ['<-10%', '-10% to -5%', '-5% to -2%', '-2% to 0%', '0% to 2%', '2% to 5%', '5% to 10%', '>10%']
    df['return_10d_bin'] = pd.cut(df['return_10d'], bins=bins, labels=labels)
    bin_counts = df['return_10d_bin'].value_counts().sort_index()
    for label, count in bin_counts.items():
        print(f"{label}: {count} signals ({count/len(df)*100:.1f}%)")
    
    session.close()

if __name__ == "__main__":
    analyse()