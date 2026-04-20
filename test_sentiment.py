import feedparser
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Initialize VADER sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

def get_sentiment_from_rss(ticker):
    """Fetch Google News RSS for ticker and return average sentiment score (-1 to 1)."""
    url = f"https://news.google.com/rss/search?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en"
    print(f"Fetching RSS for {ticker}...")
    feed = feedparser.parse(url)
    if not feed.entries:
        print("No news found.")
        return 0.0
    scores = []
    for entry in feed.entries[:10]:  # latest 10 headlines
        score = analyzer.polarity_scores(entry.title)['compound']
        scores.append(score)
        print(f"  Headline: {entry.title[:60]}... | Sentiment: {score:.3f}")
    avg_score = sum(scores) / len(scores)
    print(f"\nAverage sentiment for {ticker}: {avg_score:.3f}")
    return avg_score

if __name__ == "__main__":
    ticker = input("Enter ticker (e.g., AAPL): ").strip().upper()
    if not ticker:
        ticker = "AAPL"
    sentiment = get_sentiment_from_rss(ticker)
    print(f"Final score: {sentiment:.3f}")