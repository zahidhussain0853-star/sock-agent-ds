import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def get_sp500_tickers():
    """
    Fetch S&P 500 tickers from GitHub.
    Returns tickers with '.' replaced by '-' for Yahoo Finance compatibility.
    """
    url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
    df = pd.read_csv(url)
    return [t.replace('.', '-') for t in df['Symbol'].tolist()]

def normalize_db_url():
    """Convert Railway's postgres:// to postgresql:// for SQLAlchemy/psycopg2."""
    raw_url = os.getenv("DATABASE_URL")
    if raw_url and raw_url.startswith("postgres://"):
        return raw_url.replace("postgres://", "postgresql://", 1)
    return raw_url