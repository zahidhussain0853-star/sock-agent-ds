import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from main import InsiderSignal
from utils import normalize_db_url
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = normalize_db_url()
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def fetch_with_retry(url, headers, timeout=30, retries=3):
    """Fetch URL with retries and exponential backoff."""
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.Timeout:
            print(f"Timeout (attempt {attempt+1}/{retries}), retrying in {2**attempt}s...")
            sleep(2**attempt)
        except Exception as e:
            print(f"Request error: {e}")
            if attempt == retries - 1:
                raise
            sleep(2**attempt)
    raise Exception(f"Failed to fetch after {retries} attempts")

def scrape_to_railway():
    """
    Scrapes OpenInsider and populates the insider_signals table.
    """
    url = "http://openinsider.com/latest-insider-purchases-25"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    print(f"--- STARTING INSIDER SYNC: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    session = None
    try:
        response = fetch_with_retry(url, headers, timeout=30, retries=3)
        
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'class': 'tinytable'})
        
        if not table:
            print("Error: Could not find data table on OpenInsider.")
            return

        session = Session()
        rows_added = 0
        
        all_tr = table.find_all('tr')[1:]
        batch_tickers = [tr.find_all('td')[3].text.strip() for tr in all_tr if len(tr.find_all('td')) > 3]
        ticker_counts = pd.Series(batch_tickers).value_counts()

        for tr in all_tr:
            cols = tr.find_all('td')
            if len(cols) < 12: 
                continue
                
            ticker = cols[3].text.strip()
            trade_date_str = cols[2].text.strip()
            
            raw_change = cols[11].text.strip()
            change_val = 0.0
            if '%' in raw_change:
                try:
                    change_val = float(raw_change.replace('%', '').replace('+', ''))
                except ValueError:
                    change_val = 0.0

            raw_value = cols[10].text.strip()
            try:
                value_num = float(raw_value.replace('$', '').replace(',', ''))
            except ValueError:
                value_num = 0.0

            signal = InsiderSignal(
                ticker=ticker,
                date=datetime.strptime(trade_date_str, '%Y-%m-%d').date(),
                insider_name=cols[4].text.strip(),
                title=cols[5].text.strip(),
                change_pct=change_val,
                value_num=value_num,
                is_cluster=bool(ticker_counts.get(ticker, 0) >= 3)
            )

            session.merge(signal)
            rows_added += 1

        session.commit()
        print(f"✅ SUCCESS: Synced {rows_added} records to 'insider_signals' table.")
        
    except Exception as e:
        print(f"❌ Scraper/Sync Error: {e}")
        if session:
            session.rollback()
    finally:
        if session:
            session.close()

if __name__ == "__main__":
    scrape_to_railway()