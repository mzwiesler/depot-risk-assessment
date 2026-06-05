import time

import requests
import yahooquery as yq
import yfinance as yf


def get_wkn_from_isin(isin: str) -> str | None:
    """Get WKN by searching for ISIN and retrieving ticker info"""
    try:
        # Search for the ISIN
        search_results = yq.search(query=isin)
        if search_results.get("quotes"):
            symbol = search_results["quotes"][0]["symbol"]

            # Get ticker info
            ticker = yf.Ticker(symbol)
            info = ticker.get_info()

            # Some tickers have WKN in the info
            return info.get("wkn")
    except Exception:
        return None


def wkn_to_yahoo_symbol(wkn, max_retries=3):
    """Convert WKN to Yahoo symbol with rate limiting and retry logic"""
    url = "https://query1.finance.yahoo.com/v1/finance/search"
    params = {"q": wkn, "quotesCount": 1, "newsCount": 0}

    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

    for attempt in range(max_retries):
        try:
            # Add delay to avoid rate limiting
            if attempt > 0:
                wait_time = 2**attempt  # Exponential backoff
                print(f"Retry {attempt}/{max_retries}, waiting {wait_time}s...")
                time.sleep(wait_time)

            r = requests.get(url, params=params, headers=headers, timeout=10)

            if r.status_code == 429:
                print(f"Rate limited (429), attempt {attempt + 1}/{max_retries}")
                continue

            r.raise_for_status()
            data = r.json()

            if data.get("quotes"):
                return data["quotes"][0]["symbol"]
            return None

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            if attempt == max_retries - 1:
                return None

    return None


# Example usage with delay between calls
test_wkns = ["A41B7Q", "A2DVB9"]
for wkn in test_wkns:
    print(f"\nSearching for WKN: {wkn}")
    symbol = wkn_to_yahoo_symbol(wkn)
    print(f"Symbol: {symbol}")
    time.sleep(1)  # Delay between requests
