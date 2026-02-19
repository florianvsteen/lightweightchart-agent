
import yfinance as yf
import pandas as pd

def fetch_market_data(ticker, period="1d", interval="1m"):
    """
    Downloads data, cleans columns, and returns a DataFrame 
    formatted for analysis.
    """
    try:
        df = yf.download(ticker, period=period, interval=interval, progress=False)
        
        if df.empty:
            return pd.DataFrame()

        # Handle yfinance MultiIndex columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
            
        df = df.dropna()
        return df
    except Exception as e:
        print(f"!!! Data Fetch Error ({ticker}): {e}")
        return pd.DataFrame()

def format_for_charts(df):
    """
    Converts DataFrame into the list of dicts required by 
    Lightweight Charts.
    """
    if df.empty:
        return []
        
    return [
        {
            "time": int(idx.timestamp()), 
            "open": float(r['Open']), 
            "high": float(r['High']), 
            "low": float(r['Low']), 
            "close": float(r['Close'])
        } 
        for idx, r in df.iterrows()
    ]
