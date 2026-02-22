"""
providers/yahoo.py

Yahoo Finance data provider via yfinance.

Exposes:
  get_df(ticker, interval, period)  → pd.DataFrame  (OHLCV, DatetimeIndex)
  get_bias_df(ticker, period, interval) → pd.DataFrame  (for bias fetching in supply_demand)
  LOCK  — process-wide threading.Lock to serialize yfinance downloads
"""

import threading
import pandas as pd
import yfinance as yf

# yfinance has shared internal state — serialize all downloads process-wide
LOCK = threading.Lock()

# Map standard interval strings to yfinance period defaults
PERIOD_MAP = {
    "1m":  "1d",
    "2m":  "1d",
    "5m":  "5d",
    "15m": "5d",
    "30m": "5d",
    "1h":  "30d",
}


def get_df(ticker: str, interval: str, period: str = None) -> pd.DataFrame:
    """
    Download OHLCV data from Yahoo Finance.

    Args:
        ticker:   yfinance ticker symbol (e.g. "YM=F", "EURUSD=X")
        interval: candle interval ("1m", "5m", "15m", "30m", "1h")
        period:   lookback period ("1d", "5d", "30d" …). If None, derived from interval.

    Returns:
        pd.DataFrame with columns Open, High, Low, Close, Volume and a DatetimeIndex.
        Returns an empty DataFrame on failure.
    """
    if period is None:
        period = PERIOD_MAP.get(interval, "1d")
    with LOCK:
        df = yf.download(ticker, period=period, interval=interval, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df.dropna()


def get_bias_df(ticker: str, period: str, interval: str) -> pd.DataFrame:
    """
    Download data for bias calculation (daily / weekly candles).
    Uses the same global lock as get_df.
    """
    with LOCK:
        df = yf.download(ticker, period=period, interval=interval, progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df.dropna()
