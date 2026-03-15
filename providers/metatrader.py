"""
providers/metatrader.py

MetaTrader 5 data provider via the openclaw MT5 Flask API.
Calls the Wine-hosted Flask app running inside the MT5 Docker container.

Required env vars:
  MT5_API_URL            Base URL of the MT5 Flask API (e.g. http://mt5:5001)
  MT5_BROKER_UTC_OFFSET  Hours to subtract from broker time to get UTC (default: 2)

Exposes:
  get_df(ticker, interval, period)       → pd.DataFrame  (OHLCV, DatetimeIndex, UTC)
  get_bias_df(ticker, period, interval)  → pd.DataFrame
  LOCK  — threading.Lock (shared across all calls)

Interval → MT5 timeframe string:
  "1m"  → M1    "3m"  → M3    "5m"  → M5    "15m" → M15   "30m" → M30
  "1h"  → H1    "4h"  → H4    "1d"  → D1    "1wk" → W1

Period string → number of bars to fetch:
  "1d"  → 1440   "5d"  → 7200   "30d" → 43200
  "3mo" → 129600  "6mo" → 259200
"""

import os
import threading
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta

# ── Provider lock ─────────────────────────────────────────────────────────────
LOCK = threading.Lock()

# Most MT5 brokers run on UTC+2 (EET) or UTC+3 (EEST in summer).
# The API returns bar timestamps in broker LOCAL time with no timezone info.
# Override with: MT5_BROKER_UTC_OFFSET=3 in your environment.
_BROKER_UTC_OFFSET = int(os.environ.get("MT5_BROKER_UTC_OFFSET", "3"))

# ── Interval string → MT5 timeframe string ────────────────────────────────────
_TF_MAP = {
    "1m":   "M1",
    "2m":   "M2",
    "3m":   "M3",
    "5m":   "M5",
    "15m":  "M15",
    "30m":  "M30",
    "1h":   "H1",
    "4h":   "H4",
    "1d":   "D1",
    "1w":   "W1",
    "1wk":  "W1",
}

# ── Interval → bars needed for a given period ─────────────────────────────────
_INTERVAL_MINUTES = {
    "1m": 1, "2m": 2, "3m": 3, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "4h": 240, "1d": 1440, "1w": 10080, "1wk": 10080,
}

_PERIOD_MINUTES = {
    "1d":  1440,
    "5d":  7200,
    "30d": 43200,
    "60d": 86400,
    "3mo": 129600,
    "6mo": 259200,
    "1y":  525600,
}


def _num_bars(interval: str, period: str) -> int:
    iv = _INTERVAL_MINUTES.get(interval, 1)
    pm = _PERIOD_MINUTES.get(period, 1440)
    return max(1, pm // iv)


def _base_url() -> str:
    url = os.environ.get("MT5_API_URL", "").rstrip("/")
    if not url:
        raise RuntimeError(
            "MT5_API_URL is not set. "
            "Example: export MT5_API_URL=http://mt5:5001"
        )
    return url


# ── Core fetch ────────────────────────────────────────────────────────────────

def _fetch(ticker: str, interval: str, period: str) -> pd.DataFrame:
    tf = _TF_MAP.get(interval)
    if tf is None:
        print(f"[metatrader] Unsupported interval: {interval!r}")
        return pd.DataFrame()

    num_bars = _num_bars(interval, period)

    try:
        resp = requests.get(
            f"{_base_url()}/fetch_data_pos",
            params={
                "symbol":    ticker,
                "timeframe": tf,
                "num_bars":  num_bars,
            },
            timeout=30,
        )

        if resp.status_code == 200:
            bars = resp.json()
            if not bars:
                print(f"[metatrader] No bars returned for {ticker} {interval} ({period})")
                return pd.DataFrame()
            return _bars_to_df(bars)

        if resp.status_code == 404:
            print(f"[metatrader] Symbol not found or no data: {ticker}")
            return pd.DataFrame()

        print(f"[metatrader] HTTP {resp.status_code} for {ticker}: {resp.text[:200]}")
        return pd.DataFrame()

    except requests.Timeout:
        print(f"[metatrader] Request timed out for {ticker}")
        return pd.DataFrame()
    except requests.RequestException as e:
        print(f"[metatrader] Request error for {ticker}: {e}")
        return pd.DataFrame()


def _bars_to_df(bars: list) -> pd.DataFrame:
    """Convert /fetch_data_pos response to standard OHLCV DataFrame with UTC DatetimeIndex."""
    df = pd.DataFrame(bars)

    if df.empty:
        return df

    # Rename columns to standard names
    df = df.rename(columns={
        "time":        "time",
        "open":        "Open",
        "high":        "High",
        "low":         "Low",
        "close":       "Close",
        "tick_volume": "Volume",
    })

    # Keep only needed columns
    cols = [c for c in ["time", "Open", "High", "Low", "Close", "Volume"] if c in df.columns]
    df = df[cols]

    # Parse time — API returns naive datetime strings in broker local time (UTC+2 or UTC+3)
    # Treat as naive, subtract broker offset to get UTC, then localize as UTC
    df["time"] = pd.to_datetime(df["time"], utc=False, errors="coerce")
    df["time"] = df["time"] - pd.Timedelta(hours=_BROKER_UTC_OFFSET)
    df["time"] = df["time"].dt.tz_localize("UTC")

    df = df.dropna(subset=["time"])
    df = df.set_index("time")

    return df


# ── Public interface ──────────────────────────────────────────────────────────

def get_df(ticker: str, interval: str, period: str = None) -> pd.DataFrame:
    """
    Download OHLCV data from MetaTrader 5 via the openclaw MT5 Flask API.

    Args:
        ticker:   MT5 symbol name (e.g. "EURUSD", "XAUUSD", "US30Cash")
        interval: candle interval ("1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d")
        period:   lookback period ("1d", "5d", "30d", "3mo", "6mo"). Derived from interval if None.

    Returns:
        pd.DataFrame with Open, High, Low, Close, Volume columns and UTC DatetimeIndex.
        Returns an empty DataFrame on failure.
    """
    if period is None:
        period = _default_period(interval)

    with LOCK:
        return _fetch(ticker, interval, period)


def get_bias_df(ticker: str, period: str, interval: str) -> pd.DataFrame:
    """
    Download data for bias calculation.
    API-compatible with providers/yahoo.py.
    """
    return get_df(ticker, interval, period)


def get_symbol_info(ticker: str) -> dict | None:
    """
    Fetch symbol metadata (spread, digits, volume_min etc.) from the API.
    Returns None on failure.
    """
    try:
        resp = requests.get(
            f"{_base_url()}/symbol_info/{ticker}",
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json()
        return None
    except requests.RequestException as e:
        print(f"[metatrader] symbol_info error for {ticker}: {e}")
        return None


def get_tick(ticker: str) -> dict | None:
    """
    Fetch latest bid/ask tick for a symbol.
    Returns None on failure.
    """
    try:
        resp = requests.get(
            f"{_base_url()}/symbol_info_tick/{ticker}",
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json()
        return None
    except requests.RequestException as e:
        print(f"[metatrader] tick error for {ticker}: {e}")
        return None


def _default_period(interval: str) -> str:
    """Sensible default period for a given interval."""
    defaults = {
        "1m": "1d", "2m": "1d", "3m": "1d", "5m": "5d",
        "15m": "5d", "30m": "30d", "1h": "30d",
        "4h": "3mo", "1d": "1y", "1w": "1y", "1wk": "1y",
    }
    return defaults.get(interval, "1d")
