"""
providers/metatrader.py

MetaTrader 5 data provider via the mt5rest HTTP API.
Assumes the mt5rest server is already configured with MT5 account credentials.
This provider just obtains a session token and uses it to fetch OHLCV data.

Required env vars:
  MT5_API_URL   Base URL of the mt5rest API (e.g. https://mt5.flownet.be)

Exposes:
  get_df(ticker, interval, period)       → pd.DataFrame  (OHLCV, DatetimeIndex, UTC)
  get_bias_df(ticker, period, interval)  → pd.DataFrame
  LOCK  — threading.Lock (shared across all calls)

Interval → timeframe in minutes:
  "1m"  →  1    "5m"  →  5    "15m" → 15    "30m" → 30
  "1h"  → 60    "4h"  → 240   "1d"  → 1440  "1wk" → 10080

Period string → lookback window:
  "1d" → 1 day   "5d" → 5 days   "30d" → 30 days
  "3mo" → 90 days   "6mo" → 180 days

mt5rest Bar schema (/PriceHistory response):
  { time, openPrice, highPrice, lowPrice, closePrice, tickVolume, volume }
"""

import os
import threading
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta

# ── Provider lock (API-compatible with yahoo.py) ─────────────────────────────
LOCK = threading.Lock()

# ── Interval string → timeframe in minutes ────────────────────────────────────
_TF_MAP = {
    "1m":   1,
    "2m":   2,
    "5m":   5,
    "15m":  15,
    "30m":  30,
    "1h":   60,
    "4h":   240,
    "1d":   1440,
    "1wk":  10080,
}

# ── yfinance-style period string → timedelta ──────────────────────────────────
_PERIOD_MAP = {
    "1d":  timedelta(days=1),
    "5d":  timedelta(days=5),
    "30d": timedelta(days=30),
    "3mo": timedelta(days=90),
    "6mo": timedelta(days=180),
    "1y":  timedelta(days=365),
}

# ── mt5rest datetime format ───────────────────────────────────────────────────
_DT_FMT = "%Y-%m-%dT%H:%M:%S"

# ── Module-level session token — established once, reused across all calls ────
_token: str | None = None
_token_lock = threading.Lock()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _base_url() -> str:
    url = os.environ.get("MT5_API_URL", "").rstrip("/")
    if not url:
        raise RuntimeError(
            "MT5_API_URL is not set. "
            "Example: export MT5_API_URL=http://192.168.1.10:5000"
        )
    return url


def _connect() -> str:
    """
    Obtain a session token from the mt5rest API.
    The server already knows which MT5 account to use — no credentials needed.
    Just calls GET {MT5_API_URL}/Connect and returns the token.
    """
    resp = requests.get(f"{_base_url()}/Connect", timeout=30)

    if resp.status_code != 200:
        try:
            err = resp.json()
            detail = f"{err.get('code', '')} — {err.get('message', '')}"
        except Exception:
            detail = resp.text[:200]
        raise RuntimeError(f"[metatrader] /Connect failed: HTTP {resp.status_code} — {detail}")

    token = resp.text.strip().strip('"')
    print(f"[metatrader] Connected — token: {token[:8]}…")
    return token


def _get_token() -> str:
    """Return cached session token, connecting on first call."""
    global _token
    if _token:
        return _token
    with _token_lock:
        if not _token:
            _token = _connect()
    return _token


def _reconnect():
    """Force a reconnect — called when the API reports a session error."""
    global _token
    print("[metatrader] Session expired or lost — reconnecting…")
    with _token_lock:
        _token = None
        _token = _connect()


def _bars_to_df(bars: list) -> pd.DataFrame:
    """Convert mt5rest Bar list to a standard OHLCV DataFrame with UTC DatetimeIndex."""
    if not bars:
        return pd.DataFrame()

    df = pd.DataFrame([{
        "time":   b["time"],
        "Open":   b["openPrice"],
        "High":   b["highPrice"],
        "Low":    b["lowPrice"],
        "Close":  b["closePrice"],
        # mt5rest Bar schema: tickVolume = number of ticks (preferred), volume = real volume
        "Volume": b.get("tickVolume") or b.get("volume", 0),
    } for b in bars])

    df["time"] = pd.to_datetime(df["time"], format=_DT_FMT, utc=True)
    df = df.set_index("time")
    return df.dropna()


# ── Core fetch ────────────────────────────────────────────────────────────────

def _fetch(ticker: str, interval: str, period: str) -> pd.DataFrame:
    """
    Call /PriceHistory and return a DataFrame.
    On session error (HTTP 201 with token-related code), reconnects once and retries.
    """
    tf = _TF_MAP.get(interval)
    if tf is None:
        print(f"[metatrader] Unsupported interval: {interval!r}")
        return pd.DataFrame()

    delta     = _PERIOD_MAP.get(period, timedelta(days=1))
    now       = datetime.now(timezone.utc)
    date_from = now - delta

    params = {
        "id":        _get_token(),
        "symbol":    ticker,
        "from":      date_from.strftime(_DT_FMT),
        "to":        now.strftime(_DT_FMT),
        "timeFrame": tf,
    }

    # Retry once on session/connection errors
    for attempt in range(2):
        try:
            resp = requests.get(
                f"{_base_url()}/PriceHistory",
                params=params,
                timeout=30,
            )

            if resp.status_code == 200:
                bars = resp.json()
                if not isinstance(bars, list):
                    print(f"[metatrader] Unexpected /PriceHistory response for {ticker}: {bars}")
                    return pd.DataFrame()
                df = _bars_to_df(bars)
                if df.empty:
                    print(f"[metatrader] No bars returned for {ticker} {interval} ({period})")
                return df

            # HTTP 201 = mt5rest exception (token expired, invalid symbol, etc.)
            if resp.status_code == 201:
                try:
                    err  = resp.json()
                    code = err.get("code", "UNKNOWN")
                    msg  = err.get("message", "")
                except Exception:
                    code, msg = "UNKNOWN", resp.text[:100]

                # Token / connection errors → reconnect and retry
                TOKEN_ERRORS = {"INVALID_TOKEN", "NO_CONNECTION", "CONNECT_ERROR", "TIMEOUT"}
                if attempt == 0 and code in TOKEN_ERRORS:
                    print(f"[metatrader] Session error ({code}) for {ticker} — {msg}")
                    _reconnect()
                    params["id"] = _get_token()
                    continue

                # Symbol not found or other hard errors — don't retry
                print(f"[metatrader] API error for {ticker}: {code} — {msg}")
                return pd.DataFrame()

            print(f"[metatrader] HTTP {resp.status_code} for {ticker}: {resp.text[:200]}")
            return pd.DataFrame()

        except requests.Timeout:
            print(f"[metatrader] Request timed out for {ticker} (attempt {attempt + 1})")
            if attempt == 0:
                continue
        except requests.RequestException as e:
            print(f"[metatrader] Request error for {ticker}: {e}")
            if attempt == 0:
                _reconnect()
                params["id"] = _get_token()
                continue

    return pd.DataFrame()


# ── Public interface ──────────────────────────────────────────────────────────

def get_df(ticker: str, interval: str, period: str = None) -> pd.DataFrame:
    """
    Download OHLCV data from MetaTrader 5 via the mt5rest HTTP API.

    Args:
        ticker:   MT5 symbol name (e.g. "EURUSD", "US30Cash", "XAUUSD")
        interval: candle interval ("1m", "5m", "15m", "30m", "1h")
        period:   lookback period ("1d", "5d", "30d" …). Derived from interval if None.

    Returns:
        pd.DataFrame with Open, High, Low, Close, Volume columns and UTC DatetimeIndex.
        Returns an empty DataFrame on failure.
    """
    if period is None:
        from providers.yahoo import PERIOD_MAP
        period = PERIOD_MAP.get(interval, "1d")

    with LOCK:
        return _fetch(ticker, interval, period)


def get_bias_df(ticker: str, period: str, interval: str) -> pd.DataFrame:
    """
    Download data for bias calculation (daily / weekly candles).
    API-compatible with providers/yahoo.py.
    """
    return get_df(ticker, interval, period)
