"""
tools/market.py

Fetches key macro market data via yfinance.
Returns structured dicts ready for the Macro Desk frontend.

Re-used by: macro desk, dashboard widgets, any future alerts.

Usage:
    from tools.market import get_market_snapshot, get_chart_data
"""

import time
import threading
from datetime import datetime, timezone

try:
    import yfinance as yf
    YF_OK = True
except ImportError:
    YF_OK = False

# ── Config ────────────────────────────────────────────────────────────────────
SNAPSHOT_TTL = 5 * 60      # 5 min cache
CHART_TTL    = 15 * 60     # 15 min cache

# Core instruments tracked on the Macro Desk
INSTRUMENTS = {
    # Equities / Risk
    "SPX":   {"sym": "^GSPC",    "label": "S&P 500",        "group": "equity",   "unit": "pts"},
    "NDX":   {"sym": "^NDX",     "label": "Nasdaq 100",     "group": "equity",   "unit": "pts"},
    "VIX":   {"sym": "^VIX",     "label": "VIX",            "group": "vol",      "unit": ""},
    # Rates
    "US10Y": {"sym": "^TNX",     "label": "US 10Y Yield",   "group": "rates",    "unit": "%"},
    "US2Y":  {"sym": "^IRX",     "label": "US 2Y Yield",    "group": "rates",    "unit": "%"},
    "MOVE":  {"sym": "^MOVE",    "label": "MOVE Index",     "group": "vol",      "unit": ""},
    # FX
    "DXY":   {"sym": "DX-Y.NYB", "label": "DXY",            "group": "fx",       "unit": ""},
    "EURUSD":{"sym": "EURUSD=X", "label": "EUR/USD",        "group": "fx",       "unit": ""},
    "USDJPY":{"sym": "JPY=X",    "label": "USD/JPY",        "group": "fx",       "unit": ""},
    "GBPUSD":{"sym": "GBPUSD=X", "label": "GBP/USD",        "group": "fx",       "unit": ""},
    # Commodities
    "GOLD":  {"sym": "GC=F",     "label": "Gold",           "group": "commod",   "unit": "USD"},
    "OIL":   {"sym": "CL=F",     "label": "WTI Oil",        "group": "commod",   "unit": "USD"},
    "SILVER":{"sym": "SI=F",     "label": "Silver",         "group": "commod",   "unit": "USD"},
}

# ── Cache ─────────────────────────────────────────────────────────────────────
_snapshot_cache: dict = {}
_chart_cache:    dict = {}
_lock = threading.Lock()


def _fetch_quote(sym: str) -> dict | None:
    """Fetch latest quote + 1-day change for a single symbol."""
    if not YF_OK:
        return None
    try:
        ticker = yf.Ticker(sym)
        hist   = ticker.history(period="5d", interval="1d", auto_adjust=True)
        if hist.empty:
            return None

        last  = float(hist["Close"].iloc[-1])
        prev  = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else last
        chg   = last - prev
        chg_p = (chg / prev * 100) if prev else 0.0

        # Intraday for smoother "last" value
        intra = ticker.history(period="1d", interval="5m", auto_adjust=True)
        if not intra.empty:
            last = float(intra["Close"].iloc[-1])

        return {
            "last":    round(last, 4),
            "prev":    round(prev, 4),
            "change":  round(chg, 4),
            "change_p": round(chg_p, 3),
            "fetched_at": time.time(),
        }
    except Exception as e:
        print(f"[market] fetch error {sym}: {e}")
        return None


def get_market_snapshot(force: bool = False) -> dict:
    """
    Return latest quotes for all INSTRUMENTS.

    Returns:
        {
          "SPX":   {"last": 5200, "change_p": -0.38, "group": "equity", ...},
          "VIX":   {...},
          ...
          "fetched_at": float
        }
    """
    now = time.time()
    with _lock:
        if not force and _snapshot_cache.get("at") and (now - _snapshot_cache["at"]) < SNAPSHOT_TTL:
            return _snapshot_cache.get("data", {})

    result = {}
    for key, meta in INSTRUMENTS.items():
        quote = _fetch_quote(meta["sym"])
        result[key] = {
            "label":    meta["label"],
            "group":    meta["group"],
            "unit":     meta["unit"],
            "last":     quote["last"]    if quote else None,
            "change":   quote["change"]  if quote else None,
            "change_p": quote["change_p"] if quote else None,
            "prev":     quote["prev"]    if quote else None,
        }

    result["fetched_at"] = time.time()

    with _lock:
        _snapshot_cache["data"] = result
        _snapshot_cache["at"]   = time.time()

    return result


def get_chart_data(symbol_key: str, period: str = "1d", interval: str = "5m") -> list[dict]:
    """
    Return OHLCV candle list for a symbol key (e.g. "SPX", "GOLD").

    Returns list of {"time": unix_ts, "open", "high", "low", "close", "volume"}
    """
    cache_key = f"{symbol_key}_{period}_{interval}"
    now = time.time()

    with _lock:
        entry = _chart_cache.get(cache_key)
        if entry and (now - entry["at"]) < CHART_TTL:
            return entry["data"]

    meta = INSTRUMENTS.get(symbol_key)
    if not meta or not YF_OK:
        return []

    try:
        hist = yf.Ticker(meta["sym"]).history(period=period, interval=interval, auto_adjust=True)
        if hist.empty:
            return []

        candles = []
        for ts, row in hist.iterrows():
            unix = int(ts.timestamp())
            candles.append({
                "time":   unix,
                "open":   round(float(row["Open"]),  4),
                "high":   round(float(row["High"]),  4),
                "low":    round(float(row["Low"]),   4),
                "close":  round(float(row["Close"]), 4),
                "volume": int(row.get("Volume", 0)),
            })

        with _lock:
            _chart_cache[cache_key] = {"data": candles, "at": time.time()}

        return candles
    except Exception as e:
        print(f"[market] chart error {symbol_key}: {e}")
        return []
