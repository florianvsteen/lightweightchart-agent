"""
tools/market.py

Fetches macro market data via yfinance.
Instruments are built dynamically from config.PAIRS so the Macro Desk
always tracks exactly the pairs you trade, plus a few macro context extras
(VIX, DXY, US10Y, OIL) that aren't trading pairs.

Re-used by: macro desk, dashboard widgets, alert pipeline.

Usage:
    from tools.market import get_market_snapshot, get_chart_data, INSTRUMENTS
"""

import time
import threading

try:
    import yfinance as yf
    YF_OK = True
except ImportError:
    YF_OK = False

# ── Config ────────────────────────────────────────────────────────────────────
SNAPSHOT_TTL = 5 * 60      # 5 min cache
CHART_TTL    = 15 * 60     # 15 min cache


def _group(pair_id: str, yf_ticker: str) -> str:
    pid = pair_id.upper()
    if pid in ("US30", "US100", "SPX500"):                    return "equity"
    if pid in ("BTCUSD", "ETHUSD"):                           return "crypto"
    if yf_ticker in ("GC=F", "SI=F", "CL=F", "NG=F"):        return "commod"
    return "fx"


def _build_instruments() -> dict:
    """
    Build INSTRUMENTS from config.PAIRS (imported at call time to avoid
    circular imports at module load) plus macro-only extras.
    """
    try:
        from config import PAIRS
    except ImportError:
        PAIRS = {}

    instruments = {}

    for pair_id, cfg in PAIRS.items():
        yf_sym = cfg.get("yf_ticker")
        if not yf_sym:
            continue
        instruments[pair_id] = {
            "sym":   yf_sym,
            "label": cfg.get("label", pair_id),
            "group": _group(pair_id, yf_sym),
            "unit":  "",
        }

    # Macro context instruments — added only if not already in PAIRS
    EXTRAS = {
        "VIX":   {"sym": "^VIX",     "label": "VIX",          "group": "vol",    "unit": ""},
        "DXY":   {"sym": "DX-Y.NYB", "label": "DXY",          "group": "fx",     "unit": ""},
        "US10Y": {"sym": "^TNX",     "label": "US 10Y Yield", "group": "rates",  "unit": "%"},
        "OIL":   {"sym": "CL=F",     "label": "WTI Oil",      "group": "commod", "unit": "USD"},
    }
    for key, meta in EXTRAS.items():
        if key not in instruments:
            instruments[key] = meta

    return instruments


# Built once at import; call _build_instruments() again if PAIRS changes at runtime
INSTRUMENTS: dict = _build_instruments()

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

        # Intraday for a fresher last price
        intra = ticker.history(period="1d", interval="5m", auto_adjust=True)
        if not intra.empty:
            last = float(intra["Close"].iloc[-1])

        return {
            "last":     round(last, 4),
            "prev":     round(prev, 4),
            "change":   round(chg, 4),
            "change_p": round(chg_p, 3),
        }
    except Exception as e:
        print(f"[market] fetch error {sym}: {e}")
        return None


def get_market_snapshot(force: bool = False) -> dict:
    """
    Return latest quotes for all instruments built from config.PAIRS.

    Returns:
        {
          "US30":   {"label": "US30 (Dow Jones)", "group": "equity",
                     "last": 39500, "change_p": -0.4, ...},
          "EURUSD": {...},
          "VIX":    {...},
          "DXY":    {...},
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
            "last":     quote["last"]     if quote else None,
            "change":   quote["change"]   if quote else None,
            "change_p": quote["change_p"] if quote else None,
            "prev":     quote["prev"]     if quote else None,
        }

    result["fetched_at"] = time.time()

    with _lock:
        _snapshot_cache["data"] = result
        _snapshot_cache["at"]   = time.time()

    return result


def get_chart_data(symbol_key: str, period: str = "1d", interval: str = "5m") -> list[dict]:
    """
    Return OHLCV candles for a symbol key matching a key in INSTRUMENTS
    (e.g. "US30", "EURUSD", "XAUUSD", "VIX").

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
            candles.append({
                "time":   int(ts.timestamp()),
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
