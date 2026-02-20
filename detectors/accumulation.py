"""
detectors/accumulation.py

Detects SIDEWAYS accumulation based on DIRECTIONLESSNESS + LOW ADX.

Rules:
  - Scans the last `lookback` candles (configurable, default 100)
  - Tests windows from `min_candles` up to `lookback` size
  - max_range_pct varies per session (asian/london/new_york)
  - SLOPE must be near flat (linear regression on closes)
  - CHOPPINESS must be high (price reverses up/down frequently)
  - ADX must be below adx_threshold (default 25) — no directional strength
  - V-SHAPE check rejects windows where extreme is in inner 40-60%
  - Box boundaries use candle BODIES (open/close), not wicks
  - Prefers TIGHTER boxes (smaller range) over larger ones

Session hours in UTC:
  Asian:    01:00 – 07:00 UTC  (02:00 – 08:00 CET)
  London:   08:00 – 12:00 UTC  (09:00 – 13:00 CET)
  New York: 13:00 – 19:00 UTC  (14:00 – 20:00 CET)
"""

import numpy as np
import pandas as pd
from datetime import datetime, timezone


SESSION_WINDOWS = {
    "asian":    (1,  7),
    "london":   (8,  12),
    "new_york": (13, 19),
}


def get_current_session():
    hour = datetime.now(timezone.utc).hour
    if SESSION_WINDOWS["new_york"][0] <= hour < SESSION_WINDOWS["new_york"][1]:
        return "new_york"
    elif SESSION_WINDOWS["london"][0] <= hour < SESSION_WINDOWS["london"][1]:
        return "london"
    elif SESSION_WINDOWS["asian"][0] <= hour < SESSION_WINDOWS["asian"][1]:
        return "asian"
    return None


def _slope_pct(closes: np.ndarray, avg_p: float) -> float:
    x = np.arange(len(closes), dtype=float)
    return abs(np.polyfit(x, closes, 1)[0]) / avg_p


def _choppiness(closes: np.ndarray) -> float:
    if len(closes) < 3:
        return 0.0
    diffs = np.diff(closes)
    sign_changes = np.sum(np.sign(diffs[1:]) != np.sign(diffs[:-1]))
    return sign_changes / (len(diffs) - 1)


def _is_v_shape(closes: np.ndarray) -> bool:
    n = len(closes)
    if n < 6:
        return False
    peak_i   = int(np.argmax(closes))
    trough_i = int(np.argmin(closes))
    lo = int(n * 0.40)
    hi = int(n * 0.60)
    return (lo <= peak_i <= hi) or (lo <= trough_i <= hi)


def _adx(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> float:
    """Calculate ADX. Returns float or None if insufficient data.
    Automatically reduces period for short windows so ADX always fires."""
    n = len(closes)
    # Reduce period to fit the window — minimum period of 5
    while period > 5 and n < period * 2 + 1:
        period = max(5, period - 2)
    if n < period * 2 + 1:
        return None

    tr       = np.zeros(n)
    plus_dm  = np.zeros(n)
    minus_dm = np.zeros(n)

    for i in range(1, n):
        hl = highs[i] - lows[i]
        hc = abs(highs[i] - closes[i-1])
        lc = abs(lows[i] - closes[i-1])
        tr[i] = max(hl, hc, lc)

        up   = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        plus_dm[i]  = up   if (up > down and up > 0)   else 0.0
        minus_dm[i] = down if (down > up and down > 0) else 0.0

    def _smooth(arr, p):
        out = np.zeros(n)
        out[p] = arr[1:p+1].sum()
        for i in range(p+1, n):
            out[i] = out[i-1] - out[i-1] / p + arr[i]
        return out

    atr  = _smooth(tr, period)
    pDM  = _smooth(plus_dm, period)
    mDM  = _smooth(minus_dm, period)

    with np.errstate(invalid='ignore', divide='ignore'):
        pDI = np.where(atr > 0, 100 * pDM / atr, 0.0)
        mDI = np.where(atr > 0, 100 * mDM / atr, 0.0)
        dx  = np.where((pDI + mDI) > 0, 100 * np.abs(pDI - mDI) / (pDI + mDI), 0.0)

    adx_arr = np.zeros(n)
    start = 2 * period
    if start >= n:
        return None
    adx_arr[start] = dx[period:start+1].mean()
    for i in range(start+1, n):
        adx_arr[i] = (adx_arr[i-1] * (period - 1) + dx[i]) / period

    return float(adx_arr[-1])


def detect(
    df,
    lookback: int = 100,
    min_candles: int = 20,
    adx_threshold: float = 25,
    threshold_pct: float = 0.003,
    max_range_pct: float = None,
    asian_range_pct: float = None,
    london_range_pct: float = None,
    new_york_range_pct: float = None,
) -> dict | None:
    """
    Args:
        lookback:           How far back to scan (candles). Default 100.
        min_candles:        Minimum window size for valid accumulation. Default 20.
        adx_threshold:      Maximum ADX value allowed (default 25 = no trend).
        threshold_pct:      Slope scaling factor per instrument.
        max_range_pct:      Fallback max box height % if no session override.
        asian_range_pct:    Max box height during Asian session.
        london_range_pct:   Max box height during London session.
        new_york_range_pct: Max box height during New York session.
    """
    try:
        if len(df) < min_candles + 5:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = df.columns.get_level_values(0)
        df = df.loc[:, ~df.columns.duplicated()].copy()
        for col in ['Open', 'High', 'Low', 'Close']:
            df[col] = pd.to_numeric(df[col].squeeze(), errors='coerce')
        df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])

        session = get_current_session()
        if session is None:
            return None

        session_range = {
            "asian":    asian_range_pct,
            "london":   london_range_pct,
            "new_york": new_york_range_pct,
        }.get(session)
        effective_range_pct = session_range if session_range is not None else max_range_pct

        CHOP_FOUND     = 0.44
        CHOP_POTENTIAL = 0.36

        last_closed_open  = float(df['Open'].iloc[-2])
        last_closed_close = float(df['Close'].iloc[-2])
        last_body_high    = max(last_closed_open, last_closed_close)
        last_body_low     = min(last_closed_open, last_closed_close)

        scan_start = max(0, len(df) - lookback)

        best_found     = None   # tightest found zone
        best_potential = None   # tightest potential zone

        # Try all window sizes from min_candles up to lookback.
        # Window MUST end at the last closed candle — no historical zones.
        # As price stays in the box the window grows each cycle.
        # Prefer SMALLER windows with tighter ranges over larger ones.
        last_closed_idx = len(df) - 2
        for window_size in range(min_candles, lookback + 1):
            slope_limit = (threshold_pct * 0.15) / window_size

            # Start index so that window ends exactly at last closed candle
            i = last_closed_idx - window_size + 1
            if i < 0 or i < scan_start:
                continue

            window = df.iloc[i: i + window_size]
            closes = window['Close'].values.flatten().astype(float)
            opens  = window['Open'].values.flatten().astype(float)
            highs  = window['High'].values.flatten().astype(float)
            lows   = window['Low'].values.flatten().astype(float)

            if len(closes) < window_size:
                continue

            avg_p = closes.mean()
            if avg_p == 0:
                continue

            # Body boundaries — wicks excluded
            body_highs = np.maximum(opens, closes)
            body_lows  = np.minimum(opens, closes)
            h_max = float(body_highs.max())
            l_min = float(body_lows.min())
            range_pct = (h_max - l_min) / avg_p

            if effective_range_pct is not None:
                if range_pct > effective_range_pct:
                    continue

            slope = _slope_pct(closes, avg_p)
            if slope >= slope_limit:
                continue

            if _is_v_shape(closes):
                continue

            # ADX filter — reject if market has directional strength
            adx_val = _adx(highs, lows, closes)
            if adx_val is not None and adx_val > adx_threshold:
                continue

            chop  = _choppiness(closes)
            end_i = i + window_size - 1
            is_active = (last_body_low >= l_min) and (last_body_high <= h_max)

            zone = {
                "detector":  "accumulation",
                "session":   session,
                "start":     int(df.index[i].timestamp()),
                "end":       int(df.index[end_i].timestamp()),
                "top":       h_max,
                "bottom":    l_min,
                "is_active": is_active,
                "range_pct": round(range_pct, 6),
                "adx":       round(adx_val, 2) if adx_val is not None else None,
            }

            if chop >= CHOP_FOUND:
                zone["status"] = "found"
                # Keep tightest (smallest range) found zone
                if best_found is None or range_pct < best_found["range_pct"]:
                    best_found = zone

            elif chop >= CHOP_POTENTIAL:
                zone["status"] = "potential"
                if best_potential is None or range_pct < best_potential["range_pct"]:
                    best_potential = zone

        if best_found is not None:
            # If price already broke out, clear the zone and start looking again
            if not best_found["is_active"]:
                return {"detector": "accumulation", "status": "looking", "is_active": False}
            return best_found

        if best_potential is not None:
            if not best_potential["is_active"]:
                return {"detector": "accumulation", "status": "looking", "is_active": False}
            return best_potential

        return {"detector": "accumulation", "status": "looking", "is_active": False}

    except Exception as e:
        print(f"[accumulation] Detection error: {e}")
        return None
