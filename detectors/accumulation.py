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
  - Box boundaries use candle BODIES (open/close), not wicks

Selection priority (among all passing zones):
  1. Zones with ADX < 10 are preferred (ultra-low directional strength)
  2. Among equal ADX tiers, LOWEST SLOPE wins

Breakout validation — IMPULSIVE CANDLE:
  When price breaks OUT of the box (last closed body is above or below the
  box boundaries), the breakout is validated by checking that the breakout
  candle's body is LARGER than the average body of candles inside the window.
  This confirms the move is impulsive and not just noise. If not impulsive,
  the zone resets to "looking".

  States:
    looking   — no valid zone found
    active    — valid zone, breakout candle still inside the box
    confirmed — valid zone + impulsive breakout outside the box

Session hours in UTC:
  Asian:    01:00 – 07:00 UTC  (02:00 – 08:00 CET)
  London:   08:00 – 12:00 UTC  (09:00 – 13:00 CET)
  New York: 13:00 – 19:00 UTC  (14:00 – 20:00 CET)

Weekend halt:
  Friday  23:00 UTC → Sunday 01:00 UTC  — returns None (no detection)
"""

import numpy as np
import pandas as pd
from datetime import datetime, timezone


SESSION_WINDOWS = {
    "asian":    (1,  7),
    "london":   (8,  12),
    "new_york": (13, 19),
}


def is_weekend_halt() -> bool:
    """Return True if we are in the Fri 23:00 – Sun 22:00 UTC weekend halt window."""
    now  = datetime.now(timezone.utc)
    dow  = now.weekday()   # 0=Mon … 4=Fri … 5=Sat … 6=Sun
    hour = now.hour
    if dow == 4 and hour >= 23:   # Friday ≥ 23:00
        return True
    if dow == 5:                   # All of Saturday
        return True
    if dow == 6 and hour < 22:    # Sunday before 22:00
        return True
    return False


def get_current_session():
    if is_weekend_halt():
        return None
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
    """Calculate ADX. Auto-reduces period for short windows. Returns float or None."""
    n = len(closes)
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
    lookback: int = 40,
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
        if is_weekend_halt():
            return {"detector": "accumulation", "status": "weekend", "is_active": False}

        if len(df) < min_candles + 4:
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

        # Candle layout (newest → oldest):
        #   df[-1]  forming candle          — never touched
        #   df[-2]  breakout/impulse candle — last fully closed candle
        #   df[-3…] accumulation window     — windows end at df[-2] exclusive
        #
        # A confirmed accumulation requires:
        #   1. A valid sideways window (slope/chop/adx/range checks pass)
        #   2. df[-2] body is OUTSIDE the box (broke up or down)
        #   3. df[-2] body range > avg body range of candles inside the window (impulsive)
        #
        # If the last candle is still inside the box → status = "active"
        # If broke out AND impulsive              → status = "confirmed"
        # Otherwise                               → status = "looking"

        breakout_idx   = len(df) - 2          # last fully closed candle
        last_accum_idx = len(df) - 3          # windows end here (inclusive)
        scan_start     = max(0, len(df) - lookback)

        # Breakout candle body
        bo_open_raw  = float(df['Open'].iloc[breakout_idx])
        bo_close_raw = float(df['Close'].iloc[breakout_idx])
        bo_high_raw  = float(df['High'].iloc[breakout_idx])
        bo_low_raw   = float(df['Low'].iloc[breakout_idx])
        bo_body_size = abs(bo_close_raw - bo_open_raw)
        last_body_high = max(bo_open_raw, bo_close_raw)
        last_body_low  = min(bo_open_raw, bo_close_raw)

        # Collect all candidate zones with their slope for best-selection
        found_candidates     = []
        potential_candidates = []

        for window_size in range(min_candles, lookback + 1):
            slope_limit = (threshold_pct * 0.10) / window_size

            i = last_accum_idx - window_size + 1
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

            adx_val = _adx(highs, lows, closes)
            if adx_val is not None and adx_val > adx_threshold:
                continue

            chop   = _choppiness(closes)
            end_i  = i + window_size - 1   # = last_accum_idx for the largest window
            # is_active: breakout candle body still inside accumulation box
            is_active = (last_body_low >= l_min) and (last_body_high <= h_max)

            # Average body size of candles inside the window (for impulse check)
            bodies = np.abs(closes - opens)
            avg_body = float(bodies.mean()) if len(bodies) > 0 else 0.0

            zone = {
                "detector":  "accumulation",
                "session":   session,
                "start":     int(df.index[i].timestamp()),
                "end":       int(df.index[end_i].timestamp()),
                "top":       h_max,
                "bottom":    l_min,
                "is_active": is_active,
                "range_pct": round(range_pct, 6),
                "slope":     round(slope, 8),
                "adx":       round(adx_val, 2) if adx_val is not None else None,
                "avg_body":  round(avg_body, 6),
                "_window_start_idx": i,
            }

            if chop >= CHOP_FOUND:
                zone["status"] = "found"
                found_candidates.append(zone)
            elif chop >= CHOP_POTENTIAL:
                zone["status"] = "potential"
                potential_candidates.append(zone)

        def _rank(candidates):
            """Sort candidates: ADX<10 first, then lowest slope within each tier."""
            if not candidates:
                return []
            low_adx = sorted(
                [z for z in candidates if z["adx"] is not None and z["adx"] < 10],
                key=lambda z: z["slope"]
            )
            rest = sorted(
                [z for z in candidates if not (z["adx"] is not None and z["adx"] < 10)],
                key=lambda z: z["slope"]
            )
            return low_adx + rest

        # Primary pool: "found" zones first, then "potential"
        ranked_all = _rank(found_candidates) + _rank(potential_candidates)

        # ── Determine which zones to return ───────────────────────────────
        if not ranked_all:
            return {"detector": "accumulation", "status": "looking", "is_active": False}

        candidate      = ranked_all[0]
        secondary_zone = ranked_all[1] if len(ranked_all) > 1 else None

        # Active zone — breakout candle still inside the box
        if candidate["is_active"]:
            candidate.pop("_window_start_idx", None)
            candidate["status"] = "active"
            if secondary_zone:
                secondary_zone.pop("_window_start_idx", None)
            candidate["secondary_zone"] = secondary_zone
            return candidate

        # ── Price broke out — validate as IMPULSIVE ───────────────────────
        #
        # breakout candle = df[-2] (breakout_idx)
        # Conditions for "confirmed":
        #   1. Body exits the box (broke_up or broke_down)
        #   2. Breakout body size > avg body size of candles in the window
        #      — this confirms the move is impulsive, not just a wick poke

        box_top    = candidate["top"]
        box_bottom = candidate["bottom"]
        avg_body   = candidate["avg_body"]

        broke_up   = last_body_high > box_top
        broke_down = last_body_low  < box_bottom

        if not broke_up and not broke_down:
            # Body still inside — active (is_active flag was wrong, re-check)
            candidate.pop("_window_start_idx", None)
            candidate["is_active"] = True
            candidate["status"]    = "active"
            if secondary_zone:
                secondary_zone.pop("_window_start_idx", None)
            candidate["secondary_zone"] = secondary_zone
            return candidate

        # Check impulse: body must be bigger than avg window body
        is_impulsive = bo_body_size > avg_body

        if not is_impulsive:
            return {"detector": "accumulation", "status": "looking", "is_active": False}

        # Confirmed — impulsive breakout outside the box
        candidate.pop("_window_start_idx", None)
        candidate["is_active"]      = False
        candidate["status"]         = "confirmed"
        candidate["breakout_dir"]   = "up" if broke_up else "down"
        candidate["breakout_body"]  = round(bo_body_size, 6)
        candidate["impulse_ratio"]  = round(bo_body_size / avg_body, 2) if avg_body > 0 else None
        candidate["breakout_candle"] = {
            "time":  int(df.index[breakout_idx].timestamp()),
            "open":  round(bo_open_raw, 5), "high": round(bo_high_raw, 5),
            "low":   round(bo_low_raw, 5),  "close": round(bo_close_raw, 5),
        }
        if secondary_zone:
            secondary_zone.pop("_window_start_idx", None)
        candidate["secondary_zone"] = secondary_zone
        return candidate

    except Exception as e:
        print(f"[accumulation] Detection error: {e}")
        return None
