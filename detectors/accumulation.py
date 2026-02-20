"""
detectors/accumulation.py

Detects SIDEWAYS accumulation based purely on DIRECTIONLESSNESS.

Rules:
  - Only scans the most recent 60 candles (hard cap)
  - Window size capped at 60
  - max_range_pct varies per session (asian/london/new_york)
  - SLOPE must be near flat (linear regression)
  - CHOPPINESS must be high (price reverses up/down frequently)

Session hours match index.html exactly (CET local time):
  Asian:    02:00 – 08:00 CET
  London:   09:00 – 13:00 CET
  New York: 15:00 – 20:00 CET
  Outside:  treated as asian (quiet/off-hours)
"""

import numpy as np
import pandas as pd
from datetime import datetime, timezone


# Session windows in UTC — same for everyone worldwide.
# Asian:    01:00 – 07:00 UTC  (02:00 – 08:00 CET)
# London:   08:00 – 12:00 UTC  (09:00 – 13:00 CET)
# New York: 14:00 – 19:00 UTC  (15:00 – 20:00 CET)
SESSION_WINDOWS = {
    "asian":    (1,  7),
    "london":   (8,  12),
    "new_york": (14, 19),
}

def get_current_session():
    """Return current session name based on UTC time, or None if out of session."""
    hour = datetime.now(timezone.utc).hour
    if SESSION_WINDOWS["new_york"][0] <= hour < SESSION_WINDOWS["new_york"][1]:
        return "new_york"
    elif SESSION_WINDOWS["london"][0] <= hour < SESSION_WINDOWS["london"][1]:
        return "london"
    elif SESSION_WINDOWS["asian"][0] <= hour < SESSION_WINDOWS["asian"][1]:
        return "asian"
    return None  # out of session — no detection


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
    peak_i  = int(np.argmax(closes))
    trough_i = int(np.argmin(closes))
    third = n // 3
    # V or inverted-V: extreme (peak or trough) sits in the middle third
    return (third <= peak_i < 2 * third) or (third <= trough_i < 2 * third)


def detect(
    df,
    lookback: int = 40,
    threshold_pct: float = 0.003,
    max_range_pct: float = None,
    asian_range_pct: float = None,
    london_range_pct: float = None,
    new_york_range_pct: float = None,
) -> dict | None:
    """
    Args:
        lookback:           Window size in candles. Hard capped at 60.
        threshold_pct:      Slope scaling factor per instrument.
        max_range_pct:      Fallback max box height if no session override set.
        asian_range_pct:    Max box height during Asian session (02-08 CET).
        london_range_pct:   Max box height during London session (09-13 CET).
        new_york_range_pct: Max box height during New York session (15-20 CET).
    """
    try:
        lookback = min(lookback, 60)

        if len(df) < lookback + 5:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = df.columns.get_level_values(0)
        df = df.loc[:, ~df.columns.duplicated()].copy()
        for col in ['Open', 'High', 'Low', 'Close']:
            df[col] = pd.to_numeric(df[col].squeeze(), errors='coerce')
        df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])

        # Don't run detection outside of trading sessions
        session = get_current_session()
        if session is None:
            return None

        # Pick the right max_range_pct for current session
        session_range = {
            "asian":    asian_range_pct,
            "london":   london_range_pct,
            "new_york": new_york_range_pct,
        }.get(session)
        effective_range_pct = session_range if session_range is not None else max_range_pct

        slope_limit    = (threshold_pct * 0.15) / lookback
        CHOP_FOUND     = 0.44
        CHOP_POTENTIAL = 0.36

        # Use last CLOSED candle (iloc[-2]) for breakout detection.
        # A candle has broken out only if its entire BODY is outside the zone.
        # Wicks through the boundary don't count.
        last_closed_open  = float(df['Open'].iloc[-2])
        last_closed_close = float(df['Close'].iloc[-2])
        last_body_high    = max(last_closed_open, last_closed_close)
        last_body_low     = min(last_closed_open, last_closed_close)

        scan_start  = max(1, len(df) - 60)
        best_potential = None

        for i in range(len(df) - lookback - 1, scan_start, -1):
            window = df.iloc[i: i + lookback]

            closes = window['Close'].values.flatten().astype(float)
            highs  = window['High'].values.flatten().astype(float)
            lows   = window['Low'].values.flatten().astype(float)

            if len(closes) < lookback:
                continue

            avg_p = closes.mean()
            if avg_p == 0:
                continue

            # Use body range (max of opens/closes) to avoid wicks pushing box too wide
            body_highs = np.maximum(window['Open'].values.flatten().astype(float),
                                    window['Close'].values.flatten().astype(float))
            body_lows  = np.minimum(window['Open'].values.flatten().astype(float),
                                    window['Close'].values.flatten().astype(float))
            h_max = float(body_highs.max())
            l_min = float(body_lows.min())

            if effective_range_pct is not None:
                if (h_max - l_min) / avg_p > effective_range_pct:
                    continue

            slope = _slope_pct(closes, avg_p)
            if slope >= slope_limit:
                continue

            # Reject V-shapes and inverted-V shapes — trending up then down
            # (or down then up) has a flat slope but is not accumulation.
            if _is_v_shape(closes):
                continue

            chop  = _choppiness(closes)
            end_i = i + lookback - 1

            # is_active = last closed candle BODY is still inside the zone
            # (body did not close outside — wicks don't count)
            is_active = (last_body_low >= l_min) and (last_body_high <= h_max)

            zone = {
                "detector":  "accumulation",
                "session":   session,
                "start":     int(df.index[i].timestamp()),
                "end":       int(df.index[end_i].timestamp()),
                "top":       h_max,
                "bottom":    l_min,
                "is_active": is_active,
            }

            if chop >= CHOP_FOUND:
                zone["status"] = "found"
                return zone

            if best_potential is None and chop >= CHOP_POTENTIAL:
                zone["status"] = "potential"
                best_potential = zone

        if best_potential is not None:
            return best_potential

        # In session but no zone found — return sentinel so frontend
        # can show "Looking" instead of "Out of session"
        return {"detector": "accumulation", "status": "looking", "is_active": False}

    except Exception as e:
        print(f"[accumulation] Detection error: {e}")
        return None
