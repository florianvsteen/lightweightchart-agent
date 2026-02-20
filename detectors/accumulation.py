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
from datetime import datetime


def get_current_session() -> str:
    """
    Return the current session based on CET local time.
    Reads system local time — container must have TZ=Europe/Brussels set.
    Matches the session windows defined in index.html exactly.
    """
    hour = datetime.now().hour  # system local time (CET via TZ env var)
    if 15 <= hour < 20:
        return "new_york"
    elif 9 <= hour < 13:
        return "london"
    elif 2 <= hour < 8:
        return "asian"
    else:
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

        last_close  = float(df['Close'].iloc[-1])
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

            h_max = float(highs.max())
            l_min = float(lows.min())

            if effective_range_pct is not None:
                if (h_max - l_min) / avg_p > effective_range_pct:
                    continue

            slope = _slope_pct(closes, avg_p)
            if slope >= slope_limit:
                continue

            chop  = _choppiness(closes)
            end_i = i + lookback - 1
            is_active = (last_close >= l_min) and (last_close <= h_max)

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

        return best_potential

    except Exception as e:
        print(f"[accumulation] Detection error: {e}")
        return None
