"""
detectors/accumulation.py

Detects sideways accumulation based purely on DIRECTIONLESSNESS — not price range size.

What matters:
  1. Price reverses direction frequently (choppiness)
  2. No net directional slope (flat linear regression)

What does NOT matter:
  - How wide the range is
  - How large individual candle moves are
  - Drift / std dev / range thresholds

Scans backwards through the last 60 candles max.
Returns the most recent window that qualifies.
"""

import numpy as np
import pandas as pd


def _scalar(val) -> float:
    if hasattr(val, 'item'):
        return val.item()
    if hasattr(val, 'iloc'):
        return float(val.iloc[0])
    return float(val)


def _slope_pct(closes: np.ndarray, avg_p: float) -> float:
    """Absolute normalised slope of linear regression per candle."""
    x = np.arange(len(closes), dtype=float)
    return abs(np.polyfit(x, closes, 1)[0]) / avg_p


def _choppiness(closes: np.ndarray) -> float:
    """
    Fraction of candle-to-candle moves that reverse direction.
    Choppy sideways: ~0.45-0.60  |  Trending: ~0.10-0.25
    """
    if len(closes) < 3:
        return 0.0
    diffs = np.diff(closes)
    sign_changes = np.sum(np.sign(diffs[1:]) != np.sign(diffs[:-1]))
    return sign_changes / (len(diffs) - 1)


def detect(df, lookback: int = 40, threshold_pct: float = 0.003) -> dict | None:
    """
    Args:
        lookback:      Window size in candles. Max is capped at 60.
        threshold_pct: Only used to scale slope_limit per instrument.
                       Does NOT gate on price range size.
    """
    try:
        lookback = min(lookback, 60)  # Hard cap: never look back more than 60 candles

        if len(df) < lookback + 5:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = df.columns.get_level_values(0)

        # Slope limit: slope per candle must be below this fraction of avg price.
        # Scales with instrument volatility via threshold_pct.
        slope_limit = (threshold_pct * 0.15) / lookback

        # Choppiness thresholds
        CHOP_FOUND     = 0.44   # confirmed accumulation
        CHOP_POTENTIAL = 0.36   # forming / possible

        last_close  = _scalar(df['Close'].iloc[-1])
        best_potential = None

        # Only scan the last 60 candles worth of windows
        scan_start = max(1, len(df) - lookback - 60)

        for i in range(len(df) - lookback - 1, scan_start, -1):
            window  = df.iloc[i: i + lookback]
            closes  = window['Close'].values.astype(float)
            avg_p   = float(closes.mean())
            h_max   = float(window['High'].max())
            l_min   = float(window['Low'].min())

            # Check 1: flat slope — no directional trend
            slope = _slope_pct(closes, avg_p)
            if slope >= slope_limit:
                continue

            # Check 2: choppy — price reverses direction frequently
            chop = _choppiness(closes)

            end_i     = i + lookback - 1
            is_active = (last_close >= l_min) and (last_close <= h_max)

            zone = {
                "detector":  "accumulation",
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
