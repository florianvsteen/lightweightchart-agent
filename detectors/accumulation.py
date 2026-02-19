"""
detectors/accumulation.py

Detects STRICTLY SIDEWAYS accumulation — choppy, directionless consolidation
where price oscillates up and down repeatedly with no net trend.

Three-pillar approach:
  1. RANGE     — total High-Low range is tight
  2. SLOPE     — linear regression is near flat (strict)
  3. CHOPPINESS — price reverses direction frequently (not trending)

A trending window (even a slow one) will fail pillar 2 or 3.
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
    Fraction of candles that reverse direction vs the previous candle.
    Pure sideways chop → high value (0.4–0.6).
    Trending move      → low value (0.1–0.2, few reversals).

    Returns a value between 0 and 1.
    """
    if len(closes) < 3:
        return 0.0
    diffs = np.diff(closes)
    # Count sign changes: current diff has opposite sign to previous diff
    sign_changes = np.sum(np.sign(diffs[1:]) != np.sign(diffs[:-1]))
    return sign_changes / (len(diffs) - 1)


def detect(df, lookback: int = 40, threshold_pct: float = 0.003) -> dict | None:
    """
    Scan backwards for purely sideways accumulation zones.

    A window is "found" when ALL pass:
      1. range_pct  <= threshold_pct               (tight price range)
      2. slope      <  threshold_pct * 0.1 / lookback  (nearly flat regression)
      3. choppiness >= 0.40                         (frequent direction reversals)
      4. drift      <  threshold_pct * 0.2          (no net displacement)

    "potential" = 1+2+3 pass but drift is relaxed up to threshold_pct * 0.4

    Args:
        lookback:      candles per window (default 40)
        threshold_pct: tightness — tune per instrument:
                         US30/US100 -> 0.003
                         XAUUSD     -> 0.002
                         FX pairs   -> 0.0005
    """
    try:
        if len(df) < lookback + 5:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = df.columns.get_level_values(0)

        # Strict slope limit — 0.1x threshold per candle
        slope_limit = (threshold_pct * 0.1) / lookback
        chop_min    = 0.40   # at least 40% of candles must reverse direction

        last_close  = _scalar(df['Close'].iloc[-1])
        best_potential = None

        for i in range(len(df) - lookback - 1, 0, -1):
            window = df.iloc[i: i + lookback]

            closes  = window['Close'].values.astype(float)
            h_max   = float(window['High'].max())
            l_min   = float(window['Low'].min())
            avg_p   = float(closes.mean())
            start_p = float(closes[0])
            end_p   = float(closes[-1])

            range_pct = (h_max - l_min) / avg_p
            drift     = abs(start_p - end_p) / start_p

            # Pillar 1: tight range
            if range_pct > threshold_pct:
                continue

            # Pillar 2: flat slope — rejects slow trends
            slope = _slope_pct(closes, avg_p)
            if slope >= slope_limit:
                continue

            # Pillar 3: choppiness — rejects staircase/trending moves
            chop = _choppiness(closes)
            if chop < chop_min:
                continue

            # All structural checks pass — evaluate drift for found vs potential
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

            if drift < threshold_pct * 0.2:
                zone["status"] = "found"
                return zone

            if best_potential is None and drift < threshold_pct * 0.4:
                zone["status"] = "potential"
                best_potential = zone

        return best_potential

    except Exception as e:
        print(f"[accumulation] Detection error: {e}")
        return None
