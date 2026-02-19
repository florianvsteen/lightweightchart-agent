"""
detectors/accumulation.py

Detects SIDEWAYS accumulation phases only — price consolidating in a flat range
with no meaningful directional trend (up or down).

Key design:
  - Linear regression slope rejects any window with directional trend
  - Box end is capped at the end of the qualifying window, NOT extended
    forward into post-breakout trending price action
  - is_active = True only if current price is still inside the zone
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
    """Normalised absolute slope of a linear regression fit, per candle."""
    x = np.arange(len(closes), dtype=float)
    slope = np.polyfit(x, closes, 1)[0]
    return abs(slope) / avg_p


def detect(df, lookback: int = 40, threshold_pct: float = 0.003) -> dict | None:
    """
    Scan backwards for purely sideways accumulation zones.

    Conditions (all must pass for "found"):
      1. range_pct       — High-Low range tight vs avg price
      2. stability       — std dev of closes is low
      3. drift           — start-to-end close movement is minimal
      4. slope           — linear regression slope is near flat

    "potential" = conditions 1+2+4 pass but drift is slightly relaxed.

    Box boundaries:
      start = first candle of the qualifying window
      end   = last candle of the qualifying window (NOT extended forward)
      is_active = True if current (last) close is still inside top/bottom
    """
    try:
        if len(df) < lookback + 5:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = df.columns.get_level_values(0)

        # slope_limit scales with threshold and lookback so it works across instruments
        slope_limit = (threshold_pct * 0.35) / lookback

        last_close = _scalar(df['Close'].iloc[-1])
        best_potential = None

        for i in range(len(df) - lookback - 1, 0, -1):
            window = df.iloc[i: i + lookback]

            closes  = window['Close'].values.astype(float)
            h_max   = float(window['High'].max())
            l_min   = float(window['Low'].min())
            avg_p   = float(closes.mean())
            std_dev = float(closes.std())
            start_p = float(closes[0])
            end_p   = float(closes[-1])

            range_pct   = (h_max - l_min) / avg_p
            stability   = std_dev / avg_p
            drift       = abs(start_p - end_p) / start_p

            # Fast reject before expensive slope calc
            if range_pct > threshold_pct or stability >= threshold_pct * 0.25:
                continue

            slope = _slope_pct(closes, avg_p)

            if slope >= slope_limit:
                continue  # Trending — reject regardless of range/drift

            # Box spans exactly the qualifying window
            # end is the last candle of the window, not extended further
            end_i      = i + lookback - 1
            is_active  = (last_close >= l_min) and (last_close <= h_max)

            zone = {
                "detector":  "accumulation",
                "start":     int(df.index[i].timestamp()),
                "end":       int(df.index[end_i].timestamp()),
                "top":       h_max,
                "bottom":    l_min,
                "is_active": is_active,
            }

            # Confirmed: drift also passes
            if drift < threshold_pct * 0.3:
                zone["status"] = "found"
                return zone

            # Potential: drift slightly relaxed
            if best_potential is None and drift < threshold_pct * 0.6:
                zone["status"] = "potential"
                best_potential = zone

        return best_potential

    except Exception as e:
        print(f"[accumulation] Detection error: {e}")
        return None
