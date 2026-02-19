"""
detectors/accumulation.py

Detects SIDEWAYS accumulation phases only — price consolidating in a flat range
with no meaningful directional trend (up or down).

Trending windows (downtrend, uptrend) are explicitly rejected via linear
regression slope. Only flat, choppy, directionless consolidation qualifies.

Interface: detect(df, lookback, threshold_pct) -> dict | None

Returned dict keys:
  start      (int)   Unix timestamp of zone start
  end        (int)   Unix timestamp of zone end (or last candle if active)
  top        (float) Upper boundary of the zone
  bottom     (float) Lower boundary of the zone
  is_active  (bool)  True if price hasn't broken out yet
  status     (str)   "found" | "potential"
  detector   (str)   Always "accumulation"
"""

import numpy as np
import pandas as pd


def _scalar(val) -> float:
    """Safely extract a Python float from a pandas scalar, Series, or numpy type."""
    if hasattr(val, 'item'):
        return val.item()
    if hasattr(val, 'iloc'):
        return float(val.iloc[0])
    return float(val)


def _slope_pct(closes: np.ndarray, avg_p: float) -> float:
    """
    Fit a linear regression to the close prices and return the slope
    normalised as a fraction of avg_p per candle.

    A flat sideways window will have a slope near 0.
    A trending window will have a clearly positive or negative slope.
    """
    x = np.arange(len(closes), dtype=float)
    slope = np.polyfit(x, closes, 1)[0]
    return abs(slope) / avg_p   # normalised absolute slope per candle


def _build_zone(df, start_i: int, end_i: int, h_max: float, l_min: float, status: str) -> dict:
    """Build a zone dict spanning start_i to end_i (both inclusive)."""
    breakout_idx = end_i
    for j in range(end_i, len(df)):
        breakout_idx = j
        current_c = _scalar(df['Close'].iloc[j])
        if current_c > h_max or current_c < l_min:
            break
    return {
        "detector": "accumulation",
        "status": status,
        "start": int(df.index[start_i].timestamp()),
        "end": int(df.index[breakout_idx].timestamp()),
        "top": h_max,
        "bottom": l_min,
        "is_active": breakout_idx == (len(df) - 1),
    }


def detect(df, lookback: int = 40, threshold_pct: float = 0.003) -> dict | None:
    """
    Scan historical candles backwards for purely sideways accumulation zones.

    A window qualifies only when ALL of the following are true:
      1. range_pct   — total High-Low range is tight relative to price
      2. stability   — std dev of closes is low (no wild swings)
      3. drift       — start-to-end price movement is small (no net trend)
      4. slope       — linear regression slope is near zero (explicitly flat)

    Checks 1-3 allow a "potential" zone at a relaxed threshold.
    All 4 together (including slope) qualify as a confirmed "found" zone.

    Args:
        df:            OHLCV DataFrame
        lookback:      Candles per evaluation window (default 40)
        threshold_pct: Core tightness parameter — tune per instrument:
                         US30/US100 -> 0.003,  XAUUSD -> 0.002,  FX -> 0.0005
    """
    try:
        if len(df) < lookback + 5:
            return None

        # Flatten any residual MultiIndex columns yfinance may leave behind
        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = df.columns.get_level_values(0)

        # Max normalised slope per candle to still be considered "flat"
        # Kept proportional to threshold so it scales with instrument volatility
        slope_limit = (threshold_pct * 0.3) / lookback

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

            range_pct       = (h_max - l_min) / avg_p
            stability_score = std_dev / avg_p
            drift           = abs(start_p - end_p) / start_p

            sideways_range  = range_pct       <= threshold_pct
            sideways_stable = stability_score  < (threshold_pct * 0.25)
            sideways_drift  = drift            < (threshold_pct * 0.3)

            # Fast reject: skip slope calculation if range/stability already fail
            if not (sideways_range and sideways_stable):
                continue

            # Slope check — the primary guard against trending windows.
            # A downtrend like Image 1 will have a clearly negative slope
            # that exceeds slope_limit and gets rejected here.
            slope = _slope_pct(closes, avg_p)
            sideways_flat = slope < slope_limit

            # Confirmed accumulation: range + stable + no drift + flat slope
            if sideways_drift and sideways_flat:
                return _build_zone(df, i, i + lookback, h_max, l_min, "found")

            # Potential: range + stable + flat but drift slightly elevated
            if best_potential is None and sideways_flat and drift < (threshold_pct * 0.6):
                best_potential = _build_zone(df, i, i + lookback, h_max, l_min, "potential")

        return best_potential

    except Exception as e:
        print(f"[accumulation] Detection error: {e}")
        return None
