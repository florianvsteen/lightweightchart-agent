"""
detectors/accumulation.py

Detects SIDEWAYS accumulation based purely on DIRECTIONLESSNESS.

Rules:
  - Only scans the most recent 60 candles (hard cap)
  - Window size is also capped at 60
  - Range size does NOT matter for choppiness/slope checks
  - max_range_pct rejects boxes that are too wide (per instrument setting)
  - SLOPE must be near flat (linear regression)
  - CHOPPINESS must be high (price reverses up/down frequently)
"""

import numpy as np
import pandas as pd


def _slope_pct(closes: np.ndarray, avg_p: float) -> float:
    """Absolute normalised slope of linear regression per candle."""
    x = np.arange(len(closes), dtype=float)
    return abs(np.polyfit(x, closes, 1)[0]) / avg_p


def _choppiness(closes: np.ndarray) -> float:
    """
    Fraction of candle-to-candle moves that reverse direction.
    Choppy sideways: ~0.45-0.60
    Trending:        ~0.10-0.25
    """
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
) -> dict | None:
    """
    Args:
        lookback:      Window size in candles. Hard capped at 60.
        threshold_pct: Used only to scale slope_limit per instrument.
        max_range_pct: Maximum allowed box height as fraction of avg price.
                       e.g. 0.002 = 0.2%.  None = no cap.
                       Box is rejected if (high - low) / avg_price > max_range_pct.
    """
    try:
        lookback = min(lookback, 60)

        if len(df) < lookback + 5:
            return None

        # Flatten MultiIndex and deduplicate columns (yfinance quirks)
        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = df.columns.get_level_values(0)
        df = df.loc[:, ~df.columns.duplicated()].copy()
        for col in ['Open', 'High', 'Low', 'Close']:
            df[col] = pd.to_numeric(df[col].squeeze(), errors='coerce')
        df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])

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

            # Reject box if it's wider than the allowed max range
            if max_range_pct is not None:
                if (h_max - l_min) / avg_p > max_range_pct:
                    continue

            # Check 1: flat slope — rejects any directional trend
            slope = _slope_pct(closes, avg_p)
            if slope >= slope_limit:
                continue

            # Check 2: high choppiness — price must reverse direction often
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
