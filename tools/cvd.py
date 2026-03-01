"""
tools/cvd.py

Cumulative Volume Delta (CVD) calculator.

CVD measures the net buying vs selling pressure by estimating whether each
candle's volume was dominated by buyers (up-close) or sellers (down-close).

For OHLCV data (no tick data):
  - Bullish candle (close > open):  delta = +volume
  - Bearish candle (close < open):  delta = -volume
  - Doji (close == open):           delta = 0

  Enhanced estimation using candle structure:
  - body_ratio = |close - open| / (high - low)
  - wick_ratio = 1 - body_ratio  (wicks = indecision)
  - delta = volume * body_ratio * direction
    This scales delta by how "decisive" the candle was:
    a doji with equal wicks contributes near-zero delta,
    a marubozu contributes full volume.

CVD is the running cumulative sum of per-candle deltas.

Usage:
    from tools.cvd import compute_cvd, get_cvd_data
    cvd_points = compute_cvd(df)
    # Returns list of {"time": ts, "value": cumulative_delta}

    # Full endpoint helper (includes candles + CVD + divergence signals):
    result = get_cvd_data(df)
"""

import numpy as np
import pandas as pd
from typing import List, Dict, Any


# ── Core calculation ───────────────────────────────────────────────────────────

def compute_cvd(df: pd.DataFrame, method: str = "body_weighted") -> List[Dict[str, Any]]:
    """
    Compute Cumulative Volume Delta from an OHLCV DataFrame.

    Args:
        df:      DataFrame with Open, High, Low, Close, Volume columns
                 and a DatetimeIndex (UTC).
        method:  Calculation method:
                   "simple"        — full volume directional (+vol or -vol)
                   "body_weighted" — scale by body/range ratio (default, smoother)

    Returns:
        List of {"time": unix_timestamp, "value": cumulative_delta, "delta": bar_delta}
        Ordered oldest-first (same order as input df).
    """
    if df is None or len(df) < 2:
        return []

    try:
        # Normalise MultiIndex columns (yfinance quirk)
        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = df.columns.get_level_values(0)
        df = df.loc[:, ~df.columns.duplicated()].copy()

        for col in ["Open", "High", "Low", "Close"]:
            df[col] = pd.to_numeric(df[col].squeeze(), errors="coerce")

        # Volume: use Volume if present, fall back to 1 per candle (forex)
        if "Volume" in df.columns:
            df["Volume"] = pd.to_numeric(df["Volume"].squeeze(), errors="coerce").fillna(0)
        else:
            df["Volume"] = 1.0

        df = df.dropna(subset=["Open", "High", "Low", "Close"])

        opens  = df["Open"].values.astype(float)
        highs  = df["High"].values.astype(float)
        lows   = df["Low"].values.astype(float)
        closes = df["Close"].values.astype(float)
        vols   = df["Volume"].values.astype(float)

        direction = np.sign(closes - opens)   # +1 bull, -1 bear, 0 doji

        if method == "body_weighted":
            candle_range = highs - lows
            body         = np.abs(closes - opens)
            # Avoid divide-by-zero on doji/zero-range candles
            with np.errstate(divide="ignore", invalid="ignore"):
                body_ratio = np.where(candle_range > 0, body / candle_range, 0.0)
            delta = vols * body_ratio * direction
        else:
            # Simple: full volume assigned directionally
            delta = vols * direction

        cumulative = np.cumsum(delta)

        result = []
        for i, idx in enumerate(df.index):
            result.append({
                "time":  int(idx.timestamp()),
                "value": float(round(cumulative[i], 4)),
                "delta": float(round(delta[i], 4)),
            })

        return result

    except Exception as e:
        print(f"[cvd] compute_cvd error: {e}")
        return []


# ── Divergence detection ───────────────────────────────────────────────────────

def detect_divergences(
    candles: List[Dict],
    cvd_points: List[Dict],
    lookback: int = 20,
    min_swing_pct: float = 0.003,
) -> List[Dict]:
    """
    Detect simple price/CVD divergences over the last `lookback` candles.

    Bullish divergence: price makes lower low, CVD makes higher low
    Bearish divergence: price makes higher high, CVD makes lower high

    Args:
        candles:      List of {time, open, high, low, close} dicts
        cvd_points:   List of {time, value} dicts (same length as candles)
        lookback:     Number of candles to scan
        min_swing_pct: Minimum price move to qualify as a swing

    Returns:
        List of divergence dicts:
          {type, price_time, price_value, cvd_value, label}
    """
    if not candles or not cvd_points or len(candles) != len(cvd_points):
        return []

    try:
        n = min(len(candles), len(cvd_points))
        candles   = candles[-lookback:] if n > lookback else candles[-n:]
        cvd_pts   = cvd_points[-lookback:] if n > lookback else cvd_points[-n:]
        m         = len(candles)

        if m < 6:
            return []

        closes    = np.array([c["close"] for c in candles])
        lows      = np.array([c["low"]   for c in candles])
        highs     = np.array([c["high"]  for c in candles])
        cvd_vals  = np.array([p["value"] for p in cvd_pts])
        times     = [c["time"] for c in candles]
        avg_price = float(np.mean(closes))

        divergences = []

        # Scan last 3 swing points
        for i in range(2, m - 1):
            # ── Swing low ──────────────────────────────────────────────
            if lows[i] < lows[i - 1] and lows[i] < lows[i + 1] if i + 1 < m else True:
                # Find previous swing low in window
                prev_low_idx = None
                for j in range(i - 2, max(-1, i - 10), -1):
                    if lows[j] < lows[j - 1] if j > 0 else True:
                        if lows[j] < lows[j + 1] if j + 1 < m else True:
                            prev_low_idx = j
                            break
                if prev_low_idx is not None:
                    price_lower_low = lows[i] < lows[prev_low_idx]
                    cvd_higher_low  = cvd_vals[i] > cvd_vals[prev_low_idx]
                    swing_size      = abs(lows[prev_low_idx] - lows[i]) / avg_price
                    if price_lower_low and cvd_higher_low and swing_size >= min_swing_pct:
                        divergences.append({
                            "type":        "bullish",
                            "price_time":  times[i],
                            "price_value": float(lows[i]),
                            "cvd_value":   float(cvd_vals[i]),
                            "label":       "Bull Div",
                        })

            # ── Swing high ─────────────────────────────────────────────
            if highs[i] > highs[i - 1] and highs[i] > highs[i + 1] if i + 1 < m else True:
                prev_high_idx = None
                for j in range(i - 2, max(-1, i - 10), -1):
                    if highs[j] > highs[j - 1] if j > 0 else True:
                        if highs[j] > highs[j + 1] if j + 1 < m else True:
                            prev_high_idx = j
                            break
                if prev_high_idx is not None:
                    price_higher_high = highs[i] > highs[prev_high_idx]
                    cvd_lower_high    = cvd_vals[i] < cvd_vals[prev_high_idx]
                    swing_size        = abs(highs[i] - highs[prev_high_idx]) / avg_price
                    if price_higher_high and cvd_lower_high and swing_size >= min_swing_pct:
                        divergences.append({
                            "type":        "bearish",
                            "price_time":  times[i],
                            "price_value": float(highs[i]),
                            "cvd_value":   float(cvd_vals[i]),
                            "label":       "Bear Div",
                        })

        # Deduplicate: keep only the most recent of each type
        seen = set()
        unique = []
        for d in reversed(divergences):
            key = (d["type"], d["price_time"])
            if key not in seen:
                seen.add(key)
                unique.append(d)
        return list(reversed(unique))[-4:]  # return at most 4 recent divergences

    except Exception as e:
        print(f"[cvd] detect_divergences error: {e}")
        return []


# ── CVD OHLC candles ──────────────────────────────────────────────────────────

def compute_cvd_candles(cvd_points: List[Dict[str, Any]], df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Build OHLC candles from the CVD running total.
    Altered to match TradingView logic: wicks are derived from the 
    intrabar extremes (high/low price wicks) rather than a fixed multiplier.
    """
    if not cvd_points or df is None or len(df) != len(cvd_points):
        return []

    # Prepare price data to calculate wick intensity
    # We use the relative size of the price wicks to determine the CVD wicks
    highs = df["High"].values
    lows = df["Low"].values
    opens = df["Open"].values
    closes = df["Close"].values

    candles = []
    for i, pt in enumerate(cvd_points):
        # 1. Standard OHLC mapping
        prev_close = cvd_points[i - 1]["value"] if i > 0 else pt["value"]
        open_ = prev_close
        close = pt["value"]
        delta = pt.get("delta", close - open_)

        # 2. TRADINGVIEW WICK SIMULATION
        # Calculate how much "extra" volume movement happened based on price wicks
        # This is more accurate than 0.3 * delta because it respects the chart shape.
        price_range = highs[i] - lows[i]
        
        if price_range > 0:
            # How far did price go above the body?
            upper_price_wick = highs[i] - max(opens[i], closes[i])
            # How far did price go below the body?
            lower_price_wick = min(opens[i], closes[i]) - lows[i]
            
            # Scale the CVD wicks proportionally to the price wicks
            # (Volume often follows the path of the wick before being absorbed)
            ratio = abs(delta) / (abs(closes[i] - opens[i]) if abs(closes[i] - opens[i]) > 0 else price_range)
            
            upper_cvd_wick = upper_price_wick * ratio
            lower_cvd_wick = lower_price_wick * ratio
            
            high = max(open_, close) + upper_cvd_wick
            low  = min(open_, close) - lower_cvd_wick
        else:
            high = max(open_, close)
            low  = min(open_, close)

        candles.append({
            "time":  pt["time"],
            "open":  round(open_, 4),
            "high":  round(high, 4),
            "low":   round(low, 4),
            "close": round(close, 4),
        })
    return candles


# ── Full data endpoint helper ──────────────────────────────────────────────────

def get_cvd_data(
    df: pd.DataFrame,
    method: str = "body_weighted",
    detect_divs: bool = True,
    lookback: int = 20,
) -> Dict[str, Any]:
    """
    Compute CVD and optionally divergences from an OHLCV DataFrame.

    Returns a dict suitable for serialisation to JSON:
    {
      "cvd":          [ {time, value, delta}, ... ],
      "divergences":  [ {type, price_time, price_value, cvd_value, label}, ... ],
      "method":       str,
      "has_volume":   bool,   -- False for forex (tick-counted only)
      "stats": {
        "min":  float,
        "max":  float,
        "last": float,
        "net":  float,        -- last - first (overall buying pressure direction)
      }
    }
    """
    has_volume = bool("Volume" in df.columns and df["Volume"].sum() > len(df))

    cvd_points = compute_cvd(df, method=method)

    stats = {}
    if cvd_points:
        vals        = [p["value"] for p in cvd_points]
        stats["min"]  = round(min(vals), 4)
        stats["max"]  = round(max(vals), 4)
        stats["last"] = round(vals[-1], 4)
        stats["net"]  = round(vals[-1] - vals[0], 4)

    divergences = []
    if detect_divs and cvd_points:
        # Build minimal candle list for divergence detection
        try:
            if isinstance(df.columns, pd.MultiIndex):
                df2 = df.copy()
                df2.columns = df2.columns.get_level_values(0)
            else:
                df2 = df
            candles_for_div = [
                {
                    "time":  int(idx.timestamp()),
                    "open":  float(row["Open"]),
                    "high":  float(row["High"]),
                    "low":   float(row["Low"]),
                    "close": float(row["Close"]),
                }
                for idx, row in df2.iterrows()
            ]
            divergences = detect_divergences(candles_for_div, cvd_points, lookback=lookback)
        except Exception as e:
            print(f"[cvd] divergence prep error: {e}")

    cvd_candles = compute_cvd_candles(cvd_points)

    return {
        "cvd":         cvd_points,
        "cvd_candles": cvd_candles,
        "divergences": divergences,
        "method":      method,
        "has_volume":  has_volume,
        "stats":       stats,
    }
