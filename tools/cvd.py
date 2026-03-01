"""
tools/cvd.py

Cumulative Volume Delta (CVD) calculator with block-style (no wick) Candlesticks.
"""

import numpy as np
import pandas as pd
from typing import List, Dict, Any


# ── Core calculation ───────────────────────────────────────────────────────────

def compute_cvd(df: pd.DataFrame, method: str = "tradingview") -> List[Dict[str, Any]]:
    """
    Compute Cumulative Volume Delta from an OHLCV DataFrame.

    Methods:
        - "tradingview": Close Location Value method (default) - estimates buy/sell volume
          based on where close is relative to the high-low range. This matches TradingView's
          approach when intrabar data is not available.
          Formula: delta = volume * (2*close - high - low) / (high - low)
        - "body_weighted": Legacy method using body/range ratio with direction sign.
        - "simple": Basic method where direction = sign(close - open), delta = volume * direction.
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

        candle_range = highs - lows

        if method == "tradingview":
            # Close Location Value method - matches TradingView's CVD approximation
            # Formula: delta = volume * (2*close - high - low) / (high - low)
            # This estimates buy/sell volume based on where close is within the bar's range
            with np.errstate(divide="ignore", invalid="ignore"):
                close_location = np.where(
                    candle_range > 0,
                    (2 * closes - highs - lows) / candle_range,
                    0.0
                )
            delta = vols * close_location
        elif method == "body_weighted":
            # Legacy method using body/range ratio with direction sign
            direction = np.sign(closes - opens)
            body = np.abs(closes - opens)
            with np.errstate(divide="ignore", invalid="ignore"):
                body_ratio = np.where(candle_range > 0, body / candle_range, 0.0)
            delta = vols * body_ratio * direction
        else:
            # Simple method - just use direction
            direction = np.sign(closes - opens)
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

        for i in range(2, m - 1):
            if lows[i] < lows[i - 1] and lows[i] < lows[i + 1] if i + 1 < m else True:
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

        seen = set()
        unique = []
        for d in reversed(divergences):
            key = (d["type"], d["price_time"])
            if key not in seen:
                seen.add(key)
                unique.append(d)
        return list(reversed(unique))[-4:]

    except Exception as e:
        print(f"[cvd] detect_divergences error: {e}")
        return []


# ── CVD OHLC candles (NO WICKS) ────────────────────────────────────────────────

def compute_cvd_candles(cvd_points: List[Dict[str, Any]], df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Build OHLC candles from the CVD running total.
    Forces High and Low to equal the Open or Close, effectively removing wicks.
    """
    if not cvd_points or df is None or len(df) != len(cvd_points):
        return []

    candles = []
    for i, pt in enumerate(cvd_points):
        # Open is where the CVD ended on the previous bar
        open_val  = cvd_points[i - 1]["value"] if i > 0 else pt["value"]
        close_val = pt["value"]
        
        # To REMOVE WICKS: High/Low must never exceed the Open/Close body
        high = max(open_val, close_val)
        low  = min(open_val, close_val)

        candles.append({
            "time":  pt["time"],
            "open":  round(open_val, 4),
            "high":  round(high, 4),
            "low":   round(low, 4),
            "close": round(close_val, 4),
        })
    return candles


# ── Full data endpoint helper ──────────────────────────────────────────────────

def get_cvd_data(
    df: pd.DataFrame,
    method: str = "tradingview",
    detect_divs: bool = True,
    lookback: int = 20,
) -> Dict[str, Any]:
    
    # Pre-clean the dataframe so indices match cvd_points
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = df.columns.get_level_values(0)
    df = df.loc[:, ~df.columns.duplicated()].dropna(subset=["Open", "High", "Low", "Close"]).copy()

    has_volume = bool("Volume" in df.columns and df["Volume"].sum() > len(df))
    cvd_points = compute_cvd(df, method=method)

    stats = {}
    if cvd_points:
        vals = [p["value"] for p in cvd_points]
        stats["min"]  = round(min(vals), 4)
        stats["max"]  = round(max(vals), 4)
        stats["last"] = round(vals[-1], 4)
        stats["net"]  = round(vals[-1] - vals[0], 4)

    divergences = []
    if detect_divs and cvd_points:
        try:
            candles_for_div = [
                {
                    "time":  int(idx.timestamp()),
                    "open":  float(row["Open"]),
                    "high":  float(row["High"]),
                    "low":   float(row["Low"]),
                    "close": float(row["Close"]),
                }
                for idx, row in df.iterrows()
            ]
            divergences = detect_divergences(candles_for_div, cvd_points, lookback=lookback)
        except Exception as e:
            print(f"[cvd] divergence prep error: {e}")

    # Generate candles with NO WICKS
    cvd_candles = compute_cvd_candles(cvd_points, df)

    return {
        "cvd":         cvd_points,
        "cvd_candles": cvd_candles,
        "divergences": divergences,
        "method":      method,
        "has_volume":  has_volume,
        "stats":       stats,
    }
