"""
tools/cvd.py

Cumulative Volume Delta (CVD) calculator with TradingView-style methodology.

Key features:
- Intrabar analysis for timeframes > 1m (uses 1-minute data for accurate delta calculation)
- Close Location Value approximation for 1m timeframe
- Proper CVD candle construction with high/low tracking
- Anchor period support (daily reset) matching TradingView's behavior
"""

import numpy as np
import pandas as pd
from typing import List, Dict, Any, Optional, Callable
from datetime import datetime, timezone


# ── Intrabar analysis intervals ───────────────────────────────────────────────

INTRABAR_MAP = {
    "5m":  "1m",
    "15m": "1m",
    "30m": "1m",
    "1h":  "1m",
    "4h":  "1m",
    "1d":  "5m",
    "1wk": "1h",
}

INTERVAL_MINUTES = {
    "1m": 1, "2m": 2, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "4h": 240, "1d": 1440, "1wk": 10080,
}

# ── Anchor period options ─────────────────────────────────────────────────────
# TradingView resets CVD at the start of each anchor period
ANCHOR_PERIODS = {"session", "day", "week", "month", "year", "none"}
DEFAULT_ANCHOR = "day"  # Match TradingView default


# ── Anchor period helpers ─────────────────────────────────────────────────────

def _get_anchor_key(timestamp: pd.Timestamp, anchor: str) -> tuple:
    """
    Return a hashable key representing the anchor period for a given timestamp.
    When the anchor key changes, the CVD should reset to 0.
    """
    if anchor == "none":
        return (0,)  # Never changes - continuous accumulation
    elif anchor == "day":
        return (timestamp.year, timestamp.month, timestamp.day)
    elif anchor == "week":
        # ISO week number
        return (timestamp.year, timestamp.isocalendar()[1])
    elif anchor == "month":
        return (timestamp.year, timestamp.month)
    elif anchor == "year":
        return (timestamp.year,)
    elif anchor == "session":
        # Session anchor resets at market open - use day as approximation
        # Could be enhanced with actual session times
        return (timestamp.year, timestamp.month, timestamp.day)
    else:
        return (timestamp.year, timestamp.month, timestamp.day)  # Default to day


# ── Core calculation (single timeframe) ───────────────────────────────────────

def _compute_bar_delta_with_polarity(
    opens: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    volumes: np.ndarray,
    prev_closes: Optional[np.ndarray] = None,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Compute volume delta with TradingView's exact polarity rules.

    TradingView's polarity classification:
    1. If close > open: positive (+1)
    2. If close < open: negative (-1)
    3. If close == open (doji):
       a. If close > prev_close: positive (+1)
       b. If close < prev_close: negative (-1)
       c. If close == prev_close: inherit previous bar's polarity

    Returns:
        tuple of (delta array, polarity array) - polarity is needed for inheritance
    """
    n = len(closes)
    direction = np.zeros(n)

    # Step 1: Determine direction from close vs open
    diff = closes - opens
    direction = np.where(diff > 0, 1.0, np.where(diff < 0, -1.0, 0.0))

    # Step 2: Handle doji bars (close == open)
    doji_mask = (diff == 0)

    if prev_closes is not None and np.any(doji_mask):
        prev_diff = closes - prev_closes

        # For dojis: use prev_close comparison first
        direction = np.where(
            doji_mask & (prev_diff > 0), 1.0,
            np.where(doji_mask & (prev_diff < 0), -1.0, direction)
        )

        # Step 3: Handle the case where close == open AND close == prev_close
        # In this case, inherit the previous bar's polarity
        needs_inheritance = doji_mask & (prev_diff == 0)

        if np.any(needs_inheritance):
            # Forward-fill the polarity from the last bar that had a clear direction
            for i in range(n):
                if needs_inheritance[i]:
                    if i > 0:
                        direction[i] = direction[i - 1]
                    # If first bar needs inheritance and has no clear direction, use 0

    delta = volumes * direction
    return delta, direction


def _compute_bar_delta(
    opens: np.ndarray,
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    volumes: np.ndarray,
    prev_closes: Optional[np.ndarray] = None,
    method: str = "polarity"
) -> np.ndarray:
    """
    Compute volume delta for each bar.

    Methods:
        - "polarity": Assign full volume based on close vs open direction (TradingView intrabar style)
        - "close_location": Split volume based on close position within range (approximation)
    """
    if method == "polarity":
        delta, _ = _compute_bar_delta_with_polarity(
            opens, highs, lows, closes, volumes, prev_closes
        )
        return delta

    else:  # close_location
        # Close Location Value method - estimates buy/sell split
        # Formula: delta = volume * (2*close - high - low) / (high - low)
        candle_range = highs - lows
        with np.errstate(divide="ignore", invalid="ignore"):
            close_location = np.where(
                candle_range > 0,
                (2 * closes - highs - lows) / candle_range,
                0.0
            )
        delta = volumes * close_location
        return delta


def compute_cvd_with_intrabar(
    df: pd.DataFrame,
    intrabar_df: Optional[pd.DataFrame] = None,
    method: str = "polarity",
    anchor: str = DEFAULT_ANCHOR
) -> List[Dict[str, Any]]:
    """
    Compute CVD using intrabar analysis when available.

    This matches TradingView's approach:
    - For each chart bar, analyze lower timeframe bars within it
    - Classify each intrabar as buy (+volume) or sell (-volume) based on polarity
    - Sum to get the bar's volume delta
    - Track cumulative high/low for proper CVD candle wicks
    - Reset cumulative value at the start of each anchor period (default: daily)

    Args:
        df: Main timeframe OHLCV DataFrame
        intrabar_df: Lower timeframe DataFrame for intrabar analysis (optional)
        method: "polarity" (TradingView style) or "close_location" (approximation)
        anchor: Anchor period for CVD reset ("day", "week", "month", "year", "none")
                Default is "day" to match TradingView's default behavior.

    Returns:
        List of CVD data points with time, value, delta, cvd_high, cvd_low
    """
    if df is None or len(df) < 2:
        return []

    try:
        # Clean main dataframe
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

        results = []
        cumulative = 0.0
        prev_close = None
        prev_polarity = 1.0  # Track last polarity for inheritance
        current_anchor_key = None

        if intrabar_df is not None and len(intrabar_df) > 0:
            # Clean intrabar dataframe
            if isinstance(intrabar_df.columns, pd.MultiIndex):
                intrabar_df = intrabar_df.copy()
                intrabar_df.columns = intrabar_df.columns.get_level_values(0)
            intrabar_df = intrabar_df.loc[:, ~intrabar_df.columns.duplicated()].copy()

            for col in ["Open", "High", "Low", "Close"]:
                intrabar_df[col] = pd.to_numeric(intrabar_df[col].squeeze(), errors="coerce")
            if "Volume" in intrabar_df.columns:
                intrabar_df["Volume"] = pd.to_numeric(intrabar_df["Volume"].squeeze(), errors="coerce").fillna(0)
            else:
                intrabar_df["Volume"] = 1.0
            intrabar_df = intrabar_df.dropna(subset=["Open", "High", "Low", "Close"])

            # Process each main bar using intrabar data
            for i, (idx, row) in enumerate(df.iterrows()):
                # Check for anchor period reset (e.g., new day)
                bar_anchor_key = _get_anchor_key(idx, anchor)
                is_period_start = False
                if current_anchor_key is not None and bar_anchor_key != current_anchor_key:
                    # New anchor period - reset cumulative to 0
                    cumulative = 0.0
                    is_period_start = True
                elif current_anchor_key is None:
                    # First bar is also a period start
                    is_period_start = True
                current_anchor_key = bar_anchor_key

                bar_start = idx
                if i < len(df) - 1:
                    bar_end = df.index[i + 1]
                else:
                    # For the last bar, include all remaining intrabar data
                    bar_end = intrabar_df.index[-1] + pd.Timedelta(seconds=1)

                # Get intrabars within this main bar
                mask = (intrabar_df.index >= bar_start) & (intrabar_df.index < bar_end)
                intrabars = intrabar_df[mask]

                if len(intrabars) > 0:
                    # Compute delta for each intrabar with proper polarity inheritance
                    opens = intrabars["Open"].values.astype(float)
                    highs = intrabars["High"].values.astype(float)
                    lows = intrabars["Low"].values.astype(float)
                    closes = intrabars["Close"].values.astype(float)
                    volumes = intrabars["Volume"].values.astype(float)

                    # Create prev_closes array (shift by 1)
                    prev_closes = np.roll(closes, 1)
                    if prev_close is not None:
                        prev_closes[0] = prev_close
                    else:
                        prev_closes[0] = opens[0]

                    # Use the polarity-aware function
                    intrabar_deltas, polarities = _compute_bar_delta_with_polarity(
                        opens, highs, lows, closes, volumes, prev_closes
                    )

                    # Apply polarity inheritance from previous bar
                    # This handles the case where first intrabar has close==open==prev_close
                    if len(polarities) > 0:
                        # Check if first intrabar needs inheritance
                        diff0 = closes[0] - opens[0]
                        prev_diff0 = closes[0] - prev_closes[0] if prev_closes is not None else 0
                        if diff0 == 0 and prev_diff0 == 0:
                            # First intrabar needs to inherit from previous bar's last polarity
                            intrabar_deltas[0] = volumes[0] * prev_polarity
                            polarities[0] = prev_polarity

                        # Update prev_polarity to last non-zero polarity
                        for p in reversed(polarities):
                            if p != 0:
                                prev_polarity = p
                                break

                    # Sum deltas for this bar
                    bar_delta = float(np.sum(intrabar_deltas))

                    # Track cumulative high/low during the bar
                    running = cumulative
                    cvd_high = cumulative
                    cvd_low = cumulative
                    for d in intrabar_deltas:
                        running += d
                        cvd_high = max(cvd_high, running)
                        cvd_low = min(cvd_low, running)

                    cumulative += bar_delta
                    prev_close = closes[-1]
                else:
                    # No intrabar data - fall back to close location method
                    candle_range = row["High"] - row["Low"]
                    if candle_range > 0:
                        close_loc = (2 * row["Close"] - row["High"] - row["Low"]) / candle_range
                    else:
                        close_loc = 0.0
                    bar_delta = float(row["Volume"] * close_loc)
                    cvd_high = cumulative + max(0, bar_delta)
                    cvd_low = cumulative + min(0, bar_delta)
                    cumulative += bar_delta

                results.append({
                    "time": int(idx.timestamp()),
                    "value": round(cumulative, 4),
                    "delta": round(bar_delta, 4),
                    "cvd_high": round(cvd_high, 4),
                    "cvd_low": round(cvd_low, 4),
                    "is_period_start": is_period_start,
                })
        else:
            # No intrabar data - use close location value method
            opens = df["Open"].values.astype(float)
            highs = df["High"].values.astype(float)
            lows = df["Low"].values.astype(float)
            closes = df["Close"].values.astype(float)
            volumes = df["Volume"].values.astype(float)

            # Create prev_closes for polarity fallback
            prev_closes = np.roll(closes, 1)
            prev_closes[0] = opens[0]

            deltas = _compute_bar_delta(
                opens, highs, lows, closes, volumes, prev_closes, method=method
            )

            for i, (idx, row) in enumerate(df.iterrows()):
                # Check for anchor period reset (e.g., new day)
                bar_anchor_key = _get_anchor_key(idx, anchor)
                is_period_start = False
                if current_anchor_key is not None and bar_anchor_key != current_anchor_key:
                    # New anchor period - reset cumulative to 0
                    cumulative = 0.0
                    is_period_start = True
                elif current_anchor_key is None:
                    # First bar is also a period start
                    is_period_start = True
                current_anchor_key = bar_anchor_key

                bar_delta = deltas[i]
                cvd_high = cumulative + max(0, bar_delta)
                cvd_low = cumulative + min(0, bar_delta)
                cumulative += bar_delta

                results.append({
                    "time": int(idx.timestamp()),
                    "value": round(cumulative, 4),
                    "delta": round(bar_delta, 4),
                    "cvd_high": round(cvd_high, 4),
                    "cvd_low": round(cvd_low, 4),
                    "is_period_start": is_period_start,
                })

        return results

    except Exception as e:
        print(f"[cvd] compute_cvd_with_intrabar error: {e}")
        import traceback
        traceback.print_exc()
        return []


def compute_cvd(
    df: pd.DataFrame,
    method: str = "tradingview",
    anchor: str = DEFAULT_ANCHOR
) -> List[Dict[str, Any]]:
    """
    Compute Cumulative Volume Delta from an OHLCV DataFrame (legacy interface).

    Methods:
        - "tradingview": Close Location Value method (default)
        - "polarity": Simple direction-based (close > open = buy)
        - "body_weighted": Legacy method using body/range ratio

    Args:
        df: OHLCV DataFrame with DatetimeIndex
        method: Calculation method
        anchor: Anchor period for CVD reset ("day", "week", "month", "year", "none")
                Default is "day" to match TradingView's default behavior.
    """
    if df is None or len(df) < 2:
        return []

    try:
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

        opens = df["Open"].values.astype(float)
        highs = df["High"].values.astype(float)
        lows = df["Low"].values.astype(float)
        closes = df["Close"].values.astype(float)
        vols = df["Volume"].values.astype(float)

        # Create prev_closes
        prev_closes = np.roll(closes, 1)
        prev_closes[0] = opens[0]

        if method == "tradingview":
            deltas = _compute_bar_delta(opens, highs, lows, closes, vols, prev_closes, "close_location")
        elif method == "polarity":
            deltas = _compute_bar_delta(opens, highs, lows, closes, vols, prev_closes, "polarity")
        elif method == "body_weighted":
            direction = np.sign(closes - opens)
            body = np.abs(closes - opens)
            candle_range = highs - lows
            with np.errstate(divide="ignore", invalid="ignore"):
                body_ratio = np.where(candle_range > 0, body / candle_range, 0.0)
            deltas = vols * body_ratio * direction
        else:
            direction = np.sign(closes - opens)
            deltas = vols * direction

        # Compute cumulative with anchor period resets
        result = []
        cumulative = 0.0
        current_anchor_key = None

        for i, idx in enumerate(df.index):
            # Check for anchor period reset (e.g., new day)
            bar_anchor_key = _get_anchor_key(idx, anchor)
            is_period_start = False
            if current_anchor_key is not None and bar_anchor_key != current_anchor_key:
                # New anchor period - reset cumulative to 0
                cumulative = 0.0
                is_period_start = True
            elif current_anchor_key is None:
                # First bar is also a period start
                is_period_start = True
            current_anchor_key = bar_anchor_key

            cumulative += deltas[i]
            result.append({
                "time": int(idx.timestamp()),
                "value": float(round(cumulative, 4)),
                "delta": float(round(deltas[i], 4)),
                "is_period_start": is_period_start,
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
        candles = candles[-lookback:] if n > lookback else candles[-n:]
        cvd_pts = cvd_points[-lookback:] if n > lookback else cvd_points[-n:]
        m = len(candles)

        if m < 6:
            return []

        closes = np.array([c["close"] for c in candles])
        lows = np.array([c["low"] for c in candles])
        highs = np.array([c["high"] for c in candles])
        cvd_vals = np.array([p["value"] for p in cvd_pts])
        times = [c["time"] for c in candles]
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
                    cvd_higher_low = cvd_vals[i] > cvd_vals[prev_low_idx]
                    swing_size = abs(lows[prev_low_idx] - lows[i]) / avg_price
                    if price_lower_low and cvd_higher_low and swing_size >= min_swing_pct:
                        divergences.append({
                            "type": "bullish",
                            "price_time": times[i],
                            "price_value": float(lows[i]),
                            "cvd_value": float(cvd_vals[i]),
                            "label": "Bull Div",
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
                    cvd_lower_high = cvd_vals[i] < cvd_vals[prev_high_idx]
                    swing_size = abs(highs[i] - highs[prev_high_idx]) / avg_price
                    if price_higher_high and cvd_lower_high and swing_size >= min_swing_pct:
                        divergences.append({
                            "type": "bearish",
                            "price_time": times[i],
                            "price_value": float(highs[i]),
                            "cvd_value": float(cvd_vals[i]),
                            "label": "Bear Div",
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


# ── CVD OHLC candles ────────────────────────────────────────────────────────────

def compute_cvd_candles(
    cvd_points: List[Dict[str, Any]],
    df: pd.DataFrame,
    include_wicks: bool = True
) -> List[Dict[str, Any]]:
    """
    Build OHLC candles from CVD data.

    TradingView CVD candle construction:
    - Open: 0 if first bar of anchor period, otherwise previous CVD candle's close
    - Close: cumulative volume delta at end of bar
    - High: highest cumulative volume delta achieved during the bar
    - Low: lowest cumulative volume delta achieved during the bar

    Args:
        cvd_points: List of CVD points with value, delta, cvd_high, cvd_low, is_period_start
        df: Original OHLCV DataFrame
        include_wicks: If True, use cvd_high/cvd_low for wicks (TradingView style)
                      If False, set high=low=body extremes (no wicks)
    """
    if not cvd_points or df is None or len(df) != len(cvd_points):
        return []

    candles = []
    for i, pt in enumerate(cvd_points):
        # TradingView rule: open is 0 at period start, otherwise previous close
        is_period_start = pt.get("is_period_start", False)
        if is_period_start or i == 0:
            open_val = 0.0
        else:
            open_val = cvd_points[i - 1]["value"]

        close_val = pt["value"]

        if include_wicks and "cvd_high" in pt and "cvd_low" in pt:
            # Use tracked intrabar high/low for proper wicks
            high = pt["cvd_high"]
            low = pt["cvd_low"]
            # Ensure high/low encompass the open value too
            high = max(high, open_val)
            low = min(low, open_val)
        else:
            # No wicks - high/low = body extremes
            high = max(open_val, close_val)
            low = min(open_val, close_val)

        candles.append({
            "time": pt["time"],
            "open": round(open_val, 4),
            "high": round(high, 4),
            "low": round(low, 4),
            "close": round(close_val, 4),
        })
    return candles


# ── Full data endpoint helper ──────────────────────────────────────────────────

def get_cvd_data(
    df: pd.DataFrame,
    method: str = "tradingview",
    detect_divs: bool = True,
    lookback: int = 20,
    intrabar_df: Optional[pd.DataFrame] = None,
    include_wicks: bool = True,
    anchor: str = DEFAULT_ANCHOR,
) -> Dict[str, Any]:
    """
    Get complete CVD data including candles, divergences, and stats.

    Args:
        df: Main timeframe OHLCV DataFrame
        method: Calculation method ("tradingview", "polarity", "body_weighted")
        detect_divs: Whether to detect divergences
        lookback: Lookback period for divergence detection
        intrabar_df: Optional lower timeframe DataFrame for intrabar analysis
        include_wicks: Whether to include wicks on CVD candles
        anchor: Anchor period for CVD reset ("day", "week", "month", "year", "none")
                Default is "day" to match TradingView's default behavior.
    """
    # Pre-clean the dataframe
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = df.columns.get_level_values(0)
    df = df.loc[:, ~df.columns.duplicated()].dropna(subset=["Open", "High", "Low", "Close"]).copy()

    has_volume = bool("Volume" in df.columns and df["Volume"].sum() > len(df))

    # Use intrabar analysis if available, otherwise fall back to single-timeframe
    if intrabar_df is not None and len(intrabar_df) > 0:
        cvd_points = compute_cvd_with_intrabar(df, intrabar_df, method="polarity", anchor=anchor)
        used_method = "intrabar"
    else:
        cvd_points = compute_cvd(df, method=method, anchor=anchor)
        used_method = method

    stats = {}
    if cvd_points:
        vals = [p["value"] for p in cvd_points]
        stats["min"] = round(min(vals), 4)
        stats["max"] = round(max(vals), 4)
        stats["last"] = round(vals[-1], 4)
        stats["net"] = round(vals[-1] - vals[0], 4)

    divergences = []
    if detect_divs and cvd_points:
        try:
            candles_for_div = [
                {
                    "time": int(idx.timestamp()),
                    "open": float(row["Open"]),
                    "high": float(row["High"]),
                    "low": float(row["Low"]),
                    "close": float(row["Close"]),
                }
                for idx, row in df.iterrows()
            ]
            divergences = detect_divergences(candles_for_div, cvd_points, lookback=lookback)
        except Exception as e:
            print(f"[cvd] divergence prep error: {e}")

    cvd_candles = compute_cvd_candles(cvd_points, df, include_wicks=include_wicks)

    return {
        "cvd": cvd_points,
        "cvd_candles": cvd_candles,
        "divergences": divergences,
        "method": used_method,
        "anchor": anchor,
        "has_volume": has_volume,
        "stats": stats,
    }
