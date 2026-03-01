"""
tools/cvd.py

Cumulative Volume Delta (CVD) calculator with divergence detection.

Replicates the TradingView PineScript CVD indicator logic:
- Volume delta per bar: (close > open ? 1 : close < open ? -1 : 0) * volume
- Lower timeframe aggregation for accurate CVD high/low tracking
- Pivot detection with configurable left/right bars
- Divergence detection: price vs CVD pivot comparison
"""

import numpy as np
import pandas as pd
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


# Intrabar analysis intervals (chart interval -> lower timeframe for CVD calculation)
INTRABAR_MAP = {
    "5m":  "1m",
    "15m": "1m",
    "30m": "1m",
    "1h":  "1m",
    "4h":  "1m",
    "1d":  "5m",
    "1wk": "1h",
}


@dataclass
class PivotPoint:
    """Represents a detected pivot high or low."""
    bar_index: int
    value: float


def get_bar_delta(open_price: float, close_price: float, volume: float) -> float:
    """
    Calculate volume delta for a single bar using PineScript polarity rule.

    Args:
        open_price: Bar open price
        close_price: Bar close price
        volume: Bar volume

    Returns:
        Positive delta if bullish, negative if bearish, zero if doji
    """
    if close_price > open_price:
        return volume
    elif close_price < open_price:
        return -volume
    else:
        return 0.0


def compute_bar_deltas(
    opens: np.ndarray,
    closes: np.ndarray,
    volumes: np.ndarray
) -> np.ndarray:
    """
    Compute volume delta for each bar in arrays.

    Args:
        opens: Array of open prices
        closes: Array of close prices
        volumes: Array of volumes

    Returns:
        Array of volume deltas
    """
    direction = np.where(
        closes > opens, 1.0,
        np.where(closes < opens, -1.0, 0.0)
    )
    return volumes * direction


def build_cvd_ohlc_from_intrabar(
    main_df: pd.DataFrame,
    intrabar_df: pd.DataFrame
) -> List[Dict[str, Any]]:
    """
    Build CVD OHLC candles from lower timeframe data.

    Replicates PineScript logic:
    - For each main bar, get all intrabar deltas
    - Track running cumulative to find high/low during the bar
    - Open = previous bar's close (or 0 for first bar)
    - Close = final cumulative value

    Args:
        main_df: Main timeframe OHLCV DataFrame
        intrabar_df: Lower timeframe OHLCV DataFrame

    Returns:
        List of CVD candle dicts with time, open, high, low, close
    """
    if main_df is None or len(main_df) < 1:
        return []

    results = []
    last_close = 0.0  # CVD close of previous bar (starts at 0)

    for i, (idx, row) in enumerate(main_df.iterrows()):
        bar_start = idx

        # Determine bar end
        if i < len(main_df) - 1:
            bar_end = main_df.index[i + 1]
        else:
            # Last bar: include remaining intrabar data
            bar_end = intrabar_df.index[-1] + pd.Timedelta(seconds=1)

        # Get intrabars within this main bar
        mask = (intrabar_df.index >= bar_start) & (intrabar_df.index < bar_end)
        intrabars = intrabar_df[mask]

        # CVD open is previous bar's close
        cvd_open = last_close

        if len(intrabars) > 0:
            # Compute delta for each intrabar
            opens = intrabars["Open"].values.astype(float)
            closes = intrabars["Close"].values.astype(float)
            volumes = intrabars["Volume"].values.astype(float)

            deltas = compute_bar_deltas(opens, closes, volumes)

            # Track running cumulative to find high/low
            running = cvd_open
            cvd_high = cvd_open
            cvd_low = cvd_open

            for delta in deltas:
                running += delta
                cvd_high = max(cvd_high, running)
                cvd_low = min(cvd_low, running)

            cvd_close = running
        else:
            # No intrabar data: use main bar's polarity
            delta = get_bar_delta(
                float(row["Open"]),
                float(row["Close"]),
                float(row.get("Volume", 1.0))
            )
            cvd_close = cvd_open + delta
            cvd_high = max(cvd_open, cvd_close)
            cvd_low = min(cvd_open, cvd_close)

        results.append({
            "time": int(idx.timestamp()),
            "open": round(cvd_open, 4),
            "high": round(cvd_high, 4),
            "low": round(cvd_low, 4),
            "close": round(cvd_close, 4),
        })

        last_close = cvd_close

    return results


def build_cvd_ohlc_single_tf(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Build CVD OHLC candles from single timeframe data (no intrabar).

    Args:
        df: OHLCV DataFrame

    Returns:
        List of CVD candle dicts
    """
    if df is None or len(df) < 1:
        return []

    opens = df["Open"].values.astype(float)
    closes = df["Close"].values.astype(float)
    volumes = df["Volume"].values.astype(float) if "Volume" in df.columns else np.ones(len(df))

    deltas = compute_bar_deltas(opens, closes, volumes)

    results = []
    last_close = 0.0

    for i, (idx, row) in enumerate(df.iterrows()):
        cvd_open = last_close
        delta = deltas[i]
        cvd_close = cvd_open + delta

        # Without intrabar data, high/low are just the body extremes
        cvd_high = max(cvd_open, cvd_close)
        cvd_low = min(cvd_open, cvd_close)

        results.append({
            "time": int(idx.timestamp()),
            "open": round(cvd_open, 4),
            "high": round(cvd_high, 4),
            "low": round(cvd_low, 4),
            "close": round(cvd_close, 4),
        })

        last_close = cvd_close

    return results


def detect_pivot_highs(
    values: np.ndarray,
    left_bars: int = 5,
    right_bars: int = 5
) -> List[PivotPoint]:
    """
    Detect pivot highs in a series.

    A pivot high at bar i is confirmed when:
    - values[i] is the highest among values[i-left_bars : i+right_bars+1]

    Args:
        values: Array of values to find pivots in
        left_bars: Number of bars to the left for pivot confirmation
        right_bars: Number of bars to the right for pivot confirmation

    Returns:
        List of PivotPoint objects
    """
    pivots = []
    n = len(values)

    for i in range(left_bars, n - right_bars):
        is_pivot = True
        current = values[i]

        # Check left bars
        for j in range(i - left_bars, i):
            if values[j] >= current:
                is_pivot = False
                break

        # Check right bars
        if is_pivot:
            for j in range(i + 1, i + right_bars + 1):
                if values[j] >= current:
                    is_pivot = False
                    break

        if is_pivot:
            pivots.append(PivotPoint(bar_index=i, value=current))

    return pivots


def detect_pivot_lows(
    values: np.ndarray,
    left_bars: int = 5,
    right_bars: int = 5
) -> List[PivotPoint]:
    """
    Detect pivot lows in a series.

    A pivot low at bar i is confirmed when:
    - values[i] is the lowest among values[i-left_bars : i+right_bars+1]

    Args:
        values: Array of values to find pivots in
        left_bars: Number of bars to the left for pivot confirmation
        right_bars: Number of bars to the right for pivot confirmation

    Returns:
        List of PivotPoint objects
    """
    pivots = []
    n = len(values)

    for i in range(left_bars, n - right_bars):
        is_pivot = True
        current = values[i]

        # Check left bars
        for j in range(i - left_bars, i):
            if values[j] <= current:
                is_pivot = False
                break

        # Check right bars
        if is_pivot:
            for j in range(i + 1, i + right_bars + 1):
                if values[j] <= current:
                    is_pivot = False
                    break

        if is_pivot:
            pivots.append(PivotPoint(bar_index=i, value=current))

    return pivots


def detect_divergences(
    price_highs: np.ndarray,
    price_lows: np.ndarray,
    cvd_highs: np.ndarray,
    cvd_lows: np.ndarray,
    times: List[int],
    left_pivot: int = 5,
    right_pivot: int = 5,
    max_pivot_bar_gap: int = 8
) -> List[Dict[str, Any]]:
    """
    Detect divergences between price and CVD.

    Bearish divergence: Price makes higher high, CVD makes lower high
    Bullish divergence: Price makes lower low, CVD makes higher low

    Args:
        price_highs: Array of price highs
        price_lows: Array of price lows
        cvd_highs: Array of CVD highs
        cvd_lows: Array of CVD lows
        times: List of timestamps
        left_pivot: Left bars for pivot detection
        right_pivot: Right bars for pivot detection
        max_pivot_bar_gap: Maximum bar gap between price and CVD pivots

    Returns:
        List of divergence dicts
    """
    divergences = []

    # Detect pivots on price and CVD
    price_high_pivots = detect_pivot_highs(price_highs, left_pivot, right_pivot)
    price_low_pivots = detect_pivot_lows(price_lows, left_pivot, right_pivot)
    cvd_high_pivots = detect_pivot_highs(cvd_highs, left_pivot, right_pivot)
    cvd_low_pivots = detect_pivot_lows(cvd_lows, left_pivot, right_pivot)

    # Check for bearish divergence: price higher highs, CVD lower highs
    if len(price_high_pivots) >= 2 and len(cvd_high_pivots) >= 2:
        # Get last two pivots of each
        ph1, ph2 = price_high_pivots[-2], price_high_pivots[-1]
        ch1, ch2 = cvd_high_pivots[-2], cvd_high_pivots[-1]

        # Check conditions: price higher high AND CVD lower high
        if ph2.value > ph1.value and ch2.value < ch1.value:
            # Check bar gap constraint
            if (abs(ph2.bar_index - ch2.bar_index) <= max_pivot_bar_gap and
                abs(ph1.bar_index - ch1.bar_index) <= max_pivot_bar_gap):
                divergences.append({
                    "type": "bearish",
                    "label": "Bear Div",
                    "price_time": times[ph2.bar_index],
                    "price_value": float(ph2.value),
                    "cvd_value": float(ch2.value),
                    "price_pivot_1": {"bar": ph1.bar_index, "value": float(ph1.value)},
                    "price_pivot_2": {"bar": ph2.bar_index, "value": float(ph2.value)},
                    "cvd_pivot_1": {"bar": ch1.bar_index, "value": float(ch1.value)},
                    "cvd_pivot_2": {"bar": ch2.bar_index, "value": float(ch2.value)},
                })

    # Check for bullish divergence: price lower lows, CVD higher lows
    if len(price_low_pivots) >= 2 and len(cvd_low_pivots) >= 2:
        # Get last two pivots of each
        pl1, pl2 = price_low_pivots[-2], price_low_pivots[-1]
        cl1, cl2 = cvd_low_pivots[-2], cvd_low_pivots[-1]

        # Check conditions: price lower low AND CVD higher low
        if pl2.value < pl1.value and cl2.value > cl1.value:
            # Check bar gap constraint
            if (abs(pl2.bar_index - cl2.bar_index) <= max_pivot_bar_gap and
                abs(pl1.bar_index - cl1.bar_index) <= max_pivot_bar_gap):
                divergences.append({
                    "type": "bullish",
                    "label": "Bull Div",
                    "price_time": times[pl2.bar_index],
                    "price_value": float(pl2.value),
                    "cvd_value": float(cl2.value),
                    "price_pivot_1": {"bar": pl1.bar_index, "value": float(pl1.value)},
                    "price_pivot_2": {"bar": pl2.bar_index, "value": float(pl2.value)},
                    "cvd_pivot_1": {"bar": cl1.bar_index, "value": float(cl1.value)},
                    "cvd_pivot_2": {"bar": cl2.bar_index, "value": float(cl2.value)},
                })

    return divergences


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize a DataFrame for CVD calculation.

    Args:
        df: Input DataFrame

    Returns:
        Cleaned DataFrame with proper column types
    """
    if df is None or len(df) == 0:
        return pd.DataFrame()

    df = df.copy()

    # Handle MultiIndex columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Remove duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]

    # Convert OHLC columns to numeric
    for col in ["Open", "High", "Low", "Close"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].squeeze(), errors="coerce")

    # Handle volume
    if "Volume" in df.columns:
        df["Volume"] = pd.to_numeric(df["Volume"].squeeze(), errors="coerce").fillna(0)
    else:
        df["Volume"] = 1.0

    # Drop rows with missing OHLC data
    df = df.dropna(subset=["Open", "High", "Low", "Close"])

    return df


def get_cvd_data(
    df: pd.DataFrame,
    intrabar_df: Optional[pd.DataFrame] = None,
    left_pivot: int = 5,
    right_pivot: int = 5,
    max_pivot_bar_gap: int = 8,
    detect_divs: bool = True
) -> Dict[str, Any]:
    """
    Get complete CVD data including candles, divergences, and stats.

    Main entry point matching the interface expected by server.py.

    Args:
        df: Main timeframe OHLCV DataFrame
        intrabar_df: Optional lower timeframe DataFrame for intrabar analysis
        left_pivot: Left bars for pivot detection
        right_pivot: Right bars for pivot detection
        max_pivot_bar_gap: Maximum bar gap between price and CVD pivots
        detect_divs: Whether to detect divergences

    Returns:
        Dict with cvd_candles, divergences, stats, has_volume, method
    """
    # Clean dataframes
    df = clean_dataframe(df)
    if df is None or len(df) < 2:
        return {
            "cvd": [],
            "cvd_candles": [],
            "divergences": [],
            "stats": {},
            "has_volume": False,
            "method": "none",
        }

    has_volume = "Volume" in df.columns and df["Volume"].sum() > len(df)

    # Build CVD candles
    if intrabar_df is not None and len(intrabar_df) > 0:
        intrabar_df = clean_dataframe(intrabar_df)
        cvd_candles = build_cvd_ohlc_from_intrabar(df, intrabar_df)
        method = "intrabar"
    else:
        cvd_candles = build_cvd_ohlc_single_tf(df)
        method = "single_tf"

    if not cvd_candles:
        return {
            "cvd": [],
            "cvd_candles": [],
            "divergences": [],
            "stats": {},
            "has_volume": has_volume,
            "method": method,
        }

    # Build legacy cvd format (list of value/delta dicts)
    cvd_points = []
    for i, candle in enumerate(cvd_candles):
        delta = candle["close"] - candle["open"]
        cvd_points.append({
            "time": candle["time"],
            "value": candle["close"],
            "delta": round(delta, 4),
            "cvd_high": candle["high"],
            "cvd_low": candle["low"],
        })

    # Calculate stats
    closes = [c["close"] for c in cvd_candles]
    stats = {
        "min": round(min(closes), 4),
        "max": round(max(closes), 4),
        "last": round(closes[-1], 4),
        "net": round(closes[-1] - closes[0], 4),
    }

    # Detect divergences
    divergences = []
    if detect_divs and len(cvd_candles) >= (left_pivot + right_pivot + 2):
        price_highs = df["High"].values.astype(float)
        price_lows = df["Low"].values.astype(float)
        cvd_highs = np.array([c["high"] for c in cvd_candles])
        cvd_lows = np.array([c["low"] for c in cvd_candles])
        times = [c["time"] for c in cvd_candles]

        divergences = detect_divergences(
            price_highs=price_highs,
            price_lows=price_lows,
            cvd_highs=cvd_highs,
            cvd_lows=cvd_lows,
            times=times,
            left_pivot=left_pivot,
            right_pivot=right_pivot,
            max_pivot_bar_gap=max_pivot_bar_gap
        )

    return {
        "cvd": cvd_points,
        "cvd_candles": cvd_candles,
        "divergences": divergences,
        "stats": stats,
        "has_volume": has_volume,
        "method": method,
    }
