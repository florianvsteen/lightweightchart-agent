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
from typing import List, Dict, Tuple, Any, Optional
from dataclasses import dataclass
from detectors.divergence import detect_divergences


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

    # Defensive check for empty intrabar data
    if intrabar_df is None or len(intrabar_df) < 1:
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

    # Required OHLC columns
    required_cols = ["Open", "High", "Low", "Close"]

    # Check if all required columns exist
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        return pd.DataFrame()

    # Convert OHLC columns to numeric
    for col in required_cols:
        df[col] = pd.to_numeric(df[col].squeeze(), errors="coerce")

    # Handle volume
    if "Volume" in df.columns:
        df["Volume"] = pd.to_numeric(df["Volume"].squeeze(), errors="coerce").fillna(0)
    else:
        df["Volume"] = 1.0

    # Drop rows with missing OHLC data
    df = df.dropna(subset=required_cols)

    return df


def get_cvd_data(
    df: pd.DataFrame,
    intrabar_df: Optional[pd.DataFrame] = None,
    left_pivot: int = 3,  # Adjusted to 3 for better sensitivity as discussed
    right_pivot: int = 1, # Hardcoded to 1 for the requested 1-bar confirmation
    max_pivot_bar_gap: int = 8,
    detect_divs: bool = True
) -> Dict[str, Any]:
    """
    Get complete CVD data including candles, divergences, and stats.
    Uses the externalized synchronized fractal detector.
    """
    # 1. Clean dataframes
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

    has_volume = bool("Volume" in df.columns and df["Volume"].sum() > len(df))

    # 2. Build CVD candles
    intrabar_cleaned = None
    if intrabar_df is not None and len(intrabar_df) > 0:
        intrabar_cleaned = clean_dataframe(intrabar_df)
        if len(intrabar_cleaned) == 0:
            intrabar_cleaned = None

    if intrabar_cleaned is not None:
        cvd_candles = build_cvd_ohlc_from_intrabar(df, intrabar_cleaned)
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

    # 3. Build legacy cvd format for frontend compatibility
    cvd_points = []
    for candle in cvd_candles:
        delta = candle["close"] - candle["open"]
        cvd_points.append({
            "time": candle["time"],
            "value": candle["close"],
            "delta": round(delta, 4),
            "cvd_high": candle["high"],
            "cvd_low": candle["low"],
        })

    # 4. Calculate stats
    closes = [c["close"] for c in cvd_candles]
    stats = {
        "min": round(min(closes), 4),
        "max": round(max(closes), 4),
        "last": round(closes[-1], 4),
        "net": round(closes[-1] - closes[0], 4),
    }

    # 5. Detect divergences using the external synchronized fractal detector
    divergences = []
    if detect_divs and len(cvd_candles) >= (left_pivot + 2):
        price_highs = df["High"].values.astype(float)
        price_lows = df["Low"].values.astype(float)
        
        # Extract CVD Highs/Lows for the sync detector
        cvd_highs = np.array([c["high"] for c in cvd_candles])
        cvd_lows = np.array([c["low"] for c in cvd_candles])
        times = [c["time"] for c in cvd_candles]

        # Note: detect_divergences now returns (divergences, h_anchors, l_anchors)
        divergences, h_count, l_count = detect_divergences(
            price_highs=price_highs,
            price_lows=price_lows,
            ind_highs=cvd_highs,  # Changed from cvd_highs=cvd_highs
            ind_lows=cvd_lows,    # Changed from cvd_lows=cvd_lows
            times=times,
            max_width=15
        )

        # DEBUGGING OUTPUT
        print("\n--- DIVERGENCE DETECTOR DEBUG ---")
        print(f"Total Bars Processed: {len(times)}")
        print(f"Synchronized High Anchors (Price+CVD): {h_count}")
        print(f"Synchronized Low Anchors (Price+CVD):  {l_count}")
        print(f"Total Divergences Found: {len(divergences)}")
        print("---------------------------------\n")

    return {
        "cvd": cvd_points,
        "cvd_candles": cvd_candles,
        "divergences": divergences,
        "stats": stats,
        "has_volume": has_volume,
        "method": method,
    }
