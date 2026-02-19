"""
detectors/supply_demand.py

Detects Supply and Demand zones.

A zone forms when:
  1. INDECISION CANDLE — wicks are larger than the body (doji-like).
     The candle shows the market is undecided — buyers and sellers in balance.

  2. IMPULSE CANDLE — the very next candle makes a significant move compared
     to the average candle size on the chart. This confirms institutional intent.

Supply zone  = indecision candle followed by a strong bearish impulse (price falls)
Demand zone  = indecision candle followed by a strong bullish impulse (price rises)

Designed to run on 15m or 30m timeframes.

Returns a list of zones (most recent first), each with:
  type       "demand" | "supply"
  status     "active" | "tested" | "broken"
  start      unix timestamp of the indecision candle
  end        unix timestamp (extends to current candle if active)
  top        upper boundary of the zone (indecision candle high)
  bottom     lower boundary of the zone (indecision candle low)
  is_active  True if price has not yet returned into the zone
"""

import numpy as np
import pandas as pd


def _scalar(val) -> float:
    if hasattr(val, 'item'):
        return val.item()
    if hasattr(val, 'iloc'):
        return float(val.iloc[0])
    return float(val)


def _is_indecision(o, h, l, c, min_wick_ratio: float = 0.6) -> bool:
    """
    True if the candle has wicks larger than its body.
    min_wick_ratio: total wick / total range must exceed this.
    e.g. 0.6 means at least 60% of the candle is wicks.
    """
    body = abs(c - o)
    total_range = h - l
    if total_range == 0:
        return False
    wick = total_range - body
    return (wick / total_range) >= min_wick_ratio


def detect(
    df,
    impulse_multiplier: float = 1.8,
    wick_ratio: float = 0.6,
    max_zones: int = 5,
    timeframe: str = "15m",   # informational only, actual fetch is handled by server
) -> list | None:
    """
    Args:
        impulse_multiplier: How much larger the impulse candle must be vs avg candle size.
                            1.8 = impulse must be 1.8× the average candle range.
        wick_ratio:         Minimum fraction of candle that must be wicks for indecision.
        max_zones:          Max number of zones to return (most recent first).
    """
    try:
        if len(df) < 10:
            return None

        # Flatten MultiIndex
        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = df.columns.get_level_values(0)
        df = df.loc[:, ~df.columns.duplicated()].copy()
        for col in ['Open', 'High', 'Low', 'Close']:
            df[col] = pd.to_numeric(df[col].squeeze(), errors='coerce')
        df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])

        opens  = df['Open'].values.flatten().astype(float)
        highs  = df['High'].values.flatten().astype(float)
        lows   = df['Low'].values.flatten().astype(float)
        closes = df['Close'].values.flatten().astype(float)

        # Average candle range across the whole chart
        avg_range = float(np.mean(highs - lows))

        last_close = closes[-1]
        zones = []

        # Scan for indecision + impulse pairs (skip last candle, need i+1)
        for i in range(len(df) - 2, 0, -1):
            o, h, l, c = opens[i], highs[i], lows[i], closes[i]

            # Step 1: indecision candle
            if not _is_indecision(o, h, l, c, wick_ratio):
                continue

            # Step 2: impulse candle immediately after
            ni_range = highs[i + 1] - lows[i + 1]
            if ni_range < avg_range * impulse_multiplier:
                continue

            # Determine zone type from impulse direction
            impulse_bullish = closes[i + 1] > opens[i + 1]
            zone_type = "demand" if impulse_bullish else "supply"

            # Zone boundaries = indecision candle high/low
            top    = h
            bottom = l

            # Determine current status
            # Active   = price has not yet returned into the zone
            # Tested   = price has touched but not closed through
            # Broken   = price has closed through the zone
            if zone_type == "demand":
                broken  = last_close < bottom
                tested  = (not broken) and (last_close <= top)
                active  = last_close > top
            else:  # supply
                broken  = last_close > top
                tested  = (not broken) and (last_close >= bottom)
                active  = last_close < bottom

            if broken:
                status = "broken"
            elif tested:
                status = "tested"
            else:
                status = "active"

            end_i = len(df) - 1  # zone extends to current candle

            zones.append({
                "detector":  "supply_demand",
                "type":      zone_type,
                "status":    status,
                "is_active": status == "active",
                "start":     int(df.index[i].timestamp()),
                "end":       int(df.index[end_i].timestamp()),
                "top":       float(top),
                "bottom":    float(bottom),
            })

            if len(zones) >= max_zones:
                break

        return zones if zones else None

    except Exception as e:
        print(f"[supply_demand] Detection error: {e}")
        return None
