"""
detectors/supply_demand.py

Detects Supply and Demand zones.

A zone forms when:
  1. INDECISION CANDLE — wicks are larger than the body (doji-like).
  2. IMPULSE CANDLE — the very next candle makes a significant move vs avg candle size.

Supply zone  = indecision + strong bearish impulse
Demand zone  = indecision + strong bullish impulse

Session filtering:
  - Only zones CREATED during configured sessions are returned.
  - Zones can be up to `max_age_days` old (default 3) — they persist across sessions.
  - Which sessions are valid is set via `valid_sessions` in config.

Session windows (UTC):
  Asian:    01:00 – 07:00 UTC
  London:   08:00 – 12:00 UTC
  New York: 14:00 – 19:00 UTC
"""

import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta


SESSION_WINDOWS = {
    "asian":    (1,  7),
    "london":   (8,  12),
    "new_york": (14, 19),
}


def _candle_session(ts: int) -> str | None:
    """Return the session name for a given UTC unix timestamp, or None if out of session."""
    hour = datetime.fromtimestamp(ts, tz=timezone.utc).hour
    for name, (start, end) in SESSION_WINDOWS.items():
        if start <= hour < end:
            return name
    return None


def _is_indecision(o, h, l, c, min_wick_ratio: float = 0.6) -> bool:
    body = abs(c - o)
    total_range = h - l
    if total_range == 0:
        return False
    return (total_range - body) / total_range >= min_wick_ratio


def detect(
    df,
    impulse_multiplier: float = 1.8,
    wick_ratio: float = 0.6,
    max_zones: int = 5,
    max_age_days: int = 3,
    valid_sessions: list = None,  # e.g. ["london", "new_york"]
) -> list | None:
    """
    Args:
        impulse_multiplier: Impulse candle must be this × avg candle range.
        wick_ratio:         Min fraction of candle that must be wicks.
        max_zones:          Max zones to return (most recent first).
        max_age_days:       Max age of a zone in days (default 3).
        valid_sessions:     Sessions in which a zone must have been created.
                            None = allow all sessions.
    """
    try:
        if valid_sessions is None:
            valid_sessions = list(SESSION_WINDOWS.keys())

        if len(df) < 10:
            return None

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

        avg_range  = float(np.mean(highs - lows))
        last_close = closes[-1]
        now_ts     = datetime.now(timezone.utc).timestamp()
        cutoff_ts  = now_ts - (max_age_days * 86400)

        zones = []

        for i in range(len(df) - 2, 0, -1):
            candle_ts = int(df.index[i].timestamp())

            # Skip zones older than max_age_days
            if candle_ts < cutoff_ts:
                break

            # Only keep zones created during a valid session
            candle_session = _candle_session(candle_ts)
            if candle_session not in valid_sessions:
                continue

            o, h, l, c = opens[i], highs[i], lows[i], closes[i]

            if not _is_indecision(o, h, l, c, wick_ratio):
                continue

            ni_range = highs[i + 1] - lows[i + 1]
            if ni_range < avg_range * impulse_multiplier:
                continue

            impulse_bullish = closes[i + 1] > opens[i + 1]
            zone_type = "demand" if impulse_bullish else "supply"

            top    = h
            bottom = l

            if zone_type == "demand":
                broken = last_close < bottom
                tested = (not broken) and (last_close <= top)
                active = last_close > top
            else:
                broken = last_close > top
                tested = (not broken) and (last_close >= bottom)
                active = last_close < bottom

            if broken:
                status = "broken"
            elif tested:
                status = "tested"
            else:
                status = "active"

            zones.append({
                "detector":  "supply_demand",
                "type":      zone_type,
                "status":    status,
                "session":   candle_session,
                "is_active": status == "active",
                "start":     candle_ts,
                "end":       int(df.index[-1].timestamp()),
                "top":       float(top),
                "bottom":    float(bottom),
            })

            if len(zones) >= max_zones:
                break

        return zones if zones else None

    except Exception as e:
        print(f"[supply_demand] Detection error: {e}")
        return None
