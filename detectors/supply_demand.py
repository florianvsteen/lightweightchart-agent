"""
detectors/supply_demand.py

Detects Supply and Demand zones with directional bias filtering.

BIAS CHECK (runs first):
  - Fetches previous daily candle and previous weekly candle via yfinance.
  - Both must be bullish OR both must be bearish to proceed.
  - Misaligned = no zone detection.
  - Bullish bias  → only DEMAND zones returned.
  - Bearish bias  → only SUPPLY zones returned.

ZONE DETECTION:
  1. INDECISION CANDLE — wicks > body
  2. IMPULSE CANDLE    — next candle significantly larger than avg
  - Only zones created during valid_sessions are kept.
  - Zones up to max_age_days old are returned.

Session windows (UTC):
  Asian:    01:00 – 07:00 UTC
  London:   08:00 – 12:00 UTC
  New York: 14:00 – 19:00 UTC
"""

import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone, timedelta


SESSION_WINDOWS = {
    "asian":    (1,  7),
    "london":   (8,  12),
    "new_york": (14, 19),
}


def _candle_session(ts: int) -> str | None:
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


def _get_bias(ticker: str) -> dict:
    """
    Fetch previous completed daily and weekly candles for the ticker.
    Returns a dict with:
      daily_bias:   "bullish" | "bearish"
      weekly_bias:  "bullish" | "bearish"
      bias:         "bullish" | "bearish" | "misaligned"
      daily_open, daily_close, weekly_open, weekly_close
    """
    try:
        # Fetch last 5 daily candles — use iloc[-2] for previous completed day
        df_d = yf.download(ticker, period="5d", interval="1d", progress=False)
        if isinstance(df_d.columns, pd.MultiIndex):
            df_d.columns = df_d.columns.get_level_values(0)
        df_d = df_d.dropna()

        # Fetch last 3 weekly candles — use iloc[-2] for previous completed week
        df_w = yf.download(ticker, period="3mo", interval="1wk", progress=False)
        if isinstance(df_w.columns, pd.MultiIndex):
            df_w.columns = df_w.columns.get_level_values(0)
        df_w = df_w.dropna()

        if len(df_d) < 2 or len(df_w) < 2:
            return {"bias": "misaligned", "reason": "insufficient data"}

        d_open  = float(df_d['Open'].iloc[-2])
        d_close = float(df_d['Close'].iloc[-2])
        w_open  = float(df_w['Open'].iloc[-2])
        w_close = float(df_w['Close'].iloc[-2])

        daily_bias  = "bullish" if d_close > d_open else "bearish"
        weekly_bias = "bullish" if w_close > w_open else "bearish"

        if daily_bias == weekly_bias:
            bias = daily_bias
        else:
            bias = "misaligned"

        return {
            "bias":         bias,
            "daily_bias":   daily_bias,
            "weekly_bias":  weekly_bias,
            "daily_open":   d_open,
            "daily_close":  d_close,
            "weekly_open":  w_open,
            "weekly_close": w_close,
        }

    except Exception as e:
        print(f"[supply_demand] Bias fetch error: {e}")
        return {"bias": "misaligned", "reason": str(e)}


def detect(
    df,
    ticker: str = None,
    impulse_multiplier: float = 1.8,
    wick_ratio: float = 0.6,
    max_zones: int = 5,
    max_age_days: int = 3,
    valid_sessions: list = None,
) -> dict | None:
    """
    Returns a dict with:
      bias:   bias info dict (always present)
      zones:  list of zone dicts (empty if misaligned or none found)
    """
    try:
        if valid_sessions is None:
            valid_sessions = list(SESSION_WINDOWS.keys())

        # Always run bias check first
        bias_info = _get_bias(ticker) if ticker else {"bias": "misaligned", "reason": "no ticker"}

        result = {
            "detector": "supply_demand",
            "bias":     bias_info,
            "zones":    [],
        }

        # Stop here if bias is misaligned
        if bias_info["bias"] == "misaligned":
            return result

        # Determine which zone type to look for based on bias
        look_for = "demand" if bias_info["bias"] == "bullish" else "supply"

        if len(df) < 10:
            return result

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

            if candle_ts < cutoff_ts:
                break

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

            # Only keep zones matching the bias direction
            if zone_type != look_for:
                continue

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

            status = "broken" if broken else ("tested" if tested else "active")

            zones.append({
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

        result["zones"] = zones
        return result

    except Exception as e:
        print(f"[supply_demand] Detection error: {e}")
        return None
