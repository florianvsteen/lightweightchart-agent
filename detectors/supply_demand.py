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
  1. INDECISION CANDLE — wicks > body (wick ratio check)
  2. IMPULSE CANDLE    — body of next candle must be significantly larger
                         than average candle BODY size (wicks excluded).
  - Indecision candle may be the last candle before session open (so the
    first candle of the session can be the impulse).
  - Only zones created during valid_sessions are kept.
  - Zones up to max_age_days old are returned.

Session windows (UTC):
  Asian:    01:00 – 07:00 UTC
  London:   08:00 – 12:00 UTC
  New York: 13:00 – 19:00 UTC

NOTE: _get_bias() downloads are wrapped in the caller's _YF_LOCK via
      the `yf_lock` parameter to avoid concurrent download collisions.
"""

import numpy as np
import pandas as pd
import yfinance as yf
import threading
from datetime import datetime, timezone


SESSION_WINDOWS = {
    "asian":    (1,  7),
    "london":   (8,  12),
    "new_york": (13, 19),
}

# One candle before session open is also valid as the indecision candle
# so the first session candle can be the impulse.
# This maps session name -> (allowed_start_hour_inclusive)
# i.e. indecision candle can start from (session_start - 1) UTC
SESSION_PRE_OPEN = {
    "asian":    0,   # 00:00 UTC
    "london":   7,   # 07:00 UTC
    "new_york": 13,  # 13:00 UTC
}


def _candle_session_or_pre(ts: int) -> str | None:
    """
    Return session name if candle falls within session OR one hour before session open.
    This allows the pre-open candle to be the indecision candle.
    """
    hour = datetime.fromtimestamp(ts, tz=timezone.utc).hour
    for name, (start, end) in SESSION_WINDOWS.items():
        pre = SESSION_PRE_OPEN[name]
        if pre <= hour < end:
            return name
    return None


def _in_session(ts: int, valid_sessions: list) -> bool:
    """True if timestamp falls within a valid session (or one candle before open)."""
    session = _candle_session_or_pre(ts)
    return session in valid_sessions


def _is_indecision(o, h, l, c, min_wick_ratio: float = 0.6) -> bool:
    """Wicks must make up at least min_wick_ratio of the total candle range."""
    body = abs(c - o)
    total_range = h - l
    if total_range == 0:
        return False
    return (total_range - body) / total_range >= min_wick_ratio


def _get_bias(ticker: str, yf_lock: threading.Lock = None) -> dict:
    """
    Fetch previous completed daily and weekly candles.
    Uses yf_lock if provided to serialize yfinance downloads.
    """
    try:
        def _dl(t, period, interval):
            if yf_lock:
                with yf_lock:
                    return yf.download(t, period=period, interval=interval, progress=False)
            return yf.download(t, period=period, interval=interval, progress=False)

        df_d = _dl(ticker, "5d", "1d")
        if isinstance(df_d.columns, pd.MultiIndex):
            df_d.columns = df_d.columns.get_level_values(0)
        df_d = df_d.dropna()

        df_w = _dl(ticker, "3mo", "1wk")
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
        bias        = daily_bias if daily_bias == weekly_bias else "misaligned"

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
    yf_lock: threading.Lock = None,  # passed from server to serialize downloads
) -> dict:
    """
    Returns a dict with:
      bias:   bias info dict (always present)
      zones:  list of zone dicts (empty if misaligned or none found)
    """
    try:
        if valid_sessions is None:
            valid_sessions = list(SESSION_WINDOWS.keys())

        bias_info = _get_bias(ticker, yf_lock) if ticker else {"bias": "misaligned", "reason": "no ticker"}

        result = {
            "detector": "supply_demand",
            "bias":     bias_info,
            "zones":    [],
        }

        if bias_info["bias"] == "misaligned":
            return result

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

        # Average BODY size across all candles (wicks excluded)
        bodies    = np.abs(closes - opens)
        avg_body  = float(np.mean(bodies))

        # Last fully closed candle for removal checks
        last_closed_close = closes[-2]
        now_ts     = datetime.now(timezone.utc).timestamp()
        cutoff_ts  = now_ts - (max_age_days * 86400)

        zones = []

        # len(df)-1 = currently forming candle (not closed yet, skip)
        # len(df)-2 = last closed candle (can be impulse)
        # len(df)-3 = candle before that (can be indecision, with i+1 being the closed impulse)
        # So we scan indecision candidates up to len(df)-3 so impulse at i+1 is always closed.
        for i in range(len(df) - 3, 0, -1):
            candle_ts = int(df.index[i].timestamp())

            if candle_ts < cutoff_ts:
                break

            # Indecision candle must be in session or one candle before session open
            if not _in_session(candle_ts, valid_sessions):
                continue

            o, h, l, c = opens[i], highs[i], lows[i], closes[i]

            if not _is_indecision(o, h, l, c, wick_ratio):
                continue

            # Impulse check 1: body must be larger than avg body * multiplier
            impulse_body  = abs(closes[i + 1] - opens[i + 1])
            if impulse_body < avg_body * impulse_multiplier:
                continue

            # Impulse check 2: body must be >= 60% of total candle range (max 30% wicks)
            impulse_range = highs[i + 1] - lows[i + 1]
            if impulse_range > 0 and (impulse_body / impulse_range) < 0.60:
                continue

            impulse_bullish = closes[i + 1] > opens[i + 1]
            zone_type = "demand" if impulse_bullish else "supply"

            if zone_type != look_for:
                continue

            top    = h
            bottom = l

            if zone_type == "demand":
                # Remove if wick has touched the zone (low at or below zone top)
                if lows[-2] <= top:
                    continue
            else:  # supply
                # Remove if wick has touched the zone (high at or above zone bottom)
                if highs[-2] >= bottom:
                    continue

            status = "active"

            zones.append({
                "type":      zone_type,
                "status":    status,
                "session":   _candle_session_or_pre(candle_ts),
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
        return {"detector": "supply_demand", "bias": {"bias": "misaligned", "reason": str(e)}, "zones": []}
