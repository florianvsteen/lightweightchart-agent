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

Session logic lives in sessions.py.

NOTE: _get_bias() downloads are wrapped in the caller's _YF_LOCK via
      the `yf_lock` parameter to avoid concurrent download collisions.
"""

import numpy as np
import pandas as pd
import threading
from datetime import datetime, timezone
from providers import get_bias_df as _provider_get_bias_df
from sessions import candle_session_or_pre, in_session, FOREX

# Backward-compat aliases used by debug.html server routes
SESSION_WINDOWS = {
    "asian":    (1,  7),
    "london":   (8,  12),
    "new_york": (13, 19),
}

def _candle_session_or_pre(ts: int, market_timing: str = FOREX) -> str | None:
    return candle_session_or_pre(ts, market_timing)

def _in_session(ts: int, valid_sessions: list, market_timing: str = FOREX) -> bool:
    return in_session(ts, valid_sessions, market_timing)


def _is_indecision(o, h, l, c, min_wick_ratio: float = 0.6) -> bool:
    """Wicks must make up at least min_wick_ratio of the total candle range."""
    body = abs(c - o)
    total_range = h - l
    if total_range == 0:
        return False
    return (total_range - body) / total_range >= min_wick_ratio


def _get_bias(ticker: str, yf_lock=None) -> dict:
    """
    Fetch previous completed daily and weekly candles via the active data provider.
    yf_lock param retained for backward compatibility but no longer used.
    """
    try:
        df_d = _provider_get_bias_df(ticker, "5d", "1d").dropna()
        df_w = _provider_get_bias_df(ticker, "3mo", "1wk").dropna()

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
    market_timing: str = FOREX,
    yf_lock: threading.Lock = None,
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
            if not _in_session(candle_ts, valid_sessions, market_timing):
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

            # Replace your mitigation block temporarily
            zone_mitigated = False
            for j in range(i + 2, len(df) - 1):
                body_top    = max(opens[j], closes[j])
                body_bottom = min(opens[j], closes[j])
            
                if zone_type == "demand":
                    if body_bottom <= bottom:
                        zone_mitigated = True
                        break
                else:  # supply
                    if body_top >= top:
                        print(f"[MITIGATED] supply zone {bottom:.5f}-{top:.5f} | candle[{j}] body_top={body_top:.5f} >= top={top:.5f}")
                        zone_mitigated = True
                        break
            
            if not zone_mitigated:
                print(f"[SURVIVED] {zone_type} zone {bottom:.5f}-{top:.5f} | checking {len(df)-1 - (i+2)} candles after impulse")
                # Print the last few candle bodies for inspection
                for j in range(max(i+2, len(df)-6), len(df)-1):
                    print(f"  candle[{j}] body: {min(opens[j], closes[j]):.5f} - {max(opens[j], closes[j]):.5f}")

            status = "active"

            zones.append({
                "type":      zone_type,
                "status":    status,
                "session":   _candle_session_or_pre(candle_ts, market_timing),
                "is_active": status == "active",
                "start":     candle_ts,
                "end":       int(df.index[-1].timestamp()),
                "top":       float(top),
                "bottom":    float(bottom),
            })

            if len(zones) >= max_zones:
                break

        result["zones"] = zones  # active zones only
        return result

    except Exception as e:
        print(f"[supply_demand] Detection error: {e}")
        return {"detector": "supply_demand", "bias": {"bias": "misaligned", "reason": str(e)}, "zones": []}
