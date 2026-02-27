"""
detectors/supply_demand.py

Detects Supply and Demand zones with directional bias filtering.

BIAS CHECK (from detectors/bias.py):
  - Fetches previous daily and weekly candles via the active data provider.
  - Bias levels: strong_bullish, bullish, bearish, strong_bearish, misaligned.
  - When aligned bullish  → demand zones only.
  - When aligned bearish  → supply zones only.
  - When MISALIGNED       → both zone types detected, but flagged as lower-confidence
                            (is_misaligned=True on each zone).

ZONE DETECTION:
  1. INDECISION CANDLE — wicks > body (wick ratio check)
  2. IMPULSE CANDLE    — body of next candle must be significantly larger
                         than average candle BODY size (wicks excluded).
  - Only zones created during valid_sessions are kept.
  - Zones up to max_age_days old are returned.

Session logic lives in sessions.py.
Bias logic lives in detectors/bias.py.
"""

import numpy as np
import pandas as pd
import threading
from datetime import datetime, timezone
from detectors.bias import get_bias as _get_bias_from_module
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
    Wrapper around detectors/bias.py for backward compatibility.
    yf_lock param retained for backward compatibility but no longer used.
    """
    return _get_bias_from_module(ticker)


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
    debug: bool = False,
) -> dict:
    """
    Returns a dict with:
      bias:   bias info dict (always present, uses new strong/weak levels)
      zones:  list of zone dicts.
              When bias is misaligned, zones are still detected but flagged
              with is_misaligned=True so the UI can warn the user.
    """
    try:
        if valid_sessions is None:
            valid_sessions = list(SESSION_WINDOWS.keys())

        bias_info = _get_bias_from_module(ticker) if ticker else {
            "bias": "misaligned", "aligned": False, "reason": "no ticker"
        }

        result = {
            "detector": "supply_demand",
            "bias":     bias_info,
            "zones":    [],
        }

        is_misaligned = bias_info["bias"] == "misaligned"

        # Determine which zone type(s) to look for
        from detectors.bias import is_bullish, is_bearish
        if not is_misaligned:
            look_for = "demand" if is_bullish(bias_info) else "supply"
        else:
            look_for = None   # misaligned → scan both directions

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

        bodies   = np.abs(closes - opens)
        avg_body = float(np.mean(bodies))

        now_ts    = datetime.now(timezone.utc).timestamp()
        cutoff_ts = now_ts - (max_age_days * 86400)

        zones = []
        candidates = []

        for i in range(len(df) - 3, 0, -1):
            candle_ts = int(df.index[i].timestamp())
            if candle_ts < cutoff_ts:
                break

            o, h, l, c = opens[i], highs[i], lows[i], closes[i]
            reject_reason = None

            if not _in_session(candle_ts, valid_sessions, market_timing):
                reject_reason = f"session '{_candle_session_or_pre(candle_ts, market_timing)}' not in {valid_sessions}"
            elif not _is_indecision(o, h, l, c, wick_ratio):
                body = abs(c - o)
                total_range = h - l
                wick_frac = round((total_range - body) / total_range, 3) if total_range else 0
                reject_reason = f"not indecision (wicks {wick_frac*100:.1f}% < {wick_ratio*100:.0f}%)"
            else:
                impulse_body  = abs(closes[i + 1] - opens[i + 1])
                impulse_range = highs[i + 1] - lows[i + 1]
                if impulse_body < avg_body * impulse_multiplier:
                    reject_reason = f"impulse body {impulse_body:.5f} < avg×{impulse_multiplier} ({avg_body*impulse_multiplier:.5f})"
                elif impulse_range > 0 and (impulse_body / impulse_range) < 0.60:
                    reject_reason = f"impulse wicks too large (body {impulse_body/impulse_range*100:.1f}% of range)"

            impulse_bullish = closes[i + 1] > opens[i + 1]
            zone_type = "demand" if impulse_bullish else "supply"

            # Direction filter — skip only when we have a clear directional bias
            if not reject_reason and look_for is not None and zone_type != look_for:
                reject_reason = f"wrong direction ({zone_type}) — bias requires {look_for}"

            if not reject_reason:
                zone_mitigated = False
                for j in range(i + 2, len(df) - 1):
                    body_top    = max(opens[j], closes[j])
                    body_bottom = min(opens[j], closes[j])
                    if zone_type == "demand" and body_bottom <= l:
                        zone_mitigated = True
                        break
                    elif zone_type == "supply" and body_top >= h:
                        zone_mitigated = True
                        break
                if zone_mitigated:
                    reject_reason = f"mitigated — body closed {'below' if zone_type == 'demand' else 'above'} zone"

            is_active = reject_reason is None

            if debug:
                candidates.append({
                    "type":          zone_type,
                    "is_active":     is_active,
                    "reject_reason": reject_reason,
                    "session":       _candle_session_or_pre(candle_ts, market_timing),
                    "start":         candle_ts,
                    "end":           int(df.index[-1].timestamp()),
                    "top":           float(h),
                    "bottom":        float(l),
                    "is_misaligned": is_misaligned,
                })

            if is_active:
                zones.append({
                    "type":          zone_type,
                    "status":        "active",
                    "session":       _candle_session_or_pre(candle_ts, market_timing),
                    "is_active":     True,
                    "is_misaligned": is_misaligned,
                    "start":         candle_ts,
                    "end":           int(df.index[-1].timestamp()),
                    "top":           float(h),
                    "bottom":        float(l),
                })
                if len(zones) >= max_zones:
                    break

        result["zones"] = zones
        if debug:
            result["candidates"] = candidates
        return result

    except Exception as e:
        print(f"[supply_demand] Detection error: {e}")
        return {"detector": "supply_demand", "bias": {"bias": "misaligned", "aligned": False, "reason": str(e)}, "zones": []}



def explain_candle(
    df,
    ci: int,
    params: dict,
    market_timing: str = FOREX,
    ticker: str = None,
) -> list[str]:
    """
    Explain why candle at index `ci` is or isn't a valid S&D zone base candle.

    Calls detect() with debug=True on a df sliced to ci+2, then finds the
    candidate entry for candle[ci] in the debug candidates list and narrates it.
    No detection logic is duplicated here.
    """
    if ci < 0 or ci + 2 > len(df):
        return ["Candle index out of range."]

    c = df.iloc[ci]
    o, h, l, cl = float(c["Open"]), float(c["High"]), float(c["Low"]), float(c["Close"])
    body        = abs(cl - o)
    total_range = h - l
    is_bull     = cl >= o

    lines = []
    lines.append(
        f"{'Bullish' if is_bull else 'Bearish'} candle — "
        f"body {body:.5f}  range {total_range:.5f}"
    )

    # Slice so detect() sees candle[ci] as the last evaluable base candidate
    df_slice = df.iloc[: ci + 2]

    result = detect(
        df_slice,
        ticker=ticker,
        market_timing=market_timing,
        debug=True,
        **params,
    )

    bias_info     = result.get("bias", {})
    bias          = bias_info.get("bias", "misaligned")
    is_misaligned = bias == "misaligned"

    if is_misaligned:
        lines.append(
            f"⚡ Bias misaligned (daily: {bias_info.get('daily_bias', '?')} / "
            f"weekly: {bias_info.get('weekly_bias', '?')}) — "
            "zone still detected but flagged lower confidence"
        )
    else:
        lines.append(f"Bias: {bias}")

    # Find this candle's entry in the debug candidates list
    candle_ts  = int(df.index[ci].timestamp())
    candidates = result.get("candidates", [])
    candidate  = next((c for c in candidates if c["start"] == candle_ts), None)

    if candidate is None:
        lines.append(
            "This candle was not evaluated — it may be outside the scan window "
            "or too close to the edge of the data."
        )
        return lines

    zone_type = candidate.get("type", "?")
    is_active = candidate.get("is_active", False)
    reason    = candidate.get("reject_reason")
    session   = candidate.get("session", "?")

    if is_active:
        lines.append(f"✓ Valid {zone_type.upper()} zone base — session '{session}'.")
        lines.append(f"  Zone: {candidate['bottom']:.5f}–{candidate['top']:.5f}")
        if candidate.get("is_misaligned"):
            lines.append("  ⚡ Flagged as lower-confidence (bias misaligned).")
    else:
        lines.append(f"Not a valid {zone_type} zone base:")
        lines.append(f"  • {reason}")

    # Always show the impulse numbers for context
    if isinstance(df_slice.columns, pd.MultiIndex):
        df_slice = df_slice.copy()
        df_slice.columns = df_slice.columns.get_level_values(0)
    bodies   = (df_slice["Close"] - df_slice["Open"]).abs()
    avg_body = float(bodies.mean())
    impulse_multiplier = params.get("impulse_multiplier", 1.8)
    lines.append(
        f"  avg body: {avg_body:.5f}  ·  this body: {body:.5f}  ·  "
        f"impulse threshold: {avg_body * impulse_multiplier:.5f}"
    )
    return lines
