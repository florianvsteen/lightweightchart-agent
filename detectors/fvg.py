"""
detectors/fvg.py

Fair Value Gap (FVG) Detector.

A Fair Value Gap is an IMBALANCE zone created when price moves so fast
that it leaves an unfilled gap between the wicks of three consecutive candles:

    Candle N-1 (before)   ←── candle preceding the impulse
    Candle N   (impulse)  ←── the strong move that creates the gap
    Candle N+1 (after)    ←── candle following the impulse

  Bullish FVG:  low[N+1]  > high[N-1]   → gap zone = (high[N-1], low[N+1])
  Bearish FVG:  high[N+1] < low[N-1]    → gap zone = (high[N+1], low[N-1])

Requirements beyond the raw gap:
  1. GAP SIZE  — must be at least `min_gap_pct` of price (default 0.01%)
                 This eliminates bid/ask spread noise.
  2. IMPULSE   — candle N body must be at least `impulse_body_pct` of its
                 full range (default 60%). Doji/spinning-top "impulses" rejected.
  3. DIRECTION — gap direction must match the impulse candle direction.
                 Bullish FVG requires a bullish N candle; bearish FVG requires bearish N.

Standalone detect() scans the last `lookback` closed candles and returns a
list of all valid FVGs, newest first.

_check_fvg() is the single-candle check used by accumulation.py — it validates
the FVG at a specific breakout index and returns the FVG zone dict or None.
"""

import numpy as np
import pandas as pd
from datetime import datetime, timezone


# ── Tuneable defaults ─────────────────────────────────────────────────────────
DEFAULT_MIN_GAP_PCT      = 0.0001   # 0.01% of price — minimum meaningful gap
DEFAULT_IMPULSE_BODY_PCT = 0.60     # impulse candle body must be ≥ 60% of its range
DEFAULT_LOOKBACK         = 80       # how many closed candles to scan


# ── Core gap check ────────────────────────────────────────────────────────────

def _check_fvg(
    df,
    candle_n_idx: int,
    min_gap_pct: float  = DEFAULT_MIN_GAP_PCT,
    impulse_body_pct: float = DEFAULT_IMPULSE_BODY_PCT,
) -> dict | None:
    """
    Check whether candle at `candle_n_idx` is the impulse leg of a valid FVG.

    Returns a dict describing the FVG zone if valid, else None.

    Dict keys:
        fvg_type    "bullish" | "bearish"
        top         upper bound of the gap zone
        bottom      lower bound of the gap zone
        gap_pct     gap size as fraction of price
        time        timestamp of the impulse candle (candle N)
        candle_n    {open, high, low, close, time}  — impulse candle
        candle_nm1  {high, low, time}               — candle N-1
        candle_np1  {high, low, time}               — candle N+1
        gap_check   dict with the raw condition strings (for debug display)
    """
    try:
        n = len(df)
        # Need candle N-1 and N+1 to both exist and be closed
        if candle_n_idx < 1 or candle_n_idx + 1 >= n:
            return None

        c_prev = df.iloc[candle_n_idx - 1]
        c_now  = df.iloc[candle_n_idx]
        c_next = df.iloc[candle_n_idx + 1]

        h_prev = float(c_prev['High'])
        l_prev = float(c_prev['Low'])
        h_next = float(c_next['High'])
        l_next = float(c_next['Low'])

        o_now  = float(c_now['Open'])
        h_now  = float(c_now['High'])
        l_now  = float(c_now['Low'])
        c_now_ = float(c_now['Close'])
        avg_p  = (h_now + l_now) / 2.0
        if avg_p == 0:
            return None

        # ── Gap condition ─────────────────────────────────────────────────
        raw_bullish = l_next > h_prev   # gap above: space between h[N-1] and l[N+1]
        raw_bearish = h_next < l_prev   # gap below: space between h[N+1] and l[N-1]

        if not raw_bullish and not raw_bearish:
            return None

        fvg_type = "bullish" if raw_bullish else "bearish"

        # ── Gap size filter ───────────────────────────────────────────────
        if fvg_type == "bullish":
            gap_size = l_next - h_prev          # positive
            gap_top, gap_bottom = l_next, h_prev
        else:
            gap_size = l_prev - h_next          # positive
            gap_top, gap_bottom = l_prev, h_next

        gap_pct = gap_size / avg_p
        if gap_pct < min_gap_pct:
            return None                         # too small — noise

        # ── Impulse direction must match gap direction ─────────────────────
        is_bull_candle = c_now_ > o_now
        is_bear_candle = c_now_ < o_now
        if fvg_type == "bullish" and not is_bull_candle:
            return None
        if fvg_type == "bearish" and not is_bear_candle:
            return None

        # ── Impulse body size ─────────────────────────────────────────────
        candle_range = h_now - l_now
        if candle_range > 0:
            body = abs(c_now_ - o_now)
            body_ratio = body / candle_range
            if body_ratio < impulse_body_pct:
                return None                     # doji / spinning top — not a true impulse

        return {
            "fvg_type":  fvg_type,
            "top":       round(gap_top,    6),
            "bottom":    round(gap_bottom, 6),
            "gap_pct":   round(gap_pct,    8),
            "time":      int(df.index[candle_n_idx].timestamp()),
            "candle_n":  {
                "open":  o_now,  "high": h_now,
                "low":   l_now,  "close": c_now_,
                "time":  int(df.index[candle_n_idx].timestamp()),
            },
            "candle_nm1": {
                "high": h_prev, "low": l_prev,
                "time": int(df.index[candle_n_idx - 1].timestamp()),
            },
            "candle_np1": {
                "high": h_next, "low": l_next,
                "time": int(df.index[candle_n_idx + 1].timestamp()),
            },
            "gap_check": {
                "condition": (
                    f"low[N+1] {l_next:.5f} > high[N-1] {h_prev:.5f}"
                    if fvg_type == "bullish" else
                    f"high[N+1] {h_next:.5f} < low[N-1] {l_prev:.5f}"
                ),
                "gap_size_pct": f"{gap_pct*100:.4f}%",
                "min_required": f"{min_gap_pct*100:.4f}%",
                "impulse_body_ratio": f"{body_ratio*100:.1f}%" if candle_range > 0 else "N/A",
            },
        }
    except Exception:
        return None


# ── Standalone scanner ────────────────────────────────────────────────────────

def detect(
    df,
    lookback:         int   = DEFAULT_LOOKBACK,
    min_gap_pct:      float = DEFAULT_MIN_GAP_PCT,
    impulse_body_pct: float = DEFAULT_IMPULSE_BODY_PCT,
) -> dict:
    """
    Scan the last `lookback` closed candles for FVG patterns.

    Returns:
        {
          "detector":   "fvg",
          "fvgs":       [ <fvg_dict>, ... ],   newest first, only valid FVGs
          "total":      int,   # candles scanned
          "found":      int,   # FVGs confirmed
          "bullish":    int,
          "bearish":    int,
        }
    """
    try:
        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = df.columns.get_level_values(0)
        df = df.loc[:, ~df.columns.duplicated()].copy()
        for col in ['Open', 'High', 'Low', 'Close']:
            df[col] = pd.to_numeric(df[col].squeeze(), errors='coerce')
        df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])

        # Scan closed candles only — stop 1 before end so N+1 is closed too
        scan_end   = len(df) - 2   # last fully closed candle that has a closed N+1
        scan_start = max(1, scan_end - lookback)

        fvgs = []
        for i in range(scan_end, scan_start, -1):
            result = _check_fvg(df, i, min_gap_pct, impulse_body_pct)
            if result:
                fvgs.append(result)

        bullish = sum(1 for f in fvgs if f["fvg_type"] == "bullish")
        bearish = sum(1 for f in fvgs if f["fvg_type"] == "bearish")

        return {
            "detector": "fvg",
            "fvgs":     fvgs,
            "total":    scan_end - scan_start,
            "found":    len(fvgs),
            "bullish":  bullish,
            "bearish":  bearish,
        }

    except Exception as e:
        print(f"[fvg] Detection error: {e}")
        return {"detector": "fvg", "fvgs": [], "total": 0, "found": 0,
                "bullish": 0, "bearish": 0}
