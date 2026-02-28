"""
detectors/accumulation.py

Detects SIDEWAYS accumulation based on DIRECTIONLESSNESS + LOW ADX.

Rules:
  - Scans the last `lookback` candles (configurable, default 100)
  - Tests windows from `min_candles` up to `lookback` size
  - SLOPE must be near flat (linear regression on closes)
  - CHOPPINESS must be high (price reverses up/down frequently)
  - ADX must be below adx_threshold (default 25) — no directional strength
  - Box boundaries use candle BODIES (open/close), not wicks

Selection priority (among all passing zones):
  1. Zones with ADX < 10 are preferred (ultra-low directional strength)
  2. Among equal ADX tiers, LOWEST SLOPE wins

Breakout validation — IMPULSIVE CANDLE:
  When price breaks OUT of the box (last closed body is above or below the
  box boundaries), the breakout is validated by checking that the breakout
  candle's body is LARGER than the average body of candles inside the window.
  This confirms the move is impulsive and not just noise. If not impulsive,
  the zone resets to "looking".

  States:
    looking   — no valid zone found
    active    — valid zone, breakout candle still inside the box
    confirmed — valid zone + impulsive breakout outside the box

Touchpoint counting (alternating):
  A touch on the top boundary must be followed by a touch on the bottom
  before the next top touch is counted, and vice versa. This ensures only
  genuine bounces between the walls are counted, not repeated tags of the
  same side. min_touchpoints is configured per-pair in config.py.

Session hours and weekend halt logic live in sessions.py.
"""

import numpy as np
import pandas as pd
from datetime import datetime, timezone
from sessions import (
    is_weekend_halt, get_current_session, FOREX
)

# Kept for backward-compat with any external code that imports these directly
SESSION_WINDOWS = {
    "asian":    (1,  7),
    "london":   (8,  12),
    "new_york": (13, 19),
}


def _slope_pct(closes: np.ndarray, avg_p: float) -> float:
    x = np.arange(len(closes), dtype=float)
    return abs(np.polyfit(x, closes, 1)[0]) / avg_p


def _choppiness(closes: np.ndarray) -> float:
    if len(closes) < 3:
        return 0.0
    diffs = np.diff(closes)
    sign_changes = np.sum(np.sign(diffs[1:]) != np.sign(diffs[:-1]))
    return sign_changes / (len(diffs) - 1)


def _is_v_shape(closes: np.ndarray) -> bool:
    n = len(closes)
    if n < 6:
        return False
    peak_i   = int(np.argmax(closes))
    trough_i = int(np.argmin(closes))
    lo = int(n * 0.40)
    hi = int(n * 0.60)
    return (lo <= peak_i <= hi) or (lo <= trough_i <= hi)


def _count_touchpoints(
    highs: np.ndarray,
    lows: np.ndarray,
    box_top: float,
    box_bottom: float,
) -> int:
    """
    Count alternating wick touches: high >= box_top or low <= box_bottom.
    Consecutive touches on the same side are ignored (must alternate top/bottom).
    """
    if box_top <= box_bottom:
        return 0
    last_side = None
    count = 0
    for i in range(len(highs)):
        if highs[i] >= box_top and last_side != 'top':
            last_side = 'top'
            count += 1
        elif lows[i] <= box_bottom and last_side != 'bottom':
            last_side = 'bottom'
            count += 1
    return count


def _get_touchpoint_indices(highs, lows, box_top, box_bottom):
    """
    Same logic as _count_touchpoints but returns [(candle_index, side), ...]
    so callers can map touches back to specific candle timestamps.
    """
    if box_top <= box_bottom:
        return []
    last_side = None
    touches = []
    for i in range(len(highs)):
        if highs[i] >= box_top and last_side != 'top':
            last_side = 'top'
            touches.append((i, 'top'))
        elif lows[i] <= box_bottom and last_side != 'bottom':
            last_side = 'bottom'
            touches.append((i, 'bottom'))
    return touches


def _adx(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> float:
    """Calculate ADX. Auto-reduces period for short windows. Returns float or None."""
    n = len(closes)
    while period > 5 and n < period * 2 + 1:
        period = max(5, period - 2)
    if n < period * 2 + 1:
        return None

    tr       = np.zeros(n)
    plus_dm  = np.zeros(n)
    minus_dm = np.zeros(n)

    for i in range(1, n):
        hl = highs[i] - lows[i]
        hc = abs(highs[i] - closes[i-1])
        lc = abs(lows[i] - closes[i-1])
        tr[i] = max(hl, hc, lc)

        up   = highs[i] - highs[i-1]
        down = lows[i-1] - lows[i]
        plus_dm[i]  = up   if (up > down and up > 0)   else 0.0
        minus_dm[i] = down if (down > up and down > 0) else 0.0

    def _smooth(arr, p):
        out = np.zeros(n)
        out[p] = arr[1:p+1].sum()
        for i in range(p+1, n):
            out[i] = out[i-1] - out[i-1] / p + arr[i]
        return out

    atr  = _smooth(tr, period)
    pDM  = _smooth(plus_dm, period)
    mDM  = _smooth(minus_dm, period)

    with np.errstate(invalid='ignore', divide='ignore'):
        pDI = np.where(atr > 0, 100 * pDM / atr, 0.0)
        mDI = np.where(atr > 0, 100 * mDM / atr, 0.0)
        dx  = np.where((pDI + mDI) > 0, 100 * np.abs(pDI - mDI) / (pDI + mDI), 0.0)

    adx_arr = np.zeros(n)
    start = 2 * period
    if start >= n:
        return None
    adx_arr[start] = dx[period:start+1].mean()
    for i in range(start+1, n):
        adx_arr[i] = (adx_arr[i-1] * (period - 1) + dx[i]) / period

    return float(adx_arr[-1])


def detect(
    df,
    lookback: int = 40,
    min_candles: int = 20,
    adx_threshold: float = 25,
    threshold_pct: float = 0.003,
    valid_sessions: list = None,
    market_timing: str = FOREX,
    always_open: bool = False,   # deprecated — use market_timing instead
    alert_cooldown_minutes: int = 15,
    min_touchpoints: int = 0,
    debug: bool = False,
) -> dict | None:
    """
    Args:
        lookback:           How far back to scan (candles). Default 100.
        min_candles:        Minimum window size for valid accumulation. Default 20.
        adx_threshold:      Maximum ADX value allowed (default 25 = no trend).
        threshold_pct:      Slope scaling factor per instrument.
        valid_sessions:         List of session names in which detection is active
                                (e.g. ["london", "new_york"]). None = all sessions.
        market_timing:          Market type — FOREX, NYSE, or CRYPTO (from sessions.py).
        always_open:            Deprecated. Use market_timing=CRYPTO instead.
        alert_cooldown_minutes: Accepted for forward-compat; enforced by server, not here.
        min_touchpoints:        Minimum number of alternating body touches required.
                                Counted as: top → bottom → top → bottom...
                                Each touch on the opposite wall increments the count.
                                Repeated touches on the same wall are ignored until
                                the opposite wall is tagged.
                                0 = disabled (default). Configured per-pair in config.py.
                                Example: 4 requires at least 2 top touches and 2 bottom
                                touches in alternating order.
    """
    try:
        if is_weekend_halt(market_timing):
            return {"detector": "accumulation", "status": "weekend", "is_active": False}

        if len(df) < min_candles + 4:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = df.columns.get_level_values(0)
        df = df.loc[:, ~df.columns.duplicated()].copy()
        for col in ['Open', 'High', 'Low', 'Close']:
            df[col] = pd.to_numeric(df[col].squeeze(), errors='coerce')
        df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])

        session = get_current_session(market_timing)
        if session is None:
            return None

        # Gate detection to configured sessions only.
        if valid_sessions and session not in valid_sessions:
            return {"detector": "accumulation", "status": "out_of_session", "is_active": False}

        CHOP_FOUND     = 0.44
        CHOP_POTENTIAL = 0.36

        # Candle layout (newest → oldest):
        #   df[-1]  forming candle          — never touched
        #   df[-2]  breakout/impulse candle — last fully closed candle
        #   df[-3…] accumulation window     — windows end at df[-2] exclusive
        #
        # A confirmed accumulation requires:
        #   1. A valid sideways window (slope/chop/adx checks pass)
        #   2. df[-2] body is OUTSIDE the box (broke up or down)
        #   3. df[-2] body range > avg body range of candles inside the window (impulsive)
        #
        # If the last candle is still inside the box → status = "active"
        # If broke out AND impulsive              → status = "confirmed"
        # Otherwise                               → status = "looking"

        breakout_idx   = len(df) - 2          # last fully closed candle
        last_accum_idx = len(df) - 3          # windows end here (inclusive)
        scan_start     = max(0, len(df) - lookback)

        # Breakout candle body
        bo_open_raw  = float(df['Open'].iloc[breakout_idx])
        bo_close_raw = float(df['Close'].iloc[breakout_idx])
        bo_high_raw  = float(df['High'].iloc[breakout_idx])
        bo_low_raw   = float(df['Low'].iloc[breakout_idx])
        bo_body_size = abs(bo_close_raw - bo_open_raw)
        last_body_high = max(bo_open_raw, bo_close_raw)
        last_body_low  = min(bo_open_raw, bo_close_raw)

        # Collect all candidate zones with their slope for best-selection
        found_candidates     = []
        potential_candidates = []

        debug_windows = [] if debug else None

        for window_size in range(min_candles, lookback + 1):
            slope_limit = (threshold_pct * 0.10) / window_size

            i = last_accum_idx - window_size + 1
            if i < 0 or i < scan_start:
                if debug:
                    debug_windows.append({"window": window_size, "skip": "out of scan range"})
                continue

            window = df.iloc[i: i + window_size]
            closes = window['Close'].values.flatten().astype(float)
            opens  = window['Open'].values.flatten().astype(float)
            highs  = window['High'].values.flatten().astype(float)
            lows   = window['Low'].values.flatten().astype(float)

            if len(closes) < window_size:
                continue

            avg_p = closes.mean()
            if avg_p == 0:
                continue

            # Box boundaries: highest wick high and lowest wick low
            h_max = float(highs.max())
            l_min = float(lows.min())

            body_highs = np.maximum(opens, closes)
            body_lows  = np.minimum(opens, closes)

            slope     = _slope_pct(closes, avg_p)
            adx_val   = _adx(highs, lows, closes)
            chop      = _choppiness(closes)
            end_i     = i + window_size - 1
            is_active = (bo_close_raw >= l_min) and (bo_close_raw <= h_max)
            broke_up   = bo_close_raw > h_max
            broke_down = bo_close_raw < l_min
            broke_out  = broke_up or broke_down
            bodies     = np.abs(closes - opens)
            avg_body   = float(bodies.mean()) if len(bodies) > 0 else 0.0
            bo_body    = abs(bo_close_raw - bo_open_raw)
            is_impulsive  = bo_body > avg_body if broke_out else False
            impulse_ratio = round(bo_body / avg_body, 2) if (broke_out and avg_body > 0) else None
            is_confirmed  = broke_out and is_impulsive

            # Touchpoints — using wicks against box boundaries
            touchpoints = _count_touchpoints(highs, lows, h_max, l_min)
            touch_ts    = [
                {"time": int(df.index[i + tidx].timestamp()), "side": side}
                for tidx, side in _get_touchpoint_indices(highs, lows, h_max, l_min)
            ]

            # Determine rejection reason
            reject = None
            if slope >= slope_limit:
                reject = f"slope {round(slope,8)} >= limit {round(slope_limit,8)}"
            elif adx_val is not None and adx_val > adx_threshold:
                reject = f"adx {round(adx_val,2)} > {adx_threshold}"
            elif chop < 0.36:
                reject = f"chop {round(chop,4)} < 0.36"
            elif min_touchpoints > 0 and touchpoints < min_touchpoints:
                reject = f"touchpoints {touchpoints} < {min_touchpoints}"

            if debug:
                debug_windows.append({
                    "window":          window_size,
                    "start_ts":        int(df.index[i].timestamp()),
                    "end_ts":          int(df.index[end_i].timestamp()),
                    "top":             round(h_max, 5),
                    "bottom":          round(l_min, 5),
                    "slope":           round(slope, 8),
                    "slope_limit":     round(slope_limit, 8),
                    "chop":            round(chop, 4),
                    "adx":             round(adx_val, 2) if adx_val is not None else None,
                    "adx_limit":       adx_threshold,
                    "avg_body":        round(avg_body, 6),
                    "touchpoints":     touchpoints,
                    "touch_ts":        touch_ts,
                    "min_touchpoints": min_touchpoints,
                    "is_active":       is_active,
                    "broke_out":       broke_out,
                    "broke_up":        broke_up,
                    "broke_down":      broke_down,
                    "is_impulsive":    is_impulsive,
                    "impulse_ratio":   impulse_ratio,
                    "is_confirmed":    is_confirmed,
                    "reject":          reject,
                    "pass":            reject is None,
                })

            # Early-exit for non-debug path
            if reject:
                continue

            zone = {
                "detector":    "accumulation",
                "session":     session,
                "start":       int(df.index[i].timestamp()),
                "end":         int(df.index[end_i].timestamp()),
                "top":         h_max,
                "bottom":      l_min,
                "is_active":   is_active,
                "slope":       round(slope, 8),
                "adx":         round(adx_val, 2) if adx_val is not None else None,
                "avg_body":    round(avg_body, 6),
                "touchpoints": touchpoints,
                "_window_start_idx": i,
            }

            if chop >= CHOP_FOUND:
                zone["status"] = "found"
                found_candidates.append(zone)
            elif chop >= CHOP_POTENTIAL:
                zone["status"] = "potential"
                potential_candidates.append(zone)

        def _rank(candidates):
            """Sort candidates: ADX<10 first, then lowest slope within each tier."""
            if not candidates:
                return []
            low_adx = sorted(
                [z for z in candidates if z["adx"] is not None and z["adx"] < 10],
                key=lambda z: z["slope"]
            )
            rest = sorted(
                [z for z in candidates if not (z["adx"] is not None and z["adx"] < 10)],
                key=lambda z: z["slope"]
            )
            return low_adx + rest

        # Primary pool: "found" zones first, then "potential"
        ranked_all = _rank(found_candidates) + _rank(potential_candidates)

        def _with_debug(result):
            if debug and debug_windows is not None:
                result["windows"]          = debug_windows
                result["windows_checked"]  = len([w for w in debug_windows if "skip" not in w])
                result["passed"]           = len([w for w in debug_windows if w.get("pass")])
                result["breakout_candle"]  = result.get("breakout_candle") or {
                    "time":  int(df.index[breakout_idx].timestamp()),
                    "open":  round(bo_open_raw, 5), "high": round(bo_high_raw, 5),
                    "low":   round(bo_low_raw, 5),  "close": round(bo_close_raw, 5),
                }
                result["candles"] = [
                    {"time": int(idx.timestamp()), "open": round(float(r["Open"]),5),
                     "high": round(float(r["High"]),5), "low": round(float(r["Low"]),5),
                     "close": round(float(r["Close"]),5)}
                    for idx, r in df.iterrows()
                ]
            return result

        # ── Determine which zones to return ───────────────────────────────
        if not ranked_all:
            return _with_debug({"detector": "accumulation", "status": "looking", "is_active": False,
                                 "best_zone": None, "secondary_zone": None})

        candidate      = ranked_all[0]
        secondary_zone = ranked_all[1] if len(ranked_all) > 1 else None

        # Active zone — breakout candle still inside the box
        if candidate["is_active"]:
            candidate.pop("_window_start_idx", None)
            candidate["status"] = "active"
            if secondary_zone:
                secondary_zone.pop("_window_start_idx", None)
            candidate["secondary_zone"] = secondary_zone
            candidate["best_zone"] = {k: v for k, v in candidate.items() if k not in ("secondary_zone", "windows", "candles")}
            return _with_debug(candidate)

        # ── Price broke out — validate as IMPULSIVE ───────────────────────
        box_top    = candidate["top"]
        box_bottom = candidate["bottom"]
        avg_body   = candidate["avg_body"]

        broke_up   = bo_close_raw > box_top
        broke_down = bo_close_raw < box_bottom

        if not broke_up and not broke_down:
            candidate.pop("_window_start_idx", None)
            candidate["is_active"] = True
            candidate["status"]    = "active"
            if secondary_zone:
                secondary_zone.pop("_window_start_idx", None)
            candidate["secondary_zone"] = secondary_zone
            candidate["best_zone"] = {k: v for k, v in candidate.items() if k not in ("secondary_zone", "windows", "candles")}
            return _with_debug(candidate)

        # Check impulse: body must be bigger than avg window body
        is_impulsive = bo_body_size > avg_body

        if not is_impulsive:
            return _with_debug({"detector": "accumulation", "status": "looking", "is_active": False,
                                 "best_zone": None, "secondary_zone": None})

        # Confirmed — impulsive breakout outside the box
        candidate.pop("_window_start_idx", None)
        candidate["is_active"]      = False
        candidate["status"]         = "confirmed"
        candidate["breakout_dir"]   = "up" if broke_up else "down"
        candidate["breakout_body"]  = round(bo_body_size, 6)
        candidate["impulse_ratio"]  = round(bo_body_size / avg_body, 2) if avg_body > 0 else None
        candidate["breakout_candle"] = {
            "time":  int(df.index[breakout_idx].timestamp()),
            "open":  round(bo_open_raw, 5), "high": round(bo_high_raw, 5),
            "low":   round(bo_low_raw, 5),  "close": round(bo_close_raw, 5),
        }
        if secondary_zone:
            secondary_zone.pop("_window_start_idx", None)
        candidate["secondary_zone"] = secondary_zone
        candidate["best_zone"] = {k: v for k, v in candidate.items() if k not in ("secondary_zone", "windows", "candles")}
        return _with_debug(candidate)

    except Exception as e:
        print(f"[accumulation] Detection error: {e}")
        return None

def explain_candle(
    df,
    ci: int,
    params: dict,
    market_timing: str = FOREX,
) -> list[str]:
    """
    Explain why candle at index `ci` is or isn't a valid accumulation aggressor.

    Slices df to ci+2 so that detect() treats candle[ci] as the "last closed"
    breakout candidate. Then narrates whatever detect() returned.
    No detection logic is duplicated here.
    """
    if ci < 0 or ci + 2 > len(df):
        return ["Candle index out of range."]

    c = df.iloc[ci]
    o, h, l, cl = float(c["Open"]), float(c["High"]), float(c["Low"]), float(c["Close"])
    body    = abs(cl - o)
    is_bull = cl >= o

    lines = []
    lines.append(
        f"{'Bullish' if is_bull else 'Bearish'} candle — "
        f"body {body:.5f}  range {h - l:.5f}"
    )

    # Slice so detect() sees candle[ci] as df[-2] (last closed candle)
    df_slice = df.iloc[: ci + 2]

    result = detect(df_slice, market_timing=market_timing, **params)

    if result is None:
        lines.append("Not enough data to evaluate.")
        return lines

    status = result.get("status")

    if status == "weekend":
        lines.append("Market is closed (weekend).")
        return lines

    if status == "out_of_session":
        lines.append(
            "Outside configured trading sessions — "
            "accumulation detection is inactive at this time."
        )
        return lines

    if status == "looking":
        lines.append(
            "No valid accumulation zone found before this candle. "
            "The scan window either had no candidate zones, or all candidates "
            "were rejected (too wide, trending, or not choppy enough)."
        )
        return lines

    # A zone was found (active, found, potential, or confirmed)
    top    = result.get("top", 0)
    bot    = result.get("bottom", 0)
    adx    = result.get("adx")
    avg_body = result.get("avg_body", 0)
    touches  = result.get("touchpoints", 0)

    lines.append(
        f"Found valid accumulation zone ({bot:.5f}–{top:.5f})"
    )
    if adx is not None:
        lines.append(
            f"  ADX {adx:.1f}  ·  avg body {avg_body:.5f}  ·  alternating touches {touches}"
        )

    if status in ("active", "found", "potential"):
        lines.append(
            "This candle is still inside the zone — hasn't broken out yet."
        )
        lines.append(
            f"  Zone avg body: {avg_body:.5f}  ·  This body: {body:.5f}"
        )
        lines.append(
            "An aggressor must close its body above the zone high (bullish) "
            "or below the zone low (bearish)."
        )
        return lines

    if status == "confirmed":
        dir_      = result.get("breakout_dir", "?")
        ratio     = result.get("impulse_ratio")
        lines.append(f"✓ Valid aggressor — broke {dir_} out of zone.")
        lines.append(
            f"  Body {body:.5f} > zone avg body {avg_body:.5f} "
            f"({ratio}× — impulsive)."
        )
        return lines

    lines.append(
        f"Broke out of zone ({bot:.5f}–{top:.5f}) but not impulsive enough."
    )
    lines.append(
        f"  Body {body:.5f}  ·  zone avg body {avg_body:.5f}  ·  need > 1.0×."
    )
    lines.append(
        "A valid aggressor must have a body larger than the average candle in the zone."
    )
    return lines
