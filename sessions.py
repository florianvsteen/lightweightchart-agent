"""
sessions.py — Central market session definitions.

All session logic for every detector and server component should import from here.
Adding a new market type or adjusting session hours only needs to happen in this file.

Market types (set via config.py "market_timing" key per pair):
  FOREX  — Forex pairs (EUR/USD, GBP/USD etc.)
           Sessions: Asian 01-07, London 08-12, New York 13-19 UTC
           Weekend halt: Fri 23:00 – Sun 22:00 UTC

  NYSE   — US equity indices and stocks (US30, US100, XAUUSD)
           Sessions: London 08-12 UTC, New York 14:30-21:00 UTC
           Note: NYSE opens 14:30 UTC (09:30 ET), not 13:00
           Weekend halt: Fri 23:00 – Sun 22:00 UTC

  CRYPTO — Crypto markets (BTC, ETH etc.)
           Sessions: same windows as FOREX but market never halts
           No weekend halt — runs 24/7
"""

from datetime import datetime, timezone

# ── Market type constants ──────────────────────────────────────────────────────
FOREX  = "FOREX"
NYSE   = "NYSE"
CRYPTO = "CRYPTO"

# ── Session definitions per market type ───────────────────────────────────────
# Each session is (start_hour_utc_inclusive, end_hour_utc_exclusive)
# Pre-session window (for supply/demand indecision candle): 1 hour before open

SESSIONS = {
    FOREX: {
        "asian":    (1,  7),
        "london":   (8,  12),
        "new_york": (13, 19),
    },
    NYSE: {
        "london":   (8,    12),
        "new_york": (14,   21),   # NYSE opens 14:30 UTC; we use 14 to catch pre-open
    },
    CRYPTO: {
        "asian":    (1,  7),
        "london":   (8,  12),
        "new_york": (13, 19),
    },
}

# ── Weekend halt windows ───────────────────────────────────────────────────────
# CRYPTO never halts. FOREX and NYSE halt over the weekend.
# Format: list of (day_of_week, hour_start) → (day_of_week, hour_end)
# day_of_week: 0=Mon … 4=Fri, 5=Sat, 6=Sun

def is_weekend_halt(market_timing: str = FOREX) -> bool:
    """
    Return True if the market is currently in its weekend halt window.
    CRYPTO never halts. FOREX and NYSE halt Fri 23:00 – Sun 22:00 UTC.
    """
    if market_timing == CRYPTO:
        return False

    now = datetime.now(timezone.utc)
    dow  = now.weekday()   # 0=Mon … 6=Sun
    hour = now.hour

    if dow == 4 and hour >= 23:   # Friday ≥ 23:00
        return True
    if dow == 5:                   # All of Saturday
        return True
    if dow == 6 and hour < 22:    # Sunday before 22:00
        return True
    return False


def get_current_session(market_timing: str = FOREX) -> str | None:
    """
    Return the name of the currently active session, or None if out of session.
    Returns None during weekend halt.
    """
    if is_weekend_halt(market_timing):
        return None

    hour = datetime.now(timezone.utc).hour
    windows = SESSIONS.get(market_timing, SESSIONS[FOREX])

    # Check in priority order: new_york → london → asian
    for name in ("new_york", "london", "asian"):
        if name not in windows:
            continue
        start, end = windows[name]
        if start <= hour < end:
            return name
    return None


def candle_session_or_pre(ts: int, market_timing: str = FOREX) -> str | None:
    """
    Return session name if a candle timestamp falls within a session OR
    one hour before session open (pre-session window for S/D indecision candles).
    Returns None if outside all windows.
    """
    hour = datetime.fromtimestamp(ts, tz=timezone.utc).hour
    windows = SESSIONS.get(market_timing, SESSIONS[FOREX])

    for name, (start, end) in windows.items():
        pre = max(0, start - 1)
        if pre <= hour < end:
            return name
    return None


def in_session(ts: int, valid_sessions: list, market_timing: str = FOREX) -> bool:
    """
    Return True if timestamp falls within one of the valid sessions
    (or one candle before session open).
    """
    return candle_session_or_pre(ts, market_timing) in valid_sessions


def session_range_key(market_timing: str = FOREX) -> str | None:
    """
    Return the detector_params key for the current session's range override,
    e.g. 'new_york_range_pct'. Returns None if out of session.
    """
    session = get_current_session(market_timing)
    return f"{session}_range_pct" if session else None


def is_always_open(market_timing: str) -> bool:
    """Return True if this market type never has a weekend halt."""
    return market_timing == CRYPTO
