"""
tools/sessions.py — Central market session definitions.

All session logic for every detector and server component should import from here.
Adding a new market type or adjusting session hours only needs to happen in this file.

Market types (set via config.py "market_timing" key per pair):
  FOREX  — Forex pairs (EUR/USD, GBP/USD etc.)
           Sessions: Asian 01:00-07:00, London 08:00-12:00, New York 13:00-19:00 UTC
           Weekend halt: Fri 23:00 – Sun 22:00 UTC

  NYSE   — US equity indices and stocks (US30, US100, XAUUSD)
           Sessions: London 08:00-12:00 UTC, New York 14:30-21:00 UTC
           NYSE opens 14:30 UTC (09:30 ET)
           Weekend halt: Fri 23:00 – Sun 22:00 UTC

  CRYPTO — Crypto markets (BTC, ETH etc.)
           Sessions: same windows as FOREX but market never halts
           No weekend halt — runs 24/7

Session tuples are (start_hour, start_minute, end_hour, end_minute) in UTC.
Pre-session window for S/D indecision candles: 60 minutes before session open.
"""

from datetime import datetime, timezone

# ── Market type constants ──────────────────────────────────────────────────────
FOREX  = "FOREX"
NYSE   = "NYSE"
CRYPTO = "CRYPTO"

# ── Session definitions per market type ───────────────────────────────────────
# Each session: (start_hour, start_minute, end_hour, end_minute) UTC

SESSIONS = {
    FOREX: {
        "asian":    (1,  0,  7,  0),
        "london":   (8,  0,  12, 0),
        "new_york": (13, 0,  19, 0),
    },
    NYSE: {
        "london":   (8,  0,  12, 0),
        "new_york": (14, 30, 19, 0),
    },
    CRYPTO: {
        "asian":    (1,  0,  7,  0),
        "london":   (8,  0,  12, 0),
        "new_york": (13, 0,  19, 0),
    },
}


def _now_minutes() -> int:
    """Current UTC time as total minutes since midnight."""
    now = datetime.now(timezone.utc)
    return now.hour * 60 + now.minute


def _ts_minutes(ts: int) -> int:
    """Timestamp as total minutes since midnight UTC."""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.hour * 60 + dt.minute


def is_weekend_halt(market_timing: str = FOREX, at_time: datetime = None) -> bool:
    """
    Return True if the market is currently in its weekend halt window.
    CRYPTO never halts. FOREX and NYSE halt Fri 23:00 – Sun 22:00 UTC.
    If at_time is provided, evaluate at that time instead of now.
    """
    if market_timing == CRYPTO:
        return False
    now  = at_time if at_time is not None else datetime.now(timezone.utc)
    dow  = now.weekday()   # 0=Mon … 6=Sun
    hour = now.hour
    if dow == 4 and hour >= 23:   # Friday ≥ 23:00
        return True
    if dow == 5:                   # All of Saturday
        return True
    if dow == 6 and hour < 22:    # Sunday before 22:00
        return True
    return False


def get_current_session(market_timing: str = FOREX, at_time: datetime = None) -> str | None:
    """
    Return the name of the currently active session, or None if out of session.
    Returns None during weekend halt.
    If at_time is provided, evaluate session at that time instead of now.
    """
    if is_weekend_halt(market_timing, at_time=at_time):
        return None
    mins    = _now_minutes() if at_time is None else (at_time.hour * 60 + at_time.minute)
    windows = SESSIONS.get(market_timing, SESSIONS[FOREX])
    for name, (sh, sm, eh, em) in windows.items():
        start = sh * 60 + sm
        end   = eh * 60 + em
        if start <= mins < end:
            return name
    return None


def candle_session_or_pre(ts: int, market_timing: str = FOREX) -> str | None:
    """
    Return session name if a candle timestamp falls within a session OR
    within 60 minutes before session open (pre-session window for S/D
    indecision candles). Returns None if outside all windows.
    """
    mins    = _ts_minutes(ts)
    windows = SESSIONS.get(market_timing, SESSIONS[FOREX])

    for name, (sh, sm, eh, em) in windows.items():
        start = sh * 60 + sm
        end   = eh * 60 + em
        pre   = max(0, start - 60)   # 60 min before open
        if pre <= mins < end:
            return name
    return None


def in_session(ts: int, valid_sessions: list, market_timing: str = FOREX) -> bool:
    """
    Return True if timestamp falls within one of the valid sessions
    (or 60 minutes before session open).
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
