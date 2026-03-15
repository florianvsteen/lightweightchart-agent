"""
tools/calendar.py  (v2 — replaces the ForexFactory scraper)

Uses the public ForexFactory CDN JSON feed — no scraping, no API key:
  https://cdn-nfs.faireconomy.media/ff_calendar_thisweek.json

Returns the current week's events. Filters to High/Medium impact for
EUR, GBP, USD, JPY. Calls Claude Haiku for a short AI analysis per event.

Caches:
  - Calendar data  : 30 minutes
  - AI analyses    : 6 hours  (keyed by date+time+currency+event name)
"""

import os
import time
import threading
import requests
from datetime import datetime, timezone

# ── Config ─────────────────────────────────────────────────────────────────────
CALENDAR_URL  = "https://cdn-nfs.faireconomy.media/ff_calendar_thisweek.json"
CURRENCIES    = {"EUR", "GBP", "USD", "JPY"}
IMPACTS       = {"High", "Medium"}
EVENTS_TTL    = 30 * 60      # 30 min
AI_TTL        = 6 * 3600     # 6 hours

# ── Caches ─────────────────────────────────────────────────────────────────────
_events_cache: dict = {}   # { "data": [...], "at": float }
_ai_cache: dict     = {}   # { key: { "analysis": str, "at": float } }
_cache_lock = threading.Lock()


def _fetch_raw() -> list[dict]:
    """Download and return the raw JSON list from the CDN."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    }
    resp = requests.get(CALENDAR_URL, headers=headers, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _parse_event_time(event: dict) -> datetime | None:
    """
    Parse the event's date string to a UTC-aware datetime.
    FF JSON uses ISO-style strings like "2026-03-16T10:30:00" in Eastern time.
    We treat them as UTC-5 (EST) for approximate countdown display.
    """
    raw = event.get("date", "")
    if not raw:
        return None
    try:
        # FF sends naive datetimes in Eastern time (UTC-5 approx)
        from datetime import timedelta
        naive = datetime.fromisoformat(raw)
        # Convert EST → UTC by adding 5h
        return naive.replace(tzinfo=timezone.utc) + timedelta(hours=5)
    except Exception:
        return None


def _event_key(event: dict) -> str:
    return f"{event.get('date','')}|{event.get('currency','')}|{event.get('title','')}"


def _call_claude_analysis(event: dict) -> str:
    """
    Call Claude Haiku to produce a 1-2 sentence trader-focused analysis.
    Returns empty string if ANTHROPIC_API_KEY is not set or call fails.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return ""

    parts = [f"{event.get('currency','')} {event.get('title','')}"]
    if event.get("previous"):
        parts.append(f"previous: {event['previous']}")
    if event.get("forecast"):
        parts.append(f"forecast: {event['forecast']}")
    if event.get("actual"):
        parts.append(f"actual: {event['actual']}")

    prompt = (
        f"Economic event: {'; '.join(parts)}.\n\n"
        "Write a 1–2 sentence trader-focused analysis. "
        "Mention the last reading, what a beat/miss means for the currency, "
        "and any relevant context. Be concise and direct. No disclaimers."
    )

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 150,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=20,
        )
        data = resp.json()
        blocks = data.get("content", [])
        return " ".join(b["text"] for b in blocks if b.get("type") == "text").strip()
    except Exception as e:
        print(f"[calendar] Claude analysis error: {e}")
        return ""


def get_calendar(force_refresh: bool = False) -> list[dict]:
    """
    Return this week's filtered + AI-enriched calendar events.

    Each event dict has:
      date, currency, impact, title, actual, forecast, previous,
      country, analysis, event_time (ISO UTC string)
    """
    now = time.time()

    with _cache_lock:
        cached = _events_cache.get("data")
        if cached and not force_refresh and (now - _events_cache.get("at", 0)) < EVENTS_TTL:
            return cached

    # ── Fetch ──────────────────────────────────────────────────────────────
    try:
        raw_events = _fetch_raw()
    except Exception as e:
        print(f"[calendar] Fetch error: {e}")
        # Return stale cache on error rather than empty
        with _cache_lock:
            return _events_cache.get("data", [])

    # ── Filter ─────────────────────────────────────────────────────────────
    # FF JSON fields: title, country, date, impact, actual, forecast, previous
    # "country" is the currency code (USD, EUR, GBP, JPY, etc.)
    filtered = []
    for ev in raw_events:
        currency = ev.get("country", "").upper()
        impact   = ev.get("impact", "")

        if currency not in CURRENCIES:
            continue
        if impact not in IMPACTS:
            continue

        # Parse event time for countdown display
        evt_dt = _parse_event_time(ev)

        filtered.append({
            "date":       ev.get("date", ""),
            "currency":   currency,
            "impact":     impact,
            "title":      ev.get("title", ""),
            "actual":     ev.get("actual", ""),
            "forecast":   ev.get("forecast", ""),
            "previous":   ev.get("previous", ""),
            "country":    currency,
            "event_time": evt_dt.isoformat() if evt_dt else "",
            "analysis":   "",   # filled below
        })

    # Sort by event time
    filtered.sort(key=lambda e: e["date"])

    # ── AI enrichment ──────────────────────────────────────────────────────
    enriched = []
    for ev in filtered:
        key = _event_key(ev)
        with _cache_lock:
            cached_ai = _ai_cache.get(key)

        if cached_ai and (now - cached_ai["at"]) < AI_TTL:
            ev["analysis"] = cached_ai["analysis"]
        else:
            analysis = _call_claude_analysis(ev)
            ev["analysis"] = analysis
            with _cache_lock:
                _ai_cache[key] = {"analysis": analysis, "at": now}

        enriched.append(ev)

    with _cache_lock:
        _events_cache["data"] = enriched
        _events_cache["at"]   = now

    return enriched
