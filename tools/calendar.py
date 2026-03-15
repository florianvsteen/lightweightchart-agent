"""
tools/calendar.py

Fetches this week's economic calendar from the ForexFactory public CDN:
  https://nfs.faireconomy.media/ff_calendar_thisweek.json

- No API key required
- FF only updates the file once per hour — we cache for 2 hours
- Falls back to stale cache on any error (rate limit, network, etc.)
- Filters to High/Medium impact events for EUR, GBP, USD, JPY only
- Calls Claude Haiku for a short AI analysis per event (cached 6 hours)

FF JSON fields:
  title, country, date (ISO 8601 with TZ offset), impact, actual, forecast, previous

impact values: "High", "Medium", "Low", "Non-Economic", "Holiday"
"""

import os
import time
import threading
import requests
from datetime import datetime, timezone

# ── Config ─────────────────────────────────────────────────────────────────────
CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
CURRENCIES   = {"EUR", "GBP", "USD", "JPY"}
IMPACTS      = {"High", "Medium"}
EVENTS_TTL   = 2 * 60 * 60   # 2 hours — FF updates once/hour, no need to poll faster
AI_TTL       = 6 * 60 * 60   # 6 hours

# ── Caches ─────────────────────────────────────────────────────────────────────
_events_cache: dict = {}   # { "data": [...], "at": float }
_ai_cache: dict     = {}   # { key: { "analysis": str, "at": float } }
_cache_lock = threading.Lock()


# ── Fetch ───────────────────────────────────────────────────────────────────────
def _fetch_raw() -> list:
    """
    Download the FF CDN JSON feed.
    Raises ValueError if FF returns an HTML rate-limit page instead of JSON.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
        "Cache-Control": "max-age=3600",
    }
    resp = requests.get(CALENDAR_URL, headers=headers, timeout=15)
    resp.raise_for_status()

    # FF returns an HTML "Request Denied" page when rate-limited
    ct = resp.headers.get("content-type", "")
    if "html" in ct.lower():
        raise ValueError("Rate limited — FF returned HTML instead of JSON")

    data = resp.json()
    print(f"[calendar] Fetched {len(data)} raw events from FF CDN")
    return data


# ── Date parsing ────────────────────────────────────────────────────────────────
def _parse_date(date_str: str) -> datetime | None:
    """
    Parse FF ISO 8601 date strings like "2026-03-17T13:30:00-04:00".
    Python's datetime.fromisoformat() handles this natively in 3.7+.
    Returns a timezone-aware datetime, or None on failure.
    """
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str)
    except Exception as e:
        print(f"[calendar] Could not parse date: {date_str!r} — {e}")
        return None


# ── AI analysis ─────────────────────────────────────────────────────────────────
def _event_key(ev: dict) -> str:
    return f"{ev.get('date', '')}|{ev.get('currency', '')}|{ev.get('title', '')}"


def _call_claude_analysis(ev: dict) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print("[calendar] ANTHROPIC_API_KEY not set — skipping AI analysis")
        return ""

    parts = [f"{ev.get('currency', '')} {ev.get('title', '')}"]
    if ev.get("previous"):
        parts.append(f"previous: {ev['previous']}")
    if ev.get("forecast"):
        parts.append(f"forecast: {ev['forecast']}")
    if ev.get("actual"):
        parts.append(f"actual: {ev['actual']}")

    prompt = (
        f"Economic event: {'; '.join(parts)}.\n\n"
        "Write 1–2 sentences, trader-focused: mention the last reading, "
        "what a beat/miss means for the currency, and any relevant context. "
        "Be concise and direct. No disclaimers."
    )

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         api_key,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":    "claude-haiku-4-5",
                "max_tokens": 150,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=20,
        )
        print(f"[calendar] Claude API status: {resp.status_code}")
        if resp.status_code != 200:
          print(f"[calendar] Claude API error: {resp.text}")
        blocks = resp.json().get("content", [])
        return " ".join(b["text"] for b in blocks if b.get("type") == "text").strip()
    except Exception as e:
        print(f"[calendar] Claude error: {type(e).__name__}: {e}")
        return ""


# ── Public API ──────────────────────────────────────────────────────────────────
def get_calendar(force_refresh: bool = False) -> list[dict]:
    """
    Return this week's filtered calendar events with AI analysis.

    Each event dict:
      title, currency, impact, date (raw FF string),
      actual, forecast, previous,
      event_time (ISO UTC string for JS),
      analysis (Claude AI text, may be empty)

    On any fetch error, returns the last cached data so the UI
    doesn't go blank just because FF rate-limited us.
    """
    now = time.time()

    # Serve from cache if fresh enough
    with _cache_lock:
        cached = _events_cache.get("data")
        if cached and not force_refresh and (now - _events_cache.get("at", 0)) < EVENTS_TTL:
            return cached

    # ── Fetch from FF ──────────────────────────────────────────────────────
    try:
        raw = _fetch_raw()
    except Exception as e:
        print(f"[calendar] Fetch failed: {e}")
        # Return stale cache rather than empty list
        with _cache_lock:
            stale = _events_cache.get("data", [])
        print(f"[calendar] Returning {len(stale)} stale cached events")
        return stale

    # ── Filter ─────────────────────────────────────────────────────────────
    filtered = []
    for ev in raw:
        currency = (ev.get("country") or "").strip().upper()
        impact   = (ev.get("impact")  or "").strip()

        if currency not in CURRENCIES:
            continue
        if impact not in IMPACTS:
            continue

        raw_date = ev.get("date", "")
        evt_dt   = _parse_date(raw_date)

        # Convert to UTC ISO string for the frontend
        event_time_utc = ""
        if evt_dt is not None:
            try:
                event_time_utc = evt_dt.astimezone(timezone.utc).isoformat()
            except Exception:
                event_time_utc = evt_dt.isoformat()

        filtered.append({
            "date":       raw_date,
            "currency":   currency,
            "impact":     impact,
            "title":      (ev.get("title")    or "").strip(),
            "actual":     (ev.get("actual")   or "").strip(),
            "forecast":   (ev.get("forecast") or "").strip(),
            "previous":   (ev.get("previous") or "").strip(),
            "event_time": event_time_utc,
            "analysis":   "",
        })

    # Sort chronologically
    filtered.sort(key=lambda e: e["event_time"] or e["date"])

    print(f"[calendar] {len(filtered)} events after filtering ({len(raw)} raw)")

    # ── AI enrichment ──────────────────────────────────────────────────────
    enriched = []
    for ev in filtered:
        key = _event_key(ev)

        with _cache_lock:
            cached_ai = _ai_cache.get(key)

        if cached_ai and (now - cached_ai["at"]) < AI_TTL:
            ev["analysis"] = cached_ai["analysis"]
        else:
            ev["analysis"] = _call_claude_analysis(ev)
            with _cache_lock:
                _ai_cache[key] = {"analysis": ev["analysis"], "at": now}

        enriched.append(ev)

    # ── Store in cache ─────────────────────────────────────────────────────
    with _cache_lock:
        _events_cache["data"] = enriched
        _events_cache["at"]   = now

    return enriched
