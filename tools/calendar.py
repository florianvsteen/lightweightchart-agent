"""
tools/calendar.py  (v3)

ForexFactory public CDN JSON feed — no auth, no scraping:
  https://nfs.faireconomy.media/ff_calendar_thisweek.json

Known FF JSON field names (verified from community usage):
  title, country, date, impact, actual, forecast, previous

impact values: "High", "Medium", "Low", "Non-Economic"
country values: currency codes — "USD", "EUR", "GBP", "JPY", etc.
date format: "03/17/2026 8:30am"  (Eastern Time, US)
"""

import os
import time
import threading
import requests
from datetime import datetime, timezone, timedelta

# ── Config ─────────────────────────────────────────────────────────────────────
CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
CURRENCIES   = {"EUR", "GBP", "USD", "JPY"}
IMPACTS      = {"High", "Medium"}
EVENTS_TTL = 2 * 60 * 60   # 2 hours — FF rate limits at ~2 req/5min
AI_TTL       = 6 * 3600  # 6 hours

# ── Caches ─────────────────────────────────────────────────────────────────────
_events_cache: dict = {}
_ai_cache: dict     = {}
_cache_lock = threading.Lock()

def _fetch_raw() -> list:
    """Download FF CDN JSON with caching headers to avoid rate limits."""
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

    ct = resp.headers.get("content-type", "")
    if "html" in ct.lower():
        raise ValueError(f"Got HTML instead of JSON (rate limited)")

    data = resp.json()
    print(f"[calendar] Fetched {len(data)} raw events")
    return data
 
def _parse_ff_date(date_str: str) -> datetime | None:
    """FF date is already ISO 8601 with offset e.g. '2026-03-17T13:30:00-04:00'"""
    if not date_str:
        return None
    try:
        # Python 3.7+ handles ISO 8601 with timezone offset natively
        return datetime.fromisoformat(date_str)
    except Exception as e:
        print(f"[calendar] Could not parse date: {date_str!r} — {e}")
        return None

def _event_key(ev: dict) -> str:
    return f"{ev.get('date','')}|{ev.get('country','')}|{ev.get('title','')}"


def _call_claude_analysis(ev: dict) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return ""

    parts = [f"{ev.get('country','')} {ev.get('title','')}"]
    if ev.get("previous"):
        parts.append(f"previous: {ev['previous']}")
    if ev.get("forecast"):
        parts.append(f"forecast: {ev['forecast']}")
    if ev.get("actual"):
        parts.append(f"actual: {ev['actual']}")

    prompt = (
        f"Economic event: {'; '.join(parts)}.\n\n"
        "Write 1–2 sentences, trader-focused: mention last reading, "
        "what a beat/miss means for the currency, any relevant context. "
        "Be concise. No disclaimers."
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
        blocks = resp.json().get("content", [])
        return " ".join(b["text"] for b in blocks if b.get("type") == "text").strip()
    except Exception as e:
        print(f"[calendar] Claude error: {e}")
        return ""


def get_calendar(force_refresh: bool = False) -> list[dict]:
    now = time.time()

    with _cache_lock:
        cached = _events_cache.get("data")
        if cached and not force_refresh and (now - _events_cache.get("at", 0)) < EVENTS_TTL:
            return cached

    try:
        raw = _fetch_raw()
    except Exception as e:
        print(f"[calendar] Fetch failed: {e}")
        with _cache_lock:
            return _events_cache.get("data", [])

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
        evt_dt   = _parse_ff_date(raw_date)

        filtered.append({
            "date":       raw_date,
            "currency":   currency,
            "impact":     impact,
            "title":      (ev.get("title") or "").strip(),
            "actual":     (ev.get("actual")   or "").strip(),
            "forecast":   (ev.get("forecast") or "").strip(),
            "previous":   (ev.get("previous") or "").strip(),
            "event_time": evt_dt.isoformat() if evt_dt else "",
            "analysis":   "",
        })

    # Sort chronologically
    filtered.sort(key=lambda e: e["event_time"] or e["date"])

    print(f"[calendar] {len(filtered)} events after filtering (from {len(raw)} raw)")

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

    with _cache_lock:
        _events_cache["data"] = enriched
        _events_cache["at"]   = now

    return enriched
