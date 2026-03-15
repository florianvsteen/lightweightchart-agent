"""
tools/calendar.py

Fetches this week's economic calendar from the ForexFactory public CDN:
  https://nfs.faireconomy.media/ff_calendar_thisweek.json

- No API key required for calendar data
- FF only updates the file once per hour — we cache for 2 hours
- Persists cache to disk so restarts don't cause unnecessary FF requests
- Falls back to disk cache on rate limit / network errors
- Filters to High/Medium impact events for EUR, GBP, USD, JPY only
- Calls Google Gemini 2.0 Flash-Lite for AI analysis in a SINGLE batch
  request (1 API call for all events) — stays well within free tier limits
  Set GEMINI_API_KEY env var — free key from aistudio.google.com
"""

import os
import re
import json
import time
import threading
import requests
from datetime import datetime, timezone

# ── Config ─────────────────────────────────────────────────────────────────────
CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
GEMINI_URL   = (
    "https://generativelanguage.googleapis.com/v1beta/models"
    "/gemini-2.0-flash-lite:generateContent"
)
CURRENCIES   = {"EUR", "GBP", "USD", "JPY"}
IMPACTS      = {"High", "Medium"}
EVENTS_TTL   = 2 * 60 * 60   # 2 hours — FF updates once/hour
AI_TTL       = 6 * 60 * 60   # 6 hours

CACHE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "data", "calendar_cache.json"
)

# ── In-memory caches ───────────────────────────────────────────────────────────
_events_cache: dict = {}
_ai_cache: dict     = {}
_cache_lock = threading.Lock()


# ── Disk cache ──────────────────────────────────────────────────────────────────
def _save_cache(data: list):
    try:
        os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
        with open(CACHE_FILE, "w") as f:
            json.dump({"data": data, "at": time.time()}, f)
        print(f"[calendar] Saved {len(data)} events to disk cache")
    except Exception as e:
        print(f"[calendar] Cache save error: {e}")


def _load_cache() -> tuple[list, float]:
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "r") as f:
                saved = json.load(f)
                data = saved.get("data", [])
                at   = saved.get("at", 0)
                age  = int((time.time() - at) / 60)
                print(f"[calendar] Loaded {len(data)} events from disk cache (age: {age}m)")
                return data, at
    except Exception as e:
        print(f"[calendar] Cache load error: {e}")
    return [], 0


# ── FF fetch ────────────────────────────────────────────────────────────────────
def _fetch_raw() -> list:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept":        "application/json",
        "Cache-Control": "max-age=3600",
    }
    resp = requests.get(CALENDAR_URL, headers=headers, timeout=15)
    resp.raise_for_status()

    ct = resp.headers.get("content-type", "")
    if "html" in ct.lower():
        raise ValueError("Rate limited — FF returned HTML instead of JSON")

    data = resp.json()
    print(f"[calendar] Fetched {len(data)} raw events from FF CDN")
    return data


# ── Date parsing ────────────────────────────────────────────────────────────────
def _parse_date(date_str: str) -> datetime | None:
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str)
    except Exception as e:
        print(f"[calendar] Could not parse date: {date_str!r} — {e}")
        return None


# ── AI analysis — single batch call ────────────────────────────────────────────
def _event_key(ev: dict) -> str:
    return f"{ev.get('date', '')}|{ev.get('currency', '')}|{ev.get('title', '')}"


def _call_gemini_batch(events: list) -> dict:
    """
    Send ALL events in ONE Gemini API request.
    Returns { event_key: analysis_text } for each event.
    Uses 1 API call regardless of how many events there are.
    """
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        print("[calendar] GEMINI_API_KEY not set — skipping AI analysis")
        return {}

    if not events:
        return {}

    # Build numbered event list
    lines = []
    for i, ev in enumerate(events, 1):
        prev = ev.get("previous") or "N/A"
        fcst = ev.get("forecast") or "N/A"
        actl = ev.get("actual")
        actual_part = f" | Actual: {actl}" if actl else ""
        lines.append(
            f"{i}. {ev.get('currency', '')} {ev.get('title', '')}"
            f" — Prev: {prev} | Forecast: {fcst}{actual_part}"
        )

    prompt = (
        "You are a concise forex market analyst. "
        "For each numbered economic event below, write exactly ONE sentence "
        "explaining what a beat or miss vs forecast would mean for the currency. "
        "Do NOT repeat the event name. Be direct and specific.\n\n"
        "Respond in this exact format only:\n"
        "1. <one sentence>\n"
        "2. <one sentence>\n"
        "etc.\n\n"
        "Events:\n" + "\n".join(lines)
    )

    try:
        resp = requests.post(
            f"{GEMINI_URL}?key={api_key}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "maxOutputTokens": 1024,
                    "temperature":     0.3,
                },
            },
            timeout=30,
        )

        if resp.status_code != 200:
            print(f"[calendar] Gemini API error {resp.status_code}: {resp.text[:300]}")
            return {}

        data      = resp.json()
        candidates = data.get("candidates", [])
        if not candidates:
            return {}

        raw_text = " ".join(
            p.get("text", "")
            for p in candidates[0].get("content", {}).get("parts", [])
        ).strip()

        # Parse "1. sentence\n2. sentence" into dict
        result  = {}
        matches = re.findall(r"(\d+)\.\s+(.+?)(?=\n\d+\.|$)", raw_text, re.DOTALL)
        for num_str, text in matches:
            idx = int(num_str) - 1
            if 0 <= idx < len(events):
                result[_event_key(events[idx])] = text.strip()

        print(f"[calendar] Gemini batch: {len(result)}/{len(events)} analyses in 1 API call")
        return result

    except Exception as e:
        print(f"[calendar] Gemini batch error: {type(e).__name__}: {e}")
        return {}


# ── Public API ──────────────────────────────────────────────────────────────────
def get_calendar(force_refresh: bool = False) -> list[dict]:
    """
    Return this week's filtered calendar events with AI analysis.

    Cache priority:
      1. In-memory (fastest)
      2. Disk (survives restarts)
      3. Live FF fetch
      4. Stale fallback (if fetch fails)
    """
    now = time.time()

    # 1. In-memory cache
    with _cache_lock:
        mem_data = _events_cache.get("data")
        mem_at   = _events_cache.get("at", 0)

    if mem_data and not force_refresh and (now - mem_at) < EVENTS_TTL:
        return mem_data

    # 2. Disk cache (after restart)
    if not mem_data:
        disk_data, disk_at = _load_cache()
        if disk_data:
            with _cache_lock:
                _events_cache["data"] = disk_data
                _events_cache["at"]   = disk_at
            if not force_refresh and (now - disk_at) < EVENTS_TTL:
                print(f"[calendar] Serving {len(disk_data)} events from fresh disk cache")
                return disk_data
            mem_data = disk_data  # stale but keep as fallback

    # 3. Live fetch
    try:
        raw = _fetch_raw()
    except Exception as e:
        print(f"[calendar] Fetch failed: {e}")
        fallback = mem_data or []
        print(f"[calendar] Returning {len(fallback)} stale cached events as fallback")
        return fallback

    # Filter
    filtered = []
    for ev in raw:
        currency = (ev.get("country") or "").strip().upper()
        impact   = (ev.get("impact")  or "").strip()

        if currency not in CURRENCIES or impact not in IMPACTS:
            continue

        raw_date = ev.get("date", "")
        evt_dt   = _parse_date(raw_date)

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

    filtered.sort(key=lambda e: e["event_time"] or e["date"])
    print(f"[calendar] {len(filtered)} events after filtering ({len(raw)} raw)")

    # AI enrichment — split into cached vs needs-analysis
    cached_events   = []
    uncached_events = []

    for ev in filtered:
        key = _event_key(ev)
        with _cache_lock:
            cached_ai = _ai_cache.get(key)

        if cached_ai and (now - cached_ai["at"]) < AI_TTL:
            ev["analysis"] = cached_ai["analysis"]
            cached_events.append(ev)
        else:
            uncached_events.append(ev)

    # Single batch call for all uncached events
    if uncached_events:
        print(f"[calendar] Fetching AI analysis for {len(uncached_events)} new events (1 API call)")
        analyses = _call_gemini_batch(uncached_events)
        for ev in uncached_events:
            key          = _event_key(ev)
            ev["analysis"] = analyses.get(key, "")
            with _cache_lock:
                _ai_cache[key] = {"analysis": ev["analysis"], "at": now}

    enriched = cached_events + uncached_events
    enriched.sort(key=lambda e: e["event_time"] or e["date"])

    # Persist
    with _cache_lock:
        _events_cache["data"] = enriched
        _events_cache["at"]   = now

    _save_cache(enriched)
    return enriched
