"""
tools/calendar.py

Fetches this week's economic calendar from the ForexFactory CDN JSON feed.
Filters to High/Medium impact events for EUR, GBP, USD, JPY.
Builds prompts here, runs them via tools.ai.ask().

Caches:
    Calendar data : 2 hours  (disk + memory, survives restarts)
    AI analyses   : 6 hours  (memory only, keyed per event)
"""

import json
import os
import time
import threading
import requests
from datetime import datetime, timezone, timedelta

from tools.ai import ask

# ── Config ───────────────────────────────────────────────────────────────────────
CALENDAR_URL  = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
CURRENCIES    = {"EUR", "GBP", "USD", "JPY"}
IMPACTS       = {"High", "Medium"}
EVENTS_TTL    = 2 * 3600    # 2 hours  (FF updates ~hourly)
AI_TTL        = 6 * 3600    # 6 hours

DISK_CACHE_PATH = os.path.join(
    os.environ.get("DATA_DIR", "data"), "calendar_cache.json"
)

# ── In-memory caches ─────────────────────────────────────────────────────────────
_events_cache: dict = {}   # { "data": [...], "at": float }
_ai_cache: dict     = {}   # { key: { "analysis": str, "at": float } }
_ai_progress: dict  = {}   # { "total": int, "done": int, "running": bool }
_cache_lock = threading.Lock()


# ── Disk cache ───────────────────────────────────────────────────────────────────
def _load_disk_cache() -> list[dict]:
    try:
        with open(DISK_CACHE_PATH) as f:
            data = json.load(f)
            return data.get("events", [])
    except Exception:
        return []


def _save_disk_cache(events: list[dict]) -> None:
    try:
        os.makedirs(os.path.dirname(DISK_CACHE_PATH), exist_ok=True)
        with open(DISK_CACHE_PATH, "w") as f:
            json.dump({"events": events, "saved_at": time.time()}, f)
    except Exception as e:
        print(f"[calendar] disk cache write error: {e}")


# ── Fetch ────────────────────────────────────────────────────────────────────────
def _fetch_raw() -> list[dict]:
    """
    Download the raw JSON from the FF CDN.
    Raises on network/parse error so callers can fall back to disk cache.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    }
    resp = requests.get(CALENDAR_URL, headers=headers, timeout=15)

    # FF returns an HTML "Request Denied" page on rate-limit — detect it
    ct = resp.headers.get("content-type", "")
    if "html" in ct or resp.text.strip().startswith("<"):
        raise ValueError("FF returned HTML (rate-limited or blocked)")

    resp.raise_for_status()
    return resp.json()


# ── Parse ────────────────────────────────────────────────────────────────────────
def _parse_event_time(raw_date: str) -> str:
    """
    FF CDN sends ISO 8601 strings with a timezone offset, e.g.:
        "2026-03-16T10:30:00-04:00"
    Parse to a UTC ISO string for the frontend.
    Returns "" on failure.
    """
    if not raw_date:
        return ""
    try:
        dt = datetime.fromisoformat(raw_date)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return ""


def _event_key(ev: dict) -> str:
    return f"{ev.get('date','')}|{ev.get('currency','')}|{ev.get('title','')}"


def _filter_events(raw: list[dict]) -> list[dict]:
    """Filter raw FF events to our currencies + impact levels."""
    results = []
    for ev in raw:
        currency = ev.get("country", "").upper()
        impact   = ev.get("impact", "")
        if currency not in CURRENCIES or impact not in IMPACTS:
            continue
        results.append({
            "date":       ev.get("date", ""),
            "currency":   currency,
            "impact":     impact,
            "title":      ev.get("title", ""),
            "actual":     ev.get("actual", ""),
            "forecast":   ev.get("forecast", ""),
            "previous":   ev.get("previous", ""),
            "event_time": _parse_event_time(ev.get("date", "")),
            "analysis":   "",
        })
    results.sort(key=lambda e: e["date"])
    return results


# ── Prompt building (stays in calendar.py) ────────────────────────────────────────
def _build_prompt(events: list[dict]) -> str:
    """
    Build the batch AI prompt for a list of events.
    All prompt logic lives here — execution is handled by tools.ai.ask().
    """
    lines = []
    for i, ev in enumerate(events, 1):
        parts = [f"{ev['currency']} {ev['title']}"]
        if ev.get("previous"):  parts.append(f"previous: {ev['previous']}")
        if ev.get("forecast"):  parts.append(f"forecast: {ev['forecast']}")
        if ev.get("actual"):    parts.append(f"actual: {ev['actual']}")
        lines.append(f"{i}. {'; '.join(parts)}")

    event_block = "\n".join(lines)

    return (
        f"You are a professional forex analyst. Analyze these {len(events)} economic events.\n\n"
        f"{event_block}\n\n"
        "For EACH event write exactly ONE sentence of trader-focused analysis.\n"
        "Rules:\n"
        "- Number each response to match the event number above\n"
        "- Include the previous reading and what a beat/miss means for the currency\n"
        "- Be specific with numbers, not vague\n"
        "- No disclaimers, no fluff\n"
        "- Do NOT start with 'A beat or miss' — vary your openers\n\n"
        "Example format:\n"
        "1. With the previous at 3.2%, a reading above forecast would strengthen USD as it signals persistent inflation.\n"
        "2. EUR employment data came in at 6.1% last time; a higher number would weigh on EUR/USD.\n\n"
        "Now analyze:"
    )


def _parse_batch_response(text: str, count: int) -> list[str]:
    """Extract numbered lines from a batch AI response into a list of strings."""
    results = [""] * count
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        for i in range(count, 0, -1):
            if line.startswith(f"{i}.") or line.startswith(f"{i})"):
                results[i - 1] = line.split(".", 1)[-1].strip().lstrip(")").strip()
                break
    return results


# ── AI enrichment ────────────────────────────────────────────────────────────────
_CHUNK_SIZE = 5   # events per AI call (keeps prompts small, works well with Ollama)


def _run_ai_analysis(events: list[dict]) -> None:
    """
    Background thread: fill in .analysis for every event that doesn't have one.
    Uses tools.ai.ask() — provider is transparent to this module.
    """
    now = time.time()

    # Separate events that need analysis
    pending = []
    for ev in events:
        key = _event_key(ev)
        with _cache_lock:
            cached = _ai_cache.get(key)
        if cached and (now - cached["at"]) < AI_TTL:
            ev["analysis"] = cached["analysis"]
        else:
            pending.append(ev)

    with _cache_lock:
        _ai_progress.update({"total": len(pending), "done": 0, "running": True})

    print(f"[calendar] AI analysis: {len(pending)} events to process")

    # Process in chunks
    for chunk_start in range(0, len(pending), _CHUNK_SIZE):
        chunk = pending[chunk_start : chunk_start + _CHUNK_SIZE]
        prompt = _build_prompt(chunk)

        response = ask(prompt, max_tokens=800, temperature=0.3)

        if response:
            analyses = _parse_batch_response(response, len(chunk))
            for ev, analysis in zip(chunk, analyses):
                ev["analysis"] = analysis
                with _cache_lock:
                    _ai_cache[_event_key(ev)] = {"analysis": analysis, "at": now}
        else:
            print(f"[calendar] AI returned empty for chunk starting at {chunk_start}")

        with _cache_lock:
            _ai_progress["done"] += len(chunk)

    with _cache_lock:
        _ai_progress["running"] = False

    # Persist updated events to disk cache
    with _cache_lock:
        all_events = _events_cache.get("data", [])
    _save_disk_cache(all_events)

    print("[calendar] AI analysis complete")


# ── Public API ────────────────────────────────────────────────────────────────────
def get_calendar(force_refresh: bool = False) -> list[dict]:
    """
    Return this week's filtered calendar events.

    AI analysis runs in a background thread — this call returns immediately
    with whatever analyses are already cached. The frontend polls /api/calendar
    to pick up analyses as they complete.

    Each event dict:
        date, currency, impact, title, actual, forecast, previous,
        event_time (ISO UTC str), analysis (str, may be empty initially)
    """
    now = time.time()

    # 1. Return memory cache if fresh
    with _cache_lock:
        cached = _events_cache.get("data")
        cache_age = now - _events_cache.get("at", 0)
        if cached and not force_refresh and cache_age < EVENTS_TTL:
            return cached

    # 2. Try fetching fresh data
    try:
        raw = _fetch_raw()
        events = _filter_events(raw)
        print(f"[calendar] fetched {len(events)} events from FF CDN")
    except Exception as e:
        print(f"[calendar] fetch failed: {e} — falling back to cache")
        # Try memory cache first, then disk
        with _cache_lock:
            if _events_cache.get("data"):
                return _events_cache["data"]
        disk = _load_disk_cache()
        if disk:
            return disk
        return []

    # 3. Restore AI analyses from memory cache where available
    for ev in events:
        key = _event_key(ev)
        with _cache_lock:
            cached_ai = _ai_cache.get(key)
        if cached_ai and (now - cached_ai["at"]) < AI_TTL:
            ev["analysis"] = cached_ai["analysis"]

    # 4. Save to memory cache
    with _cache_lock:
        _events_cache["data"] = events
        _events_cache["at"]   = now

    # 5. Kick off background AI analysis for anything missing
    with _cache_lock:
        already_running = _ai_progress.get("running", False)

    needs_analysis = any(not ev["analysis"] for ev in events)
    if needs_analysis and not already_running:
        t = threading.Thread(
            target=_run_ai_analysis,
            args=(events,),
            daemon=True,
        )
        t.start()

    return events


def get_ai_progress() -> dict:
    """
    Return current AI analysis progress for the frontend progress indicator.
    { "total": int, "done": int, "running": bool }
    """
    with _cache_lock:
        return dict(_ai_progress)
