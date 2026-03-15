"""
tools/calendar.py

Fetches this week's economic calendar from the ForexFactory public CDN:
  https://nfs.faireconomy.media/ff_calendar_thisweek.json

Requires: GEMINI_API_KEY env var (free at aistudio.google.com)
Model: gemini-2.0-flash-lite

- Caches FF data for 2 hours (FF only updates once/hour)
- Persists cache to disk so restarts don't hit FF again
- AI analyses cached 6 hours, fetched in a single batch call
- AI enrichment runs in background thread — never blocks HTTP response
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

CURRENCIES = {"EUR", "GBP", "USD", "JPY"}
IMPACTS    = {"High", "Medium"}
EVENTS_TTL = 2 * 60 * 60   # 2 hours
AI_TTL     = 6 * 60 * 60   # 6 hours

CACHE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "data", "calendar_cache.json"
)

# ── In-memory caches ───────────────────────────────────────────────────────────
_events_cache = {}
_ai_cache     = {}
_cache_lock   = threading.Lock()


# ── Disk cache ──────────────────────────────────────────────────────────────────
def _save_cache(data):
    try:
        os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
        with open(CACHE_FILE, "w") as f:
            json.dump({"data": data, "at": time.time()}, f)
        print(f"[calendar] Saved {len(data)} events to disk cache")
    except Exception as e:
        print(f"[calendar] Cache save error: {e}")


def _load_cache():
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "r") as f:
                saved = json.load(f)
                data = saved.get("data", [])
                at   = saved.get("at", 0)
                age  = int((time.time() - at) / 60)
                print(f"[calendar] Loaded {len(data)} events from disk (age: {age}m)")
                return data, at
    except Exception as e:
        print(f"[calendar] Cache load error: {e}")
    return [], 0


# ── FF fetch ────────────────────────────────────────────────────────────────────
def _fetch_raw():
    resp = requests.get(
        CALENDAR_URL,
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        timeout=15,
    )
    resp.raise_for_status()
    if "html" in resp.headers.get("content-type", "").lower():
        raise ValueError("FF returned HTML (rate limited)")
    data = resp.json()
    print(f"[calendar] Fetched {len(data)} raw events")
    return data


# ── Date parsing ────────────────────────────────────────────────────────────────
def _parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str)
    except Exception:
        return None


# ── AI helpers ──────────────────────────────────────────────────────────────────
def _event_key(ev):
    return f"{ev.get('date', '')}|{ev.get('currency', '')}|{ev.get('title', '')}"


def _build_prompt(events):
    lines = []
    for i, ev in enumerate(events, 1):
        prev = ev.get("previous") or "N/A"
        fcst = ev.get("forecast") or "N/A"
        actl = ev.get("actual")
        actl_part = f" | Actual: {actl}" if actl else ""
        lines.append(
            f"{i}. {ev.get('currency', '')} {ev.get('title', '')}"
            f" — Prev: {prev} | Forecast: {fcst}{actl_part}"
        )
    return (
        "You are a concise forex market analyst. "
        "For each numbered economic event below, write exactly ONE sentence "
        "explaining what a beat or miss vs forecast would mean for the currency. "
        "Do NOT repeat the event name. Be direct and specific.\n\n"
        "Respond in this exact format:\n"
        "1. <one sentence>\n"
        "2. <one sentence>\n"
        "etc.\n\n"
        "Events:\n" + "\n".join(lines)
    )


def _parse_response(raw_text, events):
    result  = {}
    matches = re.findall(r"(\d+)\.\s+(.+?)(?=\n\d+\.|$)", raw_text, re.DOTALL)
    for num_str, text in matches:
        idx = int(num_str) - 1
        if 0 <= idx < len(events):
            result[_event_key(events[idx])] = text.strip()
    return result


def _call_gemini_batch(events):
    """Single Gemini call for all events. Returns {event_key: analysis}."""
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        print("[calendar] GEMINI_API_KEY not set")
        return {}
    if not events:
        return {}

    try:
        resp = requests.post(
            f"{GEMINI_URL}?key={api_key}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{"parts": [{"text": _build_prompt(events)}]}],
                "generationConfig": {"maxOutputTokens": 1024, "temperature": 0.3},
            },
            timeout=30,
        )
        if resp.status_code != 200:
            print(f"[calendar] Gemini error {resp.status_code}: {resp.text[:200]}")
            return {}
        candidates = resp.json().get("candidates", [])
        if not candidates:
            return {}
        raw = " ".join(
            p.get("text", "")
            for p in candidates[0].get("content", {}).get("parts", [])
        ).strip()
        result = _parse_response(raw, events)
        print(f"[calendar] Gemini: {len(result)}/{len(events)} analyses")
        return result
    except Exception as e:
        print(f"[calendar] Gemini error: {e}")
        return {}


# ── Public API ──────────────────────────────────────────────────────────────────
def get_calendar(force_refresh=False):
    """
    Returns filtered calendar events with AI analysis.
    Never raises — always returns a list (may be empty or stale).
    AI enrichment runs in background so this always returns immediately.
    """
    try:
        return _get_calendar_impl(force_refresh)
    except Exception as e:
        import traceback
        print(f"[calendar] Error: {e}")
        traceback.print_exc()
        with _cache_lock:
            return _events_cache.get("data", [])


def _get_calendar_impl(force_refresh):
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
                print(f"[calendar] Serving {len(disk_data)} events from disk cache")
                return disk_data
            mem_data = disk_data

    # 3. Live fetch from FF
    try:
        raw = _fetch_raw()
    except Exception as e:
        print(f"[calendar] Fetch failed: {e}")
        fallback = mem_data or []
        print(f"[calendar] Returning {len(fallback)} stale events")
        return fallback

    # Filter to relevant events
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
    print(f"[calendar] {len(filtered)} events after filtering")

    # Apply cached AI analyses
    uncached = []
    for ev in filtered:
        key = _event_key(ev)
        with _cache_lock:
            cached_ai = _ai_cache.get(key)
        if cached_ai and (now - cached_ai["at"]) < AI_TTL:
            ev["analysis"] = cached_ai["analysis"]
        else:
            uncached.append(ev)

    # Persist immediately — return fast, don't wait for AI
    with _cache_lock:
        _events_cache["data"] = filtered
        _events_cache["at"]   = now
    _save_cache(filtered)

    # Run AI enrichment in background thread
    if uncached:
        def _bg_ai(events_copy, ts):
            try:
                print(f"[calendar] Background AI for {len(events_copy)} events")
                analyses = _call_gemini_batch(events_copy)
                if not analyses:
                    return
                with _cache_lock:
                    current = _events_cache.get("data", [])
                    for ev in current:
                        key = _event_key(ev)
                        if key in analyses:
                            ev["analysis"] = analyses[key]
                            _ai_cache[key] = {"analysis": analyses[key], "at": ts}
                    data_snapshot = list(current)
                _save_cache(data_snapshot)
                print(f"[calendar] Background AI done: {len(analyses)} analyses")
            except Exception as e:
                import traceback
                print(f"[calendar] Background AI error: {e}")
                traceback.print_exc()

        threading.Thread(
            target=_bg_ai,
            args=(list(uncached), now),
            daemon=True,
            name="calendar-ai-bg",
        ).start()

    return filtered
