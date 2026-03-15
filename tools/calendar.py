"""
tools/calendar.py

Fetches this week's economic calendar from the ForexFactory public CDN:
  https://nfs.faireconomy.media/ff_calendar_thisweek.json

AI provider — set via AI_PROVIDER env var:
  gemini  (default, free) — set GEMINI_API_KEY (aistudio.google.com)
  openai                  — set OPENAI_API_KEY
  ollama                  — set OLLAMA_URL (default: http://localhost:11434)

Models:
  Gemini:  gemini-2.0-flash-lite
  OpenAI:  gpt-4o-mini  (override with OPENAI_MODEL)
  Ollama:  glm4:latest  (override with OLLAMA_MODEL)
           GLM Flash 4.7 — pull with: ollama pull glm4

Both API providers use a single batch call. AI runs in background thread.
"""

import os
import re
import json
import time
import threading
import requests
from datetime import datetime, timezone

CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
AI_PROVIDER  = os.environ.get("AI_PROVIDER", "gemini").lower()

GEMINI_URL   = ("https://generativelanguage.googleapis.com/v1beta/models"
                "/gemini-2.0-flash-lite:generateContent")

OPENAI_URL   = "https://api.openai.com/v1/chat/completions"
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

OLLAMA_URL   = os.environ.get("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "glm4:latest")

CURRENCIES = {"EUR", "GBP", "USD", "JPY"}
IMPACTS    = {"High", "Medium"}
EVENTS_TTL = 2 * 60 * 60
AI_TTL     = 6 * 60 * 60

CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "..", "data", "calendar_cache.json")

_events_cache = {}
_ai_cache     = {}
_cache_lock   = threading.Lock()


# ── Disk cache ──────────────────────────────────────────────────────────────────
def _save_cache(data):
    try:
        os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
        with open(CACHE_FILE, "w") as f:
            json.dump({"data": data, "at": time.time()}, f)
        print(f"[calendar] Saved {len(data)} events to disk")
    except Exception as e:
        print(f"[calendar] Cache save error: {e}")


def _load_cache():
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "r") as f:
                saved = json.load(f)
                data, at = saved.get("data", []), saved.get("at", 0)
                print(f"[calendar] Loaded {len(data)} events from disk (age: {int((time.time()-at)/60)}m)")
                return data, at
    except Exception as e:
        print(f"[calendar] Cache load error: {e}")
    return [], 0


# ── FF fetch ────────────────────────────────────────────────────────────────────
def _fetch_raw():
    resp = requests.get(CALENDAR_URL,
                        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
                        timeout=15)
    resp.raise_for_status()
    if "html" in resp.headers.get("content-type", "").lower():
        raise ValueError("FF returned HTML (rate limited)")
    data = resp.json()
    print(f"[calendar] Fetched {len(data)} raw events")
    return data


# ── Date parsing ────────────────────────────────────────────────────────────────
def _parse_date(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


# ── Shared AI helpers ───────────────────────────────────────────────────────────
def _event_key(ev):
    return f"{ev.get('date','')}|{ev.get('currency','')}|{ev.get('title','')}"


def _build_prompt(events):
    lines = []
    for i, ev in enumerate(events, 1):
        prev = ev.get("previous") or "N/A"
        fcst = ev.get("forecast") or "N/A"
        actl = ev.get("actual")
        lines.append(
            f"{i}. {ev.get('currency','')} {ev.get('title','')}"
            f" — Prev: {prev} | Forecast: {fcst}" + (f" | Actual: {actl}" if actl else "")
        )
    return (
        "You are a sharp forex trader writing quick notes for yourself. "
        "For each event below, write ONE punchy sentence about what this print means for the currency. "
        "Use specific numbers from the data. Vary your sentence starters — do NOT start with 'A beat' or 'A miss'. "
        "Examples of good starters: 'Stronger than expected...', 'With previous at X...', "
        "'Markets will watch...', 'Upside surprise...', 'Downside risk...', 'If it prints above X...'\n\n"
        "Respond ONLY as a numbered list:\n"
        "1. <sentence>\n"
        "2. <sentence>\netc.\n\n"
        "Events:\n" + "\n".join(lines)
    )

def _parse_response(raw_text, events):
    result = {}
    for num_str, text in re.findall(r"(\d+)\.\s+(.+?)(?=\n\d+\.|$)", raw_text, re.DOTALL):
        idx = int(num_str) - 1
        if 0 <= idx < len(events):
            result[_event_key(events[idx])] = text.strip()
    return result


# ── Gemini provider ─────────────────────────────────────────────────────────────
def _call_gemini_batch(events):
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        print("[calendar] GEMINI_API_KEY not set")
        return {}
    try:
        resp = requests.post(
            f"{GEMINI_URL}?key={api_key}",
            headers={"Content-Type": "application/json"},
            json={"contents": [{"parts": [{"text": _build_prompt(events)}]}],
                  "generationConfig": {"maxOutputTokens": 1024, "temperature": 0.3}},
            timeout=30,
        )
        if resp.status_code != 200:
            print(f"[calendar] Gemini error {resp.status_code}: {resp.text[:200]}")
            return {}
        candidates = resp.json().get("candidates", [])
        if not candidates:
            return {}
        raw = " ".join(p.get("text", "") for p in
                       candidates[0].get("content", {}).get("parts", [])).strip()
        result = _parse_response(raw, events)
        print(f"[calendar] Gemini: {len(result)}/{len(events)} analyses")
        return result
    except Exception as e:
        print(f"[calendar] Gemini error: {e}")
        return {}


# ── OpenAI provider ─────────────────────────────────────────────────────────────
def _call_openai_batch(events):
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        print("[calendar] OPENAI_API_KEY not set")
        return {}
    try:
        resp = requests.post(
            OPENAI_URL,
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {api_key}"},
            json={"model": OPENAI_MODEL,
                  "messages": [{"role": "user", "content": _build_prompt(events)}],
                  "max_tokens": 1024, "temperature": 0.3},
            timeout=30,
        )
        if resp.status_code != 200:
            print(f"[calendar] OpenAI error {resp.status_code}: {resp.text[:200]}")
            return {}
        raw    = resp.json()["choices"][0]["message"]["content"].strip()
        result = _parse_response(raw, events)
        print(f"[calendar] OpenAI ({OPENAI_MODEL}): {len(result)}/{len(events)} analyses")
        return result
    except Exception as e:
        print(f"[calendar] OpenAI error: {e}")
        return {}


# ── Ollama provider ─────────────────────────────────────────────────────────────
def _call_ollama_batch(events):
    """
    Calls Ollama via the native /api/chat endpoint.
    Uses think:false to disable reasoning mode on qwen3/thinking models.
    Sends events in chunks of 5 to avoid timeouts.
    """
    base_url    = OLLAMA_URL.rstrip("/")
    url         = f"{base_url}/api/chat"
    CHUNK_SIZE  = 5
    all_results = {}

    chunks = [events[i:i+CHUNK_SIZE] for i in range(0, len(events), CHUNK_SIZE)]
    print(f"[calendar] Ollama: {len(chunks)} chunks via {url} model={OLLAMA_MODEL}")

    for idx, chunk in enumerate(chunks):
        print(f"[calendar] Ollama chunk {idx+1}/{len(chunks)}")
        try:
            resp = requests.post(
                url,
                headers={"Content-Type": "application/json"},
                json={
                    "model":   OLLAMA_MODEL,
                    "think":   False,
                    "stream":  False,
                    "options": {"temperature": 0.3, "num_predict": 512},
                    "messages": [{"role": "user", "content": _build_prompt(chunk)}],
                },
                timeout=90,
            )
            print(f"[calendar] Ollama chunk {idx+1} status: {resp.status_code}")
            if resp.status_code != 200:
                print(f"[calendar] Ollama error: {resp.text[:300]}")
                continue

            data = resp.json()
            raw  = (data.get("message") or {}).get("content", "").strip()
            print(f"[calendar] Ollama chunk {idx+1} raw (first 200): {raw[:200]}")

            chunk_result = _parse_response(raw, chunk)
            all_results.update(chunk_result)
            print(f"[calendar] Ollama chunk {idx+1}: {len(chunk_result)}/{len(chunk)} parsed")

        except requests.exceptions.Timeout:
            print(f"[calendar] Ollama chunk {idx+1} TIMEOUT")
        except requests.exceptions.ConnectionError as e:
            print(f"[calendar] Ollama CONNECTION ERROR: {e}")
            break
        except Exception as e:
            import traceback
            print(f"[calendar] Ollama chunk {idx+1} error: {e}")
            traceback.print_exc()

    print(f"[calendar] Ollama total: {len(all_results)}/{len(events)} analyses")
    return all_results


# ── Provider router ─────────────────────────────────────────────────────────────
def _call_ai_batch(events):
    print(f"[calendar] AI provider: {AI_PROVIDER}")
    if AI_PROVIDER == "openai":
        return _call_openai_batch(events)
    if AI_PROVIDER == "ollama":
        return _call_ollama_batch(events)
    return _call_gemini_batch(events)


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
                analyses = _call_ai_batch(events_copy)
                if not analyses:
                    return
                with _cache_lock:
                    current = _events_cache.get("data", [])
                    for ev in current:
                        key = _event_key(ev)
                        if key in analyses:
                            ev["analysis"] = analyses[key]
                            _ai_cache[key] = {"analysis": analyses[key], "at": ts}
                    snapshot = list(current)
                _save_cache(snapshot)
                print(f"[calendar] Background AI done: {len(analyses)} analyses")
            except Exception as e:
                import traceback
                print(f"[calendar] Background AI error: {e}")
                traceback.print_exc()

        threading.Thread(target=_bg_ai, args=(list(uncached), now),
                         daemon=True, name="calendar-ai-bg").start()

    return filtered
