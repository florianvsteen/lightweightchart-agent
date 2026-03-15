"""
tools/calendar.py

Scrapes today's + tomorrow's economic calendar from ForexFactory.
Filters to high/medium impact events for EUR, GBP, USD, JPY.
Calls Claude via the Anthropic API to generate a short AI analysis per event.

Cache: events are cached for 30 minutes, AI analyses for 6 hours.
"""

import os
import time
import threading
import requests
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup

# ── Cache ──────────────────────────────────────────────────────────────────────
_events_cache: dict = {}           # { "events": [...], "fetched_at": float }
_ai_cache: dict = {}               # { event_key: { "analysis": str, "at": float } }
_cache_lock = threading.Lock()

EVENTS_TTL  = 30 * 60      # 30 min
AI_TTL      = 6 * 3600     # 6 hours

CURRENCIES  = {"EUR", "GBP", "USD", "JPY"}
IMPACTS     = {"high", "medium"}   # forexfactory CSS classes: "icon icon--ff-impact-red" etc.

# Map FF impact icon class fragment → label
_IMPACT_MAP = {
    "red":    "High",
    "orange": "Medium",
    "yellow": "Low",
    "gray":   "Holiday",
}


def _fetch_ff_calendar(date: datetime) -> list[dict]:
    """
    Scrape ForexFactory calendar for a given date.
    Returns list of raw event dicts.
    """
    date_str = date.strftime("%b%d.%Y").lower()   # e.g. mar15.2026
    url = f"https://www.forexfactory.com/calendar?day={date_str}"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"[calendar] FF fetch error for {date_str}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", class_="calendar__table")
    if not table:
        return []

    events = []
    current_time = ""

    for row in table.find_all("tr", class_=lambda c: c and "calendar__row" in c):
        # Time cell — may be empty if same as previous row
        time_cell = row.find("td", class_="calendar__time")
        if time_cell:
            t = time_cell.get_text(strip=True)
            if t:
                current_time = t

        # Currency
        cur_cell = row.find("td", class_="calendar__currency")
        currency = cur_cell.get_text(strip=True) if cur_cell else ""
        if currency not in CURRENCIES:
            continue

        # Impact
        impact_cell = row.find("td", class_="calendar__impact")
        impact = "Low"
        if impact_cell:
            icon = impact_cell.find("span") or impact_cell.find("i")
            if icon:
                cls = " ".join(icon.get("class", []))
                for key, label in _IMPACT_MAP.items():
                    if key in cls:
                        impact = label
                        break

        if impact.lower() not in IMPACTS:
            continue

        # Event name
        name_cell = row.find("td", class_="calendar__event")
        event_name = name_cell.get_text(strip=True) if name_cell else ""
        if not event_name:
            continue

        # Actual / Forecast / Previous
        actual_cell   = row.find("td", class_="calendar__actual")
        forecast_cell = row.find("td", class_="calendar__forecast")
        previous_cell = row.find("td", class_="calendar__previous")

        actual   = actual_cell.get_text(strip=True)   if actual_cell   else ""
        forecast = forecast_cell.get_text(strip=True) if forecast_cell else ""
        previous = previous_cell.get_text(strip=True) if previous_cell else ""

        events.append({
            "date":     date.strftime("%Y-%m-%d"),
            "time":     current_time,
            "currency": currency,
            "impact":   impact,
            "event":    event_name,
            "actual":   actual,
            "forecast": forecast,
            "previous": previous,
        })

    return events


def _event_key(event: dict) -> str:
    return f"{event['date']}|{event['time']}|{event['currency']}|{event['event']}"


def _call_claude_analysis(event: dict) -> str:
    """
    Call Claude Haiku via the Anthropic API to produce a short AI analysis
    of this economic event (1-2 sentences max, trader-focused).
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return ""

    context_parts = [
        f"{event['currency']} {event['event']}",
        f"due {event['date']} {event['time']} UTC",
    ]
    if event.get("previous"):
        context_parts.append(f"previous: {event['previous']}")
    if event.get("forecast"):
        context_parts.append(f"forecast: {event['forecast']}")
    if event.get("actual"):
        context_parts.append(f"actual: {event['actual']}")

    context = "; ".join(context_parts) + "."

    prompt = (
        f"Economic event: {context}\n\n"
        "Write a 1–2 sentence trader-focused analysis of this event. "
        "Mention the last reading, what a beat/miss would mean for the currency, "
        "and any relevant upcoming events that interact with this one. "
        "Be concise and direct. No disclaimers."
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
    Return today's + tomorrow's filtered calendar events with AI analysis.
    Cached for EVENTS_TTL. AI analyses cached separately for AI_TTL.
    """
    now = time.time()

    with _cache_lock:
        cached = _events_cache.get("data")
        if cached and not force_refresh and (now - _events_cache.get("at", 0)) < EVENTS_TTL:
            return cached

    # Fetch outside lock
    today    = datetime.now(timezone.utc)
    tomorrow = today + timedelta(days=1)

    events = _fetch_ff_calendar(today) + _fetch_ff_calendar(tomorrow)

    # Attach AI analyses (use cache where available)
    enriched = []
    for ev in events:
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
