"""
tools/macro_news.py

Fetches live market news headlines from free public RSS feeds.
No API key required. Parses, deduplicates, and caches results.

Re-used by: Macro Desk news panel, any alert pipeline.

Usage:
    from tools.news import get_headlines
    headlines = get_headlines(limit=20)
"""

import time
import threading
import hashlib
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

try:
    import requests
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

# ── Feed sources ──────────────────────────────────────────────────────────────
RSS_FEEDS = [
    {
        "name":     "Reuters Markets",
        "url":      "https://feeds.reuters.com/reuters/businessNews",
        "domain":   "reuters.com",
        "priority": 1,
    },
    {
        "name":     "Bloomberg",
        "url":      "https://feeds.bloomberg.com/markets/news.rss",
        "domain":   "bloomberg.com",
        "priority": 1,
    },
    {
        "name":     "FT Markets",
        "url":      "https://www.ft.com/markets?format=rss",
        "domain":   "ft.com",
        "priority": 2,
    },
    {
        "name":     "MarketWatch",
        "url":      "http://feeds.marketwatch.com/marketwatch/topstories/",
        "domain":   "marketwatch.com",
        "priority": 2,
    },
    {
        "name":     "Investing.com",
        "url":      "https://www.investing.com/rss/news.rss",
        "domain":   "investing.com",
        "priority": 3,
    },
    {
        "name":     "Yahoo Finance",
        "url":      "https://finance.yahoo.com/news/rssindex",
        "domain":   "yahoo.com",
        "priority": 3,
    },
]

CACHE_TTL = 3 * 60    # 3 min — news is time-sensitive
MAX_AGE_H = 24        # ignore articles older than 24h

_cache:     list  = []
_cache_at:  float = 0
_lock = threading.Lock()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; MacroDesk/1.0)",
    "Accept":     "application/rss+xml, application/xml, text/xml",
}


def _parse_rss(xml_text: str, source_name: str, source_domain: str) -> list[dict]:
    """Parse RSS XML into a list of headline dicts."""
    items = []
    try:
        root = ET.fromstring(xml_text)
        # Handle both RSS and Atom
        ns = {"atom": "http://www.w3.org/2005/Atom"}

        # RSS 2.0
        for item in root.iter("item"):
            title = (item.findtext("title") or "").strip()
            link  = (item.findtext("link")  or "").strip()
            desc  = (item.findtext("description") or "").strip()
            pub   = item.findtext("pubDate") or ""

            if not title:
                continue

            # Parse timestamp
            ts = _parse_ts(pub)

            # Clean HTML from description
            desc = re.sub(r"<[^>]+>", "", desc).strip()[:300]

            items.append({
                "id":      hashlib.md5((title + link).encode()).hexdigest()[:12],
                "title":   title,
                "url":     link,
                "summary": desc,
                "source":  source_name,
                "domain":  source_domain,
                "ts":      ts,
                "age_min": _age_minutes(ts),
            })

    except ET.ParseError:
        pass
    return items


def _parse_ts(raw: str) -> float:
    """Parse RFC 2822 or ISO date string to unix timestamp. Returns 0 on failure."""
    if not raw:
        return 0.0
    try:
        return parsedate_to_datetime(raw).timestamp()
    except Exception:
        pass
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _age_minutes(ts: float) -> int:
    if not ts:
        return 9999
    return int((time.time() - ts) / 60)


def _fetch_feed(feed: dict) -> list[dict]:
    """Fetch and parse a single RSS feed. Returns [] on failure."""
    if not REQUESTS_OK:
        return []
    try:
        resp = requests.get(feed["url"], headers=HEADERS, timeout=8)
        if resp.status_code != 200:
            return []
        return _parse_rss(resp.text, feed["name"], feed["domain"])
    except Exception as e:
        print(f"[news] feed error {feed['name']}: {e}")
        return []


def get_headlines(limit: int = 30, force: bool = False) -> list[dict]:
    """
    Return deduplicated, time-sorted news headlines from all RSS feeds.

    Each headline:
        id, title, url, summary, source, domain, ts (unix), age_min

    Results cached for CACHE_TTL seconds.
    """
    now = time.time()
    with _lock:
        if not force and _cache and (now - _cache_at) < CACHE_TTL:
            return _cache[:limit]

    all_items = []
    seen_ids  = set()

    for feed in sorted(RSS_FEEDS, key=lambda f: f["priority"]):
        items = _fetch_feed(feed)
        for item in items:
            # Skip old articles
            if item["age_min"] > MAX_AGE_H * 60:
                continue
            # Deduplicate by id
            if item["id"] in seen_ids:
                continue
            seen_ids.add(item["id"])
            all_items.append(item)

    # Sort by timestamp descending (newest first)
    all_items.sort(key=lambda x: x["ts"] or 0, reverse=True)

    with _lock:
        _cache.clear()
        _cache.extend(all_items)
        globals()["_cache_at"] = time.time()

    return all_items[:limit]


def format_age(age_min: int) -> str:
    """Human-readable age string: '5m ago', '2h ago', etc."""
    if age_min < 1:
        return "just now"
    if age_min < 60:
        return f"{age_min}m ago"
    h = age_min // 60
    if h < 24:
        return f"{h}h ago"
    return f"{h // 24}d ago"
