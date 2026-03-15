"""
tools/news_macro.py

Fetches live market news from RSS feeds, with pair-aware filtering.

Two modes:
  1. get_headlines(limit)       — global macro headlines for the Macro Desk overview
  2. get_pair_headlines(pair_id) — headlines filtered/ranked for a specific trading pair

Feed coverage:
  - Global macro: Reuters, Bloomberg, FT, MarketWatch, Investing.com, Yahoo Finance
  - FX specific:  ForexLive, DailyFX, FXStreet
  - Equities:     CNBC Markets, Seeking Alpha, Barron's
  - Commodities:  Kitco (gold/silver), OilPrice.com
  - Crypto:       CoinDesk, CryptoPanic

Pair routing:
  Each pair maps to a set of "relevance keywords" and preferred feed groups.
  Headlines are scored by keyword matches in title+description.
  Higher score = shown first.
"""

import time
import threading
import hashlib
import re
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from datetime import datetime

try:
    import requests
    REQUESTS_OK = True
except ImportError:
    REQUESTS_OK = False

# ── Cache settings ─────────────────────────────────────────────────────────────
CACHE_TTL  = 3 * 60   # 3 min
MAX_AGE_H  = 48        # ignore articles older than 48h

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; InnerFlows/1.0)",
    "Accept":     "application/rss+xml, application/xml, text/xml",
}

# ── Feed registry ──────────────────────────────────────────────────────────────
# group: which asset class this feed covers
FEEDS = [
    # ── Global macro ──
    {"name": "Reuters Business",  "url": "https://feeds.reuters.com/reuters/businessNews",           "domain": "reuters.com",       "group": "macro"},
    {"name": "Reuters Markets",   "url": "https://feeds.reuters.com/reuters/UKBusinessNews",          "domain": "reuters.com",       "group": "macro"},
    {"name": "FT Markets",        "url": "https://www.ft.com/markets?format=rss",                     "domain": "ft.com",            "group": "macro"},
    {"name": "MarketWatch",       "url": "http://feeds.marketwatch.com/marketwatch/topstories/",       "domain": "marketwatch.com",   "group": "macro"},
    {"name": "Investing.com",     "url": "https://www.investing.com/rss/news.rss",                    "domain": "investing.com",     "group": "macro"},
    {"name": "Yahoo Finance",     "url": "https://finance.yahoo.com/news/rssindex",                   "domain": "yahoo.com",         "group": "macro"},
    {"name": "CNBC Markets",      "url": "https://www.cnbc.com/id/20910258/device/rss/rss.html",      "domain": "cnbc.com",          "group": "equity"},
    {"name": "Seeking Alpha",     "url": "https://seekingalpha.com/feed.xml",                         "domain": "seekingalpha.com",  "group": "equity"},
    # ── FX focused ──
    {"name": "ForexLive",         "url": "https://www.forexlive.com/feed/news",                       "domain": "forexlive.com",     "group": "fx"},
    {"name": "DailyFX",          "url": "https://www.dailyfx.com/feeds/all",                          "domain": "dailyfx.com",       "group": "fx"},
    {"name": "FXStreet",          "url": "https://www.fxstreet.com/rss/news",                         "domain": "fxstreet.com",      "group": "fx"},
    {"name": "Investing FX",      "url": "https://www.investing.com/rss/news_285.rss",                "domain": "investing.com",     "group": "fx"},
    # ── Commodities ──
    {"name": "Kitco Gold",        "url": "https://www.kitco.com/rss/kitconews.rss",                   "domain": "kitco.com",         "group": "commod"},
    {"name": "OilPrice",          "url": "https://oilprice.com/rss/main",                             "domain": "oilprice.com",      "group": "commod"},
    {"name": "Investing Commod",  "url": "https://www.investing.com/rss/news_11.rss",                 "domain": "investing.com",     "group": "commod"},
    # ── Crypto ──
    {"name": "CoinDesk",          "url": "https://www.coindesk.com/arc/outboundfeeds/rss/",           "domain": "coindesk.com",      "group": "crypto"},
    {"name": "CoinTelegraph",     "url": "https://cointelegraph.com/rss",                             "domain": "cointelegraph.com", "group": "crypto"},
    # ── Central banks / macro policy ──
    {"name": "Econbrowser",       "url": "https://econbrowser.com/feed",                              "domain": "econbrowser.com",   "group": "rates"},
    {"name": "WSJ Economy",       "url": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",             "domain": "wsj.com",           "group": "rates"},
]

# ── Pair → relevance config ────────────────────────────────────────────────────
# keywords: title/description words that boost relevance score
# groups:   which feed groups to pull from (in priority order)
# blocklist: words that strongly suggest the article is NOT about this pair

PAIR_CONFIG = {
    # ── FX ──
    "EURUSD": {
        "groups":    ["fx", "macro", "rates"],
        "keywords":  ["euro", "eur", "eurusd", "ecb", "european central bank", "eurozone",
                      "dollar", "usd", "federal reserve", "fed", "inflation", "rate"],
        "blocklist": ["bitcoin", "crypto", "gold", "oil", "nikkei", "ftse"],
    },
    "GBPUSD": {
        "groups":    ["fx", "macro", "rates"],
        "keywords":  ["pound", "sterling", "gbp", "gbpusd", "boe", "bank of england",
                      "uk economy", "britain", "dollar", "usd", "fed"],
        "blocklist": ["bitcoin", "crypto", "gold", "yen", "euro"],
    },
    "EURGBP": {
        "groups":    ["fx", "macro"],
        "keywords":  ["euro", "eur", "pound", "sterling", "gbp", "eurgbp", "ecb",
                      "bank of england", "boe", "eurozone", "uk"],
        "blocklist": ["bitcoin", "crypto", "gold", "yen", "dollar"],
    },
    "USDJPY": {
        "groups":    ["fx", "macro", "rates"],
        "keywords":  ["yen", "jpy", "usdjpy", "boj", "bank of japan", "japan", "nikkei",
                      "dollar", "usd", "fed", "yield", "carry trade"],
        "blocklist": ["bitcoin", "crypto", "gold", "euro", "pound"],
    },
    "AUDUSD": {
        "groups":    ["fx", "macro", "commod"],
        "keywords":  ["aussie", "aud", "audusd", "rba", "reserve bank australia",
                      "australia", "china", "commodities", "iron ore", "dollar"],
        "blocklist": ["bitcoin", "crypto", "yen", "pound"],
    },
    "USDCAD": {
        "groups":    ["fx", "macro", "commod"],
        "keywords":  ["cad", "usdcad", "loonie", "bank of canada", "canada", "oil",
                      "crude", "dollar", "fed"],
        "blocklist": ["bitcoin", "crypto", "yen", "euro"],
    },
    "USDCHF": {
        "groups":    ["fx", "macro"],
        "keywords":  ["franc", "chf", "usdchf", "snb", "swiss national bank", "switzerland",
                      "safe haven", "dollar"],
        "blocklist": ["bitcoin", "crypto"],
    },
    "NZDUSD": {
        "groups":    ["fx", "macro"],
        "keywords":  ["kiwi", "nzd", "nzdusd", "rbnz", "new zealand", "dollar"],
        "blocklist": ["bitcoin", "crypto"],
    },
    # ── Indices ──
    "US30": {
        "groups":    ["equity", "macro", "rates"],
        "keywords":  ["dow jones", "dow", "us30", "djia", "wall street", "s&p", "nasdaq",
                      "us stocks", "fed", "earnings", "inflation", "rate cut"],
        "blocklist": ["bitcoin", "crypto", "gold", "oil", "yen", "euro"],
    },
    "US100": {
        "groups":    ["equity", "macro", "rates"],
        "keywords":  ["nasdaq", "ndx", "us100", "tech stocks", "technology", "nvidia",
                      "apple", "microsoft", "meta", "alphabet", "fed", "rate",
                      "s&p 500", "wall street", "earnings"],
        "blocklist": ["bitcoin", "crypto", "gold", "yen", "pound"],
    },
    # ── Commodities ──
    "XAUUSD": {
        "groups":    ["commod", "macro", "fx"],
        "keywords":  ["gold", "xau", "xauusd", "precious metal", "bullion", "safe haven",
                      "dollar", "inflation", "fed", "rate", "geopolitical"],
        "blocklist": ["bitcoin", "crypto", "silver only", "oil"],
    },
    "XAGUSD": {
        "groups":    ["commod", "macro"],
        "keywords":  ["silver", "xag", "xagusd", "precious metal", "gold", "industrial metal"],
        "blocklist": ["bitcoin", "crypto"],
    },
    "OIL": {
        "groups":    ["commod", "macro"],
        "keywords":  ["oil", "crude", "wti", "brent", "opec", "energy", "petroleum",
                      "barrel", "natural gas", "supply"],
        "blocklist": ["bitcoin", "crypto", "gold"],
    },
    # ── Crypto ──
    "BTCUSD": {
        "groups":    ["crypto", "macro"],
        "keywords":  ["bitcoin", "btc", "btcusd", "crypto", "cryptocurrency", "blockchain",
                      "coinbase", "binance", "digital asset", "etf"],
        "blocklist": ["gold", "oil", "yen", "pound", "euro"],
    },
    "ETHUSD": {
        "groups":    ["crypto", "macro"],
        "keywords":  ["ethereum", "eth", "ethusd", "crypto", "defi", "smart contract",
                      "blockchain", "altcoin"],
        "blocklist": ["bitcoin only", "gold", "oil"],
    },
}

# Default config for unknown pairs
DEFAULT_CONFIG = {
    "groups":   ["macro", "fx"],
    "keywords": [],
    "blocklist": [],
}

# ── In-memory cache ────────────────────────────────────────────────────────────
_feed_cache:  dict = {}   # url → {items, at}
_pair_cache:  dict = {}   # pair_id → {items, at}
_global_cache: dict = {"items": [], "at": 0}
_lock = threading.Lock()


# ── RSS parsing ────────────────────────────────────────────────────────────────
def _parse_ts(raw: str) -> float:
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


def _parse_rss(xml_text: str, source: str, domain: str) -> list[dict]:
    items = []
    try:
        root = ET.fromstring(xml_text)
        for item in root.iter("item"):
            title = (item.findtext("title") or "").strip()
            link  = (item.findtext("link")  or "").strip()
            desc  = (item.findtext("description") or "").strip()
            pub   = item.findtext("pubDate") or ""
            if not title:
                continue
            desc  = re.sub(r"<[^>]+>", "", desc).strip()[:300]
            ts    = _parse_ts(pub)
            items.append({
                "id":      hashlib.md5((title + link).encode()).hexdigest()[:12],
                "title":   title,
                "url":     link,
                "summary": desc,
                "source":  source,
                "domain":  domain,
                "ts":      ts,
                "age_min": _age_minutes(ts),
            })
    except ET.ParseError:
        pass
    return items


def _fetch_feed(feed: dict) -> list[dict]:
    """Fetch one feed with 5min cache per URL."""
    url = feed["url"]
    now = time.time()

    with _lock:
        cached = _feed_cache.get(url)
        if cached and (now - cached["at"]) < 5 * 60:
            return cached["items"]

    if not REQUESTS_OK:
        return []

    try:
        resp = requests.get(url, headers=HEADERS, timeout=8)
        if resp.status_code != 200:
            return []
        items = _parse_rss(resp.text, feed["name"], feed["domain"])
        with _lock:
            _feed_cache[url] = {"items": items, "at": time.time()}
        return items
    except Exception as e:
        print(f"[news_macro] feed error {feed['name']}: {e}")
        return []


def _score_item(item: dict, keywords: list[str], blocklist: list[str]) -> int:
    """
    Score an item for relevance to a pair.
    Returns 0 if blocklisted, otherwise count of keyword matches.
    """
    text = (item["title"] + " " + item.get("summary", "")).lower()

    # Blocklist check — if any blocklist word is prominent, skip
    for word in blocklist:
        if word.lower() in text:
            return -1

    # Score by keyword matches
    score = 0
    for kw in keywords:
        if kw.lower() in text:
            score += 1

    return score


# ── Public API ─────────────────────────────────────────────────────────────────

def get_headlines(limit: int = 30, force: bool = False) -> list[dict]:
    """
    Global macro headlines — for the Macro Desk overview news panel.
    Pulls from macro + equity feeds, deduplicates, sorts by recency.
    """
    now = time.time()
    with _lock:
        gc = _global_cache
        if not force and gc["items"] and (now - gc["at"]) < CACHE_TTL:
            return gc["items"][:limit]

    macro_feeds = [f for f in FEEDS if f["group"] in ("macro", "equity", "rates")]
    all_items, seen = [], set()

    for feed in macro_feeds:
        for item in _fetch_feed(feed):
            if item["age_min"] > MAX_AGE_H * 60:
                continue
            if item["id"] in seen:
                continue
            seen.add(item["id"])
            all_items.append(item)

    all_items.sort(key=lambda x: x["ts"] or 0, reverse=True)

    with _lock:
        _global_cache["items"] = all_items
        _global_cache["at"]    = time.time()

    return all_items[:limit]


def get_pair_headlines(pair_id: str, limit: int = 20, force: bool = False) -> list[dict]:
    """
    Pair-specific headlines — for the Deep Dive news sidebar.
    
    Strategy:
    1. Pull from feeds relevant to the pair's asset class first
    2. Score every item by keyword relevance
    3. Drop items with score <= 0 (blocklisted or no match)
    4. Sort: relevant items first (by score desc), then by recency
    5. Fall back to general macro news if not enough relevant items found
    """
    pair_id = pair_id.upper()
    now     = time.time()

    with _lock:
        cached = _pair_cache.get(pair_id)
        if not force and cached and (now - cached["at"]) < CACHE_TTL:
            return cached["items"][:limit]

    cfg      = PAIR_CONFIG.get(pair_id, DEFAULT_CONFIG)
    keywords = cfg["keywords"]
    blocklist= cfg["blocklist"]
    groups   = cfg["groups"]

    # Pull from relevant feeds first, then fill from macro
    feed_order = (
        [f for f in FEEDS if f["group"] in groups] +
        [f for f in FEEDS if f["group"] not in groups]
    )

    scored, seen = [], set()

    for feed in feed_order:
        for item in _fetch_feed(feed):
            if item["id"] in seen:
                continue
            if item["age_min"] > MAX_AGE_H * 60:
                continue
            seen.add(item["id"])

            score = _score_item(item, keywords, blocklist)
            if score < 0:
                continue  # blocklisted

            scored.append({**item, "relevance": score})

    # Sort: high relevance first, then by recency within same relevance bucket
    scored.sort(key=lambda x: (x["relevance"], x["ts"] or 0), reverse=True)

    # If we have fewer than 5 relevant items, include zero-score items (general macro)
    relevant = [i for i in scored if i["relevance"] > 0]
    fallback = [i for i in scored if i["relevance"] == 0]

    if len(relevant) < 5:
        result = relevant + fallback[: limit - len(relevant)]
    else:
        result = relevant

    result = result[:limit]

    with _lock:
        _pair_cache[pair_id] = {"items": result, "at": time.time()}

    return result


def format_age(age_min: int) -> str:
    if age_min < 1:   return "just now"
    if age_min < 60:  return f"{age_min}m ago"
    h = age_min // 60
    if h < 24:        return f"{h}h ago"
    return f"{h // 24}d ago"
