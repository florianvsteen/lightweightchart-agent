"""
tools/news.py

Fetches recent news headlines for a trading pair using yfinance.

yfinance exposes a .news property on Ticker objects that returns a list of
recent articles from Yahoo Finance. Each article contains:
  title, publisher, link, providerPublishTime, type, thumbnail, relatedTickers

This module normalises that into a clean list of dicts and adds a simple
keyword-based sentiment tag (bullish / bearish / neutral).

Usage:
    from news import get_news
    articles = get_news("GBPUSD")   # returns list of article dicts

The result is cached in-memory per pair for NEWS_TTL_SECONDS (default 300).
The cache is thread-safe.
"""

import time
import threading
import yfinance as yf

# ── Config ─────────────────────────────────────────────────────────────────────
NEWS_TTL_SECONDS = 300      # 5 minutes
MAX_ARTICLES     = 8

# Map pair IDs to yfinance ticker symbols
# These are the same as yf_ticker in config.py
PAIR_TICKER_MAP = {
    "US30":   "YM=F",
    "US100":  "NQ=F",
    "XAUUSD": "GC=F",
    "EURUSD": "EURUSD=X",
    "EURGBP": "EURGBP=X",
    "GBPUSD": "GBPUSD=X",
    "USDJPY": "USDJPY=X",
    "USDCAD": "USDCAD=X",
    "USDCHF": "USDCHF=X",
    "AUDUSD": "AUDUSD=X",
    "NZDUSD": "NZDUSD=X",
    "EURJPY": "EURJPY=X",
    "GBPJPY": "GBPJPY=X",
    "AUDJPY": "AUDJPY=X",
    "XAGUSD": "SI=F",
    "BTCUSD": "BTC-USD",
    "ETHUSD": "ETH-USD",
    "SPX500": "^GSPC",
    "NAS100": "NQ=F",
}

# ── Sentiment keywords ─────────────────────────────────────────────────────────
_BULLISH_WORDS = {
    "surge", "surges", "surging", "rally", "rallies", "rallying", "rise",
    "rises", "rising", "gain", "gains", "gaining", "jump", "jumps",
    "jumping", "climb", "climbs", "climbing", "boost", "boosts", "boosting",
    "strengthen", "strengthens", "strengthening", "high", "highs", "record",
    "optimism", "bullish", "buy", "upside", "upgrade", "beat", "beats",
    "strong", "strength", "positive", "growth", "recover", "recovery",
    "hawkish",
}

_BEARISH_WORDS = {
    "fall", "falls", "falling", "drop", "drops", "dropping", "decline",
    "declines", "declining", "sink", "sinks", "sinking", "slide", "slides",
    "sliding", "tumble", "tumbles", "tumbling", "plunge", "plunges",
    "plunging", "weak", "weakness", "weakens", "loss", "losses", "low",
    "lows", "bearish", "sell", "downside", "downgrade", "miss", "misses",
    "concern", "concerns", "risk", "risks", "uncertain", "uncertainty",
    "slowdown", "recession", "inflation", "dovish", "cut", "cuts",
    "pressure", "pressured",
}


def _sentiment(title: str) -> str:
    """
    Simple keyword-based sentiment from headline text.
    Returns 'bullish', 'bearish', or 'neutral'.
    """
    words = set(title.lower().split())
    bull = len(words & _BULLISH_WORDS)
    bear = len(words & _BEARISH_WORDS)
    if bull > bear:
        return "bullish"
    if bear > bull:
        return "bearish"
    return "neutral"


def _age_label(publish_ts: int) -> str:
    """Human-readable age string: '2h ago', '35m ago', etc."""
    diff = int(time.time()) - publish_ts
    if diff < 60:
        return "just now"
    if diff < 3600:
        return f"{diff // 60}m ago"
    if diff < 86400:
        return f"{diff // 3600}h ago"
    return f"{diff // 86400}d ago"


# ── In-memory cache ────────────────────────────────────────────────────────────
_cache: dict[str, dict] = {}
_cache_lock = threading.Lock()


def _resolve_ticker(pair_id: str, yf_ticker: str | None = None) -> str:
    """
    Resolve the yfinance ticker for a pair.
    Uses the explicit yf_ticker if provided, otherwise looks up PAIR_TICKER_MAP.
    Falls back to pair_id itself so yfinance can try anyway.
    """
    if yf_ticker:
        return yf_ticker
    return PAIR_TICKER_MAP.get(pair_id.upper(), pair_id)


def _fetch(ticker_symbol: str) -> list[dict]:
    """
    Fetch raw news from yfinance and normalise to a clean list.
    Returns [] on any error.
    """
    try:
        ticker = yf.Ticker(ticker_symbol)
        raw = ticker.news or []
    except Exception as e:
        print(f"[news] yfinance fetch error for {ticker_symbol}: {e}")
        return []

    articles = []
    for item in raw[:MAX_ARTICLES]:
        try:
            # yfinance news item structure varies slightly by version —
            # handle both flat dicts and nested 'content' dicts (newer yfinance)
            if "content" in item and isinstance(item["content"], dict):
                content   = item["content"]
                title     = content.get("title", "")
                publisher = content.get("provider", {}).get("displayName", "") if isinstance(content.get("provider"), dict) else content.get("provider", "")
                link      = content.get("canonicalUrl", {}).get("url", "") if isinstance(content.get("canonicalUrl"), dict) else content.get("url", "")
                pub_ts    = content.get("pubDate", "") 
                # pubDate may be ISO string in newer versions
                if isinstance(pub_ts, str):
                    import datetime
                    try:
                        pub_ts = int(datetime.datetime.fromisoformat(pub_ts.replace("Z", "+00:00")).timestamp())
                    except Exception:
                        pub_ts = 0
            else:
                title     = item.get("title", "")
                publisher = item.get("publisher", "")
                link      = item.get("link", "")
                pub_ts    = int(item.get("providerPublishTime", 0))

            if not title:
                continue

            articles.append({
                "headline":  title[:300],
                "source":    publisher[:80],
                "link":      link,
                "sentiment": _sentiment(title),
                "age":       _age_label(pub_ts) if pub_ts else "",
                "ts":        pub_ts,
            })
        except Exception:
            continue

    return articles


def get_news(pair_id: str, yf_ticker: str | None = None) -> list[dict]:
    """
    Return cached news for a pair, refreshing if the cache is stale.

    Args:
        pair_id:   The pair identifier (e.g. "GBPUSD", "XAUUSD").
        yf_ticker: Optional explicit yfinance ticker. If omitted, looked up
                   from PAIR_TICKER_MAP using pair_id.

    Returns:
        List of article dicts:
            headline  str   — article title
            source    str   — publisher name
            link      str   — article URL
            sentiment str   — 'bullish' | 'bearish' | 'neutral'
            age       str   — human-readable age ('2h ago')
            ts        int   — unix publish timestamp
    """
    key = pair_id.upper()

    with _cache_lock:
        cached = _cache.get(key)
        if cached and (time.time() - cached["ts"]) < NEWS_TTL_SECONDS:
            return cached["articles"]

    # Fetch outside lock so we don't block other pairs
    ticker_symbol = _resolve_ticker(pair_id, yf_ticker)
    articles      = _fetch(ticker_symbol)

    with _cache_lock:
        _cache[key] = {"ts": time.time(), "articles": articles}

    return articles


def invalidate(pair_id: str):
    """Force the next call to re-fetch, bypassing the cache."""
    with _cache_lock:
        _cache.pop(pair_id.upper(), None)
