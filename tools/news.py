"""
tools/news.py
Enhanced News Fetcher with Weighted Sentiment Analysis and Recency Bias.
"""

import time
import threading
import yfinance as yf
import datetime

# ── Config ─────────────────────────────────────────────────────────────────────
NEWS_TTL_SECONDS = 300
MAX_ARTICLES     = 10

# ── Weighted Sentiment Dictionary ──────────────────────────────────────────────
# Higher numbers = Higher conviction/intensity of the move
_SENTIMENT_WEIGHTS = {
    # Bullish
    "surge": 3, "rally": 3, "rocket": 4, "breakout": 3, "climb": 1, 
    "gain": 1, "jump": 2, "bullish": 2, "soar": 3, "highs": 2,
    "recovery": 2, "optimism": 1, "hawkish": 2, "beat": 2,
    
    # Bearish
    "crash": 4, "tumble": 3, "plunge": 3, "sink": 2, "drop": 1,
    "fall": 1, "bearish": 2, "slide": 2, "lows": 2, "recession": 3,
    "dovish": 2, "miss": 2, "fears": 2, "sink": 3, "selloff": 3
}

PAIR_TICKER_MAP = {
    "US30": "YM=F", "US100": "NQ=F", "XAUUSD": "GC=F", "EURUSD": "EURUSD=X",
    "GBPUSD": "GBPUSD=X", "BTCUSD": "BTC-USD", "ETHUSD": "ETH-USD", "SPX500": "^GSPC"
}

_cache = {}
_cache_lock = threading.Lock()

def _analyze_headline(title: str) -> dict:
    """Calculates weighted sentiment for a single headline."""
    words = title.lower().replace(":", "").replace("?", "").split()
    score = 0
    for word in words:
        score += _SENTIMENT_WEIGHTS.get(word, 0)
    
    label = "neutral"
    if score >= 2: label = "bullish"
    elif score <= -2: label = "bearish"
    
    return {"label": label, "score": score}

def _age_label(publish_ts: int) -> str:
    diff = int(time.time()) - publish_ts
    if diff < 3600: return f"{diff // 60}m ago"
    if diff < 86400: return f"{diff // 3600}h ago"
    return f"{diff // 86400}d ago"

def _fetch_and_summarize(ticker_symbol: str) -> dict:
    """Fetches news and generates a net sentiment summary."""
    try:
        ticker = yf.Ticker(ticker_symbol)
        raw = ticker.news or []
    except Exception as e:
        print(f"[news] Fetch error: {e}")
        return {"summary": {"sentiment": "neutral", "score": 0}, "articles": []}

    articles = []
    total_net_score = 0
    
    for item in raw[:MAX_ARTICLES]:
        # Handle different yfinance versions
        content = item.get("content", item)
        title = content.get("title", "")
        pub_ts = content.get("providerPublishTime", content.get("pubDate", 0))
        
        if isinstance(pub_ts, str):
            try:
                pub_ts = int(datetime.datetime.fromisoformat(pub_ts.replace("Z", "+00:00")).timestamp())
            except: pub_ts = 0

        analysis = _analyze_headline(title)
        
        # Apply Recency Decay: News older than 4 hours has 50% impact
        age_seconds = time.time() - pub_ts
        weight = 1.0 if age_seconds < 14400 else 0.5
        total_net_score += (analysis["score"] * weight)

        articles.append({
            "headline": title,
            "source": content.get("publisher", content.get("provider", "")),
            "sentiment": analysis["label"],
            "score": analysis["score"],
            "age": _age_label(pub_ts),
            "link": content.get("link", content.get("url", ""))
        })

    # Generate Summary
    summary_label = "neutral"
    if total_net_score > 3: summary_label = "bullish"
    elif total_net_score < -3: summary_label = "bearish"

    return {
        "summary": {
            "sentiment": summary_label,
            "net_score": round(total_net_score, 2),
            "article_count": len(articles)
        },
        "articles": articles
    }

def get_news(pair_id: str) -> dict:
    """Entry point with caching (reverted name to fix ImportError)."""
    key = pair_id.upper()
    with _cache_lock:
        if key in _cache and (time.time() - _cache[key]["ts"]) < NEWS_TTL_SECONDS:
            return _cache[key]["data"]

    ticker = PAIR_TICKER_MAP.get(key, key)
    data = _fetch_and_summarize(ticker)

    with _cache_lock:
        _cache[key] = {"ts": time.time(), "data": data}
    return data
