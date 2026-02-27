"""
news.py

Fetches recent news headlines for a trading pair using yfinance, with
sentiment analysis powered by FinBERT (ProsusAI/finbert) — a BERT model
fine-tuned on financial text.

FinBERT is loaded once on first use and kept in memory. First load takes
~3-5 seconds while the model is downloaded (~440MB, cached to
~/.cache/huggingface after first run).

Usage:
    from news import get_news
    articles = get_news("GBPUSD")

Install deps on your server:
    pip install transformers torch --break-system-packages
"""

import time
import threading
import yfinance as yf

# ── Config ─────────────────────────────────────────────────────────────────────
NEWS_TTL_SECONDS = 300      # cache lifetime — 5 minutes
MAX_ARTICLES     = 8
FINBERT_MODEL    = "ProsusAI/finbert"

# ── Pair → yfinance ticker map ─────────────────────────────────────────────────
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

# ── FinBERT — lazy-loaded singleton ───────────────────────────────────────────
_finbert_lock     = threading.Lock()
_finbert_pipeline = None   # loaded on first use
_finbert_failed   = False  # True if transformers/torch unavailable → keyword fallback


def _load_finbert():
    global _finbert_pipeline, _finbert_failed

    if _finbert_pipeline is not None or _finbert_failed:
        return

    with _finbert_lock:
        if _finbert_pipeline is not None or _finbert_failed:
            return
        try:
            from transformers import pipeline as hf_pipeline
            print("[news] Loading FinBERT model (first run — may take a moment)…")
            _finbert_pipeline = hf_pipeline(
                task="text-classification",
                model=FINBERT_MODEL,
                tokenizer=FINBERT_MODEL,
                device=-1,       # CPU — GPU not required for short headlines
                truncation=True,
                max_length=512,
            )
            print("[news] FinBERT loaded ✓")
        except Exception as e:
            print(f"[news] FinBERT unavailable — falling back to keyword sentiment: {e}")
            _finbert_failed = True


def _finbert_sentiment(titles: list[str]) -> list[str]:
    """
    Batch-score headlines with FinBERT.
    FinBERT labels: positive → bullish, negative → bearish, neutral → neutral.
    Falls back to keyword matching if FinBERT isn't installed.
    """
    _load_finbert()

    if _finbert_pipeline is None:
        return [_keyword_sentiment(t) for t in titles]

    try:
        results = _finbert_pipeline(titles, batch_size=len(titles))
        mapping = {"positive": "bullish", "negative": "bearish", "neutral": "neutral"}
        return [mapping.get(r["label"].lower(), "neutral") for r in results]
    except Exception as e:
        print(f"[news] FinBERT inference error: {e}")
        return [_keyword_sentiment(t) for t in titles]


# ── Keyword fallback (used only when transformers/torch not installed) ─────────
_BULLISH_WORDS = {
    "surge", "surges", "surging", "rally", "rallies", "rallying", "rise",
    "rises", "rising", "gain", "gains", "gaining", "jump", "jumps",
    "jumping", "climb", "climbs", "climbing", "boost", "boosts", "boosting",
    "strengthen", "strengthens", "strengthening", "high", "highs", "record",
    "optimism", "bullish", "upside", "upgrade", "beat", "beats",
    "strong", "strength", "positive", "growth", "recover", "recovery",
    "hawkish",
}

_BEARISH_WORDS = {
    "fall", "falls", "falling", "drop", "drops", "dropping", "decline",
    "declines", "declining", "sink", "sinks", "sinking", "slide", "slides",
    "sliding", "tumble", "tumbles", "tumbling", "plunge", "plunges",
    "plunging", "weak", "weakness", "weakens", "loss", "losses", "low",
    "lows", "bearish", "downside", "downgrade", "miss", "misses",
    "concern", "concerns", "risk", "risks", "uncertain", "uncertainty",
    "slowdown", "recession", "inflation", "dovish", "cut", "cuts",
    "pressure", "pressured",
}


def _keyword_sentiment(title: str) -> str:
    words = set(title.lower().split())
    bull  = len(words & _BULLISH_WORDS)
    bear  = len(words & _BEARISH_WORDS)
    if bull > bear:
        return "bullish"
    if bear > bull:
        return "bearish"
    return "neutral"


# ── Age label ─────────────────────────────────────────────────────────────────

def _age_label(publish_ts: int) -> str:
    diff = int(time.time()) - publish_ts
    if diff < 60:
        return "just now"
    if diff < 3600:
        return f"{diff // 60}m ago"
    if diff < 86400:
        return f"{diff // 3600}h ago"
    return f"{diff // 86400}d ago"


# ── yfinance fetch ────────────────────────────────────────────────────────────

def _fetch(ticker_symbol: str) -> list[dict]:
    try:
        raw = yf.Ticker(ticker_symbol).news or []
    except Exception as e:
        print(f"[news] yfinance fetch error for {ticker_symbol}: {e}")
        return []

    if not raw:
        return []

    normalised = []
    for item in raw[:MAX_ARTICLES]:
        try:
            if "content" in item and isinstance(item["content"], dict):
                # Newer yfinance API structure
                content   = item["content"]
                title     = content.get("title", "")
                publisher = (
                    content.get("provider", {}).get("displayName", "")
                    if isinstance(content.get("provider"), dict)
                    else content.get("provider", "")
                )
                link = (
                    content.get("canonicalUrl", {}).get("url", "")
                    if isinstance(content.get("canonicalUrl"), dict)
                    else content.get("url", "")
                )
                pub_ts = content.get("pubDate", 0)
                if isinstance(pub_ts, str):
                    import datetime
                    try:
                        pub_ts = int(
                            datetime.datetime.fromisoformat(
                                pub_ts.replace("Z", "+00:00")
                            ).timestamp()
                        )
                    except Exception:
                        pub_ts = 0
            else:
                # Older / flat yfinance structure
                title     = item.get("title", "")
                publisher = item.get("publisher", "")
                link      = item.get("link", "")
                pub_ts    = int(item.get("providerPublishTime", 0))

            if title:
                normalised.append({
                    "title":     title[:300],
                    "publisher": publisher[:80],
                    "link":      link,
                    "ts":        pub_ts,
                })
        except Exception:
            continue

    if not normalised:
        return []

    titles     = [n["title"] for n in normalised]
    sentiments = _finbert_sentiment(titles)

    return [
        {
            "headline":  n["title"],
            "source":    n["publisher"],
            "link":      n["link"],
            "sentiment": s,
            "age":       _age_label(n["ts"]) if n["ts"] else "",
            "ts":        n["ts"],
        }
        for n, s in zip(normalised, sentiments)
    ]


# ── In-memory cache ───────────────────────────────────────────────────────────

_cache:      dict = {}
_cache_lock        = threading.Lock()


def _resolve_ticker(pair_id: str, yf_ticker: str | None = None) -> str:
    return yf_ticker or PAIR_TICKER_MAP.get(pair_id.upper(), pair_id)


def get_news(pair_id: str, yf_ticker: str | None = None) -> list[dict]:
    """
    Return cached news for a pair, refreshing if stale.

    Args:
        pair_id:   e.g. "GBPUSD", "XAUUSD"
        yf_ticker: optional ticker override — uses PAIR_TICKER_MAP if omitted

    Returns list of dicts:
        headline  str  — article title
        source    str  — publisher name
        link      str  — article URL
        sentiment str  — 'bullish' | 'bearish' | 'neutral'  (FinBERT)
        age       str  — '2h ago', '35m ago', etc.
        ts        int  — unix publish timestamp
    """
    key = pair_id.upper()

    with _cache_lock:
        cached = _cache.get(key)
        if cached and (time.time() - cached["ts"]) < NEWS_TTL_SECONDS:
            return cached["articles"]

    articles = _fetch(_resolve_ticker(pair_id, yf_ticker))

    with _cache_lock:
        _cache[key] = {"ts": time.time(), "articles": articles}

    return articles


def invalidate(pair_id: str):
    """Force the next call to re-fetch, bypassing the TTL cache."""
    with _cache_lock:
        _cache.pop(pair_id.upper(), None)


def warmup():
    """
    Pre-load FinBERT at startup so the first news request is instant.
    Call this in a background thread from mission_control.py:

        import threading
        from news import warmup as news_warmup
        threading.Thread(target=news_warmup, daemon=True).start()
    """
    _load_finbert()
