"""
tools/macro_scheduler.py

Background scheduler that pre-warms pair-specific macro caches
so the frontend never waits for AI calls on page load.

What gets warmed per pair:
  1. get_pair_all_modules(pair_id)  → /api/macro/pair/<id>/modules
     (mood, policy, flow, bearing, pulse — all pair-specific)
  2. The /api/macro/pair/<id> route runs inline AI, but its result is
     cached via tools.macro._cache — so warming get_pair_all_modules
     is sufficient since pair/<id> is fast (single AI call per pair).

Schedule:
  - All pair modules : every 60 min + immediately on startup
  - Market snapshot  : every 5 min (fast yfinance, no AI)

Usage in mission_control.py:
    from tools.macro_scheduler import start as start_macro_scheduler
    start_macro_scheduler()
"""

import threading
import time
import logging

log = logging.getLogger(__name__)


def _get_config_pairs() -> list[str]:
    try:
        from config import PAIRS
        return list(PAIRS.keys())
    except ImportError:
        return []


def _warm_pair_modules(pair_id: str) -> None:
    """
    Pre-warm ALL pair-specific modules for one pair:
      - get_pair_mood, get_pair_policy, get_pair_flow,
        get_pair_bearing, get_pair_pulse  (via get_pair_all_modules)
    These map directly to /api/macro/pair/<id>/modules
    """
    try:
        from tools.macro import get_pair_all_modules
        log.info(f"[scheduler] warming {pair_id} modules")
        result = get_pair_all_modules(pair_id, force=True)
        labels = {k: v.get("label", "?") for k, v in result.items() if v}
        log.info(f"[scheduler] {pair_id} done: {labels}")
    except Exception as e:
        log.error(f"[scheduler] error warming {pair_id}: {e}", exc_info=True)


def _warm_pair_analysis(pair_id: str) -> None:
    """
    Pre-warm the pair AI analysis (text + sentiment + confidence).
    Maps to /api/macro/pair/<id>
    Uses the same _cache in tools.macro via cache_key = f"analysis_{pair_id}"
    """
    try:
        from tools.ai     import ask
        from tools.market import get_market_snapshot, INSTRUMENTS
        from tools.news   import get_news
        from tools.macro  import _cached, _set_cache, _pair_context
        import json, re

        cache_key = f"pair_analysis_{pair_id}"
        # Already fresh — skip
        if _cached(cache_key):
            return

        meta  = INSTRUMENTS.get(pair_id, {})
        snap  = get_market_snapshot()
        d     = snap.get(pair_id, {})
        price = d.get("last")
        chg_p = d.get("change_p")

        price_ctx = ""
        if price is not None:
            sign = "+" if (chg_p or 0) >= 0 else ""
            price_ctx = f"{pair_id} ({meta.get('label', pair_id)}) is at {price} ({sign}{chg_p:.2f}% today)."

        news_items = get_news(pair_id, yf_ticker=meta.get("sym"))
        news_ctx = ""
        if news_items:
            headlines = "\n".join(f"- {n['headline']}" for n in news_items[:5])
            news_ctx  = f"\n\nRecent headlines:\n{headlines}"

        prompt = (
            f"You are a macro FX/trading analyst. Analyze {pair_id} specifically:\n\n"
            f"{price_ctx}{news_ctx}\n\n"
            "Respond ONLY with valid JSON (no markdown):\n"
            '{\n'
            '  "text": "<2-3 sentence analysis citing price action, key driver, what to watch>",\n'
            '  "sentiment": "<Bullish | Bearish | Neutral>",\n'
            '  "confidence": <integer 50-95>\n'
            '}'
        )

        raw  = ask(prompt, max_tokens=300, temperature=0.3)
        cleaned = re.sub(r"^```[a-z]*\n?", "", raw.strip())
        cleaned = re.sub(r"\n?```$", "", cleaned)
        data = json.loads(cleaned)

        result = {
            "text":       data.get("text", ""),
            "sentiment":  data.get("sentiment", "Neutral"),
            "confidence": int(data.get("confidence", 70)),
            "news":       news_items[:5],
        }
        _set_cache(cache_key, result)
        log.info(f"[scheduler] {pair_id} analysis done: {result['sentiment']}, {result['confidence']}%")
    except Exception as e:
        log.error(f"[scheduler] analysis error {pair_id}: {e}", exc_info=True)


def _warm_snapshot() -> None:
    try:
        from tools.market import get_market_snapshot
        get_market_snapshot(force=True)
    except Exception as e:
        log.error(f"[scheduler] snapshot error: {e}", exc_info=True)


def _warm_all_pairs(force: bool = False) -> None:
    """Warm both analysis + modules for every config pair, staggered."""
    pairs = _get_config_pairs()
    log.info(f"[scheduler] warming {len(pairs)} pairs: {pairs}")

    for pair_id in pairs:
        # Run analysis + modules for this pair
        _warm_pair_analysis(pair_id)
        _warm_pair_modules(pair_id)
        time.sleep(2)   # stagger AI calls to avoid rate limits

    log.info("[scheduler] all pairs warmed")


def _run_scheduler() -> None:
    """Main scheduler loop — runs in a background daemon thread."""
    log.info("[scheduler] starting")

    PAIR_INTERVAL     = 60 * 60    # 60 min
    SNAPSHOT_INTERVAL =  5 * 60   #  5 min

    # Initial warm-up — run immediately on startup in a thread
    warmup_thread = threading.Thread(
        target=lambda: [_warm_snapshot(), _warm_all_pairs(force=True)],
        daemon=True,
        name="macro-warmup"
    )
    warmup_thread.start()

    last_pair_run     = time.time()
    last_snapshot_run = time.time()

    while True:
        time.sleep(60)
        now = time.time()

        if now - last_snapshot_run >= SNAPSHOT_INTERVAL:
            last_snapshot_run = now
            threading.Thread(target=_warm_snapshot, daemon=True).start()

        if now - last_pair_run >= PAIR_INTERVAL:
            last_pair_run = now
            threading.Thread(
                target=_warm_all_pairs,
                kwargs={"force": True},
                daemon=True,
                name="macro-hourly-refresh"
            ).start()


_started = False
_lock    = threading.Lock()


def start() -> None:
    """
    Start the background pre-warm scheduler (idempotent).
    Call once from mission_control.py after imports.
    """
    global _started
    with _lock:
        if _started:
            return
        _started = True

    t = threading.Thread(target=_run_scheduler, daemon=True, name="macro-scheduler")
    t.start()
    log.info("[scheduler] macro scheduler thread started")
