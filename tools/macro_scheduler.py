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
    """Pre-warm pair card analysis — uses get_pair_card_analysis so cache key matches the route."""
    try:
        from tools.macro import get_pair_card_analysis
        result = get_pair_card_analysis(pair_id, force=True)
        log.info(f"[scheduler] {pair_id} analysis: {result.get('sentiment')} {result.get('confidence')}%")
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
