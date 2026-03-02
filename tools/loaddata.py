"""
tools/loaddata.py

Centralized data loader with WebSocket broadcast support.

This module:
  - Fetches data for all configured pairs in a background thread every 15 seconds
  - Caches the data in memory for instant access
  - Broadcasts updates to WebSocket subscribers when new data is available

Usage:
  from tools.loaddata import DataLoader
  loader = DataLoader()
  loader.start()

  # Get cached data (instant, no provider call)
  data = loader.get_data("US30", "1m")

  # Subscribe to updates (via SocketIO integration in mission_control.py)
"""

import time
from typing import Optional

# Use eventlet's green threading if available, otherwise fall back to standard threading
try:
    import eventlet
    from eventlet.green import threading
    USING_EVENTLET = True
except ImportError:
    import threading
    USING_EVENTLET = False

from config import PAIRS
from providers import get_df as _provider_get_df, get_bias_df as _provider_get_bias_df, LOCK as _YF_LOCK
from detectors import REGISTRY

FETCH_INTERVAL = 15  # seconds between fetches
PERIOD_MAP = {
    "1m":  "1d",
    "2m":  "1d",
    "5m":  "5d",
    "15m": "5d",
    "30m": "5d",
    "1h":  "30d",
}


class DataLoader:
    """Centralized data loader with background fetching and caching."""

    _instance: Optional["DataLoader"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "DataLoader":
        """Singleton pattern to ensure only one loader exists."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True

        # Cache: {pair_id: {interval: {candles, detectors, ts}}}
        self._cache: dict = {}
        self._cache_lock = threading.Lock()

        # Bias cache: {pair_id: {bias_data, ts}}
        self._bias_cache: dict = {}

        # WebSocket broadcast callback (set by mission_control.py)
        self._broadcast_callback = None

        # Background thread control
        self._running = False
        self._thread: Optional[threading.Thread] = None

        print("[DataLoader] Initialized")

    def set_broadcast_callback(self, callback):
        """Set the callback function for broadcasting updates via WebSocket."""
        self._broadcast_callback = callback
        print("[DataLoader] Broadcast callback registered")

    def start(self):
        """Start the background fetch thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._fetch_loop, daemon=True)
        self._thread.start()
        print("[DataLoader] Background fetch thread started (interval: {}s)".format(FETCH_INTERVAL))

    def stop(self):
        """Stop the background fetch thread."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        print("[DataLoader] Background fetch thread stopped")

    def _fetch_loop(self):
        """Background loop that fetches data for all pairs."""
        while self._running:
            try:
                self._fetch_all_pairs()
            except Exception as e:
                print(f"[DataLoader] Error in fetch loop: {e}")
            # Use eventlet.sleep if available for proper greenlet yielding
            if USING_EVENTLET:
                eventlet.sleep(FETCH_INTERVAL)
            else:
                time.sleep(FETCH_INTERVAL)

    def _fetch_all_pairs(self):
        """Fetch data for all configured pairs and intervals."""
        for pair_id, config in PAIRS.items():
            if not self._running:
                break
            try:
                self._fetch_pair_data(pair_id, config)
            except Exception as e:
                print(f"[DataLoader] Error fetching {pair_id}: {e}")

    def _fetch_pair_data(self, pair_id: str, config: dict):
        """Fetch data for a single pair."""
        import os
        _provider = os.environ.get("DATA_PROVIDER", "yahoo").lower().strip()
        if _provider == "metatrader":
            ticker = config.get("mt5_ticker") or config.get("yf_ticker") or config.get("ticker")
        else:
            ticker = config.get("yf_ticker") or config.get("ticker")

        if not ticker:
            return

        default_interval = config.get("default_interval", config.get("interval", "1m"))
        period = PERIOD_MAP.get(default_interval, config.get("period", "1d"))

        # Fetch the data
        with _YF_LOCK:
            df = _provider_get_df(ticker, default_interval, period)

        if df is None or df.empty:
            return

        # Convert to candles
        candles = []
        for idx, row in df.iterrows():
            ts = int(idx.timestamp())
            candles.append({
                "time": ts,
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": float(row.get("Volume", 0)),
            })

        # Run detectors
        detector_names = config.get("detectors", [])
        detector_params = config.get("detector_params", {})
        detector_results = {}

        for det_name in detector_names:
            det_cls = REGISTRY.get(det_name)
            if not det_cls:
                continue
            params = detector_params.get(det_name, {})
            try:
                det = det_cls(**params)
                result = det.detect(df)
                detector_results[det_name] = result
            except Exception as e:
                print(f"[DataLoader] Detector {det_name} error for {pair_id}: {e}")

        # Update cache
        with self._cache_lock:
            if pair_id not in self._cache:
                self._cache[pair_id] = {}
            self._cache[pair_id][default_interval] = {
                "candles": candles,
                "detectors": detector_results,
                "timestamp": time.time(),
            }

        # Broadcast update if callback is set
        if self._broadcast_callback:
            try:
                self._broadcast_callback(pair_id, default_interval, {
                    "candles": candles,
                    "detectors": detector_results,
                })
            except Exception as e:
                print(f"[DataLoader] Broadcast error for {pair_id}: {e}")

    def get_data(self, pair_id: str, interval: str) -> Optional[dict]:
        """
        Get cached data for a pair/interval.
        Returns None if no data is cached.
        """
        with self._cache_lock:
            pair_cache = self._cache.get(pair_id.upper(), {})
            return pair_cache.get(interval)

    def get_all_cached_pairs(self) -> list:
        """Get list of all pairs with cached data."""
        with self._cache_lock:
            return list(self._cache.keys())


# Global loader instance
_loader: Optional[DataLoader] = None


def get_loader() -> DataLoader:
    """Get or create the global DataLoader instance."""
    global _loader
    if _loader is None:
        _loader = DataLoader()
    return _loader
