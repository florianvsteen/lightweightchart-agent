"""
server.py

PairServer — a self-contained Flask server instance for a single trading pair.
Each pair runs in its own thread on its own port.

Detection runs in a background thread every 30 seconds — completely independent
of whether anyone has the browser open. Discord alerts fire from there.
The Flask routes only serve chart data to the browser when it's open.
"""

import os
import json
import time
import threading
import pandas as pd
import yfinance as yf
from flask import Flask, render_template, jsonify, request

from detectors import REGISTRY

try:
    from discord_webhook import DiscordWebhook, DiscordEmbed
    DISCORD_AVAILABLE = True
except ImportError:
    DISCORD_AVAILABLE = False

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL')

# ── Debug page HTML ─────────────────────────────────────────────────────────
# Global lock — yfinance has shared internal state and returns wrong data
# when multiple tickers download simultaneously across threads.
_YF_LOCK = threading.Lock()

PERIOD_MAP = {
    "1m":  "1d",
    "2m":  "1d",
    "5m":  "5d",
    "15m": "5d",
    "30m": "5d",
    "1h":  "30d",
}

# How often the background detector loop runs (seconds)
DETECTION_INTERVAL = 30


class PairServer:

    def __init__(self, pair_id: str, config: dict):
        self.pair_id = pair_id
        self.ticker = config["ticker"]
        self.port = config["port"]
        self.label = config["label"]
        self.interval = config.get("interval", "1m")
        self.period = config.get("period", "1d")
        self.detector_names = config.get("detectors", [])
        self.detector_params = config.get("detector_params", {})
        self.default_interval = config.get("default_interval", self.interval)
        self.always_open = config.get("always_open", False)

        # Alert dedup — persisted to disk so restarts don't re-fire old alerts
        self._alerted_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f".alerted_{pair_id}.json"
        )
        self.last_alerted: dict[str, int] = self._load_alerted()
        self.last_active_zone: dict[str, dict] = {}

        # Per-request DataFrame cache (cleared each cycle)
        self._df_cache: dict[str, pd.DataFrame] = {}
        self._cache_lock = threading.Lock()

        self._detection_lock = threading.Lock()
        self._stagger_seconds = 0  # set by app.py before run()

        root = os.path.dirname(os.path.abspath(__file__))
        self.app = Flask(
            __name__,
            template_folder=os.path.join(root, "templates"),
            static_folder=os.path.join(root, "static") if os.path.exists(os.path.join(root, "static")) else None,
        )
        self._register_routes()

    # ------------------------------------------------------------------ #
    # Routes
    # ------------------------------------------------------------------ #

    def _register_routes(self):
        app = self.app
        pair_id = self.pair_id

        def _index():
            tz = os.environ.get("TZ", "UTC")
            return render_template("index.html", pair_id=pair_id, label=self.label, port=self.port, timezone=tz, default_interval=self.default_interval)
        _index.__name__ = f"index_{pair_id}"
        app.route("/")(_index)

        def _get_data():
            return self._api_data()
        _get_data.__name__ = f"get_data_{pair_id}"
        app.route("/api/data")(_get_data)

        def _test_alert():
            return self._test_alert()
        _test_alert.__name__ = f"test_alert_{pair_id}"
        app.route("/test-alert")(_test_alert)

        def _debug():
            return self._debug()
        _debug.__name__ = f"debug_{pair_id}"
        app.route("/debug")(_debug)

        def _debug_data():
            return self._debug_data()
        _debug_data.__name__ = f"debug_data_{pair_id}"
        app.route("/debug/data")(_debug_data)

        def _debug_replay():
            return self._debug_replay()
        _debug_replay.__name__ = f"debug_replay_{pair_id}"
        app.route("/debug/replay")(_debug_replay)

        def _debug_sd():
            return self._debug_sd()
        _debug_sd.__name__ = f"debug_sd_{pair_id}"
        app.route("/debug/sd")(_debug_sd)

        def _debug_fvg():
            return self._debug_fvg()
        _debug_fvg.__name__ = f"debug_fvg_{pair_id}"
        app.route("/debug/fvg")(_debug_fvg)

    # ------------------------------------------------------------------ #
    # Data fetching
    # ------------------------------------------------------------------ #

    def _fetch_df(self, interval: str) -> pd.DataFrame:
        period = PERIOD_MAP.get(interval, self.period)
        with _YF_LOCK:  # serialize all yfinance downloads process-wide
            df = yf.download(self.ticker, period=period, interval=interval, progress=False)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df.dropna()

    def _get_df(self, interval: str, cache: dict) -> pd.DataFrame:
        """Return cached DataFrame for this interval within a single cycle."""
        if interval not in cache:
            cache[interval] = self._fetch_df(interval)
        return cache[interval]

    # ------------------------------------------------------------------ #
    # Detection (shared by background loop and browser API)
    # ------------------------------------------------------------------ #

    def _run_detectors(self, cache: dict) -> dict:
        """Run all detectors using their configured timeframes. Returns results dict."""
        results = {}
        for name in self.detector_names:
            params = dict(self.detector_params.get(name, {}))
            detector_interval = params.pop("timeframe", "1m")
            df = self._get_df(detector_interval, cache)
            fn = REGISTRY.get(name)
            if fn is None:
                print(f"[WARN] Detector '{name}' not found in registry.")
                results[name] = None
            else:
                try:
                    # Pass yf_lock to detectors that do their own downloads (supply_demand)
                    if name == "supply_demand":
                        params["yf_lock"] = _YF_LOCK
                    results[name] = fn(df, **params)
                except Exception as e:
                    print(f"[ERROR] Detector '{name}' failed: {e}")
                    results[name] = None
        return results

    def _process_alerts(self, detector_results: dict):
        """Check results and fire Discord alerts on breakout."""
        # Never send alerts during the weekend market halt (Fri 23:00 – Mon 01:00 UTC)
        from detectors.accumulation import is_weekend_halt
        if is_weekend_halt(always_open=self.always_open):
            return

        for name, result in detector_results.items():

            # ── Accumulation ──────────────────────────────────────────
            if name == "accumulation":
                # Clean up alerted timestamps older than 4 hours
                cutoff = int(time.time()) - (4 * 3600)
                if name in self.last_alerted and isinstance(self.last_alerted[name], int):
                    if self.last_alerted[name] < cutoff:
                        del self.last_alerted[name]
                        self._save_alerted()

                prev = self.last_active_zone.get(name)
                zone = result if (result and isinstance(result, dict)) else None
                is_active_found = (
                    zone is not None
                    and zone.get("is_active")
                    and zone.get("status") == "active"
                )

                # ── State machine ──────────────────────────────────────
                # looking   → active    (zone formed, breakout candle still inside)
                # active    → confirmed (impulsive breakout — alert dispatched)
                # confirmed → looking   (next cycle, reset)
                prev_status = (prev or {}).get("status")

                if is_active_found:
                    # Zone alive — keep tracking
                    zone_start = zone["start"]
                    already_alerted = self.last_alerted.get(name, 0)
                    if zone_start != already_alerted:
                        self.last_active_zone[name] = zone

                elif prev_status == "active" and (
                    zone is None
                    or not zone.get("is_active")
                    or zone.get("status") in ("looking", "confirmed")
                ):
                    # Breakout detected this cycle. Mark "confirmed" so the browser
                    # still renders the box for one more cycle while the screenshot runs.
                    zone_start = prev["start"]
                    already_alerted = self.last_alerted.get(name, 0)
                    if zone_start != already_alerted:
                        confirmed_zone = dict(prev)
                        confirmed_zone["status"] = "confirmed"
                        self.last_active_zone[name] = confirmed_zone
                        self.last_alerted[name] = zone_start
                        self._save_alerted()
                        threading.Thread(
                            target=self._send_discord_alert,
                            args=(confirmed_zone,),
                            daemon=True,
                        ).start()

                elif prev_status == "confirmed":
                    # Screenshot was dispatched last cycle — now truly reset
                    self.last_active_zone[name] = None

            # ── Supply & Demand ───────────────────────────────────────
            elif name == "supply_demand":
                if not result or not isinstance(result, dict):
                    continue
                zones = result.get("zones", [])
                curr_active = {z["start"] for z in zones if z.get("is_active")}
                prev_starts = set(self.last_active_zone.get(name + "_starts", []))

                # Remove invalidated zones from last_alerted so they can re-fire if they return
                invalidated = prev_starts - curr_active
                changed = False
                for start_ts in invalidated:
                    key = f"{name}_{start_ts}"
                    if key in self.last_alerted:
                        del self.last_alerted[key]
                        changed = True
                        print(f"[{self.pair_id}] Removed invalidated zone {key} from alerted state")
                if changed:
                    self._save_alerted()

                # Alert only once per zone (keyed by start timestamp)
                for z in zones:
                    if not z.get("is_active"):
                        continue
                    start_ts = z["start"]
                    alert_key = f"{name}_{start_ts}"
                    if self.last_alerted.get(alert_key):
                        continue
                    self.last_alerted[alert_key] = 1
                    self._save_alerted()
                    alert_zone = {
                        "detector": z.get("type", "supply_demand"),
                        "start":    start_ts,
                        "end":      z["end"],
                    }
                    threading.Thread(
                        target=self._send_discord_alert,
                        args=(alert_zone,),
                        daemon=True,
                    ).start()

                self.last_active_zone[name + "_starts"] = list(curr_active)

    # ------------------------------------------------------------------ #
    # Background detection loop — runs regardless of browser
    # ------------------------------------------------------------------ #

    def _detection_loop(self):
        # Stagger startup so pairs don't all hit yfinance simultaneously
        if self._stagger_seconds:
            time.sleep(self._stagger_seconds)
        print(f"[{self.pair_id}] Background detector started (every {DETECTION_INTERVAL}s)")
        while True:
            try:
                with self._detection_lock:
                    cache = {}
                    results = self._run_detectors(cache)
                    self._process_alerts(results)
                print(f"[{self.pair_id}] Detection cycle complete: {list(results.keys())}")
            except Exception as e:
                print(f"[{self.pair_id}] Detection loop error: {e}")
            time.sleep(DETECTION_INTERVAL)

    # ------------------------------------------------------------------ #
    # Flask API — serves chart data to browser when open
    # ------------------------------------------------------------------ #

    def _api_data(self):
        try:
            chart_interval = request.args.get("interval", self.interval)
            cache = {}

            # Run detectors fresh for the browser response
            detector_results = self._run_detectors(cache)

            # If a "confirmed" zone is held in state (breakout just detected,
            # screenshot in-flight), override the fresh result so the browser
            # still renders the box for one cycle while Playwright screenshots it.
            for det_name in self.detector_names:
                if det_name == "accumulation":
                    held = self.last_active_zone.get(det_name)
                    if held and held.get("status") == "confirmed":
                        detector_results[det_name] = held

            # Fetch chart candles at the requested interval
            df_chart = self._get_df(chart_interval, cache)
            candles = [
                {
                    "time": int(idx.timestamp()),
                    "open": float(r["Open"]),
                    "high": float(r["High"]),
                    "low": float(r["Low"]),
                    "close": float(r["Close"]),
                }
                for idx, r in df_chart.iterrows()
            ]

            return jsonify({
                "pair": self.pair_id,
                "label": self.label,
                "candles": candles,
                "detectors": detector_results,
            })

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    def _debug(self):
        """Rich debug page — served from templates/debug.html"""
        tz = os.environ.get("TZ", "Europe/Brussels")
        return render_template("debug.html",
            pair_id=self.pair_id,
            label=self.label,
            timezone=tz,
        )

    def _debug_data(self):
        """Return detailed rejection analysis JSON for the debug page."""
        try:
            import numpy as np
            from detectors.accumulation import (
                get_current_session, _slope_pct, _choppiness, _adx
            )

            interval = request.args.get("interval", "1m")
            cache = {}
            df = self._get_df(interval, cache)

            if df is None or len(df) < 5:
                return jsonify({
                    "pair": self.pair_id, "session": None, "effective_range": None,
                    "adx_threshold": None, "last_close": None,
                    "windows_checked": 0, "passed": 0,
                    "rejection_summary": {}, "windows": [], "best_zone": None,
                    "secondary_zone": None, "candles": [],
                    "error": "No data available (market closed or download failed)",
                })

            params        = dict(self.detector_params.get("accumulation", {}))
            params.pop("timeframe", None)
            lookback      = params.get("lookback", 40)
            min_candles   = params.get("min_candles", 20)
            adx_threshold = params.get("adx_threshold", 25)
            threshold_pct = params.get("threshold_pct", 0.003)

            session = get_current_session()
            session_range_key = f"{session}_range_pct" if session else None
            effective_range_pct = params.get(session_range_key) or params.get("max_range_pct")

            if isinstance(df.columns, __import__('pandas').MultiIndex):
                df = df.copy()
                df.columns = df.columns.get_level_values(0)
            df = df.loc[:, ~df.columns.duplicated()].copy()
            for col in ['Open','High','Low','Close']:
                df[col] = __import__('pandas').to_numeric(df[col].squeeze(), errors='coerce')
            df = df.dropna(subset=['Open','High','Low','Close'])

            last_closed_idx = len(df) - 2
            scan_start      = max(0, len(df) - lookback)

            last_closed_open  = float(df['Open'].iloc[-2])
            last_closed_close = float(df['Close'].iloc[-2])
            last_body_high    = max(last_closed_open, last_closed_close)
            last_body_low     = min(last_closed_open, last_closed_close)

            # Export candle data for the chart
            candles = [
                {
                    "time":  int(idx.timestamp()),
                    "open":  float(r["Open"]),
                    "high":  float(r["High"]),
                    "low":   float(r["Low"]),
                    "close": float(r["Close"]),
                }
                for idx, r in df.iterrows()
            ]

            windows = []
            for window_size in range(min_candles, lookback + 1):
                slope_limit = (threshold_pct * 0.10) / window_size
                i = last_closed_idx - window_size + 1
                if i < 0 or i < scan_start:
                    windows.append({"window": window_size, "skip": "out of scan range"})
                    continue

                window = df.iloc[i: i + window_size]
                closes = window['Close'].values.flatten().astype(float)
                opens  = window['Open'].values.flatten().astype(float)
                highs  = window['High'].values.flatten().astype(float)
                lows   = window['Low'].values.flatten().astype(float)

                avg_p = closes.mean()
                body_highs = np.maximum(opens, closes)
                body_lows  = np.minimum(opens, closes)
                h_max = float(body_highs.max())
                l_min = float(body_lows.min())
                range_pct = round((h_max - l_min) / avg_p, 6)
                slope     = round(_slope_pct(closes, avg_p), 8)
                chop      = round(_choppiness(closes), 4)
                adx_val   = _adx(highs, lows, closes)
                is_active = (last_body_low >= l_min) and (last_body_high <= h_max)

                reject = None
                if effective_range_pct and range_pct > effective_range_pct:
                    reject = f"range {range_pct} > limit {effective_range_pct}"
                elif slope >= slope_limit:
                    reject = f"slope {slope} >= limit {round(slope_limit,8)}"
                elif adx_val is not None and adx_val > adx_threshold:
                    reject = f"adx {round(adx_val,2)} > {adx_threshold}"
                elif chop < 0.36:
                    reject = f"chop {chop} < 0.36"

                windows.append({
                    "window":      window_size,
                    "start_ts":    int(df.index[i].timestamp()),
                    "end_ts":      int(df.index[i + window_size - 1].timestamp()),
                    "top":         round(h_max, 5),
                    "bottom":      round(l_min, 5),
                    "range_pct":   range_pct,
                    "range_limit": effective_range_pct,
                    "slope":       slope,
                    "slope_limit": round(slope_limit, 8),
                    "chop":        chop,
                    "adx":         round(adx_val, 2) if adx_val is not None else None,
                    "adx_limit":   adx_threshold,
                    "is_active":   is_active,
                    "reject":      reject,
                    "pass":        reject is None,
                })

            passed   = [w for w in windows if w.get("pass")]
            rejected = [w for w in windows if not w.get("pass") and "skip" not in w]
            reasons  = {}
            for r in rejected:
                key = r["reject"].split(" ")[0] if r.get("reject") else "unknown"
                reasons[key] = reasons.get(key, 0) + 1

            # Best zone: ADX<10 preferred, then lowest slope. Secondary = runner-up.
            def _rank_windows(ws):
                low_adx = sorted(
                    [w for w in ws if w.get("adx") is not None and w["adx"] < 10],
                    key=lambda w: w["slope"]
                )
                rest = sorted(
                    [w for w in ws if not (w.get("adx") is not None and w["adx"] < 10)],
                    key=lambda w: w["slope"]
                )
                return low_adx + rest

            ranked = _rank_windows(passed)
            best_zone      = ranked[0] if ranked else None
            secondary_zone = ranked[1] if len(ranked) > 1 else None
            return jsonify({
                "pair":              self.pair_id,
                "session":           session,
                "effective_range":   effective_range_pct,
                "adx_threshold":     adx_threshold,
                "last_close":        round(float(df['Close'].iloc[-2]), 5),
                "windows_checked":   len([w for w in windows if "skip" not in w]),
                "passed":            len(passed),
                "rejection_summary": reasons,
                "windows":           windows,
                "best_zone":         best_zone,
                "secondary_zone":    secondary_zone,
                "candles":           candles,
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    def _debug_replay(self):
        """
        Run the accumulation detector against only the first `idx` candles.
        Query param: idx=N (1-based candle index to replay up to)
        """
        # Read query param immediately while Flask request context is guaranteed active
        try:
            raw_idx = int(request.args.get("idx", -1))
        except Exception:
            raw_idx = -1

        try:
            import numpy as np
            import pandas as pd
            from datetime import timezone
            from detectors.accumulation import _slope_pct, _choppiness, _adx

            acquired = _YF_LOCK.acquire(timeout=10)
            try:
                full_df = yf.download(self.ticker, period=self.period, interval="1m", progress=False)
            finally:
                if acquired:
                    _YF_LOCK.release()

            if isinstance(full_df.columns, pd.MultiIndex):
                full_df.columns = full_df.columns.get_level_values(0)
            full_df = full_df.dropna()

            if full_df is None or len(full_df) < 5:
                return jsonify({
                    "idx": 0, "total": 0, "session": None, "effective_range": None,
                    "adx_threshold": None, "last_close": None,
                    "windows_checked": 0, "passed": 0,
                    "rejection_summary": {}, "windows": [], "best_zone": None,
                    "secondary_zone": None, "breakout_candle": None, "candles": [],
                    "error": "No data available (market closed or download failed)",
                })

            params        = dict(self.detector_params.get("accumulation", {}))
            params.pop("timeframe", None)
            lookback      = params.get("lookback", 40)
            min_candles   = params.get("min_candles", 15)
            adx_threshold = params.get("adx_threshold", 25)
            threshold_pct = params.get("threshold_pct", 0.003)

            total = len(full_df)
            idx = raw_idx if raw_idx > 0 else total
            idx = max(min_candles + 3, min(idx, total))

            # Slice the dataframe — this is what the detector would have seen at candle N
            df = full_df.iloc[:idx].copy()

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.loc[:, ~df.columns.duplicated()].copy()
            for col in ['Open', 'High', 'Low', 'Close']:
                df[col] = pd.to_numeric(df[col].squeeze(), errors='coerce')
            df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])

            # Determine session from the last candle's timestamp (not wall clock)
            last_ts = df.index[-1]
            if last_ts.tzinfo is None:
                last_ts = last_ts.tz_localize('UTC')
            else:
                last_ts = last_ts.tz_convert('UTC')
            hour = last_ts.hour

            session = None
            if 1 <= hour < 7:    session = "asian"
            elif 8 <= hour < 12: session = "london"
            elif 13 <= hour < 19: session = "new_york"

            # Resolve effective range — fall back through session → generic → None
            session_range_key   = f"{session}_range_pct" if session else None
            effective_range_pct = (
                params.get(session_range_key)
                or params.get("max_range_pct")
            )

            # Candle layout — must match accumulation.py exactly:
            #   df[-1]  forming candle          — never touched
            #   df[-2]  breakout/impulse candle — last fully closed
            #   df[-3…] accumulation window     — windows end at df[-2] exclusive
            breakout_idx   = len(df) - 2
            last_accum_idx = len(df) - 3
            scan_start     = max(0, len(df) - lookback)

            bo_open_raw  = float(df['Open'].iloc[breakout_idx])
            bo_close_raw = float(df['Close'].iloc[breakout_idx])
            bo_high_raw  = float(df['High'].iloc[breakout_idx])
            bo_low_raw   = float(df['Low'].iloc[breakout_idx])
            bo_body_size = abs(bo_close_raw - bo_open_raw)
            last_body_high = max(bo_open_raw, bo_close_raw)
            last_body_low  = min(bo_open_raw, bo_close_raw)

            breakout_candle = {
                "time":  int(df.index[breakout_idx].timestamp()),
                "open":  round(bo_open_raw, 5), "high": round(bo_high_raw, 5),
                "low":   round(bo_low_raw, 5),  "close": round(bo_close_raw, 5),
            }

            windows = []
            for window_size in range(min_candles, lookback + 1):
                slope_limit = (threshold_pct * 0.10) / window_size
                i = last_accum_idx - window_size + 1
                if i < 0 or i < scan_start:
                    continue

                window = df.iloc[i: i + window_size]
                closes = window['Close'].values.flatten().astype(float)
                opens  = window['Open'].values.flatten().astype(float)
                highs  = window['High'].values.flatten().astype(float)
                lows   = window['Low'].values.flatten().astype(float)

                avg_p = closes.mean()
                if avg_p == 0:
                    continue
                body_highs = np.maximum(opens, closes)
                body_lows  = np.minimum(opens, closes)
                h_max     = float(body_highs.max())
                l_min     = float(body_lows.min())
                avg_body  = float(np.abs(closes - opens).mean())
                range_pct = round((h_max - l_min) / avg_p, 6)
                slope     = round(_slope_pct(closes, avg_p), 8)
                chop      = round(_choppiness(closes), 4)
                adx_val   = _adx(highs, lows, closes)

                is_active  = (last_body_low >= l_min) and (last_body_high <= h_max)
                broke_up   = last_body_high > h_max
                broke_down = last_body_low  < l_min
                broke_out  = broke_up or broke_down

                # Impulse check: breakout body > avg window body
                is_impulsive   = bo_body_size > avg_body if broke_out else False
                impulse_ratio  = round(bo_body_size / avg_body, 2) if (broke_out and avg_body > 0) else None
                is_confirmed   = broke_out and is_impulsive

                reject = None
                if effective_range_pct and range_pct > effective_range_pct:
                    reject = f"range {range_pct} > limit {effective_range_pct}"
                elif slope >= slope_limit:
                    reject = f"slope {slope} >= limit {round(slope_limit,8)}"
                elif adx_val is not None and adx_val > adx_threshold:
                    reject = f"adx {round(adx_val,2)} > {adx_threshold}"
                elif chop < 0.36:
                    reject = f"chop {chop} < 0.36"

                windows.append({
                    "window":        window_size,
                    "start_ts":      int(df.index[i].timestamp()),
                    "end_ts":        int(df.index[i + window_size - 1].timestamp()),
                    "top":           round(h_max, 5),
                    "bottom":        round(l_min, 5),
                    "avg_body":      round(avg_body, 6),
                    "range_pct":     range_pct,
                    "range_limit":   effective_range_pct,
                    "slope":         slope,
                    "slope_limit":   round(slope_limit, 8),
                    "chop":          chop,
                    "adx":           round(adx_val, 2) if adx_val is not None else None,
                    "adx_limit":     adx_threshold,
                    "is_active":     is_active,
                    "broke_out":     broke_out,
                    "broke_up":      broke_up,
                    "broke_down":    broke_down,
                    "is_impulsive":  is_impulsive,
                    "impulse_ratio": impulse_ratio,
                    "is_confirmed":  is_confirmed,
                    "reject":        reject,
                    "pass":          reject is None,
                })

            passed   = [w for w in windows if w.get("pass")]
            rejected = [w for w in windows if not w.get("pass")]
            reasons  = {}
            for r in rejected:
                key = r["reject"].split(" ")[0] if r.get("reject") else "unknown"
                reasons[key] = reasons.get(key, 0) + 1

            # Best zone: ADX<10 preferred, then lowest slope. Secondary = runner-up.
            def _rank_windows(ws):
                low_adx = sorted(
                    [w for w in ws if w.get("adx") is not None and w["adx"] < 10],
                    key=lambda w: w["slope"]
                )
                rest = sorted(
                    [w for w in ws if not (w.get("adx") is not None and w["adx"] < 10)],
                    key=lambda w: w["slope"]
                )
                return low_adx + rest

            ranked    = _rank_windows(passed)
            best_zone      = ranked[0] if ranked else None
            secondary_zone = ranked[1] if len(ranked) > 1 else None

            return jsonify({
                "idx":               idx,
                "total":             total,
                "session":           session,
                "effective_range":   effective_range_pct,
                "adx_threshold":     adx_threshold,
                "last_close":        round(float(df['Close'].iloc[-2]), 5),
                "windows_checked":   len(windows),
                "passed":            len(passed),
                "rejection_summary": reasons,
                "windows":           windows,
                "best_zone":         best_zone,
                "secondary_zone":    secondary_zone,
                "breakout_candle":   breakout_candle,
                "candles":           [
                    {"time": int(r.Index.timestamp()), "open": round(float(r.Open),5),
                     "high": round(float(r.High),5),  "low":  round(float(r.Low),5),
                     "close": round(float(r.Close),5)}
                    for r in df.itertuples() if not (
                        __import__('math').isnan(float(r.Open)) or
                        __import__('math').isnan(float(r.Close))
                    )
                ],
            })
        except Exception as e:
            import traceback
            return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

    def _debug_sd(self):
        """Return detailed Supply & Demand analysis JSON for the debug page."""
        try:
            import yfinance as yf
            import pandas as pd
            import numpy as np
            from detectors.supply_demand import (
                _get_bias, _is_indecision, _in_session, _candle_session_or_pre
            )

            interval = request.args.get("interval", None)
            cache = {}
            params = dict(self.detector_params.get("supply_demand", {}))
            params.pop("timeframe", None)
            ticker           = params.get("ticker", self.ticker)
            impulse_mult     = params.get("impulse_multiplier", 1.8)
            wick_ratio       = params.get("wick_ratio", 0.6)
            max_zones        = params.get("max_zones", 5)
            max_age_days     = params.get("max_age_days", 3)
            valid_sessions   = params.get("valid_sessions", ["london", "new_york"])

            # Use requested interval or fall back to configured detector timeframe
            detector_interval = interval or self.detector_params.get("supply_demand", {}).get("timeframe", "30m")
            df = self._get_df(detector_interval, cache)

            # Get bias
            bias_info = _get_bias(ticker, _YF_LOCK)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.loc[:, ~df.columns.duplicated()].copy()
            for col in ['Open','High','Low','Close']:
                df[col] = pd.to_numeric(df[col].squeeze(), errors='coerce')
            df = df.dropna(subset=['Open','High','Low','Close'])

            opens  = df['Open'].values.flatten().astype(float)
            highs  = df['High'].values.flatten().astype(float)
            lows   = df['Low'].values.flatten().astype(float)
            closes = df['Close'].values.flatten().astype(float)
            bodies = np.abs(closes - opens)
            avg_body = float(np.mean(bodies))

            from datetime import datetime, timezone
            now_ts = datetime.now(timezone.utc).timestamp()
            cutoff_ts = now_ts - (max_age_days * 86400)

            last_close = closes[-2]
            last_high  = highs[-2]
            last_low   = lows[-2]

            look_for = None
            if bias_info["bias"] != "misaligned":
                look_for = "demand" if bias_info["bias"] == "bullish" else "supply"

            candidates = []

            for i in range(len(df) - 3, 0, -1):
                candle_ts = int(df.index[i].timestamp())
                if candle_ts < cutoff_ts:
                    break

                o, h, l, c = opens[i], highs[i], lows[i], closes[i]
                session = _candle_session_or_pre(candle_ts)

                reject_reason = None

                # Session check
                if not _in_session(candle_ts, valid_sessions):
                    reject_reason = f"session '{session}' not in {valid_sessions}"

                # Indecision check
                if not reject_reason and not _is_indecision(o, h, l, c, wick_ratio):
                    body = abs(c - o)
                    total_range = h - l
                    wick_frac = round((total_range - body) / total_range, 3) if total_range else 0
                    reject_reason = f"not indecision (wicks {wick_frac*100:.1f}% < {wick_ratio*100:.0f}%)"

                # Impulse body check
                if not reject_reason:
                    imp_body  = abs(closes[i+1] - opens[i+1])
                    imp_range = highs[i+1] - lows[i+1]
                    if imp_body < avg_body * impulse_mult:
                        reject_reason = f"impulse body {imp_body:.5f} < avg×{impulse_mult} ({avg_body*impulse_mult:.5f})"
                    elif imp_range > 0 and (imp_body / imp_range) < 0.60:
                        reject_reason = f"impulse wicks too large (body {imp_body/imp_range*100:.1f}% of range)"

                # Direction vs bias
                impulse_bullish = closes[i+1] > opens[i+1]
                zone_type = "demand" if impulse_bullish else "supply"
                if not reject_reason and look_for and zone_type != look_for:
                    reject_reason = f"wrong direction ({zone_type}) — bias requires {look_for}"

                # Bias misaligned
                if not reject_reason and not look_for:
                    reject_reason = "bias misaligned — detection skipped"

                # Touch check
                if not reject_reason:
                    if zone_type == "demand" and last_low <= h:
                        reject_reason = f"demand zone touched/crossed (low {last_low:.5f} ≤ zone top {h:.5f})"
                    elif zone_type == "supply" and last_high >= l:
                        reject_reason = f"supply zone touched/crossed (high {last_high:.5f} ≥ zone bot {l:.5f})"

                is_active = reject_reason is None

                # Calculate impulse metrics for display
                imp_body_val  = round(abs(closes[i+1] - opens[i+1]), 6) if i+1 < len(df) else None
                imp_mult_used = round(imp_body_val / avg_body, 2) if imp_body_val and avg_body else None
                body_size     = round(abs(c - o), 6)
                total_range   = round(h - l, 6)
                wick_pct      = round((total_range - body_size) / total_range * 100, 1) if total_range else 0

                candidates.append({
                    "start":         candle_ts,
                    "end":           int(df.index[-1].timestamp()),
                    "top":           float(h),
                    "bottom":        float(l),
                    "type":          zone_type,
                    "session":       session,
                    "is_active":     is_active,
                    "reject_reason": reject_reason,
                    "wick_pct":      wick_pct,
                    "body_size":     body_size,
                    "impulse_body":  imp_body_val,
                    "impulse_mult":  imp_mult_used,
                    "avg_body":      round(avg_body, 6),
                })

            return jsonify({
                "pair":       self.pair_id,
                "bias":       bias_info,
                "look_for":   look_for,
                "avg_body":   round(avg_body, 6),
                "candidates": candidates,
            })
        except Exception as e:
            import traceback
            return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

    def _debug_fvg(self):
        """
        Run the standalone FVG detector and return full candidate details.
        Uses fvg.detect() which applies min_gap_pct and impulse_body_pct filters.
        """
        try:
            import pandas as pd
            from detectors.fvg import detect as fvg_detect, _check_fvg, DEFAULT_MIN_GAP_PCT, DEFAULT_IMPULSE_BODY_PCT

            interval = request.args.get("interval", None)
            cache = {}
            det_interval = interval or self.detector_params.get("accumulation", {}).get("timeframe", "1m")
            df = self._get_df(det_interval, cache)

            if isinstance(df.columns, __import__("pandas").MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.loc[:, ~df.columns.duplicated()].copy()
            for col in ["Open","High","Low","Close"]:
                df[col] = __import__("pandas").to_numeric(df[col].squeeze(), errors="coerce")
            df = df.dropna(subset=["Open","High","Low","Close"])

            min_gap_pct      = self.detector_params.get("fvg", {}).get("min_gap_pct",      DEFAULT_MIN_GAP_PCT)
            impulse_body_pct = self.detector_params.get("fvg", {}).get("impulse_body_pct", DEFAULT_IMPULSE_BODY_PCT)
            lookback         = self.detector_params.get("fvg", {}).get("lookback",         80)

            result = fvg_detect(df, lookback=lookback, min_gap_pct=min_gap_pct,
                                impulse_body_pct=impulse_body_pct)

            scan_end   = len(df) - 2
            scan_start = max(1, scan_end - lookback)
            all_candidates = []
            for i in range(scan_end, scan_start, -1):
                fvg = _check_fvg(df, i, min_gap_pct, impulse_body_pct)
                c_prev = df.iloc[i - 1]
                c_now  = df.iloc[i]
                c_next = df.iloc[i + 1]
                h_prev, l_prev = float(c_prev["High"]), float(c_prev["Low"])
                h_next, l_next = float(c_next["High"]), float(c_next["Low"])
                o_now  = float(c_now["Open"])
                h_now  = float(c_now["High"])
                l_now  = float(c_now["Low"])
                c_now_ = float(c_now["Close"])
                body       = abs(c_now_ - o_now)
                crange     = h_now - l_now
                body_ratio = body / crange if crange > 0 else 0
                avg_p      = (h_now + l_now) / 2.0
                raw_bull   = l_next - h_prev
                raw_bear   = l_prev - h_next
                has_raw_gap = raw_bull > 0 or raw_bear > 0
                fvg_type_raw = "bullish" if raw_bull > 0 else ("bearish" if raw_bear > 0 else None)
                gap_size_raw = max(raw_bull, raw_bear)
                gap_pct_raw  = gap_size_raw / avg_p if avg_p > 0 else 0

                reject_reason = None
                # Zero-range candle guards (same as fvg.py)
                if h_prev <= 0 or l_prev <= 0 or h_next <= 0 or l_next <= 0:
                    reject_reason = "zero/negative price in N-1 or N+1"
                elif h_prev == l_prev:
                    reject_reason = "zero-range candle N-1 (no wick data)"
                elif h_next == l_next:
                    reject_reason = "zero-range candle N+1 (no wick data)"
                elif crange == 0:
                    reject_reason = "zero-range impulse candle N"
                elif not has_raw_gap:
                    reject_reason = "no gap (wicks overlap)"
                elif gap_pct_raw < min_gap_pct:
                    reject_reason = f"gap too small ({gap_pct_raw*100:.4f}% < min {min_gap_pct*100:.4f}%)"
                elif fvg_type_raw == "bullish" and c_now_ <= o_now:
                    reject_reason = "gap bullish but candle N is bearish"
                elif fvg_type_raw == "bearish" and c_now_ >= o_now:
                    reject_reason = "gap bearish but candle N is bullish"
                elif body_ratio < impulse_body_pct:
                    reject_reason = f"impulse body {body_ratio*100:.1f}% < {impulse_body_pct*100:.0f}%"

                all_candidates.append({
                    "candle_idx":    i,
                    "has_fvg":       fvg is not None,
                    "fvg_type":      fvg["fvg_type"] if fvg else fvg_type_raw,
                    "reject_reason": reject_reason,
                    "gap_top":       fvg["top"]     if fvg else None,
                    "gap_bottom":    fvg["bottom"]  if fvg else None,
                    "gap_pct":       fvg["gap_pct"] if fvg else round(gap_pct_raw, 8),
                    "raw_bull_gap":  round(raw_bull, 6),
                    "raw_bear_gap":  round(raw_bear, 6),
                    "body_ratio":    round(body_ratio, 3),
                    "candle_n": {
                        "time":  int(df.index[i].timestamp()),
                        "open":  o_now, "high": h_now,
                        "low":   l_now, "close": c_now_,
                    },
                    "candle_nm1": {
                        "time": int(df.index[i-1].timestamp()),
                        "high": h_prev, "low": l_prev,
                    },
                    "candle_np1": {
                        "time": int(df.index[i+1].timestamp()),
                        "high": h_next, "low": l_next,
                    },
                })

            candles_out = [
                {"time": int(idx.timestamp()), "open": float(r["Open"]),
                 "high": float(r["High"]), "low": float(r["Low"]), "close": float(r["Close"])}
                for idx, r in df.iterrows()
            ]

            return jsonify({
                "pair":             self.pair_id,
                "interval":         det_interval,
                "min_gap_pct":      min_gap_pct,
                "impulse_body_pct": impulse_body_pct,
                "total":            result["total"],
                "passed":           result["found"],
                "bullish":          result["bullish"],
                "bearish":          result["bearish"],
                "candidates":       all_candidates,
                "candles":          candles_out,
            })
        except Exception as e:
            import traceback
            return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

    def _test_alert(self):
        test_zone = {
            "detector": "accumulation",
            "start": int(time.time()),
            "end": int(time.time()),
            "top": 0,
            "bottom": 0,
            "is_active": True,
        }
        threading.Thread(target=self._send_discord_alert, args=(test_zone,), daemon=True).start()
        return f"Test alert triggered for {self.pair_id}. Check terminal and Discord."

    # ------------------------------------------------------------------ #
    # Discord
    # ------------------------------------------------------------------ #

    def _send_discord_alert(self, zone: dict):
        if not DISCORD_WEBHOOK_URL:
            print(f"[{self.pair_id}] Discord webhook URL not set.")
            return
        if not DISCORD_AVAILABLE:
            print(f"[{self.pair_id}] discord-webhook package not installed.")
            return

        screenshot_path = f"alert_{self.pair_id}_{int(time.time())}.png"
        raw = zone.get("detector", "unknown")
        if raw in ("demand", "supply"):
            detector_name = f"{raw.capitalize()} Zone"
        else:
            detector_name = raw.replace("_", " ").title()
        print(f"[{self.pair_id}] Sending Discord alert for {detector_name}...")

        try:
            if PLAYWRIGHT_AVAILABLE:
                highlight_ts = zone.get("start", "")
                page_url = f"http://127.0.0.1:{self.port}?highlight={highlight_ts}"
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page(viewport={"width": 1280, "height": 720})
                    page.goto(page_url)
                    page.wait_for_timeout(6000)
                    page.screenshot(path=screenshot_path)
                    browser.close()

            if zone.get("detector") in ("demand", "supply"):
                emoji = "📈" if zone.get("detector") == "demand" else "📉"
                content = f"{emoji} **{self.pair_id} — {detector_name} Found**"
            else:
                content = f"🚀 **{self.pair_id} — {detector_name} Confirmed**"
            webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, content=content)

            if PLAYWRIGHT_AVAILABLE and os.path.exists(screenshot_path):
                with open(screenshot_path, "rb") as f:
                    webhook.add_file(file=f.read(), filename="chart.png")

            webhook.execute()
            print(f"[{self.pair_id}] Discord alert sent.")

        except Exception as e:
            print(f"[{self.pair_id}] Discord error: {e}")
        finally:
            if os.path.exists(screenshot_path):
                os.remove(screenshot_path)

    # ------------------------------------------------------------------ #
    # Start
    # ------------------------------------------------------------------ #

    def _load_alerted(self) -> dict:
        try:
            if os.path.exists(self._alerted_file):
                with open(self._alerted_file, 'r') as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

    def _save_alerted(self):
        try:
            with open(self._alerted_file, 'w') as f:
                json.dump(self.last_alerted, f)
        except Exception as e:
            print(f"[{self.pair_id}] Failed to save alerted state: {e}")

    def run(self):
        print(f"[{self.pair_id}] Starting on http://0.0.0.0:{self.port}")

        # Start background detection loop in a daemon thread
        t = threading.Thread(target=self._detection_loop, daemon=True, name=f"detector-{self.pair_id}")
        t.start()

        # Start Flask (blocks this thread) — threaded so slow /debug/replay
        # requests don't block concurrent requests for the HTML page or chart data
        self.app.run(host="0.0.0.0", port=self.port, use_reloader=False, threaded=True)
