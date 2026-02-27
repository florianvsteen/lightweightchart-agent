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
from flask import Flask, render_template, jsonify, request

from detectors import REGISTRY
from providers import get_df as _provider_get_df, get_bias_df as _provider_get_bias_df, LOCK as _YF_LOCK

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

PERIOD_MAP = {
    "1m":  "1d",
    "2m":  "1d",
    "5m":  "5d",
    "15m": "5d",
    "30m": "5d",
    "1h":  "30d",
}

DETECTION_INTERVAL = 30

INTERVAL_SECONDS = {
    "1m":  60,
    "2m":  120,
    "5m":  300,
    "15m": 900,
    "30m": 1800,
    "1h":  3600,
    "4h":  14400,
    "1d":  86400,
    "1wk": 604800,
}


class PairServer:

    def __init__(self, pair_id: str, config: dict):
        self.pair_id = pair_id
        self.port = config["port"]
        self.label = config["label"]
        self.interval = config.get("interval", "1m")
        self.period = config.get("period", "1d")
        self.detector_names = config.get("detectors", [])
        self.detector_params = config.get("detector_params", {})
        self.default_interval = config.get("default_interval", self.interval)
        self.always_open = config.get("always_open", False)
        self.market_timing = config.get("market_timing", "FOREX")
        self._config = config

        _provider = os.environ.get("DATA_PROVIDER", "yahoo").lower().strip()
        if _provider == "metatrader":
            self.ticker = (
                config.get("mt5_ticker")
                or config.get("yf_ticker")
                or config.get("ticker")
            )
        else:
            self.ticker = (
                config.get("yf_ticker")
                or config.get("ticker")
            )
        if not self.ticker:
            raise ValueError(f"[{pair_id}] No ticker configured for provider '{_provider}'")

        self._alerted_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f".alerted_{pair_id}.json"
        )
        self.last_alerted: dict[str, int] = self._load_alerted()
        self.last_active_zone: dict[str, dict] = {}

        # ── Restore cooldown state after restart ──────────────────────
        for det_name in config.get("detectors", []):
            if det_name == "accumulation":
                alert_ts = self.last_alerted.get(f"{det_name}_alert_ts", 0)
                if alert_ts:
                    cooldown_minutes = config.get("detector_params", {}).get(
                        "accumulation", {}
                    ).get("alert_cooldown_minutes", 15)
                    cooldown_until = alert_ts + cooldown_minutes * 60
                    if int(time.time()) < cooldown_until:
                        saved_zone = self.last_alerted.get(f"{det_name}_cooldown_zone", {})
                        self.last_active_zone[det_name] = {
                            "detector":       "accumulation",
                            "status":         "cooldown",
                            "cooldown_until": int(cooldown_until),
                            "is_active":      False,
                            "start":          saved_zone.get("start", 0),
                            "end":            saved_zone.get("end", 0),
                            "top":            saved_zone.get("top", 0),
                            "bottom":         saved_zone.get("bottom", 0),
                        }
                        print(f"[{pair_id}] Cooldown restored — expires in "
                              f"{int((cooldown_until - time.time()) / 60)}m")

        self._df_cache: dict[str, pd.DataFrame] = {}
        self._cache_lock = threading.Lock()

        self._cached_detector_results: dict = {}
        self._cached_candles: dict[str, list] = {}
        self._results_lock = threading.Lock()

        self._detection_lock = threading.Lock()
        self._stagger_seconds = 0
        self._last_detection_time: float = 0.0

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
            return render_template("agent-chart.html", pair_id=pair_id, label=self.label, port=self.port, timezone=tz, default_interval=self.default_interval, always_open=self.always_open)
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

        def _debug_sd_bias():
            return self._debug_sd_bias()
        _debug_sd_bias.__name__ = f"debug_sd_bias_{pair_id}"
        app.route("/debug/sd/bias")(_debug_sd_bias)

        def _api_bias():
            return self._api_bias()
        _api_bias.__name__ = f"api_bias_{pair_id}"
        app.route("/api/bias")(_api_bias)

    # ------------------------------------------------------------------ #
    # Data fetching
    # ------------------------------------------------------------------ #

    def _fetch_df(self, interval: str) -> pd.DataFrame:
        period = PERIOD_MAP.get(interval, self.period)
        return _provider_get_df(self.ticker, interval, period)

    def _get_df(self, interval: str, cache: dict) -> pd.DataFrame:
        if interval not in cache:
            cache[interval] = self._fetch_df(interval)
        return cache[interval]

    # ------------------------------------------------------------------ #
    # Detection
    # ------------------------------------------------------------------ #

    def _run_detectors(self, cache: dict) -> dict:
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
                    if name == "supply_demand":
                        params["ticker"] = self.ticker
                    if name in ("accumulation", "supply_demand"):
                        params["market_timing"] = self.market_timing
                    results[name] = fn(df, **params)
                except Exception as e:
                    print(f"[ERROR] Detector '{name}' failed: {e}")
                    results[name] = None
        return results

    def _process_alerts(self, detector_results: dict):
        """
        Check results and fire Discord alerts on breakout.

        Accumulation alert rules (FIXED):
          1. Only fire when the detector explicitly returns status == 'confirmed'
             (impulsive breakout). 'looking' after an active zone means the breakout
             was NOT impulsive — do NOT alert.
          2. Cooldown is enforced by setting last_active_zone to a 'cooldown' dict.
             Any new active zone discovered during cooldown is deliberately IGNORED
             to prevent the cooldown from being bypassed.
        """
        from sessions import is_weekend_halt
        if is_weekend_halt(self.market_timing):
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
                prev_status = (prev or {}).get("status")

                # ── Cooldown guard ─────────────────────────────────────
                # During cooldown, we ignore ALL zone transitions — including
                # new active zones — so the cooldown cannot be bypassed.
                if prev_status == "cooldown":
                    if int(time.time()) < (prev or {}).get("cooldown_until", 0):
                        continue  # still cooling down — skip everything
                    else:
                        # Cooldown expired — clear and fall through to normal logic
                        self.last_active_zone[name] = None
                        prev = None
                        prev_status = None

                zone = result if (result and isinstance(result, dict)) else None

                # ── Track active zone ──────────────────────────────────
                # Store the active zone so we can detect when it breaks out.
                # Only update last_active_zone when NOT in cooldown (guard above handles that).
                is_active_found = (
                    zone is not None
                    and zone.get("is_active")
                    and zone.get("status") == "active"
                )

                if is_active_found:
                    zone_start = zone["start"]
                    already_alerted = self.last_alerted.get(name, 0)
                    if zone_start != already_alerted:
                        self.last_active_zone[name] = zone

                # ── Alert ONLY on explicit 'confirmed' status ──────────
                # The detector sets 'confirmed' only when the breakout candle is
                # impulsive (body > avg window body). 'looking' after an active zone
                # means the breakout failed the impulse check — do NOT alert.
                elif zone is not None and zone.get("status") == "confirmed":
                    zone_start = zone.get("start", 0)
                    already_alerted = self.last_alerted.get(name, 0)

                    if zone_start != already_alerted:
                        # The detector confirmed the breakout — alert
                        confirmed_zone = dict(zone)
                        self.last_active_zone[name] = confirmed_zone
                        self.last_alerted[name] = zone_start
                        self.last_alerted[f"{name}_alert_ts"] = int(time.time())
                        self.last_alerted[f"{name}_cooldown_zone"] = {
                            "start":  zone.get("start"),
                            "end":    zone.get("end"),
                            "top":    zone.get("top"),
                            "bottom": zone.get("bottom"),
                        }
                        self._save_alerted()
                        threading.Thread(
                            target=self._send_discord_alert,
                            args=(confirmed_zone,),
                            daemon=True,
                        ).start()

                        # Immediately set cooldown so no further alerts fire
                        cooldown_minutes = self.detector_params.get("accumulation", {}).get(
                            "alert_cooldown_minutes", 15
                        )
                        cooldown_zone = dict(confirmed_zone)
                        cooldown_zone["status"] = "cooldown"
                        cooldown_zone["cooldown_until"] = int(time.time()) + cooldown_minutes * 60
                        self.last_active_zone[name] = cooldown_zone

                elif prev_status == "confirmed":
                    # Previous cycle was confirmed — transition to cooldown
                    cooldown_minutes = self.detector_params.get("accumulation", {}).get(
                        "alert_cooldown_minutes", 15
                    )
                    cooldown_zone = dict(prev)
                    cooldown_zone["status"] = "cooldown"
                    cooldown_zone["cooldown_until"] = (
                        self.last_alerted.get(f"{name}_alert_ts", int(time.time()))
                        + cooldown_minutes * 60
                    )
                    self.last_active_zone[name] = cooldown_zone

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
    # Background detection loop
    # ------------------------------------------------------------------ #

    def _min_poll_interval(self) -> float:
        max_seconds = DETECTION_INTERVAL
        for name in self.detector_names:
            tf = self.detector_params.get(name, {}).get("timeframe", "1m")
            tf_secs = INTERVAL_SECONDS.get(tf, DETECTION_INTERVAL)
            if tf_secs > max_seconds:
                max_seconds = tf_secs
        return float(max_seconds)

    def _detection_loop(self):
        if self._stagger_seconds:
            time.sleep(self._stagger_seconds)

        min_interval = self._min_poll_interval()
        print(
            f"[{self.pair_id}] Background detector started "
            f"(poll interval: {int(min_interval)}s)"
        )

        while True:
            now = time.time()
            elapsed = now - self._last_detection_time

            if elapsed < min_interval:
                sleep_for = min(DETECTION_INTERVAL, min_interval - elapsed)
                time.sleep(sleep_for)
                continue

            try:
                with self._detection_lock:
                    cache = {}
                    results = self._run_detectors(cache)
                    self._process_alerts(results)
                self._last_detection_time = time.time()

                intervals_to_cache = set()
                for name in self.detector_names:
                    tf = self.detector_params.get(name, {}).get("timeframe", self.interval)
                    intervals_to_cache.add(tf)
                intervals_to_cache.add(self.default_interval)
                intervals_to_cache.add(self.interval)

                candles_by_interval = {}
                for iv in intervals_to_cache:
                    try:
                        df_iv = self._fetch_df(iv)
                        candles_by_interval[iv] = [
                            {
                                "time":  int(idx.timestamp()),
                                "open":  float(r["Open"]),
                                "high":  float(r["High"]),
                                "low":   float(r["Low"]),
                                "close": float(r["Close"]),
                            }
                            for idx, r in df_iv.iterrows()
                        ]
                    except Exception as ce:
                        print(f"[{self.pair_id}] Candle cache error ({iv}): {ce}")

                with self._results_lock:
                    self._cached_detector_results = results
                    self._cached_candles.update(candles_by_interval)

                print(f"[{self.pair_id}] Detection cycle complete: {list(results.keys())}")
            except Exception as e:
                print(f"[{self.pair_id}] Detection loop error: {e}")

            time.sleep(DETECTION_INTERVAL)

    # ------------------------------------------------------------------ #
    # Flask API
    # ------------------------------------------------------------------ #

    def _api_data(self):
        try:
            chart_interval = request.args.get("interval", self.interval)

            with self._results_lock:
                detector_results = dict(self._cached_detector_results)
                candles = list(self._cached_candles.get(chart_interval, []))

            if not candles:
                try:
                    df_chart = self._fetch_df(chart_interval)
                    candles = [
                        {
                            "time":  int(idx.timestamp()),
                            "open":  float(r["Open"]),
                            "high":  float(r["High"]),
                            "low":   float(r["Low"]),
                            "close": float(r["Close"]),
                        }
                        for idx, r in df_chart.iterrows()
                    ]
                except Exception:
                    candles = []

            if not detector_results:
                return jsonify({
                    "pair":      self.pair_id,
                    "label":     self.label,
                    "candles":   candles,
                    "detectors": {},
                })

            # Accumulation state overrides (in-memory only)
            for det_name in self.detector_names:
                if det_name == "accumulation":
                    held = self.last_active_zone.get(det_name)
                    if not held:
                        continue
                    status = held.get("status")
                    if status == "cooldown":
                        if int(time.time()) < held.get("cooldown_until", 0):
                            detector_results[det_name] = held
                        else:
                            self.last_active_zone[det_name] = None
                    elif status in ("confirmed", "active"):
                        detector_results[det_name] = held

            return jsonify({
                "pair":      self.pair_id,
                "label":     self.label,
                "candles":   candles,
                "detectors": detector_results,
            })

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    def _api_bias(self):
        """Return current bias for this pair (used by mission control for all pairs)."""
        try:
            from detectors.bias import get_bias
            bias_info = get_bias(self.ticker)
            return jsonify(bias_info)
        except Exception as e:
            return jsonify({"bias": "misaligned", "aligned": False, "reason": str(e)}), 500

    def _debug(self):
        from config import PAIRS
        tz = os.environ.get("TZ", "Europe/Brussels")
        pairs_list = [
            {"id": pid, "label": cfg["label"], "port": cfg["port"]}
            for pid, cfg in PAIRS.items()
        ]
        return render_template("debug.html",
            pair_id=self.pair_id,
            label=self.label,
            timezone=tz,
            port=self.port,
            pairs=pairs_list,
        )

    def _debug_data(self):
        try:
            import numpy as np
            from detectors.accumulation import (
                get_current_session, _slope_pct, _choppiness, _adx, _count_touchpoints
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
            min_touchpoints = params.get("min_touchpoints", 0)

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
                b_highs   = np.maximum(opens, closes)
                b_lows    = np.minimum(opens, closes)
                touches   = _count_touchpoints(b_highs, b_lows, h_max, l_min)

                reject = None
                if effective_range_pct and range_pct > effective_range_pct:
                    reject = f"range {range_pct} > limit {effective_range_pct}"
                elif slope >= slope_limit:
                    reject = f"slope {slope} >= limit {round(slope_limit,8)}"
                elif adx_val is not None and adx_val > adx_threshold:
                    reject = f"adx {round(adx_val,2)} > {adx_threshold}"
                elif chop < 0.36:
                    reject = f"chop {chop} < 0.36"
                elif min_touchpoints > 0 and touches < min_touchpoints:
                    reject = f"touchpoints {touches} < {min_touchpoints}"

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
                    "touchpoints": touches,
                    "min_touchpoints": min_touchpoints,
                    "reject":      reject,
                    "pass":        reject is None,
                })

            passed   = [w for w in windows if w.get("pass")]
            rejected = [w for w in windows if not w.get("pass") and "skip" not in w]
            reasons  = {}
            for r in rejected:
                key = r["reject"].split(" ")[0] if r.get("reject") else "unknown"
                reasons[key] = reasons.get(key, 0) + 1

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
        try:
            raw_idx = int(request.args.get("idx", -1))
        except Exception:
            raw_idx = -1

        try:
            import numpy as np
            import pandas as pd
            from datetime import timezone
            from detectors.accumulation import _slope_pct, _choppiness, _adx, _count_touchpoints

            full_df = _provider_get_df(self.ticker, "1m", self.period)
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
            min_touchpoints = params.get("min_touchpoints", 0)

            total = len(full_df)
            idx = raw_idx if raw_idx > 0 else total
            idx = max(min_candles + 3, min(idx, total))

            df = full_df.iloc[:idx].copy()

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.loc[:, ~df.columns.duplicated()].copy()
            for col in ['Open', 'High', 'Low', 'Close']:
                df[col] = pd.to_numeric(df[col].squeeze(), errors='coerce')
            df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])

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

            session_range_key   = f"{session}_range_pct" if session else None
            effective_range_pct = (
                params.get(session_range_key)
                or params.get("max_range_pct")
            )

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
                b_highs   = np.maximum(opens, closes)
                b_lows    = np.minimum(opens, closes)
                touches   = _count_touchpoints(b_highs, b_lows, h_max, l_min)

                is_active  = (last_body_low >= l_min) and (last_body_high <= h_max)
                broke_up   = last_body_high > h_max
                broke_down = last_body_low  < l_min
                broke_out  = broke_up or broke_down

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
                elif min_touchpoints > 0 and touches < min_touchpoints:
                    reject = f"touchpoints {touches} < {min_touchpoints}"

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
                    "touchpoints":   touches,
                    "min_touchpoints": min_touchpoints,
                    "reject":        reject,
                    "pass":          reject is None,
                })

            passed   = [w for w in windows if w.get("pass")]
            rejected = [w for w in windows if not w.get("pass")]
            reasons  = {}
            for r in rejected:
                key = r["reject"].split(" ")[0] if r.get("reject") else "unknown"
                reasons[key] = reasons.get(key, 0) + 1

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
        try:
            import numpy as np
            from detectors.supply_demand import detect

            interval = request.args.get("interval", None)
            cache = {}
            params = dict(self.detector_params.get("supply_demand", {}))
            params.pop("timeframe", None)

            detector_interval = interval or self.detector_params.get("supply_demand", {}).get("timeframe", "30m")
            df = self._get_df(detector_interval, cache)

            result = detect(df, ticker=self.ticker, market_timing=self.market_timing, debug=True, **params)

            if isinstance(df.columns, pd.MultiIndex):
                df = df.copy()
                df.columns = df.columns.get_level_values(0)
            df = df.loc[:, ~df.columns.duplicated()].copy()
            for col in ['Open', 'High', 'Low', 'Close']:
                df[col] = pd.to_numeric(df[col].squeeze(), errors='coerce')
            df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])

            import numpy as np
            bodies   = np.abs(df['Close'].values - df['Open'].values)
            avg_body = float(np.mean(bodies))

            candles_sd = [
                {"time": int(idx.timestamp()), "open": round(float(r["Open"]), 5),
                 "high": round(float(r["High"]), 5), "low": round(float(r["Low"]), 5),
                 "close": round(float(r["Close"]), 5)}
                for idx, r in df.iterrows()
            ]

            bias = result.get("bias", {})
            zones = result.get("zones", [])

            chart_end_ts = int(df.index[-1].timestamp())
            for z in zones:
                z["end"] = chart_end_ts

            from detectors.bias import is_bullish, is_bearish
            look_for = None
            if is_bullish(bias):
                look_for = "demand"
            elif is_bearish(bias):
                look_for = "supply"

            return jsonify({
                "pair":       self.pair_id,
                "bias":       bias,
                "look_for":   look_for,
                "avg_body":   round(avg_body, 6),
                "zones":      zones,
                "candidates": result.get("candidates", []),
                "candles":    candles_sd,
            })

        except Exception as e:
            import traceback
            return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

    def _debug_fvg(self):
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

    def _debug_sd_bias(self):
        try:
            from detectors.supply_demand import _get_bias

            bias_info = _get_bias(self.ticker)

            df_d = _provider_get_bias_df(self.ticker, "5d", "1d").dropna()
            df_w = _provider_get_bias_df(self.ticker, "3mo", "1wk").dropna()

            def to_candles(df, mark_bias=True):
                rows = []
                for i, (idx, r) in enumerate(df.iterrows()):
                    rows.append({
                        "time":       int(idx.timestamp()),
                        "open":       round(float(r["Open"]), 5),
                        "high":       round(float(r["High"]), 5),
                        "low":        round(float(r["Low"]), 5),
                        "close":      round(float(r["Close"]), 5),
                        "bias_candle": mark_bias and i == len(df) - 2,
                    })
                return rows

            return jsonify({
                "pair":           self.pair_id,
                "bias":           bias_info,
                "daily_candles":  to_candles(df_d),
                "weekly_candles": to_candles(df_w),
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
    # Discord — with centered screenshot
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
                # Pass center_ts so the chart JS can scroll the breakout candle to center
                breakout_ts = ""
                if zone.get("breakout_candle"):
                    breakout_ts = zone["breakout_candle"].get("time", "")
                center_ts = breakout_ts or highlight_ts

                page_url = (
                    f"http://127.0.0.1:{self.port}"
                    f"?highlight={highlight_ts}&center={center_ts}"
                )
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page(viewport={"width": 1280, "height": 720})
                    page.goto(page_url)
                    # Wait for chart to render, then scroll to center the target candle
                    page.wait_for_timeout(4000)
                    # Execute JS to center the candle in view
                    if center_ts:
                        page.evaluate(f"""
                            (() => {{
                                // Try to center the breakout candle if chart is available
                                try {{
                                    const ts = {center_ts};
                                    if (window._chartRef) {{
                                        const tzOffset = window._tzOffset || 0;
                                        const shiftedTs = ts + tzOffset;
                                        const range = window._chartRef.timeScale().getVisibleLogicalRange();
                                        const width = range ? (range.to - range.from) : 60;
                                        const half = width / 2;
                                        window._chartRef.timeScale().setVisibleRange({{
                                            from: shiftedTs - half * 60,
                                            to:   shiftedTs + half * 60,
                                        }});
                                    }}
                                }} catch(e) {{}}
                            }})();
                        """)
                        page.wait_for_timeout(800)
                    page.screenshot(path=screenshot_path)
                    browser.close()

            if zone.get("detector") in ("demand", "supply"):
                emoji = "📈" if zone.get("detector") == "demand" else "📉"
                content = f"{emoji} **{self.pair_id} — {detector_name} Found**"
            else:
                content = f"🚀 **{self.pair_id} — Aggressor Candle Confirmed**"
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

        t = threading.Thread(target=self._detection_loop, daemon=True, name=f"detector-{self.pair_id}")
        t.start()

        self.app.run(host="0.0.0.0", port=self.port, use_reloader=False, threaded=True)
