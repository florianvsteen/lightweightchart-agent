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
    "1d":  "60d",
    "1wk": "1y",
}

DETECTION_INTERVAL = 15

INTERVAL_SECONDS = {
    "1m":  60,
    "2m":  120,
    "3m":  180,
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
        self._state_version = 0

        self._bias_cache: dict = {}
        self._bias_cache_ts: float = 0.0

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

        def _stream_data():
            return self._api_stream()
        _stream_data.__name__ = f"stream_data_{pair_id}"
        app.route("/api/stream")(_stream_data)

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

        def _api_cvd():
            return self._api_cvd()
        _api_cvd.__name__ = f"api_cvd_{pair_id}"
        app.route("/api/cvd")(_api_cvd)

        def _api_candle_explain():
            return self._api_candle_explain()
        _api_candle_explain.__name__ = f"api_candle_explain_{pair_id}"
        app.route("/api/candle-explain")(_api_candle_explain)
    
        def _api_drawings_get():
            return self._api_drawings_get()
        _api_drawings_get.__name__ = f"api_drawings_get_{pair_id}"
        app.route("/api/drawings", methods=["GET"])(_api_drawings_get)

        def _api_drawings_post():
            return self._api_drawings_post()
        _api_drawings_post.__name__ = f"api_drawings_post_{pair_id}"
        app.route("/api/drawings", methods=["POST"])(_api_drawings_post)

        def _api_drawings_delete(drawing_id):
            return self._api_drawings_delete(drawing_id)
        _api_drawings_delete.__name__ = f"api_drawings_delete_{pair_id}"
        app.route("/api/drawings/<drawing_id>", methods=["DELETE"])(_api_drawings_delete)

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
            tf_setting = params.pop("timeframe", "1m")
            
            # Convert single string to list for uniform processing
            tf_list = tf_setting if isinstance(tf_setting, list) else [tf_setting]
            
            fn = REGISTRY.get(name)
            if fn is None:
                continue

            tf_results = {}
            for tf in tf_list:
                try:
                    df = self._get_df(tf, cache)
                    p = dict(params) 
                    if name == "supply_demand": p["ticker"] = self.ticker
                    p["market_timing"] = self.market_timing
                    
                    res = fn(df, **p)
                    if res:
                        res["timeframe_id"] = tf # Tag which TF found the zone
                    tf_results[tf] = res
                except Exception as e:
                    print(f"[{self.pair_id}] {name} ({tf}) failed: {e}")
            
            # If multiple TFs, return dict. If one, return flat for UI compat.
            results[name] = tf_results if len(tf_list) > 1 else tf_results[tf_list[0]]
        return results

    def _process_alerts(self, detector_results: dict):
        """
        Check results and fire Discord alerts on breakout.
        """
        from tools.sessions import is_weekend_halt
        if is_weekend_halt(self.market_timing):
            return

        for name, result in detector_results.items():

            # ── Accumulation ──────────────────────────────────────────
            if name == "accumulation":
                cutoff = int(time.time()) - (4 * 3600)
                if name in self.last_alerted and isinstance(self.last_alerted[name], int):
                    if self.last_alerted[name] < cutoff:
                        del self.last_alerted[name]
                        self._save_alerted()

                prev = self.last_active_zone.get(name)
                prev_status = (prev or {}).get("status")

                if prev_status == "cooldown":
                    if int(time.time()) < (prev or {}).get("cooldown_until", 0):
                        continue
                    else:
                        self.last_active_zone[name] = None
                        prev = None
                        prev_status = None

                zone = result if (result and isinstance(result, dict)) else None

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

                elif zone is not None and zone.get("status") == "confirmed":
                    zone_start = zone.get("start", 0)
                    already_alerted = self.last_alerted.get(name, 0)

                    if zone_start != already_alerted:
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

                        cooldown_minutes = self.detector_params.get("accumulation", {}).get(
                            "alert_cooldown_minutes", 15
                        )
                        cooldown_zone = dict(confirmed_zone)
                        cooldown_zone["status"] = "cooldown"
                        cooldown_zone["cooldown_until"] = int(time.time()) + cooldown_minutes * 60
                        self.last_active_zone[name] = cooldown_zone

                elif prev_status == "confirmed":
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
        fastest = 60.0
        for name in self.detector_names:
            tf_setting = self.detector_params.get(name, {}).get("timeframe", "1m")
            tf_list = tf_setting if isinstance(tf_setting, list) else [tf_setting]
            for tf in tf_list:
                fastest = min(fastest, float(INTERVAL_SECONDS.get(tf, 60)))
        return fastest

    def _detection_loop(self):
        if self._stagger_seconds:
            time.sleep(self._stagger_seconds)

        min_interval = self._min_poll_interval()
        chart_update_interval = 15.0  # Force chart candles to update every 15s
        
        print(f"[{self.pair_id}] Background started. Charts: {int(chart_update_interval)}s | Detectors: {int(min_interval)}s")

        last_chart_update = 0.0

        while True:
            now = time.time()

            # ── 1. FAST LOOP: Update Chart Candles (Every 15s) ──────────────
            if now - last_chart_update >= chart_update_interval:
                try:
                    intervals_to_cache = set()
                    for name in self.detector_names:
                        tf = self.detector_params.get(name, {}).get("timeframe", self.interval)
                        intervals_to_cache.add(tf)
                    intervals_to_cache.add(self.default_interval)
                    intervals_to_cache.add(self.interval)

                    candles_by_interval = {}
                    for iv in intervals_to_cache:
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

                    with self._results_lock:
                        self._cached_candles.update(candles_by_interval)
                        self._state_version += 1  # Trigger the stream!

                    last_chart_update = time.time()
                except Exception as e:
                    print(f"[{self.pair_id}] Chart cache error: {e}")

            # ── 2. SLOW LOOP: Run Detectors (Every min_interval) ────────────
            if now - self._last_detection_time >= min_interval:
                try:
                    with self._detection_lock:
                        cache = {}
                        results = self._run_detectors(cache)
                        self._process_alerts(results)
                        
                    self._last_detection_time = time.time()

                    with self._results_lock:
                        self._cached_detector_results = results
                        self._state_version += 1  # Trigger the stream!

                    try:
                        from detectors.bias import get_bias
                        from datetime import datetime, timezone
                        
                        now_utc = datetime.now(timezone.utc)
                        current_date = now_utc.strftime("%Y-%m-%d")
                        
                        # Update if cache is empty OR (it is 1 AM UTC or later AND we haven't updated today)
                        if not self._bias_cache or (now_utc.hour >= 1 and getattr(self, '_bias_last_date', None) != current_date):
                            self._bias_cache = get_bias(self.ticker)
                            self._bias_last_date = current_date
                            print(f"[{self.pair_id}] Daily/Weekly bias updated for {current_date}")
                    except Exception as be:
                        print(f"[{self.pair_id}] Bias refresh error: {be}")

                except Exception as e:
                    print(f"[{self.pair_id}] Detection loop error: {e}")

            # Sleep 1 second so the while loop doesn't burn CPU
            time.sleep(1)

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
                    "bias":      self._bias_cache,
                })

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
                "bias":      self._bias_cache,
            })

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    def _api_stream(self):
        from flask import Response, request
        import json, time
        
        chart_interval = request.args.get("interval", self.interval)
        
        def event_stream():
            last_version = -1
            while True:
                if self._state_version > last_version:
                    last_version = self._state_version
                    with self._results_lock:
                        detector_results = dict(self._cached_detector_results)
                        candles = list(self._cached_candles.get(chart_interval, []))
                        
                    if not candles:
                        try:
                            df_chart = self._fetch_df(chart_interval)
                            candles = [{"time": int(idx.timestamp()), "open": float(r["Open"]), "high": float(r["High"]), "low": float(r["Low"]), "close": float(r["Close"])} for idx, r in df_chart.iterrows()]
                        except Exception:
                            candles = []

                    payload = {
                        "pair": self.pair_id,
                        "candles": candles,
                        "detectors": detector_results,
                        "bias": self._bias_cache
                    }

                    # --- NEW CVD STREAMING LOGIC ---
                    # Only calculate CVD if this pair uses the accumulation detector
                    if "accumulation" in self.detector_names:
                        try:
                            from tools.cvd import get_cvd_data, INTRABAR_MAP
                            df_cvd = self._fetch_df(chart_interval)
                            intrabar_df = None
                            intrabar_interval = INTRABAR_MAP.get(chart_interval)
                            
                            if intrabar_interval:
                                try:
                                    intrabar_df = self._fetch_df(intrabar_interval)
                                    if intrabar_df is not None and len(intrabar_df) < 10:
                                        intrabar_df = None
                                except Exception:
                                    intrabar_df = None

                            cvd_result = get_cvd_data(
                                df_cvd, 
                                intrabar_df=intrabar_df, 
                                left_pivot=3, 
                                detect_divs=True
                            )
                            payload["cvd_data"] = cvd_result
                        except Exception as e:
                            print(f"[{self.pair_id}] Stream CVD error: {e}")
                            payload["cvd_data"] = {"cvd": [], "divergences": [], "stats": {}, "has_volume": False}
                    # -------------------------------

                    yield f"data: {json.dumps(payload)}\n\n"
                
                time.sleep(0.5)

        # Add headers to prevent Flask/Nginx from holding the data
        return Response(
            event_stream(), 
            mimetype="text/event-stream",
            headers={
                "X-Accel-Buffering": "no",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive"
            }
        )

    def _api_bias(self):
        """Return current bias for this pair."""
        try:
            now = time.time()
            if self._bias_cache and (now - self._bias_cache_ts) < 86400:
                return jsonify(self._bias_cache)

            from detectors.bias import get_bias
            bias_info = get_bias(self.ticker)
            self._bias_cache = bias_info
            self._bias_cache_ts = now
            return jsonify(bias_info)
        except Exception as e:
            return jsonify({"bias": "misaligned", "aligned": False, "reason": str(e)}), 500

    def _api_cvd(self):
        """
        Return CVD (Cumulative Volume Delta) data for the requested interval.
        Accepts ?interval=1m  (default: pair's default_interval)

        For intervals > 1m, fetches 1m data for intrabar analysis to match
        TradingView's CVD methodology.
        """
        try:
            from tools.cvd import get_cvd_data, INTRABAR_MAP
            interval = request.args.get("interval", self.default_interval)
            df = self._fetch_df(interval)
            if df is None or len(df) < 5:
                return jsonify({
                    "cvd":         [],
                    "divergences": [],
                    "stats":       {},
                    "has_volume":  False,
                })

            # Fetch intrabar data for more accurate CVD calculation
            intrabar_df = None
            intrabar_interval = INTRABAR_MAP.get(interval)
            if intrabar_interval:
                try:
                    intrabar_df = self._fetch_df(intrabar_interval)
                    if intrabar_df is not None and len(intrabar_df) < 10:
                        intrabar_df = None
                except Exception as ie:
                    print(f"[{self.pair_id}] Intrabar fetch error: {ie}")
                    intrabar_df = None

            result = get_cvd_data(
                df, 
                intrabar_df=intrabar_df,
                left_pivot=request.args.get('left_pivot', 3, type=int),
                detect_divs=True
            )
            return jsonify(result)
        except Exception as e:
            import traceback
            print(traceback.format_exc())
            return jsonify({"error": str(e), "cvd": [], "divergences": [], "stats": {}}), 500

    def _api_candle_explain(self):
        """
        Given ?ts=<unix_ts>, explain in plain English why that candle
        is or isn't a valid aggressor (accumulation) or base candle (supply/demand).
        """
        try:
            ts_raw = request.args.get("ts")
            if not ts_raw:
                return jsonify({"error": "ts required"}), 400
            ts = int(ts_raw)

            def clean_df(df):
                if isinstance(df.columns, pd.MultiIndex):
                    df = df.copy()
                    df.columns = df.columns.get_level_values(0)
                df = df.loc[:, ~df.columns.duplicated()].copy()
                for col in ["Open", "High", "Low", "Close"]:
                    df[col] = pd.to_numeric(df[col].squeeze(), errors="coerce")
                return df.dropna(subset=["Open", "High", "Low", "Close"])

            def find_idx(df, ts):
                stamps = [int(idx.timestamp()) for idx in df.index]
                if ts in stamps:
                    return stamps.index(ts)
                return min(range(len(stamps)), key=lambda i: abs(stamps[i] - ts))

            if "accumulation" in self.detector_names:
                from detectors.accumulation import explain_candle

                params = dict(self.detector_params.get("accumulation", {}))
                det_iv = params.pop("timeframe", "1m")
                df     = clean_df(self._get_df(det_iv, {}))
                ci     = find_idx(df, ts)
                lines  = explain_candle(df, ci, params, self.market_timing)

            elif "supply_demand" in self.detector_names:
                from detectors.supply_demand import explain_candle

                params = dict(self.detector_params.get("supply_demand", {}))
                det_iv = params.pop("timeframe", "30m")
                df     = clean_df(self._get_df(det_iv, {}))
                ci     = find_idx(df, ts)
                lines  = explain_candle(df, ci, params, self.market_timing, self.ticker)

            else:
                lines = ["No detector configured for this pair."]

            return jsonify({"lines": lines})

        except Exception as e:
            import traceback
            print(traceback.format_exc())
            return jsonify({"lines": [f"Error: {e}"]}), 500


    # ── Drawing API ───────────────────────────────────────────────────── #

    def _api_drawings_get(self):
        """GET /api/drawings  — return all saved drawings for this pair."""
        try:
            from tools.draw import load_drawings
            return jsonify({"drawings": load_drawings(self.pair_id)})
        except Exception as e:
            return jsonify({"error": str(e), "drawings": []}), 500

    def _api_drawings_post(self):
        """POST /api/drawings  — save a new drawing. Body: drawing dict."""
        try:
            from tools.draw import add_drawing
            body = request.get_json(force=True) or {}
            saved = add_drawing(self.pair_id, body)
            return jsonify({"drawing": saved}), 201
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    def _api_drawings_delete(self, drawing_id):
        """DELETE /api/drawings/<id>  — remove one drawing."""
        try:
            from tools.draw import delete_drawing
            ok = delete_drawing(self.pair_id, drawing_id)
            return jsonify({"deleted": ok})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    
    def _debug(self):
        from config import PAIRS
        tz = os.environ.get("TZ", "Europe/Brussels")
        proxy_prefix = os.environ.get("PROXY_PREFIX", "").rstrip("/")
        pairs_list = [
            {"id": pid, "label": cfg["label"], "port": cfg["port"],
             "url": f"{proxy_prefix}/{pid}" if proxy_prefix else None}
            for pid, cfg in PAIRS.items()
        ]
        return render_template("debug.html",
            pair_id=self.pair_id,
            label=self.label,
            timezone=tz,
            port=self.port,
            proxy_prefix=proxy_prefix,
            pairs=pairs_list,
        )

    def _debug_data(self):
        try:
            from detectors.accumulation import detect as accum_detect
            from tools.sessions import get_current_session

            interval = request.args.get("interval", "1m")
            df = self._get_df(interval, {})

            with self._cache_lock:
                self._df_cache[interval] = df.copy()

            if df is None or len(df) < 5:
                return jsonify({"error": "No data available"}), 200

            params = dict(self.detector_params.get("accumulation", {}))
            params.pop("timeframe", None)

            result = accum_detect(df, debug=True, market_timing=self.market_timing, **params)
            if not result:
                result = {"status": "looking", "windows": [], "best_zone": None,
                          "secondary_zone": None, "candles": []}

            windows = result.get("windows", [])
            reasons = {}
            for w in windows:
                if not w.get("pass") and "skip" not in w and w.get("reject"):
                    key = w["reject"].split(" ")[0]
                    reasons[key] = reasons.get(key, 0) + 1

            return jsonify({
                "pair":              self.pair_id,
                "session":           get_current_session(self.market_timing),
                "status":            result.get("status", "looking"),
                "adx_threshold":     params.get("adx_threshold", 25),
                "last_close": round(float(df["Close"].iloc[-1]), 5),
                "windows_checked":   result.get("windows_checked", 0),
                "passed":            result.get("passed", 0),
                "rejection_summary": reasons,
                "windows":           windows,
                "best_zone":         result.get("best_zone"),
                "secondary_zone":    result.get("secondary_zone"),
                "breakout_candle":   result.get("breakout_candle"),
                "breakout_dir":      result.get("breakout_dir"),
                "breakout_body":     result.get("breakout_body"),
                "impulse_ratio":     result.get("impulse_ratio"),
                "candles":           result.get("candles", [])[:-1],
            })
        except Exception as e:
            import traceback
            return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

    def _debug_replay(self):
        try:
            raw_idx = int(request.args.get("idx", -1))
        except Exception:
            raw_idx = -1

        try:
            from detectors.accumulation import detect as accum_detect
            from tools.sessions import get_current_session

            interval = request.args.get("interval", "1m")
            full_df = _provider_get_df(self.ticker, interval, self.period)

            full_df = full_df.dropna()
            print(f"[REPLAY] raw_idx={raw_idx} raw_total={request.args.get('total')} full_df_len={len(full_df)}")

            # Filter by timestamp bounds if provided (ensures data matches client's snapshot)
            start_ts = request.args.get("start_ts")
            end_ts = request.args.get("end_ts")
            if start_ts and end_ts:
                from datetime import datetime, timezone
                start_dt = datetime.fromtimestamp(int(start_ts), tz=timezone.utc)
                end_dt = datetime.fromtimestamp(int(end_ts), tz=timezone.utc)
                # Handle both timezone-aware and timezone-naive indices
                if full_df.index.tz is None:
                    start_dt = start_dt.replace(tzinfo=None)
                    end_dt = end_dt.replace(tzinfo=None)
                full_df = full_df[(full_df.index >= start_dt) & (full_df.index <= end_dt)]
                print(f"[REPLAY] filtered by timestamps: start={start_ts} end={end_ts} df_len={len(full_df)}")
            else:
                # Fallback to index-based slicing for backward compatibility
                # REPLACE the fallback with this:
                raw_total = request.args.get("total")
                if raw_total:
                    # Instead of just iloc, ensure we don't exceed the original browser snapshot
                    full_df = full_df.iloc[:int(raw_total)]
            if full_df is None or len(full_df) < 5:
                return jsonify({"error": "No data available"}), 200
            params = dict(self.detector_params.get("accumulation", {}))
            params.pop("timeframe", None)
            min_candles = params.get("min_candles", 20)
            # In replay mode, all candles are historical (no "still forming" candle)
            # so we don't remove the last candle like we do in live mode.
            # The timestamp filtering above already ensures correct candle alignment.
            total = len(full_df)
            idx   = raw_idx if raw_idx >= 1 else total
            idx   = max(min_candles + 3, min(idx, total))
            df    = full_df.iloc[:idx].copy() if idx < total else full_df.copy()
            print(f"[REPLAY] idx={idx} total={total} df_len={len(df)} df[-1]={int(df.index[-1].timestamp())} df[-2]={int(df.index[-2].timestamp())}")

            result = accum_detect(df, debug=True, replay=True, market_timing=self.market_timing, **params)
            if not result:
                result = {"status": "looking", "windows": [], "best_zone": None,
                          "secondary_zone": None, "candles": []}

            windows = result.get("windows", [])
            reasons = {}
            for w in windows:
                if not w.get("pass") and "skip" not in w and w.get("reject"):
                    key = w["reject"].split(" ")[0]
                    reasons[key] = reasons.get(key, 0) + 1

            return jsonify({
                "idx":               idx,
                "total":             total,
                "session":           get_current_session(self.market_timing),
                "status":            result.get("status", "looking"),
                "adx_threshold":     params.get("adx_threshold", 25),
                "last_close": round(float(df["Close"].iloc[-1]), 5),
                "windows_checked":   result.get("windows_checked", 0),
                "passed":            result.get("passed", 0),
                "rejection_summary": reasons,
                "windows":           windows,
                "best_zone":         result.get("best_zone"),
                "secondary_zone":    result.get("secondary_zone"),
                "breakout_candle":   result.get("breakout_candle"),
                "breakout_dir":      result.get("breakout_dir"),
                "breakout_body":     result.get("breakout_body"),
                "impulse_ratio":     result.get("impulse_ratio"),
                "candles":           result.get("candles", []),
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

        # NEW: Extract the timeframe from the zone metadata
        tf = zone.get("timeframe_id", "unknown")

        screenshot_path = f"alert_{self.pair_id}_{int(time.time())}.png"
        raw = zone.get("detector", "unknown")
        if raw in ("demand", "supply"):
            detector_name = f"{raw.capitalize()} Zone"
        else:
            detector_name = raw.replace("_", " ").title()
        print(f"[{self.pair_id}] Sending Discord alert for {detector_name}...")


        tf = zone.get("timeframe_id", self.default_interval)
        try:
            if PLAYWRIGHT_AVAILABLE:
                highlight_ts = zone.get("start", "")
                breakout_ts = ""
                if zone.get("breakout_candle"):
                    breakout_ts = zone["breakout_candle"].get("time", "")
                center_ts = breakout_ts or highlight_ts

                # UPDATE: Pass the interval into the URL
                page_url = (
                    f"http://127.0.0.1:{self.port}"
                    f"?highlight={highlight_ts}&center={center_ts}&interval={tf}"
                )
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page(viewport={"width": 1280, "height": 720})
                    page.goto(page_url)
                    try:
                        page.wait_for_function("window._screenshotReady === true", timeout=6000)
                    except Exception:
                        pass
                    page.wait_for_timeout(300)
                    page.screenshot(path=screenshot_path)
                    browser.close()

            if zone.get("detector") in ("demand", "supply"):
                emoji = "📈" if zone.get("detector") == "demand" else "📉"
                content = f"{emoji} **{self.pair_id} ({tf}) — {detector_name} Found**"
            else:
                content = f"🚀 **{self.pair_id} ({tf}) — Aggressor Candle Confirmed**"
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

        from werkzeug.middleware.proxy_fix import ProxyFix
        self.app.wsgi_app = ProxyFix(self.app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

        t = threading.Thread(target=self._detection_loop, daemon=True, name=f"detector-{self.pair_id}")
        t.start()

        self.app.run(host="0.0.0.0", port=self.port, use_reloader=False, threaded=True)
