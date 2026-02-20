"""
server.py

PairServer — a self-contained Flask server instance for a single trading pair.
Each pair runs in its own thread on its own port.

Detection runs in a background thread every 30 seconds — completely independent
of whether anyone has the browser open. Discord alerts fire from there.
The Flask routes only serve chart data to the browser when it's open.
"""

import os
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

        # Alert dedup
        self.last_alerted: dict[str, int] = {}
        self.last_active_zone: dict[str, dict] = {}

        # Per-request DataFrame cache (cleared each cycle)
        self._df_cache: dict[str, pd.DataFrame] = {}
        self._cache_lock = threading.Lock()

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
            return render_template("index.html", pair_id=pair_id, label=self.label, port=self.port)
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

    # ------------------------------------------------------------------ #
    # Data fetching
    # ------------------------------------------------------------------ #

    def _fetch_df(self, interval: str) -> pd.DataFrame:
        period = PERIOD_MAP.get(interval, self.period)
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
                    results[name] = fn(df, **params)
                except Exception as e:
                    print(f"[ERROR] Detector '{name}' failed: {e}")
                    results[name] = None
        return results

    def _process_alerts(self, detector_results: dict):
        """Check results and fire Discord alerts on breakout."""
        for name, zone in detector_results.items():
            prev = self.last_active_zone.get(name)

            if zone and not isinstance(zone, list) and zone.get("is_active") and zone.get("status") == "found":
                self.last_active_zone[name] = zone

            elif prev is not None and (zone is None or (not isinstance(zone, list) and not zone.get("is_active"))):
                already_alerted = self.last_alerted.get(name, 0)
                if prev["start"] > already_alerted:
                    self.last_alerted[name] = prev["start"]
                    self.last_active_zone[name] = None
                    threading.Thread(
                        target=self._send_discord_alert,
                        args=(prev,),
                        daemon=True,
                    ).start()

    # ------------------------------------------------------------------ #
    # Background detection loop — runs regardless of browser
    # ------------------------------------------------------------------ #

    def _detection_loop(self):
        print(f"[{self.pair_id}] Background detector started (every {DETECTION_INTERVAL}s)")
        while True:
            try:
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
        detector_name = zone.get("detector", "unknown").capitalize()
        print(f"[{self.pair_id}] Sending Discord alert for {detector_name} zone...")

        try:
            if PLAYWRIGHT_AVAILABLE:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page(viewport={"width": 1280, "height": 720})
                    page.goto(f"http://127.0.0.1:{self.port}")
                    page.wait_for_timeout(6000)
                    page.screenshot(path=screenshot_path)
                    browser.close()

            duration_min = (zone["end"] - zone["start"]) // 60
            content = f"🚀 **{self.pair_id} — {detector_name} Confirmed ({duration_min}m)**"
            webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, content=content)

            embed = DiscordEmbed(title="Market Consolidation", color="03b2f8")
            embed.add_embed_field(name="Action", value="Draw a fixed range volume profile from the high to the low.")
            embed.add_embed_field(name="Signal", value="If a low volume pocket is found, wait for a CVDD!")
            embed.set_timestamp()
            webhook.add_embed(embed)

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

    def run(self):
        print(f"[{self.pair_id}] Starting on http://0.0.0.0:{self.port}")

        # Start background detection loop in a daemon thread
        t = threading.Thread(target=self._detection_loop, daemon=True, name=f"detector-{self.pair_id}")
        t.start()

        # Start Flask (blocks this thread)
        self.app.run(host="0.0.0.0", port=self.port, use_reloader=False)
