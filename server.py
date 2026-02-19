"""
server.py

PairServer — a self-contained Flask server instance for a single trading pair.
Each pair runs in its own thread on its own port.
"""

import os
import time
import threading
import pandas as pd
import yfinance as yf
from flask import Flask, render_template, jsonify, request

from detectors import run_detectors

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


class PairServer:
    """
    Encapsulates a Flask app and all state for a single trading pair.
    """

    def __init__(self, pair_id: str, config: dict):
        self.pair_id = pair_id
        self.ticker = config["ticker"]
        self.port = config["port"]
        self.label = config["label"]
        self.interval = config.get("interval", "1m")
        self.period = config.get("period", "1d")
        self.detector_names = config.get("detectors", [])
        self.detector_params = config.get("detector_params", {})

        # Per-pair alert dedup tracking (keyed by detector name)
        self.last_alerted: dict[str, int] = {}
        # Tracks zones seen while active so we can fire alert on breakout
        self.last_active_zone: dict[str, dict] = {}

        # Resolve templates relative to server.py's own location — NOT cwd.
        # This ensures templates are found regardless of where PM2 launches from.
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

        # Each function must have a globally unique __name__ — Flask uses the
        # function name as the endpoint key. Without unique names all pairs
        # share the same endpoint and the last one registered wins everywhere.

        def _index():
            return render_template(
                "index.html",
                pair_id=pair_id,
                label=self.label,
                port=self.port,
            )
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
    # Data + Detection
    # ------------------------------------------------------------------ #

    def _fetch_df(self, interval=None):
        interval = interval or self.interval
        # Longer timeframes need a wider period to get enough candles
        period_map = {
            "1m":  "1d",
            "2m":  "1d",
            "5m":  "5d",
            "15m": "5d",
            "30m": "5d",
            "1h":  "30d",
        }
        period = period_map.get(interval, self.period)
        df = yf.download(
            self.ticker,
            period=period,
            interval=interval,
            progress=False,
        )
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df.dropna()

    def _api_data(self):
        try:
            # Chart interval can be switched by the user via ?interval= param
            chart_interval = request.args.get("interval", self.interval)

            # Detectors always run on 1m data regardless of chart view
            df_1m = self._fetch_df(interval="1m")
            detector_results = run_detectors(self.detector_names, df_1m, self.detector_params)

            # Fetch chart candles at the requested interval
            if chart_interval == "1m":
                df = df_1m
            else:
                df = self._fetch_df(interval=chart_interval)

            # Alert on BREAKOUT: fire once when a previously-active zone is broken
            for name, zone in detector_results.items():
                prev = self.last_active_zone.get(name)

                if zone and zone.get("is_active") and zone.get("status") == "found":
                    # Zone is active and confirmed — remember it, don't alert yet
                    self.last_active_zone[name] = zone

                elif prev is not None and (zone is None or not zone.get("is_active")):
                    # Had an active zone last tick, now price has broken out — alert
                    already_alerted = self.last_alerted.get(name, 0)
                    if prev["start"] > already_alerted:
                        self.last_alerted[name] = prev["start"]
                        self.last_active_zone[name] = None
                        threading.Thread(
                            target=self._send_discord_alert,
                            args=(prev,),
                            daemon=True,
                        ).start()

            candles = [
                {
                    "time": int(idx.timestamp()),
                    "open": float(r["Open"]),
                    "high": float(r["High"]),
                    "low": float(r["Low"]),
                    "close": float(r["Close"]),
                }
                for idx, r in df.iterrows()
            ]

            return jsonify(
                {
                    "pair": self.pair_id,
                    "label": self.label,
                    "candles": candles,
                    "detectors": detector_results,
                }
            )

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
        threading.Thread(
            target=self._send_discord_alert, args=(test_zone,), daemon=True
        ).start()
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
            embed.add_embed_field(
                name="Action",
                value="Draw a fixed range volume profile from the high to the low.",
            )
            embed.add_embed_field(
                name="Signal",
                value="If a low volume pocket is found, wait for a CVDD!",
            )
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
        self.app.run(host="0.0.0.0", port=self.port, use_reloader=False)
