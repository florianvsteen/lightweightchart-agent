"""
mission_control.py

Central hub Flask app. Serves:
  /dashboard           → mission-control-dashboard.html (pair grid)
  /chart-view/<pair>   → mission-control-charts.html  (trading terminal)
  /debug               → redirects to first pair's debug page
  /debug/<pair>        → redirects to that pair's debug page
  /proxy/<pair>/api/data         → proxies to individual pair server
  /proxy/<pair>/api/bias         → proxies to individual pair server
  /proxy/<pair>/api/candle-explain → proxies to individual pair server
  /proxy/<pair>/debug            → proxies debug page from individual pair server
  /api/news/<pair>               → fetches live news via Claude API + web search
"""

import os
import json
import requests

from flask import Flask, render_template, jsonify, redirect
from news import get_news as _get_news

# ── Config ─────────────────────────────────────────────────────────────
from config import PAIRS


# News
import threading
from news import warmup as news_warmup
threading.Thread(target=news_warmup, daemon=True).start()

# ── App setup ──────────────────────────────────────────────────────────
ROOT = os.path.dirname(os.path.abspath(__file__))

app = Flask(
    __name__,
    template_folder=os.path.join(ROOT, "templates"),
    static_folder=os.path.join(ROOT, "static") if os.path.exists(os.path.join(ROOT, "static")) else None,
)

# ── Helpers ────────────────────────────────────────────────────────────

def _pairs_js():
    """Build the list of pairs to pass as JSON to dashboard template."""
    return [
        {
            "id":          pair_id,
            "label":       cfg.get("label", pair_id),
            "port":        cfg.get("port", 0),
            "type":        "supply_demand" if "supply_demand" in cfg.get("detectors", []) else "accumulation",
            "always_open": cfg.get("always_open", False),
        }
        for pair_id, cfg in PAIRS.items()
    ]


def _pairs_list():
    """Simple list for template iteration (chart view pair selector)."""
    return [{"id": pid, "label": cfg["label"]} for pid, cfg in PAIRS.items()]


# ── Routes ─────────────────────────────────────────────────────────────

@app.route("/")
@app.route("/dashboard")
def dashboard():
    return render_template(
        "mission-control-dashboard.html",
        pairs_js=json.dumps(_pairs_js()),
    )


@app.route("/chart-view/<pair_id>")
def chart_view(pair_id):
    pair_id = pair_id.upper()
    cfg = PAIRS.get(pair_id)
    if not cfg:
        return f"Unknown pair: {pair_id}", 404

    tz = os.environ.get("TZ", "UTC")
    detector_type = "supply_demand" if "supply_demand" in cfg.get("detectors", []) else "accumulation"
    default_interval = cfg.get("default_interval", cfg.get("interval", "1m"))

    return render_template(
        "mission-control-charts.html",
        pair_id=pair_id,
        label=cfg["label"],
        always_open=cfg.get("always_open", False),
        timezone=tz,
        default_interval=default_interval,
        detector_type=detector_type,
        pairs=_pairs_list(),
    )


# ── Debug routes ─────────────────────────────────────────────────────────

@app.route("/debug")
def debug_default():
    """Redirect to the debug page of the first configured pair."""
    first_pair = next(iter(PAIRS))
    return redirect(f"/debug/{first_pair}")


@app.route("/debug/<pair_id>")
def debug_pair(pair_id):
    """Redirect to that pair's debug page on its individual port."""
    pair_id = pair_id.upper()
    cfg = PAIRS.get(pair_id)
    if not cfg:
        return f"Unknown pair: {pair_id}", 404
    port = cfg["port"]
    from flask import request as flask_req
    host = flask_req.host.split(":")[0]
    return redirect(f"http://{host}:{port}/debug")


# ── Proxy routes ────────────────────────────────────────────────────────

@app.route("/proxy/<pair_id>/api/data")
def proxy_api_data(pair_id):
    cfg = PAIRS.get(pair_id.upper())
    if not cfg:
        return jsonify({"error": "unknown pair"}), 404
    try:
        from flask import request as flask_req
        qs = flask_req.query_string.decode()
        url = f"http://127.0.0.1:{cfg['port']}/api/data"
        if qs:
            url += "?" + qs
        r = requests.get(url, timeout=15)
        return (r.content, r.status_code, {"Content-Type": "application/json"})
    except Exception as e:
        return jsonify({"error": str(e)}), 502


@app.route("/proxy/<pair_id>/api/bias")
def proxy_api_bias(pair_id):
    cfg = PAIRS.get(pair_id.upper())
    if not cfg:
        return jsonify({"error": "unknown pair"}), 404
    try:
        r = requests.get(f"http://127.0.0.1:{cfg['port']}/api/bias", timeout=10)
        return (r.content, r.status_code, {"Content-Type": "application/json"})
    except Exception as e:
        return jsonify({"bias": "misaligned", "aligned": False, "reason": str(e)}), 502


@app.route("/proxy/<pair_id>/api/candle-explain")
def proxy_api_candle_explain(pair_id):
    cfg = PAIRS.get(pair_id.upper())
    if not cfg:
        return jsonify({"error": "unknown pair"}), 404
    try:
        from flask import request as flask_req
        qs = flask_req.query_string.decode()
        url = f"http://127.0.0.1:{cfg['port']}/api/candle-explain"
        if qs:
            url += "?" + qs
        r = requests.get(url, timeout=15)
        return (r.content, r.status_code, {"Content-Type": "application/json"})
    except Exception as e:
        return jsonify({"lines": [f"Error: {str(e)}"]}), 502


@app.route("/proxy/<pair_id>/debug")
def proxy_debug(pair_id):
    """Proxy the debug HTML page from the individual pair server."""
    cfg = PAIRS.get(pair_id.upper())
    if not cfg:
        return f"Unknown pair: {pair_id}", 404
    try:
        from flask import request as flask_req
        qs = flask_req.query_string.decode()
        url = f"http://127.0.0.1:{cfg['port']}/debug"
        if qs:
            url += "?" + qs
        r = requests.get(url, timeout=15)
        return (r.content, r.status_code, {"Content-Type": "text/html"})
    except Exception as e:
        return f"Could not reach pair server: {e}", 502


@app.route("/api/news/<pair_id>")
def api_news(pair_id):
    pair_id = pair_id.upper()

    if pair_id not in PAIRS:
        return jsonify({"error": "unknown pair"}), 404

    # Resolve the yfinance ticker from config so news.py uses the right symbol
    cfg       = PAIRS[pair_id]
    yf_ticker = cfg.get("yf_ticker") or cfg.get("ticker")

    articles = _get_news(pair_id, yf_ticker)
    return jsonify({"articles": articles, "cached": False})


# ── Run ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("MISSION_CONTROL_PORT", 9000))
    print(f"[MissionControl] Starting on http://0.0.0.0:{port}")
    print(f"[MissionControl] Dashboard:  http://localhost:{port}/dashboard")
    print(f"[MissionControl] Chart view: http://localhost:{port}/chart-view/<PAIR>")
    print(f"[MissionControl] Debug:      http://localhost:{port}/debug")
    app.run(host="0.0.0.0", port=port, use_reloader=False, threaded=True)
