# Eventlet monkey-patching must be done BEFORE any other imports
import eventlet
eventlet.monkey_patch()

"""
mission_control.py

Central hub Flask app with WebSocket support. Serves:
  /dashboard           → mission-control-dashboard.html (pair grid)
  /chart-view/<pair>   → mission-control-charts.html  (trading terminal)
  /debug               → redirects to first pair's debug page
  /debug/<pair>        → redirects to that pair's debug page (via proxy)
  /proxy/<pair>/api/data         → proxies to individual pair server
  /proxy/<pair>/api/bias         → proxies to individual pair server
  /proxy/<pair>/api/cvd          → proxies CVD data from individual pair server
  /proxy/<pair>/api/candle-explain → proxies to individual pair server
  /proxy/<pair>/debug            → proxies debug page from individual pair server
  /proxy/<pair>/debug/data       → proxies debug data endpoint
  /proxy/<pair>/debug/replay     → proxies debug replay endpoint
  /proxy/<pair>/debug/sd         → proxies debug S&D endpoint
  /proxy/<pair>/debug/sd/bias    → proxies debug S&D bias endpoint
  /proxy/<pair>/debug/fvg        → proxies debug FVG endpoint
  /api/news/<pair>               → fetches live news via yfinance

WebSocket events:
  subscribe     → client subscribes to pair data updates
  unsubscribe   → client unsubscribes from pair updates
  chart_data    → server pushes new chart data to subscribed clients
"""

import os
import json
import requests

from flask import Flask, render_template, jsonify, redirect, request
from flask_socketio import SocketIO, emit, join_room, leave_room

from tools.news import get_news as _get_news
from tools.loaddata import get_loader

# ── Config ─────────────────────────────────────────────────────────────
from config import PAIRS


# ── App setup ──────────────────────────────────────────────────────────
ROOT = os.path.dirname(os.path.abspath(__file__))

app = Flask(
    __name__,
    template_folder=os.path.join(ROOT, "templates"),
    static_folder=os.path.join(ROOT, "static") if os.path.exists(os.path.join(ROOT, "static")) else None,
)

# Configure SocketIO with eventlet for proper WebSocket support
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

# ── DataLoader Setup ───────────────────────────────────────────────────
loader = get_loader()


def broadcast_chart_data(pair_id: str, interval: str, data: dict):
    """Broadcast chart data to all clients subscribed to this pair."""
    room = f"pair:{pair_id.upper()}"
    socketio.emit("chart_data", {
        "pair_id": pair_id.upper(),
        "interval": interval,
        "candles": data.get("candles", []),
        "detectors": data.get("detectors", {}),
    }, room=room)


# Register the broadcast callback
loader.set_broadcast_callback(broadcast_chart_data)


# ── Helpers ────────────────────────────────────────────────────────────

def _pairs_js():
    return [
        {
            "id":          pair_id,
            "label":       cfg.get("label", pair_id),
            "port":        cfg.get("port", 0),
            "type":        "supply_demand" if "supply_demand" in cfg.get("detectors", []) else "accumulation",
            "always_open": cfg.get("market_timing") == "CRYPTO",
        }
        for pair_id, cfg in PAIRS.items()
    ]


def _pairs_list():
    return [{"id": pid, "label": cfg["label"]} for pid, cfg in PAIRS.items()]


def _proxy_to(pair_id, path):
    """Forward a request to the pair's local server, return the response."""
    cfg = PAIRS.get(pair_id.upper())
    if not cfg:
        return None, f"Unknown pair: {pair_id}", 404
    try:
        qs = request.query_string.decode()
        url = f"http://127.0.0.1:{cfg['port']}{path}"
        if qs:
            url += "?" + qs
        r = requests.get(url, timeout=15)
        return r, None, None
    except Exception as e:
        return None, str(e), 502


# ── WebSocket Events ───────────────────────────────────────────────────

@socketio.on("connect")
def handle_connect():
    """Handle client connection."""
    print(f"[WebSocket] Client connected: {request.sid}")


@socketio.on("disconnect")
def handle_disconnect():
    """Handle client disconnection."""
    print(f"[WebSocket] Client disconnected: {request.sid}")


@socketio.on("subscribe")
def handle_subscribe(data):
    """Handle client subscription to a pair's data updates."""
    pair_id = data.get("pair_id", "").upper()
    interval = data.get("interval", "1m")

    if pair_id not in PAIRS:
        emit("error", {"message": f"Unknown pair: {pair_id}"})
        return

    room = f"pair:{pair_id}"
    join_room(room)
    print(f"[WebSocket] Client {request.sid} subscribed to {pair_id}")

    # Send cached data immediately if available
    cached = loader.get_data(pair_id, interval)
    if cached:
        emit("chart_data", {
            "pair_id": pair_id,
            "interval": interval,
            "candles": cached.get("candles", []),
            "detectors": cached.get("detectors", {}),
        })


@socketio.on("unsubscribe")
def handle_unsubscribe(data):
    """Handle client unsubscription from a pair's data updates."""
    pair_id = data.get("pair_id", "").upper()
    room = f"pair:{pair_id}"
    leave_room(room)
    print(f"[WebSocket] Client {request.sid} unsubscribed from {pair_id}")


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
        always_open=cfg.get("market_timing") == "CRYPTO",
        timezone=tz,
        default_interval=default_interval,
        detector_type=detector_type,
        pairs=_pairs_list(),
    )


# ── Debug routes ────────────────────────────────────────────────────────

@app.route("/debug")
def debug_default():
    """Redirect to the first pair's debug page via the built-in proxy."""
    first_pair = next(iter(PAIRS))
    return redirect(f"/proxy/{first_pair}/debug")


@app.route("/debug/<pair_id>")
def debug_pair(pair_id):
    """Redirect to that pair's debug page via the built-in proxy."""
    pair_id = pair_id.upper()
    if pair_id not in PAIRS:
        return f"Unknown pair: {pair_id}", 404
    return redirect(f"/proxy/{pair_id}/debug")


# ── Proxy routes ─────────────────────────────────────────────────────────

def _json_proxy(pair_id, path):
    r, err, code = _proxy_to(pair_id, path)
    if err:
        return jsonify({"error": err}), code
    return (r.content, r.status_code, {"Content-Type": "application/json"})


def _html_proxy(pair_id, path):
    r, err, code = _proxy_to(pair_id, path)
    if err:
        return f"Could not reach pair server: {err}", code
    return (r.content, r.status_code, {"Content-Type": "text/html"})


@app.route("/proxy/<pair_id>/api/data")
def proxy_api_data(pair_id):
    return _json_proxy(pair_id, "/api/data")


@app.route("/proxy/<pair_id>/api/bias")
def proxy_api_bias(pair_id):
    return _json_proxy(pair_id, "/api/bias")


@app.route("/proxy/<pair_id>/api/cvd")
def proxy_api_cvd(pair_id):
    return _json_proxy(pair_id, "/api/cvd")


@app.route("/proxy/<pair_id>/api/candle-explain")
def proxy_api_candle_explain(pair_id):
    return _json_proxy(pair_id, "/api/candle-explain")


def _method_proxy(pair_id, path):
    """Forward GET / POST / DELETE with body to the pair server."""
    cfg = PAIRS.get(pair_id.upper())
    if not cfg:
        return jsonify({"error": f"Unknown pair: {pair_id}"}), 404
    try:
        qs  = request.query_string.decode()
        url = f"http://127.0.0.1:{cfg['port']}{path}"
        if qs:
            url += "?" + qs
        method = request.method
        kwargs = {"timeout": 15}
        if method in ("POST", "PUT", "PATCH"):
            kwargs["json"]    = request.get_json(force=True, silent=True)
            kwargs["headers"] = {"Content-Type": "application/json"}
        r = requests.request(method, url, **kwargs)
        return (r.content, r.status_code, {"Content-Type": "application/json"})
    except Exception as e:
        return jsonify({"error": str(e)}), 502


@app.route("/proxy/<pair_id>/api/drawings", methods=["GET", "POST"])
def proxy_api_drawings(pair_id):
    return _method_proxy(pair_id, "/api/drawings")


@app.route("/proxy/<pair_id>/api/drawings/<drawing_id>", methods=["DELETE"])
def proxy_api_drawings_delete(pair_id, drawing_id):
    return _method_proxy(pair_id, f"/api/drawings/{drawing_id}")


@app.route("/proxy/<pair_id>/debug")
def proxy_debug(pair_id):
    return _html_proxy(pair_id, "/debug")


@app.route("/proxy/<pair_id>/debug/data")
def proxy_debug_data(pair_id):
    return _json_proxy(pair_id, "/debug/data")


@app.route("/proxy/<pair_id>/debug/replay")
def proxy_debug_replay(pair_id):
    return _json_proxy(pair_id, "/debug/replay")


@app.route("/proxy/<pair_id>/debug/sd")
def proxy_debug_sd(pair_id):
    return _json_proxy(pair_id, "/debug/sd")


@app.route("/proxy/<pair_id>/debug/sd/bias")
def proxy_debug_sd_bias(pair_id):
    return _json_proxy(pair_id, "/debug/sd/bias")


@app.route("/proxy/<pair_id>/debug/fvg")
def proxy_debug_fvg(pair_id):
    return _json_proxy(pair_id, "/debug/fvg")


@app.route("/api/news/<pair_id>")
def api_news(pair_id):
    pair_id = pair_id.upper()
    if pair_id not in PAIRS:
        return jsonify({"error": "unknown pair"}), 404
    cfg = PAIRS[pair_id]
    yf_ticker = cfg.get("yf_ticker") or cfg.get("ticker")
    articles = _get_news(pair_id, yf_ticker)
    return jsonify({"articles": articles, "cached": False})


# ── Run ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("MISSION_CONTROL_PORT", 9000))

    # Start the background data loader
    loader.start()

    print(f"[MissionControl] Starting on http://0.0.0.0:{port}")
    print(f"[MissionControl] Dashboard:  http://localhost:{port}/dashboard")
    print(f"[MissionControl] Chart view: http://localhost:{port}/chart-view/<PAIR>")
    print(f"[MissionControl] Debug:      http://localhost:{port}/debug")
    print(f"[MissionControl] WebSocket:  ws://localhost:{port}/socket.io/")

    # Use SocketIO's run method instead of Flask's
    socketio.run(app, host="0.0.0.0", port=port, use_reloader=False)
