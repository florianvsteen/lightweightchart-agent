"""
mission_control.py

Central hub Flask app. Serves:
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
"""

import os
import json
import requests

from flask import Flask, render_template, jsonify, redirect, request, Response
from tools.news import get_news as _get_news

# ── Config ─────────────────────────────────────────────────────────────
from config import PAIRS


# ── App setup ──────────────────────────────────────────────────────────
ROOT = os.path.dirname(os.path.abspath(__file__))

app = Flask(
    __name__,
    template_folder=os.path.join(ROOT, "templates"),
    static_folder=os.path.join(ROOT, "static") if os.path.exists(os.path.join(ROOT, "static")) else None,
)

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


@app.route("/proxy/<pair_id>/api/stream")
def proxy_api_stream(pair_id):
    from flask import Response
    import requests
    
    cfg = PAIRS.get(pair_id.upper())
    if not cfg:
        return jsonify({"error": f"Unknown pair: {pair_id}"}), 404
        
    qs = request.query_string.decode()
    url = f"http://127.0.0.1:{cfg['port']}/api/stream"
    if qs:
        url += "?" + qs

    def stream_generator():
        try:
            with requests.get(url, stream=True, timeout=86400) as r:
                for line in r.iter_lines():
                    if line:
                        yield f"{line.decode('utf-8')}\n\n"
        except Exception as e:
            yield f"data: {{\"error\": \"{str(e)}\"}}\n\n"

    # FIX: Anti-buffering headers for the proxy hub too
    return Response(
        stream_generator(), 
        mimetype="text/event-stream", 
        headers={
            "X-Accel-Buffering": "no", 
            "Cache-Control": "no-cache", 
            "Connection": "keep-alive"
        }
    )

# ── Run ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("MISSION_CONTROL_PORT", 9000))
    print(f"[MissionControl] Starting on http://0.0.0.0:{port}")
    print(f"[MissionControl] Dashboard:  http://localhost:{port}/dashboard")
    print(f"[MissionControl] Chart view: http://localhost:{port}/chart-view/<PAIR>")
    print(f"[MissionControl] Debug:      http://localhost:{port}/debug")
    app.run(host="0.0.0.0", port=port, use_reloader=False, threaded=True)
