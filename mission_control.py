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


# ── News API ────────────────────────────────────────────────────────────

# Simple in-memory cache: { pair_id: {"ts": epoch, "articles": [...]} }
_news_cache: dict = {}
_NEWS_TTL_SECONDS = 300  # 5 minutes

PAIR_NEWS_CONTEXT = {
    # Forex
    "EURUSD": "EUR/USD forex pair, Euro US Dollar, ECB Federal Reserve monetary policy",
    "GBPUSD": "GBP/USD forex pair, British Pound US Dollar, Bank of England Fed policy",
    "USDJPY": "USD/JPY forex pair, US Dollar Japanese Yen, Bank of Japan yen intervention",
    "EURGBP": "EUR/GBP forex pair, Euro British Pound, ECB Bank of England",
    "AUDUSD": "AUD/USD forex pair, Australian Dollar, RBA rate decision",
    "USDCAD": "USD/CAD forex pair, US Dollar Canadian Dollar, oil prices Bank of Canada",
    "USDCHF": "USD/CHF forex pair, US Dollar Swiss Franc, SNB safe haven",
    "NZDUSD": "NZD/USD forex pair, New Zealand Dollar RBNZ",
    "EURJPY": "EUR/JPY cross, Euro Yen",
    "GBPJPY": "GBP/JPY cross, British Pound Yen",
    "AUDJPY": "AUD/JPY cross, Australian Dollar Yen",
    "CADJPY": "CAD/JPY cross, Canadian Dollar Yen",
    # Metals
    "XAUUSD": "Gold price XAU/USD, gold market safe haven inflation hedge",
    "XAGUSD": "Silver price XAG/USD, silver market",
    # Crypto
    "BTCUSD": "Bitcoin BTC/USD price, crypto market",
    "ETHUSD": "Ethereum ETH/USD price, crypto market",
    # Indices
    "US30":   "Dow Jones Industrial Average DJIA US30",
    "NAS100": "NASDAQ 100 tech stocks US tech market",
    "SPX500": "S&P 500 index US equities",
}


def _fetch_news_via_claude(pair_id: str) -> list:
    """
    Use the Claude API with web_search tool to fetch live news for a pair.
    Returns list of {headline, source, sentiment} dicts.
    """
    context = PAIR_NEWS_CONTEXT.get(pair_id.upper(), pair_id)
    prompt = (
        f"Search for the latest financial news about {context}. "
        f"Return ONLY a JSON array of up to 8 articles with these fields: "
        f'headline (string), source (string), sentiment ("bullish", "bearish", or "neutral"). '
        f"Focus on news from the past 24 hours. "
        f"Return ONLY the JSON array, no other text, no markdown code fences."
    )

    try:
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        headers = {
            "x-api-key":         api_key,
            "anthropic-version": "2023-06-01",
            "content-type":      "application/json",
            "anthropic-beta":    "tools-2024-04-04",
        }
        body = {
            "model":      "claude-sonnet-4-20250514",
            "max_tokens": 1000,
            "tools": [
                {"type": "web_search_20250305", "name": "web_search"}
            ],
            "messages": [
                {"role": "user", "content": prompt}
            ],
        }
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers,
            json=body,
            timeout=30,
        )
        if not r.ok:
            print(f"[news] Claude API error {r.status_code}: {r.text[:200]}")
            return []

        data = r.json()
        # Extract the text block from content
        text_content = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                text_content = block.get("text", "")
                break

        if not text_content:
            return []

        # Strip potential markdown fences
        clean = text_content.strip()
        if clean.startswith("```"):
            clean = clean.split("\n", 1)[-1]
            if clean.endswith("```"):
                clean = clean[:-3].strip()

        articles = json.loads(clean)
        if not isinstance(articles, list):
            return []

        # Validate and clamp
        result = []
        for a in articles[:8]:
            if isinstance(a, dict) and "headline" in a:
                result.append({
                    "headline":  str(a.get("headline", ""))[:300],
                    "source":    str(a.get("source", ""))[:80],
                    "sentiment": a.get("sentiment", "neutral") if a.get("sentiment") in ("bullish","bearish","neutral") else "neutral",
                })
        return result

    except Exception as e:
        print(f"[news] Error fetching news for {pair_id}: {e}")
        return []


@app.route("/api/news/<pair_id>")
def api_news(pair_id):
    import time
    pair_id = pair_id.upper()

    if pair_id not in PAIRS:
        return jsonify({"error": "unknown pair"}), 404

    cached = _news_cache.get(pair_id)
    if cached and (time.time() - cached["ts"]) < _NEWS_TTL_SECONDS:
        return jsonify({"articles": cached["articles"], "cached": True})

    articles = _fetch_news_via_claude(pair_id)

    _news_cache[pair_id] = {"ts": time.time(), "articles": articles}
    return jsonify({"articles": articles, "cached": False})


# ── Run ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("MISSION_CONTROL_PORT", 9000))
    print(f"[MissionControl] Starting on http://0.0.0.0:{port}")
    print(f"[MissionControl] Dashboard:  http://localhost:{port}/dashboard")
    print(f"[MissionControl] Chart view: http://localhost:{port}/chart-view/<PAIR>")
    print(f"[MissionControl] Debug:      http://localhost:{port}/debug")
    app.run(host="0.0.0.0", port=port, use_reloader=False, threaded=True)
