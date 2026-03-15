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
from tools.sessions import get_sessions_for_js, FOREX
from tools.calendar import get_calendar
from tools.macro  import get_all, get_ai_overview, get_market_mood, get_market_policy, get_flow_analysis, get_bearing, get_pulse
from tools.market import get_market_snapshot, get_chart_data
from tools.news_macro import get_headlines, format_age

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
        sessions_js=json.dumps(get_sessions_for_js(FOREX)),
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
    market_timing = cfg.get("market_timing", FOREX)

    return render_template(
        "mission-control-charts.html",
        pair_id=pair_id,
        label=cfg["label"],
        always_open=cfg.get("market_timing") == "CRYPTO",
        timezone=tz,
        default_interval=default_interval,
        detector_type=detector_type,
        pairs=_pairs_list(),
        sessions_js=json.dumps(get_sessions_for_js(market_timing)),
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


@app.route("/bloomberg")
def bloomberg_tv():
    return render_template("bloomberg-tv.html")

@app.route("/api/calendar")
def api_calendar():
    """
    Returns today's + tomorrow's high/medium impact economic events
    for EUR, GBP, USD, JPY — with Claude AI analysis per event.
    """
    try:
        force = request.args.get("refresh") == "1"
        events = get_calendar(force_refresh=force)
        return jsonify({"events": events, "count": len(events)})
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({"error": str(e), "events": []}), 500

@app.route('/calendar-page')
def calendar_page():
    return render_template('calendar-page.html')

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
    cfg = PAIRS.get(pair_id.upper())
    if not cfg:
        return jsonify({"error": f"Unknown pair: {pair_id}"}), 404
        
    qs = request.query_string.decode()
    url = f"http://127.0.0.1:{cfg['port']}/api/stream"
    if qs:
        url += "?" + qs

    def stream_generator():
        import requests
        try:
            # stream=True is critical here! It keeps the connection open.
            with requests.get(url, stream=True, timeout=86400) as r:
                for line in r.iter_lines():
                    if line:
                        # Yield the exact SSE format back to the browser
                        yield f"{line.decode('utf-8')}\n\n"
        except Exception as e:
            yield f"data: {{\"error\": \"{str(e)}\"}}\n\n"

    return Response(
        stream_generator(), 
        mimetype="text/event-stream",
        headers={
            "X-Accel-Buffering": "no",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
    )

"""
Currency strength is calculated by measuring each currency's average
performance against the other 3 majors, normalized to a -30/+30 scale.

Pairs used:
  EUR: EURUSD, EURGBP, EURJPY
  GBP: GBPUSD, EURGBP (inverted), GBPJPY
  USD: EURUSD (inverted), GBPUSD (inverted), USDJPY
  JPY: USDJPY (inverted), EURJPY (inverted), GBPJPY (inverted)

Each "strength" point = % change from session open, averaged across pairs.
We return ~100 data points spaced evenly over the last trading day.
"""

@app.route("/api/currency-strength")
def api_currency_strength():
    """
    Returns time-series strength data for EUR, GBP, USD, JPY.
    Each point: { time, EUR, GBP, USD, JPY }
    """
    import yfinance as yf
    import pandas as pd
    import numpy as np
    from datetime import datetime, timezone

    try:
        # Fetch 1-day 5m data for all needed pairs
        tickers = {
            "EURUSD": "EURUSD=X",
            "GBPUSD": "GBPUSD=X",
            "USDJPY": "USDJPY=X",
            "EURGBP": "EURGBP=X",
            "EURJPY": "EURJPY=X",
            "GBPJPY": "GBPJPY=X",
        }

        dfs = {}
        for name, sym in tickers.items():
            try:
                df = yf.download(sym, period="1d", interval="5m", progress=False)
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                df = df.dropna()
                if len(df) > 5:
                    dfs[name] = df["Close"].squeeze()
            except Exception:
                pass

        if not dfs:
            return jsonify({"error": "No data available"}), 500

        # Align all series to a common index
        combined = pd.DataFrame(dfs)
        combined = combined.dropna()

        if len(combined) < 3:
            return jsonify({"error": "Insufficient data"}), 500

        # Calculate % return from the first candle (session open)
        first = combined.iloc[0]
        pct = ((combined - first) / first * 100)

        # Currency strength = average of signed cross-pair performance
        # EUR: avg(EURUSD, EURGBP, EURJPY)
        # GBP: avg(GBPUSD, EURGBP inverted, GBPJPY)
        # USD: avg(EURUSD inverted, GBPUSD inverted, USDJPY)
        # JPY: avg(USDJPY inverted, EURJPY inverted, GBPJPY inverted)

        strength = pd.DataFrame(index=combined.index)

        eur_cols = [c for c in ["EURUSD", "EURGBP", "EURJPY"] if c in pct.columns]
        gbp_cols_pos = [c for c in ["GBPUSD", "GBPJPY"] if c in pct.columns]
        gbp_cols_neg = [c for c in ["EURGBP"] if c in pct.columns]
        usd_cols_neg = [c for c in ["EURUSD", "GBPUSD"] if c in pct.columns]
        usd_cols_pos = [c for c in ["USDJPY"] if c in pct.columns]
        jpy_cols_neg = [c for c in ["USDJPY", "EURJPY", "GBPJPY"] if c in pct.columns]

        def safe_mean(pos_cols, neg_cols):
            parts = []
            for c in pos_cols:
                if c in pct.columns:
                    parts.append(pct[c])
            for c in neg_cols:
                if c in pct.columns:
                    parts.append(-pct[c])
            if not parts:
                return pd.Series(0, index=combined.index)
            return pd.concat(parts, axis=1).mean(axis=1)

        strength["EUR"] = safe_mean(eur_cols, [])
        strength["GBP"] = safe_mean(gbp_cols_pos, gbp_cols_neg)
        strength["USD"] = safe_mean(usd_cols_pos, usd_cols_neg)
        strength["JPY"] = safe_mean([], jpy_cols_neg)

        # Smooth slightly (3-period rolling) to reduce noise
        strength = strength.rolling(3, min_periods=1).mean()

        # Downsample to ~120 points max
        if len(strength) > 120:
            step = len(strength) // 120
            strength = strength.iloc[::step]

        # Build output
        points = []
        for ts, row in strength.iterrows():
            try:
                t = int(ts.timestamp())
            except Exception:
                continue
            points.append({
                "time": t,
                "EUR":  round(float(row["EUR"]), 4),
                "GBP":  round(float(row["GBP"]), 4),
                "USD":  round(float(row["USD"]), 4),
                "JPY":  round(float(row["JPY"]), 4),
            })

        return jsonify({
            "points": points,
            "updated": int(datetime.now(timezone.utc).timestamp()),
        })

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@app.route("/macro")
def macro_page():
    import json
    from tools.market import INSTRUMENTS
    # Pass instrument keys to the template so the JS ticker strip
    # and chart selector are always in sync with config.PAIRS
    instruments_js = json.dumps(list(INSTRUMENTS.keys()))
    return render_template("macro.html", instruments_js=instruments_js)
 
@app.route("/macro/<pair_id>")
def macro_detail_page(pair_id):
    """Detail page for a specific pair — no pair selector bar."""
    from tools.market import INSTRUMENTS
    pair_id = pair_id.upper()
    if pair_id not in INSTRUMENTS:
        return redirect('/macro')
    return render_template("macro-detail.html", pair_id=pair_id)
 
@app.route("/api/macro/snapshot")
def api_macro_snapshot():
    """Fast endpoint — returns cached market prices only."""
    try:
        force = request.args.get("refresh") == "1"
        data  = get_market_snapshot(force=force)
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
 
 
@app.route("/api/macro/chart/<symbol>")
def api_macro_chart(symbol):
    """OHLCV candles for a given symbol key (e.g. SPX, GOLD)."""
    try:
        period   = request.args.get("period",   "1d")
        interval = request.args.get("interval", "5m")
        candles  = get_chart_data(symbol.upper(), period=period, interval=interval)
        return jsonify({"ok": True, "symbol": symbol, "candles": candles})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
 
 
@app.route("/api/macro/module/<name>")
def api_macro_module(n):
    """
    Fetch a single AI module by name.
    Names: overview | mood | policy | flow | bearing | pulse
    """
    from tools.macro import (
        get_ai_overview, get_market_mood, get_market_policy,
        get_flow_analysis, get_bearing, get_pulse
    )
    fn_map = {
        "overview": get_ai_overview,
        "mood":     get_market_mood,
        "policy":   get_market_policy,
        "flow":     get_flow_analysis,
        "bearing":  get_bearing,
        "pulse":    get_pulse,
    }
    fn = fn_map.get(n)
    if not fn:
        return jsonify({"ok": False, "error": f"Unknown module: {n}"}), 400
    try:
        force = request.args.get("refresh") == "1"
        data  = fn(force=force)
        return jsonify({"ok": True, "module": n, "data": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
 
 
@app.route("/api/macro/all")
def api_macro_all():
    """Fetch all AI modules + market snapshot in one call (parallel)."""
    try:
        force = request.args.get("refresh") == "1"
        data  = get_all(force=force)
        return jsonify({"ok": True, **data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
 
 
@app.route("/api/macro/news")
def api_macro_news():
    """Global macro headlines for the Macro Desk overview."""
    try:
        from tools.macro_news import get_headlines as get_macro_headlines, format_age as fmt
        limit = int(request.args.get("limit", 30))
        force = request.args.get("refresh") == "1"
        items = get_macro_headlines(limit=limit, force=force)
        for item in items:
            item["age_str"] = fmt(item.get("age_min", 0))
        return jsonify({"ok": True, "items": items, "count": len(items)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
 
 
@app.route("/api/macro/pair/<pair_id>")
def api_macro_pair(pair_id):
    """
    AI analysis + sentiment for a single config pair.
    Uses tools.ai.ask() with market context focused on that pair.
    Also fetches news from tools.news (your existing yfinance news module).
    """
    import json
    from tools.ai     import ask
    from tools.market import get_market_snapshot, INSTRUMENTS
    from tools.news   import get_news   # your existing pair news module
 
    pair_id = pair_id.upper()
 
    # Validate it's a known pair
    if pair_id not in INSTRUMENTS:
        return jsonify({"ok": False, "error": f"Unknown pair: {pair_id}"}), 400
 
    meta  = INSTRUMENTS[pair_id]
    snap  = get_market_snapshot()
    d     = snap.get(pair_id, {})
    price = d.get("last")
    chg_p = d.get("change_p")
 
    # Build price context string
    price_ctx = ""
    if price is not None:
        sign = "+" if (chg_p or 0) >= 0 else ""
        price_ctx = f"{pair_id} ({meta['label']}) is at {price} ({sign}{chg_p:.2f}% today)."
 
    # Fetch pair-specific news for context
    news_items = get_news(pair_id, yf_ticker=meta.get("sym"))
    news_ctx = ""
    if news_items:
        headlines = "\n".join(f"- {n['headline']}" for n in news_items[:5])
        news_ctx  = f"\n\nRecent headlines:\n{headlines}"
 
    prompt = (
        f"You are a macro FX/trading analyst. Analyze this instrument:\n\n"
        f"{price_ctx}{news_ctx}\n\n"
        "Respond ONLY with valid JSON (no markdown):\n"
        '{\n'
        '  "text": "<2-3 sentence macro analysis of this specific instrument right now, '
        'citing price action, key driver, and what to watch>",\n'
        '  "sentiment": "<Bullish | Bearish | Neutral>"\n'
        '}'
    )
 
    raw  = ask(prompt, max_tokens=300, temperature=0.3)
    data = {}
    try:
        import re
        cleaned = re.sub(r"^```[a-z]*\n?", "", raw.strip())
        cleaned = re.sub(r"\n?```$", "", cleaned)
        data    = json.loads(cleaned)
    except Exception:
        pass
 
    return jsonify({
        "ok":   True,
        "pair": pair_id,
        "data": {
            "text":      data.get("text", ""),
            "sentiment": data.get("sentiment", "Neutral"),
            "news":      news_items[:5],
        }
    })
 
 
@app.route("/api/news/<pair_id>")
def api_pair_news(pair_id):
    """Pair-specific news combining RSS relevance scoring + yfinance."""
    from tools.macro_news import get_pair_headlines, format_age as fmt
    from tools.news       import get_news
    from tools.market     import INSTRUMENTS
 
    pair_id  = pair_id.upper()
    meta     = INSTRUMENTS.get(pair_id, {})
 
    rss_items = get_pair_headlines(pair_id, limit=25)
    for item in rss_items:
        item["age_str"] = fmt(item.get("age_min", 0))
        item["headline"] = item.get("title", "")
        item["link"]     = item.get("url", "")
 
    yf_items = get_news(pair_id, yf_ticker=meta.get("sym"))
 
    rss_titles = {i["title"].lower()[:60] for i in rss_items}
    merged = list(rss_items)
    for item in yf_items:
        if item.get("headline", "")[:60].lower() not in rss_titles:
            item["title"] = item.get("headline", "")
            merged.append(item)
 
    return jsonify({"ok": True, "pair": pair_id, "articles": merged[:30]})
 
 
@app.route("/api/macro/pair/<pair_id>/modules")
def api_macro_pair_modules(pair_id):
    """
    All macro modules (mood, policy, flow, bearing, pulse) focused
    entirely on the specific pair — no cross-pair contamination.
    """
    from tools.macro import get_pair_all_modules
    pair_id = pair_id.upper()
    force   = request.args.get("refresh") == "1"
    try:
        data = get_pair_all_modules(pair_id, force=force)
        return jsonify({"ok": True, "pair": pair_id, "modules": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
 

# ── Run ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("MISSION_CONTROL_PORT", 9000))
    print(f"[MissionControl] Starting on http://0.0.0.0:{port}")
    print(f"[MissionControl] Dashboard:  http://localhost:{port}/dashboard")
    print(f"[MissionControl] Chart view: http://localhost:{port}/chart-view/<PAIR>")
    print(f"[MissionControl] Debug:      http://localhost:{port}/debug")
    app.run(host="0.0.0.0", port=port, use_reloader=False, threaded=True)
