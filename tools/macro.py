"""
tools/macro.py

AI-powered macro analysis engine for the Macro Desk.
Builds structured prompts, calls tools.ai.ask(), parses responses.

Modules:
    get_ai_overview()      → str  — one-paragraph macro summary
    get_market_mood()      → dict — sentiment gauge + explanation
    get_market_policy()    → dict — central bank stance + outlook
    get_flow_analysis()    → dict — market flow reading
    get_bearing()          → dict — trend direction + conviction
    get_pulse()            → dict — volatility regime

All functions cache results and run independently so the frontend
can fetch them progressively as they complete.

Re-uses: tools.ai.ask(), tools.market.get_market_snapshot()
"""

import json
import re
import time
import threading

from tools.ai     import ask
from tools.market import get_market_snapshot

import logging
log = logging.getLogger(__name__)

# ── Cache ─────────────────────────────────────────────────────────────────────
AI_TTL = 30 * 60   # 30 min — macro narrative doesn't change every second

_cache: dict    = {}
_lock = threading.Lock()

def _cached(key: str) -> dict | None:
    with _lock:
        entry = _cache.get(key)
    if entry and (time.time() - entry["at"]) < AI_TTL:
        return entry["data"]
    return None

def _set_cache(key: str, data: dict) -> None:
    with _lock:
        _cache[key] = {"data": data, "at": time.time()}

def _cache_age(key: str) -> int:
    """Returns age in minutes, or 999 if not cached."""
    with _lock:
        entry = _cache.get(key)
    if not entry:
        return 999
    return int((time.time() - entry["at"]) / 60)


# ── Market context builder ────────────────────────────────────────────────────
def _market_context() -> str:
    """Build a compact market snapshot string for injecting into prompts."""
    snap = get_market_snapshot()
    lines = []
    groups = {}
    for key, d in snap.items():
        if key == "fetched_at" or d.get("last") is None:
            continue
        g = d.get("group", "other")
        if g not in groups:
            groups[g] = []
        sign = "+" if (d.get("change_p") or 0) >= 0 else ""
        groups[g].append(
            f"{d['label']}: {d['last']} ({sign}{d.get('change_p', 0):.2f}%)"
        )

    order = ["equity", "rates", "fx", "commod", "vol"]
    for g in order:
        if g in groups:
            lines.append(f"{g.upper()}: " + " | ".join(groups[g]))

    return "\n".join(lines) if lines else "No market data available."


def _parse_json_response(text: str) -> dict:
    """Extract JSON from an AI response, stripping markdown fences."""
    text = text.strip()
    text = re.sub(r"^```[a-z]*\n?", "", text)
    text = re.sub(r"\n?```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {}


# ══════════════════════════════════════════════════════════════════════════════
#  MODULE 1 — AI Overview
# ══════════════════════════════════════════════════════════════════════════════
def get_ai_overview(force: bool = False) -> dict:
    """
    Returns:
        { "text": str, "age_min": int, "cached": bool }
    """
    cached = _cached("overview")
    if cached and not force:
        return {**cached, "age_min": _cache_age("overview"), "cached": True}

    ctx = _market_context()
    pairs  = _pair_list()
    prompt = (
        "You are a senior macro strategist writing a morning briefing. "
        "Based on current live market data:\n\n"
        f"{ctx}\n\n"
        f"The trader is watching: {pairs}.\n\n"
        "Write ONE paragraph (3-4 sentences). Requirements:\n"
        "- Start with the single most important price move and its magnitude\n"
        "- Name the specific macro driver (Fed policy, geopolitics, data release, etc.)\n"
        "- Explain what it means for the pairs listed above\n"
        "- End with the one key level or event to watch\n"
        "Style: Bloomberg terminal — terse, specific, no hedging language, no filler. "
        "Use actual numbers from the data."
    )

    text = ask(prompt, max_tokens=250, temperature=0.3)
    result = {"text": text or "Analysis unavailable."}
    _set_cache("overview", result)
    return {**result, "age_min": 0, "cached": False}


# ══════════════════════════════════════════════════════════════════════════════
#  MODULE 2 — Market Mood (sentiment gauge)
# ══════════════════════════════════════════════════════════════════════════════
MOOD_OPTIONS = ["EXTREME FEAR", "STRONG RISK-OFF", "RISK-OFF", "RISK-NEUTRAL", "NEUTRAL", "RISK-ON", "STRONG RISK-ON", "EUPHORIA"]

def get_market_mood(force: bool = False) -> dict:
    """
    Returns:
        {
          "label":       "RISK-OFF",          # one of MOOD_OPTIONS
          "score":       -0.7,                # -1.0 (extreme fear) to +1.0 (euphoria)
          "explanation": str,                 # 2-3 sentence explanation
          "age_min":     int,
          "cached":      bool
        }
    """
    cached = _cached("mood")
    if cached and not force:
        return {**cached, "age_min": _cache_age("mood"), "cached": True}

    ctx = _market_context()
    pairs  = _pair_list()
    prompt = (
        "You are an institutional macro strategist. Assess the CURRENT market risk sentiment "
        "based on the following live market data:\n\n"
        f"{ctx}\n\n"
        f"The trader is focused on: {pairs}.\n\n"
        "Use this scoring rubric to pick the correct label:\n"
        "  EXTREME FEAR    → score -1.0: VIX > 35, multiple assets crashing, panic selling\n"
        "  STRONG RISK-OFF → score -0.75: VIX elevated >25, equities down >1%, gold/bonds bid\n"
        "  RISK-OFF        → score -0.45: defensive positioning, DXY up, yields falling or equities weak\n"
        "  RISK-NEUTRAL    → score -0.1: mixed signals, uncertainty, no clear directional bias\n"
        "  NEUTRAL         → score  0.0: balanced market, VIX ~15-18, modest moves across the board\n"
        "  RISK-ON         → score +0.45: equities rising, DXY soft, commodities bid, VIX falling\n"
        "  STRONG RISK-ON  → score +0.75: equities rallying >1%, credit spreads tight, VIX <14\n"
        "  EUPHORIA        → score +1.0: parabolic moves, extreme greed, VIX <12, everything up\n\n"
        "Be ACCURATE and CONSERVATIVE — do not default to extremes. "
        "NEUTRAL and RISK-NEUTRAL are valid and common readings. "
        "Only use EXTREME FEAR or EUPHORIA when data clearly warrants it.\n\n"
        "Respond ONLY with valid JSON (no markdown, no extra text):\n"
        '{\n'
        '  "label": "<one of: EXTREME FEAR | STRONG RISK-OFF | RISK-OFF | RISK-NEUTRAL | NEUTRAL | RISK-ON | STRONG RISK-ON | EUPHORIA>",\n'
        '  "score": <float from -1.0 to 1.0 matching the rubric above>,\n'
        '  "explanation": "<3 sentences: (1) what the dominant signal is, (2) which specific instruments confirm it, (3) what to watch for a regime change>"\n'
        '}'
    )

    raw  = ask(prompt, max_tokens=400, temperature=0.15)
    data = _parse_json_response(raw)

    result = {
        "label":       data.get("label", "NEUTRAL"),
        "score":       float(data.get("score", 0.0)),
        "explanation": data.get("explanation", ""),
    }
    _set_cache("mood", result)
    return {**result, "age_min": 0, "cached": False}


# ══════════════════════════════════════════════════════════════════════════════
#  MODULE 3 — Market Policy (central bank stance)
# ══════════════════════════════════════════════════════════════════════════════
POLICY_OPTIONS = ["HAWKISH", "SLIGHTLY HAWKISH", "NEUTRAL", "SLIGHTLY DOVISH", "DOVISH"]

def get_market_policy(force: bool = False) -> dict:
    """
    Returns:
        {
          "label":       "NEUTRAL",
          "explanation": str,
          "outlook":     str,   # one-line forward-looking view
          "age_min":     int,
          "cached":      bool
        }
    """
    cached = _cached("policy")
    if cached and not force:
        return {**cached, "age_min": _cache_age("policy"), "cached": True}

    ctx = _market_context()
    pairs  = _pair_list()
    prompt = (
        "You are a central bank and monetary policy strategist. "
        "Assess the current global monetary policy stance based on live market data:\n\n"
        f"{ctx}\n\n"
        f"The trader is focused on: {pairs}.\n\n"
        "Use this scoring rubric:\n"
        "  HAWKISH         → rates rising or signaling hikes, fighting inflation\n"
        "  SLIGHTLY HAWKISH → cautious tightening bias, higher-for-longer language\n"
        "  NEUTRAL         → data-dependent, no clear directional bias\n"
        "  SLIGHTLY DOVISH → beginning to signal pauses or cuts\n"
        "  DOVISH          → cutting rates or clearly easing\n\n"
        "Focus on: 2Y/10Y yields, yield curve shape, DXY strength, and any rate signals in the data.\n\n"
        "Respond ONLY with valid JSON (no markdown, no extra text):\n"
        '{\n'
        '  "label": "<one of: HAWKISH | SLIGHTLY HAWKISH | NEUTRAL | SLIGHTLY DOVISH | DOVISH>",\n'
        '  "explanation": "<2-3 sentences citing specific yield levels, curve shape, and what it signals>",\n'
        '  "outlook": "<one sentence: next likely central bank move and what data would change it>"\n'
        '}'
    )

    raw  = ask(prompt, max_tokens=400, temperature=0.15)
    data = _parse_json_response(raw)

    result = {
        "label":       data.get("label", "NEUTRAL"),
        "explanation": data.get("explanation", ""),
        "outlook":     data.get("outlook", ""),
    }
    _set_cache("policy", result)
    return {**result, "age_min": 0, "cached": False}


# ══════════════════════════════════════════════════════════════════════════════
#  MODULE 4 — Flow Analysis
# ══════════════════════════════════════════════════════════════════════════════
FLOW_OPTIONS = ["THIN", "HEALTHY", "CROWDED"]

def get_flow_analysis(force: bool = False) -> dict:
    """
    Returns:
        {
          "label":       "HEALTHY",
          "score":       0.5,     # 0 (thin) → 1 (crowded)
          "explanation": str,
          "age_min":     int,
          "cached":      bool
        }
    """
    cached = _cached("flow")
    if cached and not force:
        return {**cached, "age_min": _cache_age("flow"), "cached": True}

    ctx = _market_context()
    prompt = (
        "You are a market microstructure analyst. Based on current market data:\n\n"
        f"{ctx}\n\n"
        "Assess market flow conditions. Respond ONLY with valid JSON:\n"
        '{\n'
        '  "label": "<one of: THIN | HEALTHY | CROWDED>",\n'
        '  "score": <float 0.0=thin to 1.0=crowded>,\n'
        '  "explanation": "<one sentence on participation/breadth/volume conditions>"\n'
        '}'
    )

    raw  = ask(prompt, max_tokens=200, temperature=0.2)
    data = _parse_json_response(raw)

    result = {
        "label":       data.get("label", "HEALTHY"),
        "score":       float(data.get("score", 0.5)),
        "explanation": data.get("explanation", ""),
    }
    _set_cache("flow", result)
    return {**result, "age_min": 0, "cached": False}


# ══════════════════════════════════════════════════════════════════════════════
#  MODULE 5 — Bearing (trend direction)
# ══════════════════════════════════════════════════════════════════════════════
BEARING_OPTIONS = ["STRONG UP", "UP", "NEUTRAL", "DOWN", "STRONG DOWN"]

def get_bearing(force: bool = False) -> dict:
    """
    Returns:
        {
          "label":    "STRONG DOWN",
          "bullets":  ["ADX above threshold...", "..."],
          "age_min":  int,
          "cached":   bool
        }
    """
    cached = _cached("bearing")
    if cached and not force:
        return {**cached, "age_min": _cache_age("bearing"), "cached": True}

    ctx = _market_context()
    pairs  = _pair_list()
    prompt = (
        "You are a technical trend analyst. Based on live market data:\n\n"
        f"{ctx}\n\n"
        f"Assess the primary trend for a trader watching: {pairs}.\n\n"
        "Labels:\n"
        "  STRONG UP   → clear uptrend, higher highs, momentum confirming\n"
        "  UP          → mild upward bias, trend intact but not strong\n"
        "  NEUTRAL     → sideways/range-bound, no directional edge\n"
        "  DOWN        → mild downward pressure, trend weakening\n"
        "  STRONG DOWN → clear downtrend, lower lows, momentum confirming\n\n"
        "Base your assessment on: price changes shown, directional alignment across instruments, "
        "and whether risk assets and safe-havens are moving in trend-confirming ways.\n\n"
        "Respond ONLY with valid JSON:\n"
        '{\n'
        '  "label": "<one of: STRONG UP | UP | NEUTRAL | DOWN | STRONG DOWN>",\n'
        '  "bullets": ["<bullet 1 — max 10 words, cite a specific instrument and number>", "<bullet 2>", "<bullet 3>"]\n'
        '}'
    )

    raw  = ask(prompt, max_tokens=300, temperature=0.15)
    data = _parse_json_response(raw)

    result = {
        "label":   data.get("label", "NEUTRAL"),
        "bullets": data.get("bullets", []),
    }
    _set_cache("bearing", result)
    return {**result, "age_min": 0, "cached": False}


# ══════════════════════════════════════════════════════════════════════════════
#  MODULE 6 — Pulse (volatility regime)
# ══════════════════════════════════════════════════════════════════════════════
PULSE_OPTIONS = ["QUIET", "TRADEABLE", "WILD"]

def get_pulse(force: bool = False) -> dict:
    """
    Returns:
        {
          "label":       "QUIET",
          "score":       0.2,    # 0 (quiet) → 1 (wild)
          "explanation": str,
          "age_min":     int,
          "cached":      bool
        }
    """
    cached = _cached("pulse")
    if cached and not force:
        return {**cached, "age_min": _cache_age("pulse"), "cached": True}

    ctx = _market_context()
    prompt = (
        "You are a volatility analyst. Based on current market data:\n\n"
        f"{ctx}\n\n"
        "Assess the current volatility regime. Respond ONLY with valid JSON:\n"
        '{\n'
        '  "label": "<one of: QUIET | TRADEABLE | WILD>",\n'
        '  "score": <float 0.0=quiet to 1.0=wild>,\n'
        '  "explanation": "<one sentence citing VIX, realized vol, or spread conditions>"\n'
        '}'
    )

    raw  = ask(prompt, max_tokens=200, temperature=0.2)
    data = _parse_json_response(raw)

    result = {
        "label":       data.get("label", "TRADEABLE"),
        "score":       float(data.get("score", 0.5)),
        "explanation": data.get("explanation", ""),
    }
    _set_cache("pulse", result)
    return {**result, "age_min": 0, "cached": False}


# ══════════════════════════════════════════════════════════════════════════════
#  BATCH — fetch all modules in parallel
# ══════════════════════════════════════════════════════════════════════════════
def get_all(force: bool = False) -> dict:
    """
    Fetch all macro desk modules in parallel threads.
    Returns combined dict with all module results.
    """
    results = {}
    errors  = {}

    modules = {
        "overview": get_ai_overview,
        "mood":     get_market_mood,
        "policy":   get_market_policy,
        "flow":     get_flow_analysis,
        "bearing":  get_bearing,
        "pulse":    get_pulse,
    }

    threads = []
    def run(key, fn):
        try:
            results[key] = fn(force=force)
        except Exception as e:
            errors[key]  = str(e)
            results[key] = {}

    for key, fn in modules.items():
        t = threading.Thread(target=run, args=(key, fn), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join(timeout=45)

    return {"modules": results, "errors": errors, "market": get_market_snapshot()}


# ── Config-aware pair list for prompt context ─────────────────────────────────
def _pair_list() -> str:
    """
    Returns a comma-separated string of the pairs from config.PAIRS,
    used to make AI prompts specific to the instruments you actually trade.
    e.g. "US30, US100, XAUUSD, EURUSD, EURGBP, USDJPY, GBPUSD, BTCUSD"
    """
    try:
        from config import PAIRS
        return ", ".join(PAIRS.keys())
    except ImportError:
        return "your tracked instruments"



# ══════════════════════════════════════════════════════════════════════════════
#  PAIR-SPECIFIC VERSIONS — all modules focused on one instrument
# ══════════════════════════════════════════════════════════════════════════════

def _pair_context(pair_id: str) -> str:
    """Market context with the requested pair front and center."""
    try:
        from tools.market import get_market_snapshot
    except ImportError:
        return "No market data available."

    snap  = get_market_snapshot()
    lines = []

    d = snap.get(pair_id)
    if d and d.get("last") is not None:
        sign = "+" if (d.get("change_p") or 0) >= 0 else ""
        lines.append(
            f"PRIMARY — {d['label']} ({pair_id}): "
            f"{d['last']} ({sign}{d.get('change_p', 0):.2f}% today)"
        )

    for k in ["VIX", "DXY", "US10Y", "OIL"]:
        d = snap.get(k)
        if d and d.get("last") is not None:
            sign = "+" if (d.get("change_p") or 0) >= 0 else ""
            lines.append(f"{d['label']}: {d['last']} ({sign}{d.get('change_p', 0):.2f}%)")

    return "\n".join(lines) if lines else "No market data available."


def get_pair_mood(pair_id: str, force: bool = False) -> dict:
    cache_key = f"mood_{pair_id}"
    cached = _cached(cache_key)
    if cached and not force:
        return {**cached, "age_min": _cache_age(cache_key), "cached": True}

    ctx = _pair_context(pair_id)
    json_schema = (
        '{\n'
        '  "label": "<EXTREME FEAR|STRONG RISK-OFF|RISK-OFF|RISK-NEUTRAL|NEUTRAL|RISK-ON|STRONG RISK-ON|EUPHORIA>",\n'
        '  "score": <float -1.0 to 1.0>,\n'
        '  "explanation": "<3 sentences specific to ' + pair_id + ': signal, confirming data, what changes the reading>"\n'
        '}'
    )
    prompt = (
        f"You are an institutional macro strategist analyzing {pair_id} specifically.\n\n"
        f"Market data:\n{ctx}\n\n"
        f"Assess risk sentiment as it directly affects {pair_id}. "
        f"Only reference {pair_id} and what moves it. Do NOT mention unrelated pairs.\n\n"
        "Rubric: EXTREME FEAR=-1.0, STRONG RISK-OFF=-0.75, RISK-OFF=-0.45, "
        "RISK-NEUTRAL=-0.1, NEUTRAL=0.0, RISK-ON=+0.45, STRONG RISK-ON=+0.75, EUPHORIA=+1.0\n\n"
        "Respond ONLY with valid JSON:\n" + json_schema
    )
    raw  = ask(prompt, max_tokens=400, temperature=0.15)
    data = _parse_json_response(raw)
    result = {
        "label":       data.get("label", "NEUTRAL"),
        "score":       float(data.get("score", 0.0)),
        "explanation": data.get("explanation", ""),
    }
    _set_cache(cache_key, result)
    return {**result, "age_min": 0, "cached": False}


def get_pair_policy(pair_id: str, force: bool = False) -> dict:
    cache_key = f"policy_{pair_id}"
    cached = _cached(cache_key)
    if cached and not force:
        return {**cached, "age_min": _cache_age(cache_key), "cached": True}

    ctx = _pair_context(pair_id)
    json_schema = (
        '{\n'
        '  "label": "<HAWKISH|SLIGHTLY HAWKISH|NEUTRAL|SLIGHTLY DOVISH|DOVISH>",\n'
        '  "explanation": "<2-3 sentences on how policy impacts ' + pair_id + '>",\n'
        '  "outlook": "<one sentence: next likely move for ' + pair_id + '>"\n'
        '}'
    )
    prompt = (
        f"You are a monetary policy analyst focused on {pair_id}.\n\n"
        f"Market data:\n{ctx}\n\n"
        f"Assess central bank stance as it affects {pair_id} only. "
        f"Reference only the relevant central banks for {pair_id}.\n\n"
        "Respond ONLY with valid JSON:\n" + json_schema
    )
    raw  = ask(prompt, max_tokens=350, temperature=0.15)
    data = _parse_json_response(raw)
    result = {
        "label":       data.get("label", "NEUTRAL"),
        "explanation": data.get("explanation", ""),
        "outlook":     data.get("outlook", ""),
    }
    _set_cache(cache_key, result)
    return {**result, "age_min": 0, "cached": False}


def get_pair_flow(pair_id: str, force: bool = False) -> dict:
    cache_key = f"flow_{pair_id}"
    cached = _cached(cache_key)
    if cached and not force:
        return {**cached, "age_min": _cache_age(cache_key), "cached": True}

    ctx = _pair_context(pair_id)
    json_schema = (
        '{\n'
        '  "label": "<THIN|HEALTHY|CROWDED>",\n'
        '  "score": <0.0=thin to 1.0=crowded>,\n'
        '  "explanation": "<one sentence on flow conditions for ' + pair_id + '>"\n'
        '}'
    )
    prompt = (
        f"Assess market flow for {pair_id} specifically.\n\n"
        f"Data:\n{ctx}\n\n"
        "Respond ONLY with valid JSON:\n" + json_schema
    )
    raw  = ask(prompt, max_tokens=200, temperature=0.15)
    data = _parse_json_response(raw)
    result = {
        "label":       data.get("label", "HEALTHY"),
        "score":       float(data.get("score", 0.5)),
        "explanation": data.get("explanation", ""),
    }
    _set_cache(cache_key, result)
    return {**result, "age_min": 0, "cached": False}


def get_pair_bearing(pair_id: str, force: bool = False) -> dict:
    cache_key = f"bearing_{pair_id}"
    cached = _cached(cache_key)
    if cached and not force:
        return {**cached, "age_min": _cache_age(cache_key), "cached": True}

    ctx = _pair_context(pair_id)
    json_schema = (
        '{\n'
        '  "label": "<STRONG UP|UP|NEUTRAL|DOWN|STRONG DOWN>",\n'
        '  "bullets": ["<' + pair_id + ' price action>", "<momentum/volume>", "<key level or trigger>"]\n'
        '}'
    )
    prompt = (
        f"Assess trend direction for {pair_id} specifically.\n\n"
        f"Data:\n{ctx}\n\n"
        "Respond ONLY with valid JSON:\n" + json_schema
    )
    raw  = ask(prompt, max_tokens=300, temperature=0.15)
    data = _parse_json_response(raw)
    result = {
        "label":   data.get("label", "NEUTRAL"),
        "bullets": data.get("bullets", []),
    }
    _set_cache(cache_key, result)
    return {**result, "age_min": 0, "cached": False}


def get_pair_pulse(pair_id: str, force: bool = False) -> dict:
    cache_key = f"pulse_{pair_id}"
    cached = _cached(cache_key)
    if cached and not force:
        return {**cached, "age_min": _cache_age(cache_key), "cached": True}

    ctx = _pair_context(pair_id)
    json_schema = (
        '{\n'
        '  "label": "<QUIET|TRADEABLE|WILD>",\n'
        '  "score": <0.0=quiet to 1.0=wild>,\n'
        '  "explanation": "<one sentence on volatility for ' + pair_id + '>"\n'
        '}'
    )
    prompt = (
        f"Assess volatility regime for {pair_id} specifically.\n\n"
        f"Data:\n{ctx}\n\n"
        "Respond ONLY with valid JSON:\n" + json_schema
    )
    raw  = ask(prompt, max_tokens=200, temperature=0.15)
    data = _parse_json_response(raw)
    result = {
        "label":       data.get("label", "TRADEABLE"),
        "score":       float(data.get("score", 0.5)),
        "explanation": data.get("explanation", ""),
    }
    _set_cache(cache_key, result)
    return {**result, "age_min": 0, "cached": False}


def get_pair_all_modules(pair_id: str, force: bool = False) -> dict:
    """Run all 5 pair-specific modules in parallel threads."""
    results = {}

    def run(key, fn):
        try:
            results[key] = fn(pair_id, force=force)
        except Exception as e:
            print(f"[macro] pair module error {key} {pair_id}: {e}")
            results[key] = {}

    threads = []
    for key, fn in [
        ("mood",    get_pair_mood),
        ("policy",  get_pair_policy),
        ("flow",    get_pair_flow),
        ("bearing", get_pair_bearing),
        ("pulse",   get_pair_pulse),
    ]:
        t = threading.Thread(target=run, args=(key, fn), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join(timeout=45)

    return results
