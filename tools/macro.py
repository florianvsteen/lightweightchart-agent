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
    prompt = (
        "You are a senior macro analyst. Based on current market data:\n\n"
        f"{ctx}\n\n"
        "Write ONE paragraph (3-4 sentences) summarizing the dominant macro theme "
        "right now. Focus on: the most important market move, what's driving it, "
        "and the key risk traders should watch. Be specific with numbers. "
        "Write like a Bloomberg terminal comment — direct, no fluff."
    )

    text = ask(prompt, max_tokens=200, temperature=0.4)
    result = {"text": text or "Analysis unavailable."}
    _set_cache("overview", result)
    return {**result, "age_min": 0, "cached": False}


# ══════════════════════════════════════════════════════════════════════════════
#  MODULE 2 — Market Mood (sentiment gauge)
# ══════════════════════════════════════════════════════════════════════════════
MOOD_OPTIONS = ["RISK-ON", "NEUTRAL", "RISK-OFF", "EXTREME FEAR", "EUPHORIA"]

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
        "You are a market sentiment analyst. Based on current market data:\n\n"
        f"{ctx}\n\n"
        f"The trader is watching: {pairs}.\n\n"
        "Respond ONLY with valid JSON (no markdown, no extra text):\n"
        '{\n'
        '  "label": "<one of: RISK-ON | NEUTRAL | RISK-OFF | EXTREME FEAR | EUPHORIA>",\n'
        '  "score": <float from -1.0 (extreme fear) to 1.0 (euphoria)>,\n'
        '  "explanation": "<2-3 sentences explaining the sentiment reading, citing specific data>"\n'
        '}'
    )

    raw  = ask(prompt, max_tokens=300, temperature=0.2)
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
    prompt = (
        "You are a central bank analyst. Based on current market data:\n\n"
        f"{ctx}\n\n"
        "Respond ONLY with valid JSON (no markdown, no extra text):\n"
        '{\n'
        '  "label": "<one of: HAWKISH | SLIGHTLY HAWKISH | NEUTRAL | SLIGHTLY DOVISH | DOVISH>",\n'
        '  "explanation": "<2-3 sentences on current central bank stance, citing rate data and Fed signals>",\n'
        '  "outlook": "<one sentence: what to expect next from central banks>"\n'
        '}'
    )

    raw  = ask(prompt, max_tokens=300, temperature=0.2)
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
        "You are a technical analyst. Based on current market data:\n\n"
        f"{ctx}\n\n"
        f"Focus on the trend relevant to: {pairs}.\n\n"
        "Assess the primary market trend direction. Respond ONLY with valid JSON:\n"
        '{\n'
        '  "label": "<one of: STRONG UP | UP | NEUTRAL | DOWN | STRONG DOWN>",\n'
        '  "bullets": ["<short bullet 1>", "<short bullet 2>", "<short bullet 3>"]\n'
        '}\n'
        "Bullets should be short (max 8 words each) trend observations with specific data."
    )

    raw  = ask(prompt, max_tokens=250, temperature=0.2)
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
