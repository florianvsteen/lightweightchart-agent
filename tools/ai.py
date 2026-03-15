"""
tools/ai.py

Reusable AI provider module. Import ask() from anywhere in the app.

Usage:
    from tools.ai import ask

    result = ask("Your prompt here")
    # returns str — empty string on any failure

Provider is selected via AI_PROVIDER env var:
    gemini  (default, free tier) — requires GEMINI_API_KEY
    openai                       — requires OPENAI_API_KEY
    ollama                       — requires OLLAMA_URL + OLLAMA_MODEL
"""

import os
import requests

# ── Provider config ─────────────────────────────────────────────────────────────
AI_PROVIDER  = os.environ.get("AI_PROVIDER", "gemini").lower()

GEMINI_URL   = (
    "https://generativelanguage.googleapis.com/v1beta/models"
    "/gemini-2.0-flash-lite:generateContent"
)

OPENAI_URL   = "https://api.openai.com/v1/chat/completions"
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

OLLAMA_URL   = os.environ.get("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen3:8b")


# ── Providers ────────────────────────────────────────────────────────────────────
def _ask_gemini(prompt: str, max_tokens: int, temperature: float) -> str:
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        print("[ai] GEMINI_API_KEY not set")
        return ""
    try:
        resp = requests.post(
            f"{GEMINI_URL}?key={api_key}",
            headers={"Content-Type": "application/json"},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "maxOutputTokens": max_tokens,
                    "temperature":     temperature,
                },
            },
            timeout=30,
        )
        if resp.status_code != 200:
            print(f"[ai] Gemini {resp.status_code}: {resp.text[:200]}")
            return ""
        candidates = resp.json().get("candidates", [])
        if not candidates:
            return ""
        return " ".join(
            p.get("text", "")
            for p in candidates[0].get("content", {}).get("parts", [])
        ).strip()
    except Exception as e:
        print(f"[ai] Gemini error: {e}")
        return ""


def _ask_openai(prompt: str, max_tokens: int, temperature: float) -> str:
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        print("[ai] OPENAI_API_KEY not set")
        return ""
    try:
        resp = requests.post(
            OPENAI_URL,
            headers={
                "Content-Type":  "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            json={
                "model":       OPENAI_MODEL,
                "messages":    [{"role": "user", "content": prompt}],
                "max_tokens":  max_tokens,
                "temperature": temperature,
            },
            timeout=30,
        )
        if resp.status_code != 200:
            print(f"[ai] OpenAI {resp.status_code}: {resp.text[:200]}")
            return ""
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        print(f"[ai] OpenAI error: {e}")
        return ""


def _ask_ollama(prompt: str, max_tokens: int, temperature: float) -> str:
    """
    Uses the Ollama native /api/chat endpoint.
    think:false disables chain-of-thought on qwen3, deepseek-r1, etc.
    """
    url = f"{OLLAMA_URL.rstrip('/')}/api/chat"
    try:
        resp = requests.post(
            url,
            headers={"Content-Type": "application/json"},
            json={
                "model":   OLLAMA_MODEL,
                "think":   False,
                "stream":  False,
                "options": {"temperature": temperature, "num_predict": max_tokens},
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=90,
        )
        if resp.status_code != 200:
            print(f"[ai] Ollama {resp.status_code}: {resp.text[:200]}")
            return ""
        return (resp.json().get("message") or {}).get("content", "").strip()
    except requests.exceptions.Timeout:
        print(f"[ai] Ollama timeout — is the model loaded? ({OLLAMA_MODEL})")
        return ""
    except requests.exceptions.ConnectionError as e:
        print(f"[ai] Ollama connection error: {e}")
        return ""
    except Exception as e:
        print(f"[ai] Ollama error: {e}")
        return ""


# ── Public API ───────────────────────────────────────────────────────────────────
def ask(prompt: str, max_tokens: int = 1024, temperature: float = 0.3) -> str:
    """
    Send a prompt to the configured AI provider and return the text response.

    Args:
        prompt:      The full prompt string.
        max_tokens:  Maximum tokens in the response (default 1024).
        temperature: Sampling temperature — 0 = deterministic, 1 = creative (default 0.3).

    Returns:
        Response text, or empty string on failure.

    Provider is controlled by the AI_PROVIDER env var: gemini | openai | ollama
    """
    print(f"[ai] provider={AI_PROVIDER} max_tokens={max_tokens}")
    if AI_PROVIDER == "openai":
        return _ask_openai(prompt, max_tokens, temperature)
    if AI_PROVIDER == "ollama":
        return _ask_ollama(prompt, max_tokens, temperature)
    return _ask_gemini(prompt, max_tokens, temperature)
