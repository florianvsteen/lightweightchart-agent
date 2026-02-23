"""
test_mt5_timestamps.py

Standalone diagnostic — tests whether the MT5 REST API /PriceHistory
returns timestamps that match real UTC wall clock time.

Run on the server:
    python3 test_mt5_timestamps.py

Requires the same env vars as the main app:
    MT5_API_URL, MT5_API_USER, MT5_API_PASSWORD, MT5_API_SERVER
"""

import os
import sys
import requests
import datetime
from urllib.parse import urlencode

# ── Config ────────────────────────────────────────────────────────────────────
SYMBOL    = "EURUSD"   # change to any symbol your broker has
TIMEFRAME = 1          # 1-minute bars
BARS      = 5          # just fetch the last 5 bars

# ── Helpers ───────────────────────────────────────────────────────────────────

def base_url():
    url = os.environ.get("MT5_API_URL", "").rstrip("/")
    if not url:
        print("ERROR: MT5_API_URL is not set.")
        sys.exit(1)
    return url

def connect():
    user     = os.environ.get("MT5_API_USER", "")
    password = os.environ.get("MT5_API_PASSWORD", "")
    server   = os.environ.get("MT5_API_SERVER", "")
    missing  = [k for k, v in {"MT5_API_USER": user, "MT5_API_PASSWORD": password, "MT5_API_SERVER": server}.items() if not v]
    if missing:
        print(f"ERROR: Missing env vars: {', '.join(missing)}")
        sys.exit(1)

    query = urlencode({
        "user": user, "password": password, "server": server,
        "connectTimeoutSeconds": 60,
        "connectTimeoutClusterMemberSeconds": 20,
    })
    resp = requests.get(f"{base_url()}/ConnectEx?{query}", timeout=70)
    if resp.status_code != 200:
        print(f"ERROR: /ConnectEx failed HTTP {resp.status_code}: {resp.text[:200]}")
        sys.exit(1)
    token = resp.text.strip().strip('"')
    print(f"Connected — token: {token[:8]}…\n")
    return token

# ── Main test ─────────────────────────────────────────────────────────────────

def main():
    now_utc   = datetime.datetime.now(datetime.timezone.utc)
    date_from = now_utc - datetime.timedelta(hours=1)   # last 1 hour of 1m bars

    print("=" * 60)
    print(f"Wall clock UTC now : {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Fetching           : {SYMBOL} {TIMEFRAME}m bars from last 1 hour")
    print("=" * 60)

    token = connect()

    params = {
        "id":        token,
        "symbol":    SYMBOL,
        "from":      date_from.strftime("%Y-%m-%dT%H:%M:%S"),
        "to":        now_utc.strftime("%Y-%m-%dT%H:%M:%S"),
        "timeFrame": TIMEFRAME,
    }

    resp = requests.get(f"{base_url()}/PriceHistory", params=params, timeout=30)

    if resp.status_code != 200:
        print(f"ERROR: /PriceHistory failed HTTP {resp.status_code}: {resp.text[:200]}")
        sys.exit(1)

    bars = resp.json()
    if not isinstance(bars, list) or len(bars) == 0:
        print(f"ERROR: Unexpected response: {bars}")
        sys.exit(1)

    print(f"Total bars returned: {len(bars)}")
    print(f"Showing last {BARS}:\n")

    print(f"{'#':<4} {'Raw time string':<22} {'Parsed as UTC':<22} {'Diff from now':>14}  close")
    print("-" * 75)

    for bar in bars[-BARS:]:
        raw_time = bar["time"]
        # mt5rest returns "2025-02-23T11:00:00" — no timezone info
        # We need to know: does MT5 return UTC or broker server local time?
        naive_dt  = datetime.datetime.strptime(raw_time, "%Y-%m-%dT%H:%M:%S")
        utc_dt    = naive_dt.replace(tzinfo=datetime.timezone.utc)
        diff_secs = int((now_utc - utc_dt).total_seconds())
        diff_str  = f"{diff_secs}s ago" if diff_secs >= 0 else f"{-diff_secs}s ahead"
        close     = bar.get("closePrice", "?")
        print(f"     {raw_time:<22} {utc_dt.strftime('%H:%M:%S UTC'):<22} {diff_str:>14}  {close}")

    last_bar  = bars[-1]
    last_time = datetime.datetime.strptime(last_bar["time"], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc)
    diff      = (now_utc - last_time).total_seconds()

    print("\n" + "=" * 60)
    print(f"Last bar time (treating as UTC): {last_time.strftime('%H:%M:%S UTC')}")
    print(f"Wall clock UTC now             : {now_utc.strftime('%H:%M:%S UTC')}")
    print(f"Difference                     : {int(diff)}s  ({diff/60:.1f} min)")
    print()

    if 50 <= diff <= 120:
        print("✅ Timestamps look correct — last bar is ~1 closed candle behind wall clock.")
    elif 7100 <= diff <= 7300:
        print("🔴 Difference is ~2 hours — MT5 is returning broker LOCAL time (UTC+2),")
        print("   not UTC. The bars['time'] strings need +0h treatment but are actually UTC+2.")
        print("   Fix: subtract 2h when parsing, or configure the broker's MT5 server timezone.")
    elif 14300 <= diff <= 14500:
        print("🔴 Difference is ~4 hours — MT5 is returning UTC+4 (e.g. Dubai broker time).")
    else:
        print(f"⚠️  Unexpected difference of {diff/3600:.2f} hours — check broker server timezone.")
    print("=" * 60)

if __name__ == "__main__":
    main()
