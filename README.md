# Lightweight Chart Agent

A modular, multi-pair trading chart system with live TradingView-style charts, pluggable market detectors, and Discord alerts with screenshots on signal confirmation.

---

## Overview

Each trading pair runs as a self-contained Flask server on its own port. A background detection loop runs every 30 seconds regardless of whether any browser is open. When a signal is confirmed, a Discord alert fires with a chart screenshot attached. A Mission Control dashboard aggregates all pairs into a single view.

---

## Project Structure

```
lightweightchart-agent/
├── app.py                      # Entry point — launches one server per pair
├── config.py                   # All pairs, ports, and detector settings
├── server.py                   # PairServer — Flask + detection loop per pair
├── mission_control.py          # Aggregated dashboard on port 6767
├── requirements.txt
│
├── providers/                  # Data provider abstraction layer
│   ├── __init__.py             # Loads provider based on DATA_PROVIDER env var
│   ├── yahoo.py                # Yahoo Finance via yfinance (default)
│   └── metatrader.py           # MetaTrader 5 terminal (Windows only)
│
├── detectors/
│   ├── __init__.py             # Detector registry
│   ├── accumulation.py         # Sideways consolidation detector
│   ├── supply_demand.py        # Supply & Demand zone detector
│   └── fvg.py                  # Fair Value Gap detector
│
└── templates/
    ├── index.html              # Live chart UI (per pair)
    └── debug.html              # Accumulation debug & replay UI
```

---

## Quickstart

### 1. Install dependencies

```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. Configure environment

```bash
# Data provider (optional — defaults to yahoo)
export DATA_PROVIDER=yahoo          # or: metatrader

# Discord webhook for alerts (optional)
export DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/..."
```

### 3. Run

```bash
# Start all pairs
python app.py

# Start specific pairs only
python app.py US30 XAUUSD
```

### 4. Access

| Interface        | URL                          |
|------------------|------------------------------|
| Mission Control  | http://localhost:6767        |
| US30             | http://localhost:5000        |
| US100            | http://localhost:5001        |
| XAUUSD           | http://localhost:5002        |
| EURGBP           | http://localhost:5003        |
| EURUSD           | http://localhost:5004        |
| GBPUSD           | http://localhost:5005        |

---

## Data Providers

The system abstracts data fetching behind a provider interface. Switch providers by setting the `DATA_PROVIDER` environment variable — no other code changes needed.

### Yahoo Finance (default)

Uses the `yfinance` library. Works out of the box on any platform.

```bash
export DATA_PROVIDER=yahoo
```

### MetaTrader 5

Connects to a locally running MT5 terminal via the `MetaTrader5` Python package.

```bash
export DATA_PROVIDER=metatrader
pip install MetaTrader5
```

> **Platform note:** The MetaTrader5 package only works on Windows. On Linux, run MT5 on a Windows machine and expose it via a small REST API, then update `providers/metatrader.py` to call that API instead.

### Provider interface

Both providers expose the same interface so detectors and server code are provider-agnostic:

| Function | Description |
|---|---|
| `get_df(ticker, interval, period)` | Returns an OHLCV DataFrame |
| `get_bias_df(ticker, period, interval)` | Returns a DataFrame for bias calculations (daily/weekly) |
| `LOCK` | threading.Lock for serializing downloads where needed |

To add a new provider, create `providers/myprovider.py` implementing these three exports, then add it to `providers/__init__.py`.

---

## Configuration

All pairs are defined in `config.py`. Each pair specifies its ticker, port, detector list, and per-detector parameters.

```python
"EURUSD": {
    "ticker":           "EURUSD=X",     # provider ticker symbol
    "port":             5004,
    "label":            "EUR/USD",
    "interval":         "15m",          # default chart interval
    "period":           "5d",           # data lookback for chart
    "default_interval": "30m",
    "detectors":        ["supply_demand"],
    "detector_params": {
        "supply_demand": {
            "timeframe":          "30m",
            "ticker":             "EURUSD=X",
            "impulse_multiplier": 1.8,
            "wick_ratio":         0.6,
            "max_zones":          5,
            "max_age_days":       3,
            "valid_sessions":     ["london", "new_york"],
        },
    },
},
```

> **MetaTrader note:** When using the MetaTrader provider, use MT5 symbol names as tickers (e.g. `"EURUSD"` instead of `"EURUSD=X"`, `"US30"` instead of `"YM=F"`).

---

## Detectors

### Accumulation

Identifies **sideways, directionless consolidation** — price oscillating without a net trend, signalling potential breakout energy building up.

**How it works:**

The detector scans windows of `min_candles` to `lookback` size against the last fully closed candle. For each window to pass, it must meet all of:

- **Range** — box height (body highs/lows) must be within the session's `max_range_pct`
- **Slope** — linear regression on closes must be near-flat (scaled by `threshold_pct / window_size`)
- **ADX** — directional strength must be below `adx_threshold` (default 20)
- **Choppiness** — price must reverse direction frequently (≥ 0.44 for confirmed, ≥ 0.36 for potential)

**Zone selection priority:**

1. Zones with ADX < 10 are preferred (ultra-low directional strength)
2. Among equal ADX tiers, lowest slope wins
3. Primary zone + secondary zone (runner-up) are both returned

**Breakout validation:**

When price exits the box, the breakout candle's body must be larger than the average body of candles inside the window. This filters out noise pokes and confirms the move is impulsive.

**Status levels:**

| Status | Meaning |
|---|---|
| `looking` | No qualifying zone found |
| `potential` | Slope, ADX, range pass — choppiness partially passes |
| `active` | All conditions met, breakout candle still inside box |
| `confirmed` | Impulsive breakout outside the box — Discord alert fires |
| `weekend` | Weekend halt active — detection suspended |

**Parameters:**

| Parameter | Description |
|---|---|
| `lookback` | Maximum window size to scan |
| `min_candles` | Minimum window size |
| `adx_threshold` | Maximum ADX value allowed |
| `threshold_pct` | Slope scaling factor per instrument |
| `asian_range_pct` | Max box height during Asian session |
| `london_range_pct` | Max box height during London session |
| `new_york_range_pct` | Max box height during New York session |

**Current settings:**

| Pair | Asian | London | New York |
|---|---|---|---|
| US30 | 0.1% (~44pts) | 0.2% (~88pts) | 0.3% (~132pts) |
| US100 | 0.08% (~26pts) | 0.1% (~42pts) | 0.25% (~52pts) |
| XAUUSD | 0.15% (~$4.5) | 0.2% (~$6) | 0.3% (~$9) |

---

### Supply & Demand

Identifies **institutional supply and demand zones** using an indecision + impulse candle pattern, filtered by directional bias.

**How it works:**

1. **Bias check** — fetches the previous completed daily and weekly candle. Both must agree (both bullish or both bearish). Misaligned bias = no detection.
2. **Indecision candle** — wicks must make up ≥ `wick_ratio` of the total candle range
3. **Impulse candle** — the following candle's body must exceed `impulse_multiplier × avg_body` and have ≤ 30% wicks
4. **Touch check** — zones are discarded if price has already touched them

Bullish bias → demand zones only. Bearish bias → supply zones only.

**Parameters:**

| Parameter | Description |
|---|---|
| `impulse_multiplier` | How much larger than average the impulse candle must be |
| `wick_ratio` | Minimum wick fraction of indecision candle |
| `max_zones` | Maximum zones to return |
| `max_age_days` | Zones older than this are discarded |
| `valid_sessions` | Sessions in which zone formation is valid |

---

### Fair Value Gap (FVG)

Identifies **price imbalance zones** created when price moves so fast it leaves a gap between the wicks of three consecutive candles.

A bullish FVG exists when `low[N+1] > high[N-1]`. A bearish FVG when `high[N+1] < low[N-1]`.

Additional filters ensure the impulse candle has a real body (≥ 60% body-to-range ratio) and the gap is large enough to be meaningful (≥ `min_gap_pct`).

---

## Session Windows (UTC)

| Session | Hours (UTC) |
|---|---|
| Asian | 01:00 – 07:00 |
| London | 08:00 – 12:00 |
| New York | 13:00 – 19:00 |

**Weekend halt:** Friday 23:00 UTC → Sunday 22:00 UTC. All detection, alerts, and data polling are suspended during this window.

---

## Discord Alerts

Alerts fire on signal confirmation with a chart screenshot. Each alert type:

| Detector | Trigger | Message |
|---|---|---|
| Accumulation | Impulsive breakout outside box | `🚀 PAIR — Accumulation Confirmed` |
| Supply & Demand | New active zone found | `📈 PAIR — Demand Zone Found` / `📉 PAIR — Supply Zone Found` |

When multiple S&D zones are found simultaneously, a separate message is sent per zone with that zone highlighted in the screenshot and all others dimmed.

**Deduplication:** Each zone fires at most once, keyed by its start timestamp. A zone can re-fire only if it disappears and returns.

**Weekend suppression:** No alerts are sent during the weekend halt window, regardless of detector output.

Test an alert:
```
http://localhost:5000/test-alert
```

---

## Debug Tools

### Accumulation debug page

```
http://localhost:5000/debug
```

Shows every window checked, why each was rejected, and which zone was selected as primary vs secondary. Includes a **replay mode** to step through historical candles and observe how the detector would have behaved at each point in time.

### Supply & Demand debug

Accessible from the debug page — shows all candidate zones, their rejection reasons, and bias calculation detail.

---

## Adding a New Detector

**1.** Create `detectors/my_detector.py`:

```python
def detect(df, **kwargs) -> dict | None:
    return {
        "detector":  "my_detector",
        "status":    "found",
        "start":     <unix_timestamp>,
        "end":       <unix_timestamp>,
        "top":       <float>,
        "bottom":    <float>,
        "is_active": True,
    }
```

**2.** Register in `detectors/__init__.py`:

```python
from detectors.my_detector import detect as my_detector_detect

REGISTRY = {
    ...
    "my_detector": my_detector_detect,
}
```

**3.** Add to a pair in `config.py`:

```python
"detectors": ["accumulation", "my_detector"]
```

**4.** Add a JS renderer in `templates/index.html`:

```javascript
const detectorRenderers = {
    accumulation: renderAccumulation,
    my_detector:  renderMyDetector,
};

function renderMyDetector(chart, zone) {
    const series = chart.addLineSeries({ color: '#f0b429' });
    series.setData([{ time: zone.start, value: zone.top }]);
    return [series];
}
```

---

## Running with PM2

```bash
pm2 start app.py --name "lightweightchart-agent" --interpreter python3
pm2 startup | tail -n 1 | bash
pm2 save
pm2 status
```
