# Lightweight Chart Agent

A modular, multi-pair trading chart system that runs live TradingView-style charts for multiple instruments simultaneously, detects market patterns using pluggable detectors, and sends Discord alerts with screenshots on breakout confirmation.

---

## Features

- **Multi-pair** — each trading pair runs on its own port (5000, 5001, 5002, …)
- **Modular detectors** — plug in any detection logic per pair independently
- **Accumulation detector** — identifies sideways, directionless price consolidation
- **Discord alerts** — fires on breakout with a chart screenshot attached
- **Live charts** — TradingView Lightweight Charts frontend, auto-refreshes every 2 seconds

---

## Project Structure

```
lightweightchart-agent/
├── app.py                  # Entry point — launches one server per pair
├── config.py               # All pairs, ports, and detector settings
├── server.py               # PairServer class — Flask app per pair
├── requirements.txt
├── detectors/
│   ├── __init__.py         # Detector registry + run_detectors()
│   └── accumulation.py     # Sideways accumulation detector
└── templates/
    └── index.html          # Chart UI (dynamic, works for any pair)
```

---

## Quickstart

### 1. Install dependencies

```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. Set your Discord webhook (optional)

```bash
export DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/..."
```

### 3. Run

```bash
# Start all pairs
python app.py

# Start specific pairs only
python app.py US30 XAUUSD
```

Charts are available at:

| Pair   | URL                    |
|--------|------------------------|
| US30   | http://localhost:5000  |
| US100  | http://localhost:5001  |
| XAUUSD | http://localhost:5002  |

---

## Configuration

All pairs are defined in `config.py`. To add a new pair:

```python
"EURUSD": {
    "ticker": "EURUSD=X",       # yfinance ticker
    "port": 5003,                # unique port
    "label": "EUR/USD",
    "interval": "1m",
    "period": "1d",
    "detectors": ["accumulation"],
    "detector_params": {
        "accumulation": {
            "lookback": 40,
            "threshold_pct": 0.0005,
            "max_range_pct": 0.001,   # max box height as % of price
        },
    },
},
```

### Accumulation detector parameters

| Parameter | Description |
|---|---|
| `lookback` | Candle window size (hard capped at 60) |
| `threshold_pct` | Scales the slope sensitivity per instrument |
| `max_range_pct` | Max allowed box height as % of price — rejects wide boxes |

Current settings:

| Pair   | `max_range_pct` | Max box height (approx) |
|--------|-----------------|-------------------------|
| US30   | 0.2% (0.002)    | ~84 pts                 |
| US100  | 0.25% (0.0025)  | ~52 pts                 |
| XAUUSD | 0.3% (0.003)    | ~$9                     |

---

## Accumulation Detection Logic

The detector identifies **purely sideways, directionless consolidation** — price oscillating up and down repeatedly with no net trend. It does not care about the size of individual candle moves, only directional behaviour.

Two checks must pass:

**1. Slope** — a linear regression is fit to the closing prices. If the slope exceeds `threshold_pct * 0.15 / lookback`, the window is trending and rejected.

**2. Choppiness** — the fraction of candle-to-candle moves that reverse direction. Sideways markets score ~0.45–0.60. Trending markets score ~0.10–0.25. A minimum of 0.44 is required for a confirmed zone.

**Status levels:**

| Status | Meaning |
|---|---|
| `Looking for accumulation` | No qualifying zone found |
| `Potential accumulation forming` | Slope and partial choppiness pass |
| `Accumulation found` | All conditions met — confirmed sideways zone |

The grey box is drawn on the chart while the zone is active. The Discord alert fires only on **breakout** — when price closes outside the box.

---

## Adding a New Detector

**1.** Create `detectors/my_detector.py` with a `detect(df, **kwargs) -> dict | None` function:

```python
def detect(df) -> dict | None:
    # your logic
    return {
        "detector": "my_detector",
        "status": "found",          # "found" | "potential"
        "start": <unix timestamp>,
        "end":   <unix timestamp>,
        "top":   <float>,
        "bottom": <float>,
        "is_active": <bool>,
    }
```

**2.** Register it in `detectors/__init__.py`:

```python
from detectors.my_detector import detect as my_detector_detect

REGISTRY = {
    "accumulation": accumulation_detect,
    "my_detector":  my_detector_detect,
}
```

**3.** Add it to a pair in `config.py`:

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

## Discord Alerts

Alerts fire on **breakout** — once price closes outside the accumulation box. Each alert includes:

- Pair name and detector
- Duration of the accumulation zone
- Screenshot of the chart at breakout moment

Test an alert manually:
```
http://localhost:5000/test-alert
```

---

## Running with PM2

```bash
pm2 start app.py --name "lightweightchart-agent"
pm2 startup | tail -n 1 | bash
pm2 save
```
