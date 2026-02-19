"""
config.py — Central configuration for all trading pairs.

To add a new pair:
1. Add an entry to PAIRS below.
2. Assign a unique port (5000+).
3. List the detector(s) you want enabled for that pair.

Available detectors (add more in detectors/):
  - "accumulation"
"""

PAIRS = {
    "US30": {
        "ticker": "YM=F",           # yfinance ticker
        "port": 5000,
        "label": "US30 (Dow Jones Futures)",
        "interval": "1m",
        "period": "1d",
        "detectors": ["accumulation"],
    },
    "US100": {
        "ticker": "NQ=F",
        "port": 5001,
        "label": "US100 (Nasdaq Futures)",
        "interval": "1m",
        "period": "1d",
        "detectors": ["accumulation"],
    },
    "XAUUSD": {
        "ticker": "GC=F",
        "port": 5002,
        "label": "XAUUSD (Gold Futures)",
        "interval": "1m",
        "period": "1d",
        "detectors": ["accumulation"],
    },
    # Add more pairs here:
    # "EURUSD": {
    #     "ticker": "EURUSD=X",
    #     "port": 5003,
    #     "label": "EUR/USD",
    #     "interval": "1m",
    #     "period": "1d",
    #     "detectors": ["accumulation"],
    # },
}
