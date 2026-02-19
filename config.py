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
        "ticker": "YM=F",
        "port": 5000,
        "label": "US30 (Dow Jones Futures)",
        "interval": "1m",
        "period": "1d",
        "detectors": ["accumulation"],
        # US30 ~42000 pts: 0.003 = ~126 pt range, captures real sideways chop
        "detector_params": {
            "accumulation": {"lookback": 40, "threshold_pct": 0.003},
        },
    },
    "US100": {
        "ticker": "NQ=F",
        "port": 5001,
        "label": "US100 (Nasdaq Futures)",
        "interval": "1m",
        "period": "1d",
        "detectors": ["accumulation"],
        # NQ ~21000 pts: 0.003 = ~63 pt range
        "detector_params": {
            "accumulation": {"lookback": 40, "threshold_pct": 0.003},
        },
    },
    "XAUUSD": {
        "ticker": "GC=F",
        "port": 5002,
        "label": "XAUUSD (Gold Futures)",
        "interval": "1m",
        "period": "1d",
        "detectors": ["accumulation"],
        # Gold ~3000: 0.002 = ~$6 range per zone
        "detector_params": {
            "accumulation": {"lookback": 40, "threshold_pct": 0.002},
        },
    },
    # Add more pairs here - tune threshold_pct to instrument volatility:
    # "EURUSD": {
    #     "ticker": "EURUSD=X",
    #     "port": 5003,
    #     "label": "EUR/USD",
    #     "interval": "1m",
    #     "period": "1d",
    #     "detectors": ["accumulation"],
    #     "detector_params": {
    #         "accumulation": {"lookback": 40, "threshold_pct": 0.0005},
    #     },
    # },
}
