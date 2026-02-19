"""
config.py — Central configuration for all trading pairs.

To add a new pair:
1. Add an entry to PAIRS below.
2. Assign a unique port (5000+).
3. List the detector(s) you want enabled for that pair.

detector_params for accumulation:
  lookback      — candle window size (max 60)
  threshold_pct — slope scaling factor per instrument
  max_range_pct — maximum allowed box height as % of price.
                  Box is rejected if (high - low) / avg_price > max_range_pct.
                  US30: 0.2% = 0.002  |  US100: 0.25% = 0.0025  |  XAUUSD: 0.3% = 0.003
"""

PAIRS = {
    "US30": {
        "ticker": "YM=F",
        "port": 5000,
        "label": "US30 (Dow Jones Futures)",
        "interval": "1m",
        "period": "1d",
        "detectors": ["accumulation"],
        "detector_params": {
            "accumulation": {
                "lookback": 40,
                "threshold_pct": 0.003,
                "max_range_pct": 0.002,   # 0.2% max box height
            },
        },
    },
    "US100": {
        "ticker": "NQ=F",
        "port": 5001,
        "label": "US100 (Nasdaq Futures)",
        "interval": "1m",
        "period": "1d",
        "detectors": ["accumulation"],
        "detector_params": {
            "accumulation": {
                "lookback": 40,
                "threshold_pct": 0.003,
                "max_range_pct": 0.0025,  # 0.25% max box height
            },
        },
    },
    "XAUUSD": {
        "ticker": "GC=F",
        "port": 5002,
        "label": "XAUUSD (Gold Futures)",
        "interval": "1m",
        "period": "1d",
        "detectors": ["accumulation"],
        "detector_params": {
            "accumulation": {
                "lookback": 40,
                "threshold_pct": 0.002,
                "max_range_pct": 0.003,   # 0.3% max box height
            },
        },
    },
    # "EURUSD": {
    #     "ticker": "EURUSD=X",
    #     "port": 5003,
    #     "label": "EUR/USD",
    #     "interval": "1m",
    #     "period": "1d",
    #     "detectors": ["accumulation"],
    #     "detector_params": {
    #         "accumulation": {"lookback": 40, "threshold_pct": 0.0005, "max_range_pct": 0.001},
    #     },
    # },
}
