"""
config.py — Central configuration for all trading pairs.

detector_params for accumulation:
  lookback      — candle window size (max 60)
  threshold_pct — slope scaling factor per instrument
  max_range_pct — maximum allowed box height as % of price
  timeframe     — data interval this detector runs on (always "1m" for accumulation)
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
                "max_range_pct": 0.002,
                "timeframe": "1m",      # detector always runs on 1m regardless of chart view
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
                "max_range_pct": 0.0025,
                "timeframe": "1m",
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
                "max_range_pct": 0.003,
                "timeframe": "1m",
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
    #         "accumulation": {"lookback": 40, "threshold_pct": 0.0005, "max_range_pct": 0.001, "timeframe": "1m"},
    #     },
    # },
}
