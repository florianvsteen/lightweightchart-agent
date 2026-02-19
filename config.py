"""
config.py — Central configuration for all trading pairs.

detector_params keys:
  timeframe     — data interval this detector runs on (stripped before passing to detect())
  lookback      — (accumulation) candle window size, max 60
  threshold_pct — (accumulation) slope scaling factor
  max_range_pct — (accumulation) max box height as % of price
  impulse_multiplier — (supply_demand) how much bigger impulse must be vs avg candle
  wick_ratio    — (supply_demand) min fraction of candle that must be wicks
  max_zones     — (supply_demand) max number of zones to return
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
                "timeframe": "1m",
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
    "EURUSD": {
        "ticker": "EURUSD=X",
        "port": 5003,
        "label": "EUR/USD",
        "interval": "15m",
        "period": "5d",
        "detectors": ["supply_demand"],
        "detector_params": {
            "supply_demand": {
                "timeframe": "30m",        # runs on 30m
                "impulse_multiplier": 1.8, # impulse must be 1.8x avg candle size
                "wick_ratio": 0.6,         # 60%+ of candle must be wicks
                "max_zones": 5,
            },
        },
    },
}
