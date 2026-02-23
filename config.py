"""
config.py — Central configuration for all trading pairs.

Per-session accumulation thresholds (max_range_pct):
  Asian session   — quieter, tighter ranges expected
  London session  — moderate volatility
  New York session — most volatile, widest valid boxes

market_timing options (defined in sessions.py):
  FOREX  — Forex pairs. Sessions: Asian 01-07, London 08-12, NY 13-19 UTC. Weekend halt.
  NYSE   — US equities/indices. Sessions: London 08-12, NYSE 14:30-21 UTC. Weekend halt.
  CRYPTO — Crypto. Same session windows as FOREX but no weekend halt, runs 24/7.
"""

from sessions import FOREX, NYSE, CRYPTO

PAIRS = {
    "US30": {
        "yf_ticker": "YM=F",
        "mt5_ticker": "US30",
        "port": 5000,
        "label": "US30 (Dow Jones)",
        "interval": "1m",
        "period": "1d",
        "default_interval": "1m",
        "market_timing": NYSE,
        "detectors": ["accumulation"],
        "detector_params": {
            "accumulation": {
                "timeframe": "1m",
                "lookback": 40,
                "min_candles": 20,
                "adx_threshold": 20,
                "threshold_pct": 0.003,
                "asian_range_pct":    0.001,
                "london_range_pct":   0.002,
                "new_york_range_pct": 0.003,
                "valid_sessions":     ["london", "new_york"],
                "alert_cooldown_minutes": 15,
                "min_touchpoints": 5,
            },
        },
    },
    "US100": {
        "yf_ticker": "NQ=F",
        "mt5_ticker": "NAS100",
        "port": 5001,
        "label": "US100 (Nasdaq)",
        "interval": "1m",
        "period": "1d",
        "default_interval": "1m",
        "market_timing": NYSE,
        "detectors": ["accumulation"],
        "detector_params": {
            "accumulation": {
                "timeframe": "1m",
                "lookback": 40,
                "min_candles": 20,
                "adx_threshold": 20,
                "threshold_pct": 0.003,
                "asian_range_pct":    0.0008,
                "london_range_pct":   0.001,
                "new_york_range_pct": 0.0025,
                "valid_sessions":     ["london", "new_york"],
                "alert_cooldown_minutes": 15,
                "min_touchpoints": 5,
            },
        },
    },
    "XAUUSD": {
        "yf_ticker": "GC=F",
        "mt5_ticker": "XAUUSD",
        "port": 5002,
        "label": "XAUUSD (Gold)",
        "interval": "1m",
        "period": "1d",
        "default_interval": "1m",
        "market_timing": FOREX,
        "detectors": ["accumulation"],
        "detector_params": {
            "accumulation": {
                "timeframe": "1m",
                "lookback": 40,
                "min_candles": 20,
                "adx_threshold": 20,
                "threshold_pct": 0.002,
                "asian_range_pct":    0.0015,
                "london_range_pct":   0.002,
                "new_york_range_pct": 0.003,
                "valid_sessions":     ["london", "new_york"],
                "alert_cooldown_minutes": 15,
                "min_touchpoints": 5,
            },
        },
    },
    "EURUSD": {
        "yf_ticker": "EURUSD=X",
        "mt5_ticker": "EURUSD",
        "port": 5004,
        "label": "EUR/USD",
        "interval": "15m",
        "period": "5d",
        "default_interval": "30m",
        "market_timing": FOREX,
        "detectors": ["supply_demand"],
        "detector_params": {
            "supply_demand": {
                "timeframe": "30m",
                "impulse_multiplier": 1.8,
                "wick_ratio": 0.6,
                "max_zones": 5,
                "max_age_days": 3,
                "valid_sessions": ["london", "new_york"],
            },
        },
    },
    "EURGBP": {
        "yf_ticker": "EURGBP=X",
        "mt5_ticker": "EURGBP",
        "port": 5003,
        "label": "EUR/GBP",
        "interval": "15m",
        "period": "5d",
        "default_interval": "30m",
        "market_timing": FOREX,
        "detectors": ["supply_demand"],
        "detector_params": {
            "supply_demand": {
                "timeframe": "30m",
                "impulse_multiplier": 1.8,
                "wick_ratio": 0.6,
                "max_zones": 5,
                "max_age_days": 3,
                "valid_sessions": ["london", "new_york"],
            },
        },
    },
    "USDJPY": {
        "yf_ticker": "USDJPY=X",
        "mt5_ticker": "USDJPY",
        "port": 5006,
        "label": "USD/JPY",
        "interval": "15m",
        "period": "5d",
        "default_interval": "30m",
        "market_timing": FOREX,
        "detectors": ["supply_demand"],
        "detector_params": {
            "supply_demand": {
                "timeframe": "30m",
                "impulse_multiplier": 1.8,
                "wick_ratio": 0.6,
                "max_zones": 5,
                "max_age_days": 3,
                "valid_sessions": ["london", "new_york"],
            },
        },
    },
    "GBPUSD": {
        "yf_ticker": "GBPUSD=X",
        "mt5_ticker": "GBPUSD",
        "port": 5005,
        "label": "GBPUSD",
        "interval": "15m",
        "period": "5d",
        "default_interval": "30m",
        "market_timing": FOREX,
        "detectors": ["supply_demand"],
        "detector_params": {
            "supply_demand": {
                "timeframe": "30m",
                "impulse_multiplier": 1.8,
                "wick_ratio": 0.6,
                "max_zones": 5,
                "max_age_days": 3,
                "valid_sessions": ["london", "new_york"],
            },
        },
    },
}
