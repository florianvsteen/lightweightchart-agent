"""
providers/__init__.py

Loads the correct data provider based on the DATA_PROVIDER environment variable.

Supported values:
  yahoo        — Yahoo Finance via yfinance (default)
  metatrader   — MetaTrader 5 terminal (Windows only)

Usage:
  export DATA_PROVIDER=yahoo        # default
  export DATA_PROVIDER=metatrader

The loaded provider exposes:
  get_df(ticker, interval, period)       → pd.DataFrame
  get_bias_df(ticker, period, interval)  → pd.DataFrame
  LOCK                                   → threading.Lock
"""

import os

_PROVIDER_NAME = os.environ.get("DATA_PROVIDER", "yahoo").lower().strip()

if _PROVIDER_NAME == "metatrader":
    try:
        import MetaTrader5  # noqa — verify package is installed at startup
    except ImportError:
        print("[provider] ✗ DATA_PROVIDER=metatrader but MetaTrader5 package is not installed.")
        print("[provider]   Install it with: pip install MetaTrader5  (Windows only)")
        print("[provider] ⚠ Falling back to Yahoo Finance")
        _PROVIDER_NAME = "yahoo"
    else:
        from providers.metatrader import get_df, get_bias_df, LOCK
        print("[provider] ✓ Active provider: MetaTrader 5")

if _PROVIDER_NAME == "yahoo":
    from providers.yahoo import get_df, get_bias_df, LOCK
    print("[provider] ✓ Active provider: Yahoo Finance")
elif _PROVIDER_NAME not in ("metatrader", "yahoo"):
    print(f"[provider] ⚠ Unknown DATA_PROVIDER='{_PROVIDER_NAME}' — falling back to Yahoo Finance")
    from providers.yahoo import get_df, get_bias_df, LOCK
    print("[provider] ✓ Active provider: Yahoo Finance (fallback)")

__all__ = ["get_df", "get_bias_df", "LOCK"]
