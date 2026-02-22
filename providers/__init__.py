"""
providers/__init__.py

Loads the correct data provider based on the DATA_PROVIDER environment variable.

Supported values:
  yahoo        — Yahoo Finance via yfinance (default)
  metatrader   — MetaTrader 5 via mt5rest HTTP API

Usage:
  export DATA_PROVIDER=yahoo        # default
  export DATA_PROVIDER=metatrader
  export MT5_API_URL=https://mt5.flownet.be
  export MT5_API_USER=283464
  export MT5_API_PASSWORD=yourpassword
  export MT5_API_SERVER=FusionMarkets-Live

The loaded provider exposes:
  get_df(ticker, interval, period)       → pd.DataFrame
  get_bias_df(ticker, period, interval)  → pd.DataFrame
  LOCK                                   → threading.Lock
"""

import os

_PROVIDER_NAME = os.environ.get("DATA_PROVIDER", "yahoo").lower().strip()

if _PROVIDER_NAME == "metatrader":
    from providers.metatrader import get_df, get_bias_df, LOCK
    print("[provider] ✓ Active provider: MetaTrader 5 (mt5rest HTTP API)")
elif _PROVIDER_NAME == "yahoo":
    from providers.yahoo import get_df, get_bias_df, LOCK
    print("[provider] ✓ Active provider: Yahoo Finance")
else:
    print(f"[provider] ⚠ Unknown DATA_PROVIDER='{_PROVIDER_NAME}' — falling back to Yahoo Finance")
    from providers.yahoo import get_df, get_bias_df, LOCK
    print("[provider] ✓ Active provider: Yahoo Finance (fallback)")

__all__ = ["get_df", "get_bias_df", "LOCK"]
