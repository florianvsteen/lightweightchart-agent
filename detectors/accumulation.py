import pandas as pd

"""
detectors/accumulation.py

Detects accumulation phases: sideways price consolidation with low volatility,
suggesting institutional positioning before a breakout.

Interface: detect(df) -> dict | None

Returned dict keys:
  start      (int)   Unix timestamp of zone start
  end        (int)   Unix timestamp of zone end (or last candle if active)
  top        (float) Upper boundary of the zone
  bottom     (float) Lower boundary of the zone
  is_active  (bool)  True if price hasn't broken out yet
  detector   (str)   Always "accumulation"
"""


def _scalar(val) -> float:
    """Safely extract a Python float from a pandas scalar, Series, or numpy type."""
    if hasattr(val, 'item'):
        return val.item()
    if hasattr(val, 'iloc'):
        return float(val.iloc[0])
    return float(val)


def detect(df, lookback: int = 40, threshold_pct: float = 0.001) -> dict | None:
    """
    Scan historical candles backwards and find the most recent accumulation zone.

    Args:
        df:            OHLCV DataFrame (indexed by datetime, columns: Open High Low Close Volume)
        lookback:      Number of candles to evaluate per window
        threshold_pct: Maximum allowed price range as a fraction of avg price

    Returns:
        Zone dict or None if no zone found.
    """
    try:
        if len(df) < lookback + 5:
            return None

        # Flatten any residual MultiIndex columns yfinance may leave behind
        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = df.columns.get_level_values(0)

        for i in range(len(df) - lookback - 1, 0, -1):
            window = df.iloc[i: i + lookback]

            h_max      = _scalar(window['High'].max())
            l_min      = _scalar(window['Low'].min())
            avg_p      = _scalar(window['Close'].mean())
            std_dev    = _scalar(window['Close'].std())
            start_p    = _scalar(window['Close'].iloc[0])
            end_p      = _scalar(window['Close'].iloc[-1])

            range_pct       = (h_max - l_min) / avg_p
            stability_score = std_dev / avg_p
            drift           = abs(start_p - end_p) / start_p

            if (
                range_pct <= threshold_pct
                and stability_score < (threshold_pct * 0.25)
                and drift < (threshold_pct * 0.3)
            ):
                # Find where price eventually breaks out of the zone
                breakout_idx = i + lookback
                for j in range(i + lookback, len(df)):
                    breakout_idx = j
                    current_c = _scalar(df['Close'].iloc[j])
                    if current_c > h_max or current_c < l_min:
                        break

                return {
                    "detector": "accumulation",
                    "start": int(df.index[i].timestamp()),
                    "end": int(df.index[breakout_idx].timestamp()),
                    "top": h_max,
                    "bottom": l_min,
                    "is_active": breakout_idx == (len(df) - 1),
                }

        return None

    except Exception as e:
        print(f"[accumulation] Detection error: {e}")
        return None
