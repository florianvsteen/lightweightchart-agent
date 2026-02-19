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


def _build_zone(df, i: int, h_max: float, l_min: float, status: str) -> dict:
    """Build a zone dict for a given window start index."""
    breakout_idx = i
    for j in range(i, len(df)):
        breakout_idx = j
        current_c = _scalar(df['Close'].iloc[j])
        if current_c > h_max or current_c < l_min:
            break
    return {
        "detector": "accumulation",
        "status": status,           # "found" | "potential"
        "start": int(df.index[i].timestamp()),
        "end": int(df.index[breakout_idx].timestamp()),
        "top": h_max,
        "bottom": l_min,
        "is_active": breakout_idx == (len(df) - 1),
    }


def detect(df, lookback: int = 40, threshold_pct: float = 0.001) -> dict | None:
    """
    Scan historical candles backwards for sideways accumulation zones.

    Returns a dict with a `status` field:
      "found"     — strict sideways consolidation (low range, low std, low drift)
      "potential" — range and stability pass but drift is slightly elevated
      None        — nothing found → caller should treat as "looking"

    Only truly sideways zones qualify. Trending windows are skipped entirely.
    """
    try:
        if len(df) < lookback + 5:
            return None

        # Flatten any residual MultiIndex columns yfinance may leave behind
        if isinstance(df.columns, pd.MultiIndex):
            df = df.copy()
            df.columns = df.columns.get_level_values(0)

        best_potential = None

        for i in range(len(df) - lookback - 1, 0, -1):
            window = df.iloc[i: i + lookback]

            h_max   = _scalar(window['High'].max())
            l_min   = _scalar(window['Low'].min())
            avg_p   = _scalar(window['Close'].mean())
            std_dev = _scalar(window['Close'].std())
            start_p = _scalar(window['Close'].iloc[0])
            end_p   = _scalar(window['Close'].iloc[-1])

            range_pct       = (h_max - l_min) / avg_p
            stability_score = std_dev / avg_p
            drift           = abs(start_p - end_p) / start_p

            sideways_range   = range_pct       <= threshold_pct
            sideways_stable  = stability_score  < (threshold_pct * 0.25)
            sideways_nodrift = drift            < (threshold_pct * 0.3)

            # Strict accumulation — all three sideways conditions met
            if sideways_range and sideways_stable and sideways_nodrift:
                zone = _build_zone(df, i + lookback, h_max, l_min, "found")
                # Override start to the actual window start
                zone["start"] = int(df.index[i].timestamp())
                return zone

            # Potential — range and stability are sideways but drift is slightly elevated
            # (price is still coiling but hasn't fully settled)
            if (
                best_potential is None
                and sideways_range
                and sideways_stable
                and drift < (threshold_pct * 0.6)   # relaxed drift threshold
            ):
                zone = _build_zone(df, i + lookback, h_max, l_min, "potential")
                zone["start"] = int(df.index[i].timestamp())
                best_potential = zone

        # Return best potential if no confirmed zone found
        return best_potential

    except Exception as e:
        print(f"[accumulation] Detection error: {e}")
        return None
