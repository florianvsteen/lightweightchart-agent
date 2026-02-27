"""
detectors/bias.py

Directional bias calculation from daily and weekly candles.

Bias levels:
  strong_bullish  — current candle engulfs previous candle body AND is bullish
  bullish         — close > open (bullish candle, no engulfment)
  bearish         — close < open (bearish candle, no engulfment)
  strong_bearish  — current candle engulfs previous candle body AND is bearish
  misaligned      — daily and weekly bias disagree

Engulfment: a candle is "strong" when its body (open-to-close range) fully
contains the body of the candle before it:
  body_high[N] >= body_high[N-1]  AND  body_low[N] <= body_low[N-1]
where body_high = max(open, close) and body_low = min(open, close).
"""

from providers import get_bias_df as _provider_get_bias_df


def _candle_bias(df):
    """
    Return bias string for the last fully COMPLETED candle (df[-2]).
    Engulfment is checked against the candle before it (df[-3]).
    Returns 'strong_bullish', 'bullish', 'bearish', 'strong_bearish', or None.
    """
    if len(df) < 3:
        return None

    prev2 = df.iloc[-3]   # candle before the bias candle
    prev  = df.iloc[-2]   # bias candle (last fully closed)

    o_prev2  = float(prev2['Open'])
    c_prev2  = float(prev2['Close'])
    o_prev   = float(prev['Open'])
    c_prev   = float(prev['Close'])

    # Bodies
    bh_prev2 = max(o_prev2, c_prev2)   # body high of prior candle
    bl_prev2 = min(o_prev2, c_prev2)   # body low  of prior candle
    bh_prev  = max(o_prev,  c_prev)    # body high of bias candle
    bl_prev  = min(o_prev,  c_prev)    # body low  of bias candle

    is_bullish = c_prev > o_prev
    engulfs    = (bh_prev >= bh_prev2) and (bl_prev <= bl_prev2)

    if is_bullish:
        return 'strong_bullish' if engulfs else 'bullish'
    else:
        return 'strong_bearish' if engulfs else 'bearish'


def _is_bullish_bias(bias: str) -> bool:
    return bias in ('bullish', 'strong_bullish')


def _is_bearish_bias(bias: str) -> bool:
    return bias in ('bearish', 'strong_bearish')


def _is_strong(bias: str) -> bool:
    return bias in ('strong_bullish', 'strong_bearish')


def _biases_agree(daily: str, weekly: str) -> bool:
    """Return True if daily and weekly point in the same direction."""
    return _is_bullish_bias(daily) == _is_bullish_bias(weekly)


def get_bias(ticker: str) -> dict:
    """
    Fetch previous completed daily and weekly candles and return bias info.

    Returns:
        {
          bias:          'strong_bullish' | 'bullish' | 'bearish' | 'strong_bearish' | 'misaligned'
          aligned:       bool   — True when daily and weekly agree
          daily_bias:    str
          weekly_bias:   str
          is_strong:     bool   — True if either daily or weekly is strong
          daily_open:    float
          daily_close:   float
          weekly_open:   float
          weekly_close:  float
          reason:        str    (only present on error)
        }
    """
    try:
        df_d = _provider_get_bias_df(ticker, "5d",  "1d").dropna()
        df_w = _provider_get_bias_df(ticker, "3mo", "1wk").dropna()

        if len(df_d) < 3 or len(df_w) < 3:
            return {"bias": "misaligned", "aligned": False,
                    "reason": "insufficient data for engulfment check"}

        daily_bias  = _candle_bias(df_d)
        weekly_bias = _candle_bias(df_w)

        if daily_bias is None or weekly_bias is None:
            return {"bias": "misaligned", "aligned": False, "reason": "could not determine bias"}

        aligned = _biases_agree(daily_bias, weekly_bias)

        # Combined bias: when aligned, use the strongest signal
        if aligned:
            # Pick strong if either is strong
            if _is_bullish_bias(daily_bias):
                combined = 'strong_bullish' if _is_strong(daily_bias) or _is_strong(weekly_bias) else 'bullish'
            else:
                combined = 'strong_bearish' if _is_strong(daily_bias) or _is_strong(weekly_bias) else 'bearish'
        else:
            combined = 'misaligned'

        d_open  = float(df_d['Open'].iloc[-2])
        d_close = float(df_d['Close'].iloc[-2])
        w_open  = float(df_w['Open'].iloc[-2])
        w_close = float(df_w['Close'].iloc[-2])

        return {
            "bias":         combined,
            "aligned":      aligned,
            "daily_bias":   daily_bias,
            "weekly_bias":  weekly_bias,
            "is_strong":    _is_strong(daily_bias) or _is_strong(weekly_bias),
            "daily_open":   d_open,
            "daily_close":  d_close,
            "weekly_open":  w_open,
            "weekly_close": w_close,
        }

    except Exception as e:
        print(f"[bias] Fetch error for {ticker}: {e}")
        return {"bias": "misaligned", "aligned": False, "reason": str(e)}


def is_bullish(bias_info: dict) -> bool:
    """Convenience: True if bias is bullish or strong_bullish."""
    return _is_bullish_bias(bias_info.get("bias", ""))


def is_bearish(bias_info: dict) -> bool:
    """Convenience: True if bias is bearish or strong_bearish."""
    return _is_bearish_bias(bias_info.get("bias", ""))
