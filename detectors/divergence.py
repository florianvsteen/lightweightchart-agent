import numpy as np
from typing import List, Dict, Tuple, Any

def detect_synchronized_pivots(
    price_highs: np.ndarray, 
    price_lows: np.ndarray,
    cvd_highs: np.ndarray, 
    cvd_lows: np.ndarray
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Surgical Fractal Detection: Finds every 'tip' even if flat.
    Allows for a 1-bar drift between Price and CVD for maximum detection.
    """
    sync_highs = []
    sync_lows = []
    n = len(price_highs)

    for i in range(1, n - 1):
        # --- BEARISH ANCHOR ---
        # Price Peak at 'i' OR 'i-1'
        p_is_high = price_highs[i] >= price_highs[i-1] and price_highs[i] >= price_highs[i+1]
        c_is_high = cvd_highs[i] >= cvd_highs[i-1] and cvd_highs[i] >= cvd_highs[i+1]
        
        # If both are high within the same 1-bar window, we anchor to 'i'
        if p_is_high and c_is_high:
            sync_highs.append({"index": i, "p_val": price_highs[i], "c_val": cvd_highs[i]})

        # --- BULLISH ANCHOR ---
        p_is_low = price_lows[i] <= price_lows[i-1] and price_lows[i] <= price_lows[i+1]
        c_is_low = cvd_lows[i] <= cvd_lows[i-1] and cvd_lows[i] <= cvd_lows[i+1]
        
        if p_is_low and c_is_low:
            sync_lows.append({"index": i, "p_val": price_lows[i], "c_val": cvd_lows[i]})

    return sync_highs, sync_lows


def detect_divergences(
    price_highs: np.ndarray,
    price_lows: np.ndarray,
    cvd_highs: np.ndarray,
    cvd_lows: np.ndarray,
    times: List[int],
    max_width: int = 15, # Increased slightly to match your drawings
    **kwargs
) -> List[Dict[str, Any]]:
    divergences = []
    s_highs, s_lows = detect_synchronized_pivots(price_highs, price_lows, cvd_highs, cvd_lows)

    # --- DEBUGGING OUTPUT ---
    print("\n--- FRACTAL DETECTOR DEBUG ---")
    print(f"Total Bars Processed: {len(times)}")
    print(f"Fractal High Anchors: {len(s_highs)}")
    print(f"Fractal Low Anchors:  {len(s_lows)}")
    print("------------------------------\n")


    # Bearish: Higher Price High, Lower CVD High
    for i in range(1, len(s_highs)):
        h2 = s_highs[i]
        for j in range(i-1, max(-1, i-10), -1): # Look back up to 10 anchors
            h1 = s_highs[j]
            if h2['index'] - h1['index'] > max_width: break
            
            if h2['p_val'] > h1['p_val'] and h2['c_val'] < h1['c_val']:
                divergences.append({
                    "type": "bearish", "label": "Bear Div", "price_time": times[h2['index']],
                    "price_pivot_1": {"bar": h1['index'], "value": float(h1['p_val'])},
                    "price_pivot_2": {"bar": h2['index'], "value": float(h2['p_val'])},
                    "cvd_pivot_1": {"bar": h1['index'], "value": float(h1['c_val'])},
                    "cvd_pivot_2": {"bar": h2['index'], "value": float(h2['c_val'])}
                })
                break # Found the best match for this peak

    # Bullish: Lower Price Low, Higher CVD Low
    for i in range(1, len(s_lows)):
        l2 = s_lows[i]
        for j in range(i-1, max(-1, i-10), -1):
            l1 = s_lows[j]
            if l2['index'] - l1['index'] > max_width: break
            
            if l2['p_val'] < l1['p_val'] and l2['c_val'] > l1['c_val']:
                divergences.append({
                    "type": "bullish", "label": "Bull Div", "price_time": times[l2['index']],
                    "price_pivot_1": {"bar": l1['index'], "value": float(l1['p_val'])},
                    "price_pivot_2": {"bar": l2['index'], "value": float(l2['p_val'])},
                    "cvd_pivot_1": {"bar": l1['index'], "value": float(l1['c_val'])},
                    "cvd_pivot_2": {"bar": l2['index'], "value": float(l2['c_val'])}
                })
                break

    return divergences, len(s_highs), len(s_lows)
