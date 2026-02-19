import pandas as pd

class AccumulationDetector:
    def __init__(self, asset_name, config):
        self.asset_name = asset_name
        self.lookback = config['lookback']
        self.threshold = config['threshold']
        self.last_alerted_start = 0

    def check(self, df):
        # 1. Ensure columns are capitalized to match logic below
        # This prevents the "KeyError: 'High'" if the data comes in lowercase
        df.columns = [str(col).capitalize() for col in df.columns]

        if len(df) < self.lookback + 5:
            return None

        # Logic Scan
        for i in range(len(df) - self.lookback - 1, 0, -1):
            window = df.iloc[i : i + self.lookback]
            
            # Use .max() directly; it returns a float, so float() is safe
            h_max = float(window['High'].max())
            l_min = float(window['Low'].min())
            avg_p = float(window['Close'].mean())
            
            if avg_p == 0: continue # Prevent division by zero
            
            range_pct = (h_max - l_min) / avg_p
            
            if range_pct <= self.threshold:
                breakout_idx = i + self.lookback
                
                # Check for breakout
                for j in range(i + self.lookback, len(df)):
                    breakout_idx = j
                    # iloc[j] ensures we are comparing single values
                    if df['Close'].iloc[j] > h_max or df['Close'].iloc[j] < l_min:
                        break
                
                # Ensure index is datetime for timestamp conversion
                try:
                    start_ts = int(df.index[i].timestamp())
                    end_ts = int(df.index[breakout_idx].timestamp())
                except AttributeError:
                    # If index is just integers, use them as is
                    start_ts = i
                    end_ts = breakout_idx
                
                zone = {
                    "start": start_ts,
                    "end": end_ts,
                    "top": h_max,
                    "bottom": l_min,
                    "is_active": breakout_idx == (len(df) - 1)
                }
                
                # Check if this is a new zone
                if zone['is_active'] and zone['start'] > self.last_alerted_start:
                    self.last_alerted_start = zone['start']
                    return zone
        return None
