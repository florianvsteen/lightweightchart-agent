import os
import time
import threading
import yfinance as yf
import pandas as pd
from flask import Flask, render_template, jsonify

app = Flask(__name__)

# Mock or Live Tick storage
live_tick = {"time": None, "open": None, "high": None, "low": None, "close": None}

import numpy as np

import numpy as np

def detect_accumulation(df, lookback=25, threshold_pct=0.001): # Tighter 0.1% threshold
    try:
        if len(df) < lookback + 5:
            return None

        # Scan backwards
        for i in range(len(df) - lookback - 1, 0, -1):
            window = df.iloc[i : i + lookback]
            
            h_max = float(window['High'].max())
            l_min = float(window['Low'].min())
            avg_p = float(window['Close'].mean())
            
            # 1. RANGE: Is the vertical channel tight?
            range_pct = (h_max - l_min) / avg_p
            
            # 2. STABILITY: Price must stay close to the middle (low variance)
            std_dev = window['Close'].std()
            stability_score = std_dev / avg_p

            # 3. DRIFT: Start and end prices must be nearly identical (sideways)
            start_p = window['Close'].iloc[0]
            end_p = window['Close'].iloc[-1]
            drift = abs(start_p - end_p) / start_p

            # TIGHTENED LOGIC: Lower multipliers for stricter sideways requirements
            if range_pct <= threshold_pct and stability_score < (threshold_pct * 0.25) and drift < (threshold_pct * 0.3):
                
                # BASE FOUND. Extend the box until the first breakout candle.
                breakout_idx = i + lookback
                for j in range(i + lookback, len(df)):
                    breakout_idx = j
                    current_c = df['Close'].iloc[j]
                    
                    # Breakout occurs if current candle closes outside the H/L of the base
                    if current_c > h_max or current_c < l_min:
                        break 
                
                return {
                    "start": int(df.index[i].timestamp()),
                    "end": int(df.index[breakout_idx].timestamp()),
                    "top": h_max,
                    "bottom": l_min,
                    "is_active": breakout_idx == (len(df) - 1)
                }
        return None
    except Exception as e:
        print(f"Logic Error: {e}")
        return None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data/DOW')
def get_dow_data():
    try:
        df = yf.download("YM=F", period="1d", interval="1m", progress=False)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.dropna()

        acc_zone = detect_accumulation(df)

        candles = []
        for index, row in df.iterrows():
            candles.append({
                "time": int(index.timestamp()),
                "open": float(row['Open']),
                "high": float(row['High']),
                "low": float(row['Low']),
                "close": float(row['Close'])
            })

        return jsonify({
            "candles": candles,
            "live": live_tick,
            "accumulation": acc_zone
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
