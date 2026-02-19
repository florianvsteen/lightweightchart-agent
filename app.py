import os
import time
import threading
import yfinance as yf
import pandas as pd
from flask import Flask, render_template, jsonify

app = Flask(__name__)

# Mock or Live Tick storage
live_tick = {"time": None, "open": None, "high": None, "low": None, "close": None}

def detect_accumulation(df, lookback=30, threshold_pct=0.0018):
    if len(df) < lookback + 10:
        return None

    # 1. Find a potential base (starting from some point in the past)
    # We look back slightly to find where the "sideways" action started
    for i in range(len(df) - lookback, 0, -1):
        window = df.iloc[i : i + lookback]
        high_max = window['High'].max()
        low_min = window['Low'].min()
        avg_price = window['Close'].mean()
        
        # Check if this window was tight and not trending
        start_p = window['Close'].iloc[0]
        end_p = window['Close'].iloc[-1]
        if ((high_max - low_min) / avg_price) <= threshold_pct and (abs(start_p - end_p)/start_p < threshold_pct * 0.5):
            
            # 2. We found a base! Now, look at everything AFTER this window
            # to see if/where it broke out.
            breakout_index = i + lookback
            for j in range(i + lookback, len(df)):
                current_close = df['Close'].iloc[j]
                if current_close > high_max or current_close < low_min:
                    breakout_index = j
                    break # The accumulation ended here
                else:
                    breakout_index = j # Still inside

            # Only return the box if the breakout happened recently 
            # or if we are still inside it.
            return {
                "start": int(df.index[i].timestamp()),
                "end": int(df.index[breakout_index].timestamp()),
                "top": float(high_max),
                "bottom": float(low_min),
                "is_active": breakout_index == len(df) - 1
            }
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
