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
    if len(df) < lookback:
        return None
    
    recent = df.tail(lookback)
    
    # 1. Range Calculation (Tightness)
    high_max = recent['High'].max()
    low_min = recent['Low'].min()
    current_price = recent['Close'].iloc[-1]
    actual_range_pct = (high_max - low_min) / current_price

    # 2. Trend Filter (The "Anti-Yellow" Logic)
    # Check the difference between the start and end of the box
    start_price = recent['Close'].iloc[0]
    end_price = recent['Close'].iloc[-1]
    price_change_pct = abs(start_price - end_price) / start_price

    # DEBUG logs to your terminal
    print(f"Range: {actual_range_pct:.5f} | Change: {price_change_pct:.5f}")

    # To be red (accumulation), it must be TIGHT and NOT TRENDING
    # If price_change_pct is high, it's a vertical move (Yellow)
    if actual_range_pct <= threshold_pct and price_change_pct < (threshold_pct * 0.5):
        return {
            "start": int(recent.index[0].timestamp()),
            "end": int(recent.index[-1].timestamp()),
            "top": float(high_max),
            "bottom": float(low_min)
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
