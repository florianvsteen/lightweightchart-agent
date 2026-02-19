import os
import time
import threading
import yfinance as yf
import pandas as pd
from flask import Flask, render_template, jsonify

app = Flask(__name__)

# Mock or Live Tick storage
live_tick = {"time": None, "open": None, "high": None, "low": None, "close": None}

def detect_accumulation(df, lookback=20, threshold_pct=0.0025): # Increased to 0.25%
    if len(df) < lookback:
        return None
    
    recent = df.tail(lookback)
    
    # We use 'Close' for a more stable range calculation to ignore "noise" wicks
    high_body = recent[['Open', 'Close']].max(axis=1).max()
    low_body = recent[['Open', 'Close']].min(axis=1).min()
    current_price = recent['Close'].iloc[-1]
    
    actual_range = (high_body - low_body) / current_price
    
    # DEBUG: See why it's failing in your terminal
    print(f"Current Range: {actual_range:.5f} | Threshold: {threshold_pct:.5f}")

    if actual_range <= threshold_pct:
        return {
            "start": int(recent.index[0].timestamp()),
            "end": int(recent.index[-1].timestamp()),
            "top": float(recent['High'].max()), # Box still covers the wicks
            "bottom": float(recent['Low'].min())
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
