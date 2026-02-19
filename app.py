import os
import time
import logging
import threading
import databento as db
from flask import Flask, render_template, jsonify
import yfinance as yf
import pandas as pd

# --- SILENCE LOGGING ---
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
logging.getLogger('werkzeug').setLevel(logging.ERROR)

app = Flask(__name__)

# Global storage for live updates
live_tick = {"time": None, "open": None, "high": None, "low": None, "close": None}

def detect_accumulation(df, lookback=30, threshold_pct=0.0015):
    """
    Detects recent sideways price action.
    threshold_pct=0.0015 means the high/low range is within 0.15% of the price.
    """
    if len(df) < lookback:
        return None
    
    recent = df.tail(lookback)
    high_max = recent['High'].max()
    low_min = recent['Low'].min()
    avg_price = recent['Close'].mean()
    
    price_range = high_max - low_min
    if (price_range / avg_price) <= threshold_pct:
        return {
            "start": int(recent.index[0].timestamp()),
            "end": int(recent.index[-1].timestamp()),
            "top": float(high_max),
            "bottom": float(low_min)
        }
    return None

def start_databento_stream():
    global live_tick
    api_key = os.environ.get('DATABENTO_API_KEY')
    if not api_key:
        print("!!! DATABENTO_API_KEY missing. Live stream disabled.")
        return

    client = db.Live(key=api_key)
    try:
        client.subscribe(dataset='GLBX.MDP3', schema='ohlcv-1m', symbols=['YM.HOT'])
        for record in client:
            if isinstance(record, db.OHLCVMsg):
                timestamp_s = record.ts_event // 1_000_000_000
                live_tick = {
                    "time": int(timestamp_s),
                    "open": float(record.open),
                    "high": float(record.high),
                    "low": float(record.low),
                    "close": float(record.close)
                }
    except Exception as e:
        print(f"Databento Error: {e}")
        time.sleep(5)
        start_databento_stream()

# Start background thread
#threading.Thread(target=start_databento_stream, daemon=True).start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data/DOW')
def get_dow_data():
    try:
        # Fetching History
        df = yf.download("YM=F", period="1d", interval="1m", progress=False)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.dropna()
        
        # Accumulation Detection
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
