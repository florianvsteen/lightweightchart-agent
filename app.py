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

def start_databento_stream():
    global live_tick
    api_key = os.environ.get('DATABENTO_API_KEY')
    
    if not api_key:
        print("!!! ERROR: DATABENTO_API_KEY missing")
        return

    client = db.Live(key=api_key)

    try:
        # Subscribe to E-mini Dow Futures (YM) on CME Globex
        # 'ohlcv-1m' schema gives us the minute bars directly
        client.subscribe(
            dataset='GLBX.MDP3',
            schema='ohlcv-1m',
            symbols=['YM.HOT'] # .HOT automatically tracks the front month contract
        )

        print(">>> Databento: Stream Connected (YM Futures)")

        for record in client:
            if isinstance(record, db.OHLCVMsg):
                # Databento timestamps are in nanoseconds (Unix)
                timestamp_s = record.ts_event // 1_000_000_000
                
                live_tick = {
                    "time": int(timestamp_s),
                    "open": float(record.open),
                    "high": float(record.high),
                    "low": float(record.low),
                    "close": float(record.close)
                }
                # Optional: Log the price to verify it's working
                # print(f"Databento Tick: {live_tick['close']}")

    except Exception as e:
        print(f"Databento Error: {e}")
        time.sleep(5)
        start_databento_stream() # Simple reconnect logic

# Start the live stream in the background
threading.Thread(target=start_databento_stream, daemon=True).start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data/DOW')
def get_dow_data():
    try:
        # 1. Fetch History from YFinance (Delayed)
        df = yf.download("YM=F", period="1d", interval="1m", progress=False)
        
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        df = df.dropna()
        
        candles = []
        for index, row in df.iterrows():
            candles.append({
                "time": int(index.timestamp()),
                "open": float(row['Open']),
                "high": float(row['High']),
                "low": float(row['Low']),
                "close": float(row['Close'])
            })

        # 2. Return history + the most recent live tick from Databento
        # Your frontend should use chart.update(live) to append this tick
        return jsonify({
            "candles": candles,
            "live": live_tick
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
