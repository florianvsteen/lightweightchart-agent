import os
import time
import logging
import threading
from flask import Flask, render_template, jsonify
import yfinance as yf
import pandas as pd
from tradingview_scraper.symbols.stream import Streamer

# --- SILENCE LOGGING ---
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
logging.getLogger('werkzeug').setLevel(logging.ERROR)

app = Flask(__name__)

# This holds the most recent candle from the Streamer
live_tick = {
    "time": None,
    "open": None,
    "high": None,
    "low": None,
    "close": None
}

def start_tv_streamer():
    global live_tick
    while True:
        try:
            # Your Portainer env var: ayzvl8ectj...
            token = os.environ.get('TRADINGVIEW_COOKIE', '').strip()
            
            if not token:
                print("!!! ERROR: TRADINGVIEW_COOKIE (JWT) missing in environment")
                time.sleep(10)
                continue

            # Initialize Streamer as per documentation
            streamer = Streamer(
                export_result=False,
                export_type='json',
                websocket_jwt_token=token 
            )

            # Request 1m candles for US30 from CAPITALCOM
            data_generator = streamer.stream(
                exchange="CAPITALCOM", 
                symbol="US30",
                timeframe="1m",
                numb_price_candles=1
            )
            
            print(">>> Streamer: Connected to CAPITALCOM:US30 (1m)")

            for packet in data_generator:
                # The streamer yields lists of candle updates
                if packet and len(packet) > 0:
                    candle = packet[0]
                    
                    # Update global tick for the Flask API
                    # We use system time for the chart sync
                    current_minute = int(time.time() // 60) * 60
                    
                    live_tick = {
                        "time": current_minute,
                        "open": float(candle.get('open', candle['close'])),
                        "high": float(candle.get('high', candle['close'])),
                        "low": float(candle.get('low', candle['close'])),
                        "close": float(candle['close'])
                    }
                    print(f"PRICE UPDATE: {live_tick['close']}")

        except Exception as e:
            print(f"Streamer Error: {e}. Reconnecting...")
            time.sleep(5)

# Start the streamer thread
threading.Thread(target=start_tv_streamer, daemon=True).start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data/DOW')
def get_dow_data():
    try:
        # History (from Yahoo Finance to fill the chart initially)
        df = yf.download("YM=F", period="1d", interval="1m", progress=False)
        
        # Flatten MultiIndex if necessary
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

        return jsonify({
            "candles": candles,
            "live": live_tick
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
