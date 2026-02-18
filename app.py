from flask import Flask, render_template, jsonify
import yfinance as yf
import pandas as pd
import pandas_ta_classic as ta
import threading
import time
import os
import logging
from tradingview_scraper.symbols.stream import RealTimeData

# --- SILENCE ALL LOGGING NOISE ---
# This stops the yfinance download bar and debug messages
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
# This stops the 'SELECT "t1"."key"...' SQL messages from the peewee database
logging.getLogger('peewee').setLevel(logging.CRITICAL)
# This stops the Flask "GET /api/data" messages
logging.getLogger('werkzeug').setLevel(logging.ERROR)

app = Flask(__name__)

# Global variable to hold full OHLC data
live_tick = {
    "time": None,
    "open": None,
    "high": None,
    "low": None,
    "close": None
}

def start_tv_scraper():
    global live_tick
    while True:
        try:
            # Note: This expects TRADINGVIEW_COOKIE in your environment vars
            real_time_data = RealTimeData()
            data_generator = real_time_data.get_ohlcv(exchange_symbol="CAPITALCOM:US30")
            
            print(">>> TV Scraper: Connected to Stream")

            for packet in data_generator:
                if packet.get('close') is not None:
                    try:
                        # LOGIC: We align the live tick with the CURRENT minute.
                        # This ensures it shows up on the chart even if history is delayed.
                        current_minute = int(time.time() // 60) * 60
                        
                        live_tick = {
                            "time": current_minute,
                            "open": float(packet['open']),
                            "high": float(packet['high']),
                            "low": float(packet['low']),
                            "close": float(packet['close'])
                        }
                        # Print once in a while to Portainer so you know it's alive
                        print(f"LIVE TICK RECEIVED: {live_tick['close']}")
                    except (ValueError, TypeError, KeyError):
                        continue 
                        
        except Exception as e:
            print(f"Scraper Error: {e}. Reconnecting in 5s...")
            time.sleep(5)

# Start scraper in background
threading.Thread(target=start_tv_scraper, daemon=True).start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data/<symbol>')
def get_data(symbol):
    try:
        ticker = "YM=F" if symbol.upper() == "DOW" else symbol
        
        # Download history (progress=False hides the progress bar)
        df = yf.download(ticker, period="1d", interval="1m", progress=False)
        
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        df = df.dropna()
        if df.empty:
            return jsonify({"error": "No data found"}), 404

        # Technical Indicators (Accumulation/Distribution)
        df['ad'] = ta.ad(df['High'], df['Low'], df['Close'], df['Volume'])
        df['is_accumulating'] = (df['ad'] > df['ad'].shift(20)) & \
                                 (df['Close'].rolling(20).std() / df['Close'] < 0.001)

        chart_data = []
        markers = []
        for index, row in df.iterrows():
            time_val = int(index.timestamp())
            chart_data.append({
                "time": time_val, 
                "open": float(row['Open']), 
                "high": float(row['High']), 
                "low": float(row['Low']), 
                "close": float(row['Close'])
            })
            if row['is_accumulating']:
                markers.append({
                    "time": time_val, "position": "belowBar", 
                    "color": "#2196F3", "shape": "arrowUp", "text": "ACC"
                })

        return jsonify({
            "candles": chart_data, 
            "markers": markers,
            "live": live_tick
        })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # debug=False is CRITICAL. If True, it resets logging and kills your filters.
    app.run(debug=False, host='0.0.0.0', port=5000)
