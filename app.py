from flask import Flask, render_template, jsonify
import yfinance as yf
import pandas as pd
import pandas_ta_classic as ta
import threading
import time
import os
import logging
from tradingview_scraper.symbols.stream import RealTimeData

# --- SILENCE ALL LOGGING ---
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
logging.getLogger('peewee').setLevel(logging.CRITICAL)
logging.getLogger('werkzeug').setLevel(logging.ERROR)

app = Flask(__name__)

# Global live data object
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
            # Ensure the env var is formatted exactly how the scraper likes it
            raw_id = os.environ.get('TRADINGVIEW_COOKIE', '').replace('sessionid=', '').replace(';', '').strip()
            if not raw_id:
                print("!!! ERROR: No TRADINGVIEW_COOKIE found in Portainer")
                time.sleep(10)
                continue
            
            os.environ['TRADINGVIEW_COOKIE'] = f"sessionid={raw_id}"
            
            # Use RealTimeData which is better suited for sessionid-based auth
            real_time_data = RealTimeData()
            data_generator = real_time_data.get_ohlcv(exchange_symbol="CAPITALCOM:US30")
            
            print(f">>> Scraper: Connected using session {raw_id[:8]}...")

            for packet in data_generator:
                # SAFE CHECK: Only process if 'close' exists. 
                # unauthorized sessions send packets without OHLC keys, causing the 'open' error.
                if isinstance(packet, dict) and 'close' in packet:
                    try:
                        # Align to current minute to bridge the yfinance delay gap
                        current_minute = int(time.time() // 60) * 60
                        
                        live_tick = {
                            "time": current_minute,
                            "open": float(packet.get('open', packet['close'])),
                            "high": float(packet.get('high', packet['close'])),
                            "low": float(packet.get('low', packet['close'])),
                            "close": float(packet['close'])
                        }
                        print(f"LIVE TICK: {live_tick['close']}")
                    except (ValueError, TypeError):
                        continue
                else:
                    # This silences the packets that don't have data (unauthorized heartbeats)
                    pass
                        
        except Exception as e:
            print(f"Scraper Error: {e}. Reconnecting...")
            time.sleep(5)

# Run scraper in background
threading.Thread(target=start_tv_scraper, daemon=True).start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data/<symbol>')
def get_data(symbol):
    try:
        ticker = "YM=F" if symbol.upper() == "DOW" else symbol
        # Fetching 1d history to fill the chart
        df = yf.download(ticker, period="1d", interval="1m", progress=False)
        
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        df = df.dropna()
        
        chart_data = []
        for index, row in df.iterrows():
            chart_data.append({
                "time": int(index.timestamp()), 
                "open": float(row['Open']), 
                "high": float(row['High']), 
                "low": float(row['Low']), 
                "close": float(row['Close'])
            })

        return jsonify({
            "candles": chart_data, 
            "live": live_tick
        })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)
