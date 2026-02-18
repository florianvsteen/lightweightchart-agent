from flask import Flask, render_template, jsonify
import yfinance as yf
import pandas as pd
import pandas_ta_classic as ta
import threading
import time
import os
from tradingview_scraper.symbols.stream import RealTimeData

app = Flask(__name__)

# Updated global variable to hold full OHLC data
# This allows the frontend to "morph" the candle as price moves
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
            # Portainer env TRADINGVIEW_COOKIE is used automatically
            real_time_data = RealTimeData()
            
            # Using get_ohlcv to get the full candle components
            data_generator = real_time_data.get_ohlcv(exchange_symbol="CAPITALCOM:US30")
            
            print("Successfully connected to TradingView Stream...")

            for packet in data_generator:
                # packet structure: {'time': ..., 'open': ..., 'high': ..., 'low': ..., 'close': ..., 'volume': ...}
                if packet.get('close') is not None:
                    try:
                        # Update the global object with full OHLC values
                        live_tick = {
                            "time": int(packet.get('time', time.time())),
                            "open": float(packet['open']),
                            "high": float(packet['high']),
                            "low": float(packet['low']),
                            "close": float(packet['close'])
                        }
                        # Portainer log for verification
                        print(f"LIVE OHLC: O:{live_tick['open']} C:{live_tick['close']}")
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
        # Fetching 1d history to fill the chart
        df = yf.download(ticker, period="1d", interval="1m")
        
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        df = df.dropna()
        if df.empty:
            return jsonify({"error": "No data found"}), 404

        # Technical Indicators
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
            "live": live_tick  # Now contains full OHLC
        })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # host='0.0.0.0' is required for Portainer/Docker access
    app.run(debug=False, host='0.0.0.0', port=5000)
