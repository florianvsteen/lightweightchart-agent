from flask import Flask, render_template, jsonify
import yfinance as yf
import pandas as pd
import pandas_ta_classic as ta
import threading
import time
from tradingview_scraper.symbols.stream import RealTimeData

app = Flask(__name__)

# Global variable to hold the real-time scraper data
live_tick = {"price": None, "time": None}

def start_tv_scraper():
    global live_tick
    
    # PASTE YOUR SESSION ID HERE
    # Found in Chrome -> F12 -> Application -> Cookies -> sessionid
    TV_SESSION_ID = "ayzvl8ectj41gsxjpvfhdl2ahfnbmojn"

    while True:
        try:
            # Pass the session_id to authenticate the WebSocket
            real_time_data = RealTimeData(session_id=TV_SESSION_ID)
            
            data_generator = real_time_data.get_latest_trade_info(exchange_symbol=["CAPITALCOM:US30"])
            
            for packet in data_generator:
                # 'lp' stands for Last Price in TradingView's protocol
                price = packet.get('price') or packet.get('lp')
                
                if price is not None:
                    try:
                        live_tick = {
                            "price": float(price),
                            "time": int(time.time())
                        }
                        # The print below will now show real-time data instead of delayed data
                        print(f"DEBUG: Live Tick Received -> {live_tick['price']}")
                    except ValueError:
                        continue 
                        
        except Exception as e:
            # If your session ID expires or the internet blips, it restarts here
            print(f"Scraper Error: {e}. Reconnecting in 5s...")
            time.sleep(5)

# Launch the scraper thread
threading.Thread(target=start_tv_scraper, daemon=True).start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data/<symbol>')
def get_data(symbol):
    try:
        ticker = "YM=F" if symbol.upper() == "DOW" else symbol
        df = yf.download(ticker, period="1d", interval="1m")
        
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        df = df.dropna()
        if df.empty:
            return jsonify({"error": "No data found"}), 404

        df['ad'] = ta.ad(df['High'], df['Low'], df['Close'], df['Volume'])
        df['is_accumulating'] = (df['ad'] > df['ad'].shift(20)) & \
                                 (df['Close'].rolling(20).std() / df['Close'] < 0.001)

        chart_data = []
        markers = []
        for index, row in df.iterrows():
            time_val = int(index.timestamp())
            chart_data.append({
                "time": time_val, "open": float(row['Open']), 
                "high": float(row['High']), "low": float(row['Low']), 
                "close": float(row['Close'])
            })
            if row['is_accumulating']:
                markers.append({
                    "time": time_val, "position": "belowBar", 
                    "color": "#2196F3", "shape": "arrowUp", "text": "ACC"
                })

        # We return the live tick alongside the historical candles
        return jsonify({
            "candles": chart_data, 
            "markers": markers,
            "live": live_tick
        })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
