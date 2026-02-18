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
            # Re-read fresh from env every loop
            raw_id = os.environ.get('TRADINGVIEW_COOKIE', '').strip()
            os.environ['TRADINGVIEW_COOKIE'] = f"sessionid={raw_id}"
            
            real_time_data = RealTimeData()
            data_generator = real_time_data.get_ohlcv(exchange_symbol="CAPITALCOM:US30")
            
            print(">>> TV Scraper: Stream Started")

            for packet in data_generator:
                # Use .get() to avoid KeyError 'open'
                o = packet.get('open')
                h = packet.get('high')
                l = packet.get('low')
                c = packet.get('close')

                # If we have at least the close price, we can make a 'flat' candle
                if c is not None:
                    current_minute = int(time.time() // 60) * 60
                    live_tick = {
                        "time": current_minute,
                        "open": float(o) if o is not None else float(c),
                        "high": float(h) if h is not None else float(c),
                        "low": float(l) if l is not None else float(c),
                        "close": float(c)
                    }
                    print(f"LIVE PRICE: {live_tick['close']}")
                else:
                    # This captures the 'empty' packets that were causing your crash
                    pass 

        except Exception as e:
            # This will now catch actual connection errors, not just missing data
            print(f"Scraper Loop Error: {e}")
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
