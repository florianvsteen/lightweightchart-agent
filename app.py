from flask import Flask, render_template, jsonify
import yfinance as yf
import pandas as pd
import pandas_ta_classic as ta
import threading
import time
import os
import logging
from tradingview_scraper.symbols.stream import Streamer

# Silence noise
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
logging.getLogger('peewee').setLevel(logging.CRITICAL)
logging.getLogger('werkzeug').setLevel(logging.ERROR)

app = Flask(__name__)

live_tick = {"time": None, "open": None, "high": None, "low": None, "close": None}

def start_tv_scraper():
    global live_tick
    while True:
        try:
            # Grab JWT from Portainer env
            jwt = os.environ.get('TRADINGVIEW_JWT', '').strip()
            
            if not jwt:
                print("!!! ERROR: TRADINGVIEW_JWT not found in environment variables")
                time.sleep(10)
                continue

            # 1. Initialize the Streamer with your JWT
            streamer = Streamer(
                export_result=False,
                export_type='json',
                websocket_jwt_token=jwt
            )

            # 2. Setup the generator for US30 1m
            data_generator = streamer.stream(
                exchange="CAPITALCOM",
                symbol="US30",
                timeframe="1m",
                numb_price_candles=1, # We only need the latest tick
            )
            
            print(">>> Streamer: Connected to CAPITALCOM:US30")

            for packet in data_generator:
                # The Streamer returns a list or dict depending on the update
                # We extract the latest price candle
                try:
                    # Adjusting based on Streamer's typical output format
                    candle = packet[0] if isinstance(packet, list) else packet
                    
                    if 'close' in candle:
                        current_minute = int(time.time() // 60) * 60
                        live_tick = {
                            "time": current_minute,
                            "open": float(candle.get('open', candle['close'])),
                            "high": float(candle.get('high', candle['close'])),
                            "low": float(candle.get('low', candle['close'])),
                            "close": float(candle['close'])
                        }
                        print(f"LIVE: {live_tick['close']}")
                except (IndexError, KeyError, TypeError):
                    continue

        except Exception as e:
            print(f"Streamer Error: {e}. Reconnecting...")
            time.sleep(5)

threading.Thread(target=start_tv_scraper, daemon=True).start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data/<symbol>')
def get_data(symbol):
    try:
        ticker = "YM=F" if symbol.upper() == "DOW" else symbol
        df = yf.download(ticker, period="1d", interval="1m", progress=False)
        
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        df = df.dropna()
        chart_data = []
        for index, row in df.iterrows():
            chart_data.append({
                "time": int(index.timestamp()), 
                "open": float(row['Open']), "high": float(row['High']), 
                "low": float(row['Low']), "close": float(row['Close'])
            })

        return jsonify({"candles": chart_data, "live": live_tick})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)
