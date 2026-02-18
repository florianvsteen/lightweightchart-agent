from flask import Flask, render_template, jsonify
import yfinance as yf
import pandas_ta_classic as ta  # Change this line

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data/<symbol>')
def get_data():
    symbol = "YM=F"  # Dow Jones Futures
    
    # 1. Fetch 1-minute data for the last 1 day
    df = yf.download(symbol, period="1d", interval="1m")
    
    # Fix: Flatten MultiIndex columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    
    df = df.dropna()

    # 2. Accumulation Logic (A/D line rising + Low Volatility)
    # Using a 20-minute lookback for intraday signals
    df['ad'] = ta.ad(df['High'], df['Low'], df['Close'], df['Volume'])
    df['is_accumulating'] = (df['ad'] > df['ad'].shift(20)) & \
                             (df['Close'].rolling(20).std() / df['Close'] < 0.001)

    chart_data = []
    markers = []
    
    for index, row in df.iterrows():
        # Lightweight Charts requires UTC Unix timestamps (seconds)
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
                "time": time_val,
                "position": "belowBar",
                "color": "#2196F3",
                "shape": "arrowUp",
                "text": "ACC"
            })

    return jsonify({"candles": chart_data, "markers": markers})

if __name__ == '__main__':
    # Listen on all interfaces (0.0.0.0) for container access
    app.run(debug=True, host='0.0.0.0', port=5000)
