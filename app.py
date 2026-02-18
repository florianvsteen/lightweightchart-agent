from flask import Flask, render_template, jsonify
import yfinance as yf
import pandas_ta_classic as ta  # Change this line

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data/<symbol>')
def get_data(symbol):
    # 1. Fetch Data
    df = yf.download(symbol, period="6mo", interval="1d")
    df.columns = df.columns.get_level_values(0) # Clean multi-index
    
    # 2. Accumulation Detection Logic (Using pandas_ta)
    # AD line: Measures cumulative flow of money in/out
    df['ad'] = ta.ad(df['High'], df['Low'], df['Close'], df['Volume'])
    
    # Simple accumulation check: 
    # AD line is higher than it was 10 days ago, but Price is within a 5% range.
    df['is_accumulating'] = (df['ad'] > df['ad'].shift(10)) & \
                             (df['Close'].rolling(10).std() / df['Close'] < 0.05)

    # 3. Format for Lightweight Charts
    chart_data = []
    markers = []
    
    for index, row in df.iterrows():
        timestamp = int(index.timestamp())
        chart_data.append({
            "time": timestamp,
            "open": row['Open'], "high": row['High'], 
            "low": row['Low'], "close": row['Close']
        })
        
        if row['is_accumulating']:
            markers.append({
                "time": timestamp,
                "position": "belowBar",
                "color": "#2196F3",
                "shape": "arrowUp",
                "text": "ACC"
            })

    return jsonify({"candles": chart_data, "markers": markers})

if __name__ == '__main__':
    app.run(debug=True, port=5000)
