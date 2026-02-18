from flask import Flask, render_template, jsonify
import yfinance as yf
import pandas_ta_classic as ta  # Change this line

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

# The <symbol> in the route must match the variable name in the function
@app.route('/api/data/<symbol>')
def get_data(symbol):
    try:
        # Map 'DOW' or other names to the actual Yahoo ticker
        ticker = "YM=F" if symbol.upper() == "DOW" else symbol
        
        # Fetch 1-minute data for 1 day
        df = yf.download(ticker, period="1d", interval="1m")
        
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        df = df.dropna()

        if df.empty:
            return jsonify({"error": "No data found"}), 404

        # Logic: Accumulation/Distribution
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
                    "time": time_val,
                    "position": "belowBar",
                    "color": "#2196F3",
                    "shape": "arrowUp",
                    "text": "ACC"
                })

        return jsonify({"candles": chart_data, "markers": markers})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
