from flask import Flask, render_template, jsonify
from core.config import ASSET_CONFIGS
from services.data_handler import fetch_market_data, format_for_charts
from core.accumulation_detector import AccumulationDetector

app = Flask(__name__)

@app.route('/chart/<asset_name>')
def chart_page(asset_name):
    return render_template('index.html', asset_name=asset_name)

@app.route('/api/data/<asset_name>')
def get_data(asset_name):
    config = ASSET_CONFIGS.get(asset_name)
    if not config:
        return jsonify({"error": "Asset not found"}), 404

    # 1. Fetch
    df = fetch_market_data(config['ticker'])
    
    # 2. Analyze (Modular logic)
    detector = AccumulationDetector(asset_name, config)
    acc_zone = detector.check(df)
    
    # 3. Format
    candles = format_for_charts(df)

    return jsonify({
        "candles": candles,
        "accumulation": acc_zone
    })
