from flask import Flask, render_template, jsonify
from services.data_handler import format_for_charts

app = Flask(__name__)

# This dictionary will be updated by your run_asset_loop in main.py
# Format: {"US30": {"df": DataFrame, "zone": dict, "color": str}, ...}
DATA_STORE = {}

@app.route('/chart/<asset_name>')
def chart_page(asset_name):
    # This renders the HTML. The JS in index.html will call the API below.
    return render_template('index.html', asset_name=asset_name)

@app.route('/api/data/<asset_name>')
def get_data(asset_name):
    asset_data = DATA_STORE.get(asset_name)
    
    if not asset_data:
        return jsonify({"error": f"No data available for {asset_name}"}), 404

    # Use the helper to format the DF that is already in memory
    candles = format_for_charts(asset_data['df'])

    return jsonify({
        "candles": candles,
        "accumulation": asset_data['zone'],
        "color": asset_data['color']
    })
