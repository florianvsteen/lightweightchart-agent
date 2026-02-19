# web/server.py
from flask import Flask, render_template, jsonify
# import your data_handler here

app = Flask(__name__)

@app.route('/chart/<asset_name>')
def chart_page(asset_name):
    # Renders the template, JS handles the rest
    return render_template('index.html', asset_name=asset_name)

@app.route('/api/data/<asset_name>')
def get_modular_data(asset_name):
    # 1. Look up ticker from core/config.py
    # 2. Fetch data via services/data_handler.py
    # 3. Run detection via core/accumulation_detector.py
    # 4. Return JSON
    pass
