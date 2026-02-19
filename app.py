import os
import time
import threading
import numpy as np
import pandas as pd
import yfinance as yf
from flask import Flask, render_template, jsonify
from discord_webhook import DiscordWebhook, DiscordEmbed
from playwright.sync_api import sync_playwright

app = Flask(__name__)

# Environment Variables
DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL')

# Global storage
live_tick = {"time": None, "open": None, "high": None, "low": None, "close": None}
last_alerted_start = 0  # To prevent duplicate alerts for the same box

def capture_and_send_discord(zone_info):
    """Background task to take a screenshot and send to Discord."""
    global last_alerted_start
    
    # Logic to ensure we only alert ONCE per unique accumulation start time
    if zone_info['start'] <= last_alerted_start:
        return
    
    last_alerted_start = zone_info['start']
    screenshot_path = "chart_alert.png"

    try:
        # 1. Capture Screenshot using Playwright
        with sync_playwright() as p:
            # We use chromium headless
            browser = p.chromium.launch()
            page = browser.new_page(viewport={'width': 1280, 'height': 720})
            
            # Point to your local Flask app
            page.goto("http://127.0.0.1:5000")
            
            # Wait for the chart/box to render (adjust if your internet is slow)
            page.wait_for_timeout(4000) 
            
            page.screenshot(path=screenshot_path)
            browser.close()

        # 2. Send to Discord
        if DISCORD_WEBHOOK_URL:
            webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, content="🚀 **US30 Accumulation Detected**")
            
            embed = DiscordEmbed(title="Market Consolidation", color="03b2f8")
            embed.add_embed_field(name="Range Top", value=f"{zone_info['top']:.2f}")
            embed.add_embed_field(name="Range Bottom", value=f"{zone_info['bottom']:.2f}")
            embed.add_embed_field(name="Type", value="Sideways Accumulation")
            embed.set_timestamp()

            with open(screenshot_path, "rb") as f:
                webhook.add_file(file=f.read(), filename="chart.png")
            
            webhook.add_embed(embed)
            webhook.execute()
            print(">>> Discord Alert Sent Successfully")

        # Cleanup
        if os.path.exists(screenshot_path):
            os.remove(screenshot_path)

    except Exception as e:
        print(f"!!! Webhook/Screenshot Error: {e}")

def detect_accumulation(df, lookback=40, threshold_pct=0.001):
    try:
        if len(df) < lookback + 5:
            return None

        # Scan backwards
        for i in range(len(df) - lookback - 1, 0, -1):
            window = df.iloc[i : i + lookback]
            
            h_max = float(window['High'].max())
            l_min = float(window['Low'].min())
            avg_p = float(window['Close'].mean())
            
            # 1. RANGE: Is the vertical channel tight?
            range_pct = (h_max - l_min) / avg_p
            
            # 2. STABILITY: Price must stay close to the middle (low variance)
            std_dev = window['Close'].std()
            stability_score = std_dev / avg_p

            # 3. DRIFT: Start and end prices must be nearly identical (sideways)
            start_p = window['Close'].iloc[0]
            end_p = window['Close'].iloc[-1]
            drift = abs(start_p - end_p) / start_p

            # TIGHTENED LOGIC
            if range_pct <= threshold_pct and stability_score < (threshold_pct * 0.25) and drift < (threshold_pct * 0.3):
                
                breakout_idx = i + lookback
                for j in range(i + lookback, len(df)):
                    breakout_idx = j
                    current_c = df['Close'].iloc[j]
                    
                    if current_c > h_max or current_c < l_min:
                        break 
                
                return {
                    "start": int(df.index[i].timestamp()),
                    "end": int(df.index[breakout_idx].timestamp()),
                    "top": h_max,
                    "bottom": l_min,
                    "is_active": breakout_idx == (len(df) - 1)
                }
        return None
    except Exception as e:
        print(f"Logic Error: {e}")
        return None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data/DOW')
def get_dow_data():
    try:
        df = yf.download("YM=F", period="1d", interval="1m", progress=False)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.dropna()

        acc_zone = detect_accumulation(df)

        # Trigger Discord Alert in a separate thread if an active zone is found
        if acc_zone and acc_zone['is_active']:
            threading.Thread(target=capture_and_send_discord, args=(acc_zone,), daemon=True).start()

        candles = []
        for index, row in df.iterrows():
            candles.append({
                "time": int(index.timestamp()),
                "open": float(row['Open']),
                "high": float(row['High']),
                "low": float(row['Low']),
                "close": float(row['Close'])
            })

        return jsonify({
            "candles": candles,
            "live": live_tick,
            "accumulation": acc_zone
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
