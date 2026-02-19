import os
import time
import threading
import numpy as np
import pandas as pd
import yfinance as yf
from flask import Flask, render_template, jsonify

# Handling missing modules gracefully
try:
    from discord_webhook import DiscordWebhook, DiscordEmbed
except ImportError:
    DiscordWebhook = None

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None

app = Flask(__name__)

# Environment Variables
DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL')

# Global storage
live_tick = {"time": None, "open": None, "high": None, "low": None, "close": None}
# This is the key: it tracks the START time of the last box we sent to Discord
last_alerted_start = 0 

def capture_and_send_discord(zone_info):
    """Background task to take a screenshot and send to Discord."""
    global last_alerted_start
    
    # Double-check inside the thread to prevent race conditions
    if zone_info['start'] <= last_alerted_start:
        return
    
    last_alerted_start = zone_info['start']
    screenshot_path = f"alert_{int(time.time())}.png"

    print(f">>> Processing Discord Alert for box starting at {zone_info['start']}")

    try:
        if not DISCORD_WEBHOOK_URL:
            print("!!! Discord Webhook URL not set in environment.")
            return

        # 1. Capture Screenshot using Playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={'width': 1280, 'height': 720})
            
            # Point to your local Flask app
            page.goto("http://127.0.0.1:5000")
            
            # Wait for the chart/box to render
            page.wait_for_timeout(6000) 
            
            page.screenshot(path=screenshot_path)
            browser.close()

        # 2. Send to Discord
        duration_min = (zone_info['end'] - zone_info['start']) // 60
        webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, content=f"🚀 **US30 Accumulation Confirmed ({duration_min}m)**")
        
        embed = DiscordEmbed(title="Market Consolidation", color="03b2f8")
        embed.add_embed_field(name="Range Top", value=f"{zone_info['top']:.2f}")
        embed.add_embed_field(name="Range Bottom", value=f"{zone_info['bottom']:.2f}")
        embed.add_embed_field(name="Duration", value=f"{duration_min} Minutes")
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
            
            range_pct = (h_max - l_min) / avg_p
            std_dev = window['Close'].std()
            stability_score = std_dev / avg_p
            start_p = window['Close'].iloc[0]
            end_p = window['Close'].iloc[-1]
            drift = abs(start_p - end_p) / start_p

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
    global last_alerted_start
    try:
        df = yf.download("YM=F", period="1d", interval="1m", progress=False)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.dropna()

        acc_zone = detect_accumulation(df)

        # TRIGGER LOGIC: 
        # 1. We have an active zone
        # 2. The start time is greater than the last one we alerted
        if acc_zone and acc_zone['is_active']:
            if acc_zone['start'] > last_alerted_start:
                print(f"--- NEW ACCUMULATION DETECTED! Starting Discord Thread ---")
                # Update early to prevent multiple threads for the same box
                last_alerted_start = acc_zone['start'] 
                threading.Thread(target=capture_and_send_discord, args=(acc_zone,), daemon=True).start()

        candles = [{"time": int(idx.timestamp()), "open": float(r['Open']), "high": float(r['High']), "low": float(r['Low']), "close": float(r['Close'])} for idx, r in df.iterrows()]

        return jsonify({
            "candles": candles,
            "live": live_tick,
            "accumulation": acc_zone
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/test-screenshot')
def test_screenshot():
    test_zone = {
        "start": int(time.time()), # New start time ensures it bypasses cooldown
        "end": int(time.time()),
        "top": 39000.00,
        "bottom": 38950.00,
        "is_active": True
    }
    threading.Thread(target=capture_and_send_discord, args=(test_zone,), daemon=True).start()
    return "Test triggered! Check terminal and Discord."

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
