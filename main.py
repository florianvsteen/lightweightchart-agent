import time
import threading
import os
from core.config import ASSET_CONFIGS
from core.accumulation_detector import AccumulationDetector
from services.data_handler import fetch_market_data
from services.notifier import Notifier
from web.server import app  # Import the Flask instance from your web module

# Environment setup
DISCORD_URL = os.getenv("DISCORD_WEBHOOK_URL")
notifier = Notifier(DISCORD_URL)

def run_asset_loop(name, config):
    """
    Independent loop for each asset. 
    Maintains its own detector state (last_alerted_start).
    """
    detector = AccumulationDetector(name, config)
    print(f"[*] Started monitoring {name} ({config['ticker']})")
    
    while True:
        try:
            # 1. Fetch cleaned data via modular data_handler
            df = fetch_market_data(config['ticker'])
            
            if df.empty:
                print(f"[!] No data received for {name}")
            else:
                # 2. Run detection logic
                zone = detector.check(df)
                
                # 3. Trigger Alert if a new active zone is found
                if zone:
                    print(f"[!] NEW ACCUMULATION DETECTED: {name}")
                    # Pointing Playwright to the specific dynamic route
                    chart_url = f"http://127.0.0.1:5000/chart/{name}"
                    notifier.send_alert(name, zone, chart_url)
                
        except Exception as e:
            print(f"!!! Error in {name} loop: {e}")
        
        # Poll every minute (standard for 1m candle analysis)
        time.sleep(60)

if __name__ == "__main__":
    print("--- Starting OpenClaw Accumulation Skill ---")

    # 1. Start the Flask Web Server in a background thread
    # This allows Playwright to access the charts locally
    web_thread = threading.Thread(
        target=lambda: app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False),
        daemon=True
    )
    web_thread.start()
    print("[*] Web UI server started on port 5000")

    # Wait a moment for the server to initialize before starting loops
    time.sleep(2)

    # 2. Spawn monitoring threads for each pair defined in config.py
    for asset, cfg in ASSET_CONFIGS.items():
        monitor_thread = threading.Thread(
            target=run_asset_loop, 
            args=(asset, cfg), 
            daemon=True
        )
        monitor_thread.start()

    # 3. Keep the main process alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n--- Skill stopped by user ---")
