import time
import threading
import os
from core.config import ASSET_CONFIGS
from core.accumulation_detector import AccumulationDetector
from services.data_handler import fetch_market_data
from services.notifier import Notifier
from web.server import app  # Assuming your Flask app is defined here

DISCORD_URL = os.getenv("DISCORD_WEBHOOK_URL")
notifier = Notifier(DISCORD_URL)

def run_asset_server(port):
    """Starts a Flask instance on a specific port."""
    # We use a lambda or separate function to avoid port conflicts in the same thread
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

def run_asset_loop(name, config):
    """Independent logic loop for each asset."""
    detector = AccumulationDetector(name, config)
    asset_port = config['port']  # Fetch port from config
    
    print(f"[*] Monitoring {name} on port {asset_port}")
    
    while True:
        try:
            df = fetch_market_data(config['ticker'])
            if not df.empty:
                zone = detector.check(df)
                
                if zone:
                    # Pointing Playwright to the unique port for this specific asset
                    chart_url = f"http://127.0.0.1:{asset_port}/chart/{name}"
                    notifier.send_alert(name, zone, chart_url)
                    
        except Exception as e:
            print(f"!!! Error in {name} loop (Port {asset_port}): {e}")
        
        time.sleep(60)

if __name__ == "__main__":
    print("--- Starting Multi-Port OpenClaw Skill ---")

    for name, cfg in ASSET_CONFIGS.items():
        current_port = cfg['port']

        # 1. Start a dedicated Web Server thread for this asset's port
        server_thread = threading.Thread(
            target=run_asset_server, 
            args=(current_port,), 
            daemon=True
        )
        server_thread.start()

        # 2. Start the monitoring logic thread
        monitor_thread = threading.Thread(
            target=run_asset_loop, 
            args=(name, cfg), 
            daemon=True
        )
        monitor_thread.start()
        
        print(f"[+] {name} initialized on Port {current_port}")

    # Keep main thread alive
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("\n--- Skill stopped ---")
