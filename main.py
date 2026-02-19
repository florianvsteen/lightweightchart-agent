# main.py
import time
import threading
import os
from core.config import ASSET_CONFIGS
from core.accumulation_detector import AccumulationDetector
from services.data_handler import fetch_market_data # Assume logic to fetch yf
from services.notifier import Notifier

DISCORD_URL = os.getenv("DISCORD_WEBHOOK_URL")
notifier = Notifier(DISCORD_URL)

def run_asset_loop(name, config):
    detector = AccumulationDetector(name, config)
    print(f"[*] Started monitoring {name}")
    
    while True:
        try:
            df = fetch_market_data(config['ticker'])
            zone = detector.check(df)
            
            if zone:
                print(f"[!] NEW ACCUMULATION: {name}")
                # Trigger notifier
                notifier.send_alert(name, zone, f"http://127.0.0.1:5000/chart/{name}")
                
        except Exception as e:
            print(f"Error in {name} loop: {e}")
        
        time.sleep(60) # Poll every minute

if __name__ == "__main__":
    # Start the Flask web server (from web/server.py) in a separate thread
    # Start a thread for each asset in ASSET_CONFIGS
    for asset, cfg in ASSET_CONFIGS.items():
        threading.Thread(target=run_asset_loop, args=(asset, cfg), daemon=True).start()
    
    while True: time.sleep(1)
