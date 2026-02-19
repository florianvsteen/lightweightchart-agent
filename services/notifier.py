# services/notifier.py
import os
import time
from playwright.sync_api import sync_playwright
from discord_webhook import DiscordWebhook, DiscordEmbed

class Notifier:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def send_alert(self, asset_name, zone_info, chart_url):
        screenshot_path = f"alert_{asset_name}_{int(time.time())}.png"
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page(viewport={'width': 1280, 'height': 720})
                page.goto(chart_url)
                page.wait_for_timeout(6000)
                page.screenshot(path=screenshot_path)
                browser.close()

            webhook = DiscordWebhook(url=self.webhook_url, content=f"🚀 **{asset_name} Accumulation Confirmed**")
            embed = DiscordEmbed(title=f"Market Consolidation: {asset_name}", color="03b2f8")
            embed.add_embed_field(name="Action", value="Check for Low Volume Pockets / CVDD")
            
            with open(screenshot_path, "rb") as f:
                webhook.add_file(file=f.read(), filename="chart.png")
            
            webhook.add_embed(embed)
            webhook.execute()
            
            if os.path.exists(screenshot_path):
                os.remove(screenshot_path)
        except Exception as e:
            print(f"Notification Error for {asset_name}: {e}")
