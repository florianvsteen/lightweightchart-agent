# 1. Start the bot with PM2
# --name: The nickname for your process
# --interpreter: Tells PM2 to use the Python inside your virtual environment
echo "[*] Starting the bot with PM2..."
pm2 start /root/openclaw/skills/lightweightchart-agent/app.py --name "lightweightchart-agent" --interpreter ./.venv/bin/python

# 3. Configure Startup
# This generates the command you need to run to enable PM2 on boot
echo "[*] Setting up PM2 startup script..."
pm2 startup | tail -n 1 | bash

# 4. Save the process list
# This ensures that 'trading-bot' restarts after a reboot
pm2 save

echo "--- PM2 Setup Complete ---"
pm2 status
