# 1. Start the bot with PM2
# --name: The nickname for your process
# --interpreter: Tells PM2 to use the Python inside your virtual environment
pm2 start /root/openclaw/skills/lightweightchart-agent/app.py --name "lightweightchart-agent" --interpreter python3

# 3. Configure Startup
# This generates the command you need to run to enable PM2 on boot
pm2 startup | tail -n 1 | bash

# 4. Save the process list
# This ensures that 'trading-bot' restarts after a reboot
pm2 save
pm2 status
