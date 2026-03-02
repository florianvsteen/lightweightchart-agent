#!/usr/bin/env python3
"""
run_mission_control.py

Entry point for mission_control with proper eventlet monkey-patching.

CRITICAL: eventlet.monkey_patch() MUST be called before ANY other imports
to ensure all threading primitives (locks, threads, etc.) are properly
"greened" for async compatibility.

Usage:
  python run_mission_control.py
  # or with PM2:
  pm2 start run_mission_control.py --interpreter python3
"""

# Monkey-patch MUST be first - before any other imports
import eventlet
eventlet.monkey_patch()

# Now safe to import everything else
import os
from mission_control import app, socketio, loader


if __name__ == "__main__":
    port = int(os.environ.get("MISSION_CONTROL_PORT", 9000))

    # Start the background data loader
    loader.start()

    print(f"[MissionControl] Starting on http://0.0.0.0:{port}")
    print(f"[MissionControl] Dashboard:  http://localhost:{port}/dashboard")
    print(f"[MissionControl] Chart view: http://localhost:{port}/chart-view/<PAIR>")
    print(f"[MissionControl] Debug:      http://localhost:{port}/debug")
    print(f"[MissionControl] WebSocket:  ws://localhost:{port}/socket.io/")

    # Use SocketIO's run method instead of Flask's
    socketio.run(app, host="0.0.0.0", port=port, use_reloader=False)
