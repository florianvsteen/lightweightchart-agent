"""
app.py — Main entry point.

Reads config.py and launches one Flask server per pair,
each on its own port, in a separate daemon thread.

Usage:
    python app.py                   # Start all pairs
    python app.py US30 XAUUSD      # Start specific pairs only
"""

# Eventlet monkey-patching MUST be first for WebSocket support
import eventlet
eventlet.monkey_patch()

import sys
import threading
from config import PAIRS
from server import PairServer
from mission_control import app as mission_app, socketio, loader


def launch_pair(pair_id: str, config: dict, stagger: int = 0):
    server = PairServer(pair_id, config)
    server._stagger_seconds = stagger
    server.run()


def main():
    # Optional: filter pairs from CLI args (e.g. `python app.py US30 XAUUSD`)
    requested = set(sys.argv[1:]) if len(sys.argv) > 1 else None

    pairs_to_run = {
        k: v for k, v in PAIRS.items()
        if requested is None or k in requested
    }

    if not pairs_to_run:
        print(f"No matching pairs found. Available: {list(PAIRS.keys())}")
        sys.exit(1)

    print("=" * 50)
    print("Lightweight Chart Agent — Starting")
    print("=" * 50)
    for pair_id, cfg in pairs_to_run.items():
        print(f"  {pair_id:10s} → http://localhost:{cfg['port']}   detectors: {cfg['detectors']}")
    print("=" * 50)

    threads = []
    for i, (pair_id, cfg) in enumerate(pairs_to_run.items()):
        stagger = i * 10  # stagger each pair by 10s to avoid yfinance collisions
        t = threading.Thread(
            target=launch_pair,
            args=(pair_id, cfg, stagger),
            daemon=True,
            name=f"server-{pair_id}",
        )
        t.start()
        threads.append(t)

    # Start mission control dashboard with WebSocket support
    def start_mission_control():
        loader.start()  # Start the centralized data loader
        socketio.run(mission_app, host="0.0.0.0", port=6767, use_reloader=False)

    mc_thread = threading.Thread(
        target=start_mission_control,
        daemon=True,
        name="mission-control",
    )
    mc_thread.start()
    print("  Mission Control → http://localhost:6767 (WebSocket enabled)")
    print("=" * 50)

    # Keep main thread alive
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print("\nShutting down.")


if __name__ == "__main__":
    main()
