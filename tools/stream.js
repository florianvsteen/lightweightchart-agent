// static/stream.js

class StreamManager {
    /**
     * @param {string} url - The SSE endpoint to connect to
     * @param {function} onMessageCallback - Function to run when data arrives
     */
    constructor(url, onMessageCallback) {
        this.url = url;
        this.onMessageCallback = onMessageCallback;
        this.evtSource = null;
        this.reconnectTimeout = null;
    }

    connect() {
        this.disconnect(); // Ensure we don't open duplicate connections
        
        console.log(`[Stream] Connecting to ${this.url}...`);
        this.evtSource = new EventSource(this.url);

        this.evtSource.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                this.onMessageCallback(data);
            } catch (err) {
                console.error("[Stream] Failed to parse JSON message:", err);
            }
        };

        this.evtSource.onerror = () => {
            console.warn(`[Stream] Connection lost or failed. Reconnecting in 5s...`);
            this.disconnect();
            this.reconnectTimeout = setTimeout(() => this.connect(), 5000);
        };
    }

    disconnect() {
        if (this.evtSource) {
            this.evtSource.close();
            this.evtSource = null;
        }
        if (this.reconnectTimeout) {
            clearTimeout(this.reconnectTimeout);
            this.reconnectTimeout = null;
        }
    }

    /**
     * Useful for when the user clicks a new timeframe (e.g., 1m -> 5m)
     */
    changeUrlAndReconnect(newUrl) {
        this.url = newUrl;
        this.connect();
    }
}
