#!/bin/bash
# start_local_server.sh
# Starts the local vibration monitoring server with Neon DB sync.
# On startup, server.py will auto-sync any data missed while offline.

echo "======================================================"
echo "  Vibration Monitor — Local Server with Neon Sync"
echo "======================================================"

# Stop any previous instance
pkill -f "server.py" 2>/dev/null

# Free the port
sudo fuser -k 5002/tcp 2>/dev/null

sleep 1

# Activate virtual environment
source venv/bin/activate

# Load .env file if present (exports DATABASE_URL etc.)
if [ -f .env ]; then
    echo "[.env] Loading environment variables..."
    set -a
    source .env
    set +a
fi

echo "[INFO] Starting local server on port 5002..."
if [ -n "$DATABASE_URL" ]; then
    echo "[INFO] Neon DB: enabled (sync on startup)"
else
    echo "[INFO] Neon DB: disabled (DATABASE_URL not set — local-only mode)"
fi

nohup ./venv/bin/python server.py > server.log 2>&1 &

echo ""
echo "Server started (PID: $!)"
echo "Tail logs with: tail -f server.log"
echo "Dashboard:      http://localhost:5002"
