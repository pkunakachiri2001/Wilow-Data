#!/bin/bash
echo "Stopping all Vibration Monitoring services..."

echo ""
echo "1. Stopping ngrok..."
if pkill -f "ngrok" 2>/dev/null; then
    echo "   - ngrok stopped successfully."
else
    echo "   - ngrok was not running."
fi

echo ""
echo "2. Stopping Python server (server.py)..."
if pkill -f "server.py" 2>/dev/null; then
    echo "   - server.py stopped successfully."
else
    echo "   - server.py was not running."
fi

echo ""
echo "3. Stopping Python client (rpi_client.py)..."
if pkill -f "rpi_client.py" 2>/dev/null; then
    echo "   - rpi_client.py stopped successfully."
else
    echo "   - rpi_client.py was not running."
fi

echo ""
echo "All services have been stopped successfully!"
