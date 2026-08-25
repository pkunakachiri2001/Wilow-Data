#!/bin/bash

echo "Stopping old Vibration Monitoring Server..."

# Stop previous server instances
pkill -f "server.py"

# Free the port (change 5002 if you use another port)
sudo fuser -k 5002/tcp 2>/dev/null

sleep 2

echo "Starting Vibration Monitoring Server on port 5002..."

# Activate virtual environment
source venv/bin/activate

# Start server using venv Python
nohup ./venv/bin/python server.py > server.log 2>&1 &

echo "Server started."
echo "Check logs with: tail -f server.log"