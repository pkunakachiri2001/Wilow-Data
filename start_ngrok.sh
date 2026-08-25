#!/bin/bash
echo "Starting ngrok HTTP Tunnel on Port 5002..."

# Check if ngrok is installed
if ! command -v ngrok &> /dev/null; then
    echo "ERROR: ngrok not found. Install it with:"
    echo "  curl -sSL https://ngrok-agent.s3.amazonaws.com/ngrok.asc | sudo tee /etc/apt/trusted.gpg.d/ngrok.asc >/dev/null"
    echo "  echo \"deb https://ngrok-agent.s3.amazonaws.com buster main\" | sudo tee /etc/apt/sources.list.d/ngrok.list"
    echo "  sudo apt update && sudo apt install ngrok"
    exit 1
fi

# Try to open in a new terminal window (handles GNOME, KDE, XFCE, etc.)
if command -v gnome-terminal &> /dev/null; then
    gnome-terminal -- bash -c "ngrok http 5002; exec bash"
elif command -v xterm &> /dev/null; then
    xterm -T "ngrok Tunnel" -e "ngrok http 5002; bash" &
elif command -v konsole &> /dev/null; then
    konsole --new-tab -e bash -c "ngrok http 5002; exec bash" &
elif command -v xfce4-terminal &> /dev/null; then
    xfce4-terminal --title="ngrok Tunnel" -e "bash -c 'ngrok http 5002; bash'" &
else
    # Fallback: run in background and log output
    echo "No graphical terminal found. Running ngrok in background..."
    nohup ngrok http 5002 > ngrok.log 2>&1 &
    echo "ngrok started in background. Check ngrok.log for the tunnel URL."
fi

echo "Done."
