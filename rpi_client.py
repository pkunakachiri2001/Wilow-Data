import time
import requests
import os
from datetime import datetime
from flask import Flask, request, jsonify

# ============================================================
# CONFIGURATION
# ============================================================
# The URL of your remote server.
# Make sure to keep the "/upload" path at the end!
REMOTE_SERVER_URL = "https://YOUR_NGROK_ID.ngrok-free.app/upload"  

# Where to save CSVs locally on the Raspberry Pi
LOCAL_SAVE_DIR = "downloaded_csvs"

# The port the RPi client will listen on for incoming data from the Teensy
LOCAL_PORT = 5000

# The IP address of your Teensy on the local network.
# Only requests from this IP will be accepted.
# Set this to None (e.g., TEENSY_IP = None) to allow requests from any IP (e.g., during testing).
TEENSY_IP = "192.168.1.100"

# ============================================================

if not os.path.exists(LOCAL_SAVE_DIR):
    os.makedirs(LOCAL_SAVE_DIR)

app = Flask(__name__)

@app.route('/data', methods=['POST'])
def receive_data():
    try:
        # Validate sender IP if configured
        if TEENSY_IP and request.remote_addr != TEENSY_IP:
            print(f"[Security] Blocked request from unauthorized IP: {request.remote_addr} (Expected: {TEENSY_IP})")
            return jsonify({"status": "error", "message": f"Unauthorized sender IP: {request.remote_addr}"}), 403

        if not request.is_json:
            return jsonify({"status": "error", "message": "Expected JSON payload"}), 400
            
        data = request.get_json()
        core_id = data.get("core", 0)
        samples = data.get("samples", 0)
        print(f"\n[Teensy Core {core_id}] Received data payload (Samples: {samples})")
        
        # 1. Generate timestamp and filename
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        timestamp_filename = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"Teensy_Core{core_id}_{timestamp_filename}.csv"
        
        # 2. Map data to the summary format expected by the dashboard server
        freq_z = 0.0
        if data.get("freq") and len(data["freq"]) > 0:
            freq_z = float(data["freq"][0])
            
        summary_data = {
            "filename": filename,
            "timestamp": timestamp,
            "max_x": float(data.get("max_x", 0.0)),
            "min_x": float(data.get("min_x", 0.0)),
            "avg_x": float(data.get("mean_x", 0.0)),
            "freq_x": 0.0,
            "max_y": 0.0,
            "min_y": 0.0,
            "avg_y": 0.0,
            "freq_y": 0.0,
            "max_z": float(data.get("max", 0.0)),
            "min_z": float(data.get("min", 0.0)),
            "avg_z": float(data.get("mean", 0.0)),
            "freq_z": freq_z,
            "samples": int(samples)
        }
        
        # 3. Reconstruct raw-like CSV content matching the expected parameter format
        record_start = data.get("record_start", 0)
        record_stop = data.get("record_stop", 0)
        duration_s = (record_stop - record_start) / 1000.0 if record_stop > record_start else 0.0
        
        csv_lines = [
            "=== METADATA ===",
            "Parameter,Value,Unit",
            f"Device ID,Teensy_Core{core_id},-",
            "File number,0,-",
            f"File name,{filename},-",
            "",
            "=== RECORDING INFO ===",
            "Parameter,Value,Unit",
            f"Record start,{record_start},ms from boot",
            f"Record stop,{record_stop},ms from boot",
            f"Duration,{duration_s:.3f},s",
            f"Total samples,{samples},samples",
            f"Sample rate,{data.get('sample_rate', 0.0)},Hz",
            "",
            "=== Z-AXIS STATISTICS ===",
            "Parameter,Value,Unit",
            f"Maximum Az,{data.get('max', 0.0)},m/s^2",
            f"Minimum Az,{data.get('min', 0.0)},m/s^2",
            f"Mean Az,{data.get('mean', 0.0)},m/s^2",
            f"Std Dev Az,{data.get('std_dev', 0.0)},m/s^2",
            f"Skewness Az,{data.get('skewness', 0.0)},-",
            f"Excess Kurtosis Az,{data.get('kurtosis', 0.0)},-",
            "",
            "=== X-AXIS STATISTICS ===",
            "Parameter,Value,Unit",
            f"Maximum Ax,{data.get('max_x', 0.0)},m/s^2",
            f"Minimum Ax,{data.get('min_x', 0.0)},m/s^2",
            f"Mean Ax,{data.get('mean_x', 0.0)},m/s^2",
            "",
            "=== FFT PEAKS (Z-AXIS) ===",
            "Rank,Frequency (Hz),Magnitude"
        ]
        
        freqs = data.get("freq", [])
        mags = data.get("mag", [])
        for i in range(min(len(freqs), len(mags))):
            csv_lines.append(f"{i+1},{freqs[i]},{mags[i]}")
            
        csv_lines.append("=================================================")
        csv_content = "\n".join(csv_lines)
        
        # Save locally
        local_path = os.path.join(LOCAL_SAVE_DIR, filename)
        with open(local_path, "w") as f:
            f.write(csv_content)
        print(f"[Local] Saved to {local_path}")
        
        # 4. Forward stats to dashboard server
        print(f"[Remote] Sending stats to dashboard...")
        try:
            json_response = requests.post(REMOTE_SERVER_URL, json=summary_data, timeout=10)
            if json_response.status_code == 200:
                print(f"[Remote] Dashboard updated successfully!")
            else:
                print(f"[Remote] ERROR: Failed to update dashboard. HTTP {json_response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"[Remote] ERROR: Could not connect to dashboard server for stats: {e}")
            
        # 5. Forward raw CSV file to dashboard server
        print(f"[Remote] Forwarding raw file to server...")
        try:
            files = {'file': (filename, csv_content, 'text/csv')}
            file_response = requests.post(REMOTE_SERVER_URL, files=files, timeout=10)
            if file_response.status_code == 200:
                print(f"[Remote] File forwarded successfully!")
            else:
                print(f"[Remote] ERROR: Failed to forward file. HTTP {file_response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"[Remote] ERROR: Could not connect to dashboard server for file: {e}")
            
        return jsonify({"status": "received", "filename": filename}), 200
        
    except Exception as e:
        print(f"[Error] Error processing Teensy data: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    print("="*60)
    print("Starting Raspberry Pi Teensy Client HTTP Listener...")
    print(f"Listening on: http://0.0.0.0:{LOCAL_PORT}")
    print(f"Forwarding to Remote Server: {REMOTE_SERVER_URL}")
    print(f"Local storage: {LOCAL_SAVE_DIR}/")
    print("="*60)
    
    try:
        app.run(host="0.0.0.0", port=LOCAL_PORT, debug=False)
    except KeyboardInterrupt:
        print("\nStopped by user.")
