"""
rpi_client.py — Raspberry Pi Teensy Client
Listens for JSON data from the Teensy over HTTP, then:
  1. Saves a CSV locally on the RPi
  2. Forwards the CSV to the Cloud Server (Render) — PRIMARY
  3. Forwards the CSV to the Local Server (laptop) — SECONDARY / optional
  4. Uses a retry queue: if cloud upload fails (cold start), buffers up to
     MAX_RETRY_QUEUE packets and retries after RETRY_INTERVAL seconds
"""

import time
import requests
import os
import threading
import queue
from datetime import datetime
from flask import Flask, request, jsonify

# ============================================================
# CONFIGURATION — edit these before deploying to the RPi
# ============================================================

# PRIMARY: Render cloud server URL (no ngrok needed!)
CLOUD_SERVER_URL = "https://YOUR_APP_NAME.onrender.com/upload"

# SECONDARY: Local laptop server — set to None to disable
LOCAL_SERVER_URL = "http://192.168.1.XXX:5002/upload"  # ← set your laptop's LAN IP

# Where to save CSVs locally on the RPi
LOCAL_SAVE_DIR = "downloaded_csvs"

# The port this client listens on (for Teensy → RPi traffic)
LOCAL_PORT = 5000

# The IP of your Teensy on the local network.
# Set to None to accept from any IP (useful during testing).
TEENSY_IP = None  # e.g. "192.168.1.100"

# ---- Retry queue settings ----
MAX_RETRY_QUEUE   = 10    # max buffered packets when cloud is down
RETRY_INTERVAL    = 30    # seconds between retry attempts
UPLOAD_TIMEOUT    = 15    # seconds per upload request
# ============================================================

os.makedirs(LOCAL_SAVE_DIR, exist_ok=True)

app = Flask(__name__)

# ---- Retry queue (thread-safe) ----
retry_queue: queue.Queue = queue.Queue(maxsize=MAX_RETRY_QUEUE)
retry_lock = threading.Lock()


def _forward_file_to_server(url: str, filename: str, csv_content: str,
                             label: str = "") -> bool:
    """
    POST a CSV file to a server URL.
    Returns True on success, False on any failure.
    """
    try:
        files = {'file': (filename, csv_content.encode(), 'text/csv')}
        resp  = requests.post(url, files=files, timeout=UPLOAD_TIMEOUT)
        if resp.status_code == 200:
            print(f"[{label}] ✅ Forwarded {filename} → HTTP {resp.status_code}")
            return True
        else:
            print(f"[{label}] ⚠️  HTTP {resp.status_code} from {url}")
            return False
    except requests.exceptions.ConnectionError as e:
        print(f"[{label}] ❌ Connection error: {e}")
        return False
    except requests.exceptions.Timeout:
        print(f"[{label}] ❌ Timeout after {UPLOAD_TIMEOUT}s")
        return False
    except Exception as e:
        print(f"[{label}] ❌ Unexpected error: {e}")
        return False


def _retry_worker():
    """
    Background thread: tries to drain the retry queue every RETRY_INTERVAL seconds.
    Packets are retried against the cloud server only.
    """
    while True:
        time.sleep(RETRY_INTERVAL)
        if retry_queue.empty():
            continue

        drained = []
        while not retry_queue.empty():
            try:
                drained.append(retry_queue.get_nowait())
            except queue.Empty:
                break

        print(f"[RETRY] Attempting {len(drained)} queued packet(s)...")
        still_failed = []
        for filename, csv_content in drained:
            ok = _forward_file_to_server(CLOUD_SERVER_URL, filename,
                                         csv_content, label="RETRY→CLOUD")
            if not ok:
                still_failed.append((filename, csv_content))

        # Re-queue any that still failed (up to MAX_RETRY_QUEUE)
        for item in still_failed:
            try:
                retry_queue.put_nowait(item)
            except queue.Full:
                print(f"[RETRY] Queue full — dropping oldest packet.")


# Start the retry background thread
_retry_thread = threading.Thread(target=_retry_worker, daemon=True)
_retry_thread.start()


@app.route('/data', methods=['POST'])
def receive_data():
    try:
        # Validate sender IP if configured
        if TEENSY_IP and request.remote_addr != TEENSY_IP:
            print(f"[Security] Blocked {request.remote_addr} (expected {TEENSY_IP})")
            return jsonify({"status": "error", "message": f"Unauthorized IP: {request.remote_addr}"}), 403

        if not request.is_json:
            return jsonify({"status": "error", "message": "Expected JSON payload"}), 400

        data    = request.get_json()
        core_id = data.get("core", 0)
        samples = data.get("samples", 0)
        print(f"\n[Teensy Core {core_id}] Received payload (samples: {samples})")

        # 1. Generate timestamp & filename
        now_str  = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"Teensy_Core{core_id}_{now_str}.csv"

        # 2. Reconstruct Teensy-format CSV content
        record_start = data.get("record_start", 0)
        record_stop  = data.get("record_stop", 0)
        duration_s   = (record_stop - record_start) / 1000.0 \
                       if record_stop > record_start else 0.0

        freqs = data.get("freq", [])
        mags  = data.get("mag", [])
        fft_lines = [f"{i+1},{freqs[i]},{mags[i]}"
                     for i in range(min(len(freqs), len(mags)))]

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
            "Rank,Frequency (Hz),Magnitude",
        ] + fft_lines + ["================================================="]

        csv_content = "\n".join(csv_lines)

        # 3. Save locally on RPi
        local_path = os.path.join(LOCAL_SAVE_DIR, filename)
        with open(local_path, "w") as f:
            f.write(csv_content)
        print(f"[Local] Saved → {local_path}")

        # 4a. Forward to CLOUD server (primary — guaranteed delivery)
        cloud_ok = _forward_file_to_server(
            CLOUD_SERVER_URL, filename, csv_content, label="CLOUD"
        )
        if not cloud_ok:
            # Buffer for retry
            try:
                retry_queue.put_nowait((filename, csv_content))
                print(f"[CLOUD] Packet queued for retry ({retry_queue.qsize()}/{MAX_RETRY_QUEUE})")
            except queue.Full:
                print(f"[CLOUD] Retry queue full — packet dropped (cloud may be down too long).")

        # 4b. Forward to LOCAL server (secondary — best effort)
        if LOCAL_SERVER_URL:
            _forward_file_to_server(
                LOCAL_SERVER_URL, filename, csv_content, label="LOCAL"
            )
        else:
            print("[LOCAL] Local server URL not configured — skipping.")

        return jsonify({"status": "received", "filename": filename}), 200

    except Exception as e:
        print(f"[Error] {e}")
        import traceback; traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================
# LAYER 2 RECOVERY ENDPOINTS
# Called by the local server (laptop) when it wakes up after
# a long outage to pull files that Neon may have already trimmed.
# ============================================================

@app.route('/list-pending', methods=['GET'])
def list_pending():
    """
    Return a JSON list of all CSV files saved locally on the RPi.
    Each entry includes the filename and the epoch timestamp parsed from it.
    The local server uses this to identify files it doesn't yet have.
    """
    try:
        files = []
        if os.path.exists(LOCAL_SAVE_DIR):
            for fname in sorted(os.listdir(LOCAL_SAVE_DIR)):
                if not fname.endswith('.csv'):
                    continue
                # Parse epoch from filename: Teensy_Core1_YYYYMMDD_HHMMSS.csv
                epoch = 0.0
                try:
                    base  = os.path.splitext(fname)[0]
                    parts = base.split('_')
                    if len(parts) >= 2:
                        from datetime import datetime as _dt
                        dt = _dt.strptime(parts[-2] + parts[-1], '%Y%m%d%H%M%S')
                        epoch = dt.timestamp()
                except Exception:
                    pass
                files.append({'filename': fname, 'epoch': epoch})

        return jsonify({'status': 'ok', 'count': len(files), 'files': files}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/fetch-file/<filename>', methods=['GET'])
def fetch_file(filename):
    """
    Serve a specific CSV file from local RPi storage.
    The local server downloads this when recovering from a long outage.
    """
    try:
        # Security: only allow files in LOCAL_SAVE_DIR, no path traversal
        safe_name = os.path.basename(filename)
        fpath = os.path.join(LOCAL_SAVE_DIR, safe_name)
        if not os.path.exists(fpath):
            return jsonify({'status': 'error', 'message': 'File not found'}), 404
        with open(fpath, 'r') as f:
            content = f.read()
        return content, 200, {'Content-Type': 'text/csv'}
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/acknowledge', methods=['POST'])
def acknowledge():
    """
    Mark a list of filenames as acknowledged (already synced to local server).
    Optional: moves them to an 'acknowledged/' subfolder to keep LOCAL_SAVE_DIR clean.
    """
    try:
        data      = request.get_json(silent=True) or {}
        filenames = data.get('filenames', [])
        ack_dir   = os.path.join(LOCAL_SAVE_DIR, 'acknowledged')
        os.makedirs(ack_dir, exist_ok=True)
        moved = 0
        for fname in filenames:
            safe_name = os.path.basename(fname)
            src = os.path.join(LOCAL_SAVE_DIR, safe_name)
            dst = os.path.join(ack_dir, safe_name)
            if os.path.exists(src):
                os.rename(src, dst)
                moved += 1
        return jsonify({'status': 'ok', 'acknowledged': moved}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


if __name__ == "__main__":
    print("=" * 60)
    print("Raspberry Pi Teensy Client — Cloud-Resilient Mode")
    print(f"  Listening on : http://0.0.0.0:{LOCAL_PORT}")
    print(f"  Cloud server : {CLOUD_SERVER_URL}")
    print(f"  Local server : {LOCAL_SERVER_URL or 'disabled'}")
    print(f"  Local saves  : {LOCAL_SAVE_DIR}/")
    print(f"  Retry queue  : max {MAX_RETRY_QUEUE}, interval {RETRY_INTERVAL}s")
    print(f"  Recovery API : /list-pending  /fetch-file/<name>  /acknowledge")
    print("=" * 60)
    app.run(host="0.0.0.0", port=LOCAL_PORT, debug=False)
