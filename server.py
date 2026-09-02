"""
Flask Server for Vibration Monitoring
- Parses Teensy CSV file format directly
- Stores only fields actually present in Teensy output
- Date-wise data organisation
- Syncs with Neon PostgreSQL DB on startup and every upload
"""

from flask import Flask, request, jsonify, render_template, send_from_directory
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import os
import pandas as pd
import numpy as np
from datetime import datetime
import json
import csv
from threading import Lock
import threading
import time
# Load .env if present (for DATABASE_URL etc.)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---- Neon DB (optional — graceful fallback if not configured) ----
DB_ENABLED = False
try:
    import db as _db
    if os.environ.get('DATABASE_URL'):
        _db.init_db()
        DB_ENABLED = True
        print("[LOCAL] Neon DB enabled.")
    else:
        print("[LOCAL] DATABASE_URL not set — running in local-only mode.")
except Exception as _db_init_err:
    print(f"[LOCAL WARNING] Neon DB init failed: {_db_init_err} — local-only mode.")

# ================= CONFIGURATION =================
UPLOAD_DIR = "uploads"
MASTER_CSV = "master_data.csv"
DATA_BY_DATE_DIR = "data_by_date"
STATIC_DIR = "static"
TEMPLATE_DIR = "templates"

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)
os.makedirs(TEMPLATE_DIR, exist_ok=True)
os.makedirs(DATA_BY_DATE_DIR, exist_ok=True)

# ================= THRESHOLDS =================
THRESHOLDS = {
    'z_axis': {
        'acceleration': {
            'over_positive': 45,
            'under_positive': 6.5,
            'under_negative': -6.5,
            'over_negative': -20
        }
    }
}

# ================= CSV SCHEMA =================
CSV_HEADERS = [
    'Date', 'Time_sec',
    'Max_Az', 'Min_Az', 'Mean_Az', 'Std_Az', 'Skewness_Az', 'Kurtosis_Az',
    'Max_Ax', 'Min_Ax', 'Mean_Ax',
    'FFT1_Freq', 'FFT1_Mag',
    'FFT2_Freq', 'FFT2_Mag',
    'FFT3_Freq', 'FFT3_Mag',
    'FFT4_Freq', 'FFT4_Mag',
    'FFT5_Freq', 'FFT5_Mag'
]

# ================= FLASK SETUP =================
app = Flask(__name__, static_folder=STATIC_DIR, template_folder=TEMPLATE_DIR)
app.config['SECRET_KEY'] = 'vibration-monitoring-updated'
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

csv_lock = Lock()
available_dates_cache = set()
alert_lock = Lock()

# Latest status
latest_status = {
    'timestamp': None,
    'status': 'Waiting for data...',
    'alert': None,
    'alert_time': None,
    'magnitude': 0,
    'frequency': 0
}
status_lock = Lock()

# ================= DATE FUNCTIONS =================
def get_date_csv(date_str):
    """Get CSV file path for a specific date"""
    date_folder = os.path.join(DATA_BY_DATE_DIR, date_str)
    os.makedirs(date_folder, exist_ok=True)
    available_dates_cache.add(date_str)
    return os.path.join(date_folder, f"data_{date_str}.csv")

def init_date_csv(csv_path):
    """Initialise date CSV with headers if it does not exist"""
    if not os.path.exists(csv_path):
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADERS)

def scan_existing_dates():
    """Scan for existing date folders"""
    try:
        if os.path.exists(DATA_BY_DATE_DIR):
            for date_folder in os.listdir(DATA_BY_DATE_DIR):
                if os.path.isdir(os.path.join(DATA_BY_DATE_DIR, date_folder)):
                    available_dates_cache.add(date_folder)
        print(f"[DATES] Found {len(available_dates_cache)} existing dates")
    except Exception as e:
        print(f"[DATES] Scan error: {e}")

# ================= CSV INITIALISATION =================
def initialize_master_csv():
    """Create master CSV with correct headers if missing or stale"""
    write_headers = False
    if not os.path.exists(MASTER_CSV):
        write_headers = True
    else:
        try:
            with open(MASTER_CSV, 'r') as f:
                first_line = f.readline().strip()
            if first_line != ','.join(CSV_HEADERS):
                write_headers = True
                print("[INIT] Header mismatch — re-initialising master CSV")
        except Exception:
            write_headers = True

    if write_headers:
        try:
            with open(MASTER_CSV, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(CSV_HEADERS)
            print(f"[INIT] {MASTER_CSV} initialised with new schema")
        except PermissionError:
            print(
                f"[INIT WARNING] Cannot write to {MASTER_CSV} — file is open in another program.\n"
                f"  → Close master_data.csv in your editor/Excel, then restart the server.\n"
                f"  → Server will continue but the first upload may append to the old schema."
            )

initialize_master_csv()
scan_existing_dates()

# ================= NEON STARTUP SYNC =================
RPI_CLIENT_URL = os.environ.get('RPI_CLIENT_URL', '')  # e.g. http://192.168.1.50:5000


def _pull_from_rpi(last_local_epoch: float):
    """
    Layer 2 recovery: pull missed CSV files directly from the RPi client's
    /list-pending and /fetch-file endpoints.
    Only used when the laptop has been off so long that Neon overflowed and
    trimmed data that wasn't yet synced locally.
    Requires RPI_CLIENT_URL env var to be set.
    """
    import requests as _req
    if not RPI_CLIENT_URL:
        return 0
    try:
        print(f"[SYNC-RPi] Fetching pending file list from RPi...")
        resp = _req.get(f"{RPI_CLIENT_URL}/list-pending", timeout=10)
        if resp.status_code != 200:
            print(f"[SYNC-RPi] RPi returned HTTP {resp.status_code} — skipping.")
            return 0

        files = resp.json().get('files', [])
        # Only pull files newer than what we have locally
        new_files = [f for f in files if f.get('epoch', 0) > last_local_epoch]
        if not new_files:
            print("[SYNC-RPi] No new files from RPi.")
            return 0

        print(f"[SYNC-RPi] Pulling {len(new_files)} files directly from RPi...")
        pulled = 0
        for file_info in new_files:
            fname = file_info['filename']
            try:
                fr = _req.get(f"{RPI_CLIENT_URL}/fetch-file/{fname}", timeout=10)
                if fr.status_code == 200:
                    # Save to uploads/ and parse like a normal upload
                    fpath = os.path.join(UPLOAD_DIR, fname)
                    with open(fpath, 'w') as f:
                        f.write(fr.text)
                    parsed              = parse_teensy_file(fpath)
                    date_str, epoch_t   = epoch_from_filename(fname)
                    if epoch_t > last_local_epoch:
                        store_teensy_data(fname, parsed, date_str, epoch_t)
                        pulled += 1
            except Exception as fe:
                print(f"[SYNC-RPi] Failed to fetch {fname}: {fe}")

        print(f"[SYNC-RPi] ✅ Pulled {pulled} files directly from RPi.")
        return pulled
    except Exception as e:
        print(f"[SYNC-RPi] Error: {e} — skipping RPi direct pull.")
        return 0


def sync_from_neon():
    """
    On startup, fill local CSVs from two sources:
      Layer 1 — Neon DB (rows since last local epoch)
      Layer 2 — RPi direct pull (for extreme gaps when Neon overflowed)
    After writing rows locally, DELETES them from Neon (acknowledged transit buffer).
    """
    if not DB_ENABLED:
        return
    try:
        # Find the last epoch we have locally
        last_local_epoch = 0.0
        if os.path.exists(MASTER_CSV):
            df = pd.read_csv(MASTER_CSV)
            if not df.empty and 'Time_sec' in df.columns:
                ts_col = pd.to_numeric(df['Time_sec'], errors='coerce')
                last_local_epoch = float(ts_col.max()) if not ts_col.isna().all() else 0.0

        print(f"[SYNC] Last local epoch: {last_local_epoch} — fetching newer rows from Neon...")
        rows = _db.fetch_rows_since(last_local_epoch)

        if rows:
            print(f"[SYNC] Writing {len(rows)} missed rows to local CSV files...")
            max_synced_epoch = last_local_epoch
            with csv_lock:
                for r in rows:
                    # r = (date, time_sec, max_az, min_az, mean_az, std_az, skewness_az,
                    #       kurtosis_az, max_ax, min_ax, mean_ax,
                    #       fft1_freq, fft1_mag, fft2_freq, fft2_mag,
                    #       fft3_freq, fft3_mag, fft4_freq, fft4_mag, fft5_freq, fft5_mag)
                    row_list = list(r)
                    date_str = row_list[0]
                    epoch    = float(row_list[1])

                    # Master CSV
                    with open(MASTER_CSV, 'a', newline='') as f:
                        csv.writer(f).writerow(row_list)

                    # Date-specific CSV
                    if date_str:
                        date_csv = get_date_csv(date_str)
                        init_date_csv(date_csv)
                        with open(date_csv, 'a', newline='') as f:
                            csv.writer(f).writerow(row_list)

                    if epoch > max_synced_epoch:
                        max_synced_epoch = epoch

            print(f"[SYNC] ✅ Synced {len(rows)} rows from Neon.")

            # ---- Acknowledged delete: purge synced rows from Neon ----
            try:
                deleted = _db.delete_rows_before_epoch(max_synced_epoch)
                print(f"[SYNC] 🗑  Deleted {deleted} acknowledged rows from Neon (transit buffer cleaned).")
            except Exception as del_err:
                print(f"[SYNC] Delete warning (non-fatal): {del_err}")
                
            # Tell the local dashboard to refresh its charts
            socketio.emit('data_update', {'type': 'sync', 'count': len(rows)})
        else:
            # print("[SYNC] Neon: already up to date — no new rows.") # Muted to prevent spam
            pass

        # ---- Layer 2: RPi direct pull (catches anything Neon may have trimmed) ----
        _pull_from_rpi(last_local_epoch)

    except Exception as e:
        print(f"[SYNC ERROR] Could not sync from Neon: {e} — continuing in local-only mode.")

def sync_loop():
    """Background thread to poll Neon DB for new data every 10 seconds"""
    print("[SYNC] Background sync thread started.")
    while True:
        try:
            sync_from_neon()
        except Exception as e:
            print(f"[SYNC LOOP ERROR] {e}")
        time.sleep(10)

# Run initial sync and start background polling
synced = sync_from_neon()
threading.Thread(target=sync_loop, daemon=True).start()

# ================= THRESHOLD CHECK =================
def check_threshold_violation(value, thresholds):
    """Return 'OVER' / 'UNDER' / None for a scalar value against threshold dict"""
    if value > thresholds['over_positive']:
        return 'OVER'
    elif 0 < value < thresholds['under_positive']:
        return 'UNDER'
    elif value < thresholds['over_negative']:
        return 'OVER'
    elif thresholds['under_negative'] < value < 0:
        return 'UNDER'
    return None

# ================= TEENSY FILE PARSING =================
def parse_teensy_file(filepath):
    """
    Parse a Teensy CSV statistics file and return a structured dict.
    Sections detected: Z-AXIS STATISTICS, X-AXIS STATISTICS, FFT PEAKS.
    """
    data = {
        'max_az': None, 'min_az': None, 'mean_az': None,
        'std_az': None,  'skewness_az': None, 'kurtosis_az': None,
        'max_ax': None, 'min_ax': None, 'mean_ax': None,
        'fft_peaks': []   # list of (freq, mag) up to 5
    }

    try:
        with open(filepath, 'r') as f:
            lines = f.readlines()

        current_section = None

        for line in lines:
            line = line.strip()

            # Section detection
            if '=== Z-AXIS STATISTICS' in line:
                current_section = 'z_stat'
                continue
            elif '=== X-AXIS STATISTICS' in line:
                current_section = 'x_stat'
                continue
            elif '=== FFT PEAKS' in line:
                current_section = 'fft'
                continue
            elif line.startswith('===') or line.startswith('Parameter') \
                    or line.startswith('Rank') or not line:
                continue

            parts = [p.strip() for p in line.split(',')]

            if current_section == 'z_stat' and len(parts) >= 2:
                param, val_str = parts[0], parts[1]
                try:
                    val = float(val_str)
                except ValueError:
                    continue
                mapping = {
                    'Maximum Az':       'max_az',
                    'Minimum Az':       'min_az',
                    'Mean Az':          'mean_az',
                    'Std Dev Az':       'std_az',
                    'Skewness Az':      'skewness_az',
                    'Excess Kurtosis Az': 'kurtosis_az',
                }
                if param in mapping:
                    data[mapping[param]] = val

            elif current_section == 'x_stat' and len(parts) >= 2:
                param, val_str = parts[0], parts[1]
                try:
                    val = float(val_str)
                except ValueError:
                    continue
                mapping = {
                    'Maximum Ax': 'max_ax',
                    'Minimum Ax': 'min_ax',
                    'Mean Ax':    'mean_ax',
                }
                if param in mapping:
                    data[mapping[param]] = val

            elif current_section == 'fft' and len(parts) >= 3 and len(data['fft_peaks']) < 5:
                try:
                    freq = float(parts[1])
                    mag  = float(parts[2])
                    data['fft_peaks'].append((freq, mag))
                except ValueError:
                    continue

    except Exception as e:
        print(f"[PARSE ERROR] {filepath}: {e}")

    return data


def epoch_from_filename(filename):
    """
    Extract epoch seconds and date string from the filename.
    Expected format: Teensy_Core1_YYYYMMDD_HHMMSS.csv
    e.g. 'Teensy_Core1_20260715_032114.csv'
    Falls back to current time if parsing fails.
    """
    try:
        base = os.path.splitext(os.path.basename(filename))[0]
        parts = base.split('_')
        if len(parts) >= 2:
            date_str = parts[-2]
            time_str = parts[-1]
            stamp = date_str + time_str
            dt = datetime.strptime(stamp, '%Y%m%d%H%M%S')
            return dt.strftime('%Y-%m-%d'), dt.timestamp()
        else:
            raise ValueError("Filename format not recognized")
    except Exception as e:
        print(f"[PARSE WARNING] Could not parse date from {filename}, using current time. Error: {e}")
        now = datetime.now()
        return now.strftime('%Y-%m-%d'), now.timestamp()


def store_teensy_data(filename, parsed, date_str, epoch_time):
    """Write one row to master CSV and the date-specific CSV. Returns alert status."""

    # Flatten FFT peaks to [freq1, mag1, freq2, mag2, …] padding to 5
    fft_flat = []
    for i in range(5):
        if i < len(parsed['fft_peaks']):
            fft_flat.extend([parsed['fft_peaks'][i][0], parsed['fft_peaks'][i][1]])
        else:
            fft_flat.extend(['', ''])

    row = [
        date_str, epoch_time,
        parsed['max_az'],      parsed['min_az'],      parsed['mean_az'],
        parsed['std_az'],      parsed['skewness_az'], parsed['kurtosis_az'],
        parsed['max_ax'],      parsed['min_ax'],      parsed['mean_ax'],
    ] + fft_flat

    # Threshold check on Max_Az
    alert_status = None
    max_az = parsed['max_az']
    if max_az is not None:
        violation = check_threshold_violation(max_az, THRESHOLDS['z_axis']['acceleration'])
        if violation:
            alert_status = violation
            print(f"[ALERT] {violation} | Max_Az={max_az:.4f} m/s²")
        else:
            print(f"[OK] Max_Az={max_az:.4f} m/s²")

    with csv_lock:
        # Master CSV
        with open(MASTER_CSV, 'a', newline='') as f:
            csv.writer(f).writerow(row)

        # Date-specific CSV
        if date_str:
            date_csv = get_date_csv(date_str)
            init_date_csv(date_csv)
            with open(date_csv, 'a', newline='') as f:
                csv.writer(f).writerow(row)

    # ---- Write to Neon DB (non-blocking, graceful fallback) ----
    if DB_ENABLED:
        try:
            row_dict = _db.parsed_to_row_dict(parsed, date_str, epoch_time)
            _db.insert_row(row_dict)
        except Exception as _db_err:
            print(f"[LOCAL DB WARN] insert_row failed: {_db_err} — data saved locally only.")

    print(f"[STORED] {filename} → {date_str}")
    return alert_status

# ================= DATA QUERY =================
def get_axis_data(axis='Z', last_n_minutes=None, start_time=None, end_time=None):
    """
    Return chart-ready data for the requested axis from master CSV.
    axis: 'Z' → Max_Az/Min_Az/Mean_Az   'X' → Max_Ax/Min_Ax/Mean_Ax
    Frequency is taken from FFT1_Freq (dominant peak).
    """
    try:
        df = pd.read_csv(MASTER_CSV)
        if df.empty:
            return None

        # Ensure Time_sec is numeric epoch
        df['Time_sec'] = pd.to_numeric(df['Time_sec'], errors='coerce')
        df = df.dropna(subset=['Time_sec']).sort_values('Time_sec')

        if start_time and end_time:
            df = df[(df['Time_sec'] >= float(start_time)) & (df['Time_sec'] <= float(end_time))]
        elif last_n_minutes is not None and len(df) > 0:
            cutoff = df['Time_sec'].max() - (last_n_minutes * 60)
            df = df[df['Time_sec'] >= cutoff]

        if df.empty:
            return None

        if axis == 'Z':
            max_col, min_col, avg_col = 'Max_Az', 'Min_Az', 'Mean_Az'
        else:  # X
            max_col, min_col, avg_col = 'Max_Ax', 'Min_Ax', 'Mean_Ax'

        max_values = pd.to_numeric(df[max_col], errors='coerce')
        min_values = pd.to_numeric(df[min_col], errors='coerce')
        avg_values = pd.to_numeric(df[avg_col], errors='coerce') \
                     if avg_col in df.columns \
                     else (max_values + min_values) / 2

        frequencies = pd.to_numeric(df.get('FFT1_Freq', pd.Series(dtype=float)), errors='coerce')
        timestamps  = df['Time_sec']
        dates       = df['Date']

        valid = max_values.notna() & min_values.notna()
        max_values = max_values[valid]
        min_values = min_values[valid]
        avg_values = avg_values[valid]
        frequencies = frequencies[valid]
        timestamps  = timestamps[valid]
        dates       = dates[valid]

        if len(max_values) == 0:
            return None

        datetime_labels = [
            datetime.fromtimestamp(ts).astimezone().isoformat() for ts in timestamps
        ]

        avg_freq = frequencies.mean() if not frequencies.isna().all() else None

        return {
            'timestamps':      timestamps.tolist(),
            'datetime_labels': datetime_labels,
            'max':             max_values.tolist(),
            'min':             min_values.tolist(),
            'avg':             avg_values.tolist(),
            'frequency':       float(avg_freq) if avg_freq is not None else None,
            'axis':            axis,
            'metric':          'acceleration',
            'alerts':          {'datetimes': [], 'values': []}
        }

    except Exception as e:
        print(f"[ERROR] get_axis_data: {e}")
        import traceback; traceback.print_exc()
        return None

# ================= ROUTES =================
@app.route("/")
def home():
    return render_template("dashboard.html", is_cloud=False)

@app.route("/api/available-dates", methods=['GET'])
def get_available_dates():
    try:
        dates = sorted(list(available_dates_cache), reverse=True)
        return jsonify({'success': True, 'dates': dates, 'latest': dates[0] if dates else None})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route("/upload", methods=["POST"])
def upload():
    try:
        if "file" not in request.files:
            return jsonify({"error": "no file"}), 400

        file = request.files["file"]
        filepath = os.path.join(UPLOAD_DIR, file.filename)
        file.save(filepath)

        # Parse the Teensy statistics file
        parsed   = parse_teensy_file(filepath)
        date_str, epoch_time = epoch_from_filename(file.filename)

        alert_status = store_teensy_data(file.filename, parsed, date_str, epoch_time)

        # Update latest status
        top_freq = parsed['fft_peaks'][0][0] if parsed['fft_peaks'] else 0
        with status_lock:
            latest_status['timestamp'] = datetime.fromtimestamp(epoch_time).strftime('%Y-%m-%d %H:%M:%S')
            latest_status['magnitude'] = parsed['max_az'] or 0
            latest_status['frequency'] = top_freq

            if alert_status:
                latest_status['status']     = f'{alert_status} Threshold'
                latest_status['alert']      = True
                latest_status['alert_type'] = f'Z-Axis {alert_status} Threshold'
                latest_status['alert_time'] = latest_status['timestamp']

                socketio.emit('alert_notification', {
                    'alert_type': f'Z-Axis {alert_status} Threshold',
                    'timestamp':  latest_status['timestamp'],
                    'magnitude':  parsed['max_az'],
                    'axis':       'Z'
                })
            else:
                latest_status['status']     = 'Normal'
                latest_status['alert']      = False
                latest_status['alert_type'] = None

        socketio.emit('data_update', {'type': 'file', 'filename': file.filename})
        return jsonify({"status": "received", "date": date_str, "alert": alert_status})

    except Exception as e:
        print(f"[ERROR] Upload: {e}")
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route("/api/axis_data")
def api_axis_data():
    axis        = request.args.get('axis', 'Z')
    minutes     = request.args.get('minutes')
    start_time  = request.args.get('start_time')
    end_time    = request.args.get('end_time')

    if start_time and end_time:
        data = get_axis_data(axis, start_time=start_time, end_time=end_time)
    else:
        minutes = int(minutes) if minutes else 10
        data = get_axis_data(axis, last_n_minutes=minutes)

    if data is None:
        return jsonify({"error": "No data available"}), 404
    return jsonify(data)

@app.route("/api/latest_status")
def api_latest_status():
    with status_lock:
        return jsonify(latest_status)

@app.route("/api/thresholds")
def api_thresholds():
    return jsonify(THRESHOLDS)

@app.route("/download/<filename>")
def download_file(filename):
    return send_from_directory(UPLOAD_DIR, filename, as_attachment=True)

# ================= SOCKETIO =================
@socketio.on('connect')
def handle_connect():
    print('[SOCKET] Client connected')
    emit('connection_response', {'status': 'connected'})

@socketio.on('disconnect')
def handle_disconnect():
    print('[SOCKET] Client disconnected')

@socketio.on('request_axis_data')
def handle_axis_request(data):
    axis       = data.get('axis', 'Z')
    minutes    = data.get('minutes', 10)
    start_time = data.get('start_time')
    end_time   = data.get('end_time')

    if start_time and end_time:
        graph_data = get_axis_data(axis, start_time=start_time, end_time=end_time)
    else:
        graph_data = get_axis_data(axis, last_n_minutes=minutes)

    if graph_data:
        print(f"[SOCKET] Sending {axis} data")
        emit('axis_data', graph_data)
    else:
        emit('axis_data', {'error': 'No data available', 'axis': axis})

# ================= MAIN =================
if __name__ == "__main__":
    print("=" * 60)
    print("VIBRATION MONITORING SERVER")
    print(f"CSV schema: {len(CSV_HEADERS)} columns")
    print(f"Date-wise storage: {DATA_BY_DATE_DIR}/")
    print(f"Available dates: {len(available_dates_cache)}")
    print("=" * 60)
    socketio.run(app, host="0.0.0.0", port=5002, debug=False)