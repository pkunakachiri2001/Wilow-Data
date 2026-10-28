"""
Flask Server for Vibration Monitoring
- Parses Teensy CSV file format directly
- Stores only fields actually present in Teensy output
- Date-wise data organisation
- Syncs with Aiven PostgreSQL DB on startup and every upload
"""

from flask import Flask, request, jsonify, render_template, send_from_directory
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import os
import math
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
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

# ---- Aiven DB (optional — graceful fallback if not configured) ----
DB_ENABLED = False
try:
    import db as _db
    if os.environ.get('DATABASE_URL'):
        _db.init_db()
        DB_ENABLED = True
        print("[LOCAL] Aiven DB enabled.")
    else:
        print("[LOCAL] DATABASE_URL not set — running in local-only mode.")
except Exception as _db_init_err:
    print(f"[LOCAL WARNING] Aiven DB init failed: {_db_init_err} — local-only mode.")

# ================= CONFIGURATION =================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
MASTER_CSV = os.path.join(BASE_DIR, "master_data.csv")
DATA_BY_DATE_DIR = os.path.join(BASE_DIR, "data_by_date")
STATIC_DIR = os.path.join(BASE_DIR, "static")
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")

# Remote forwarding (optional: if this RPi server forwards to cloud/remote dashboard)
FORWARD_URL = os.environ.get('FORWARD_URL') or os.environ.get('REMOTE_SERVER_URL', '')

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)
os.makedirs(TEMPLATE_DIR, exist_ok=True)
os.makedirs(DATA_BY_DATE_DIR, exist_ok=True)

# ================= THRESHOLDS =================
THRESHOLDS = {
    'z_axis': {
        'acceleration': {
            'over_positive': 70,
            'under_positive': 0,
            'under_negative': 0,
            'over_negative': -70
        }
    },
    'x_axis': {
        'acceleration': {
            'over_positive': 70,
            'under_positive': 0,
            'under_negative': 0,
            'over_negative': -70
        }
    },
    'y_axis': {
        'acceleration': {
            'over_positive': 70,
            'under_positive': 0,
            'under_negative': 0,
            'over_negative': -70
        }
    }
}

# ================= CSV SCHEMA =================
CSV_HEADERS = [
    'Date', 'Time_sec',
    'Max_Az', 'Min_Az', 'Mean_Az', 'Std_Az', 'Skewness_Az', 'Kurtosis_Az',
    'Max_Ax', 'Min_Ax', 'Mean_Ax',
    'Max_Ay', 'Min_Ay', 'Mean_Ay',
    'Disp_Peak_Z_mm', 'Disp_P2P_Z_mm', 'Disp_RMS_Z_mm',
    'Disp_Peak_X_mm', 'Disp_P2P_X_mm', 'Disp_RMS_X_mm',
    'Disp_Peak_Y_mm', 'Disp_P2P_Y_mm', 'Disp_RMS_Y_mm',
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

# ---- Latest KPI readings (updated on every /data or /upload POST) ----
# Displacement: mm  (derived from Disp_Peak_*/Disp_P2P_* columns - stored)
# Velocity: m/s     (captured from raw JSON before it is discarded - NOT stored in CSV)
# Acceleration: m/s^2 (derived from Max_A*/Min_A* columns - stored)
#   peak_acc  = max(|max_acc|, |min_acc|)
#   p2p_acc   = max_acc - min_acc
latest_kpi = {
    'timestamp': None,          # ISO string of last reception
    'displacement': {
        'X': {'peak': None, 'p2p': None},
        'Y': {'peak': None, 'p2p': None},
        'Z': {'peak': None, 'p2p': None},
    },
    'velocity': {
        'X': {'peak': None, 'p2p': None},
        'Y': {'peak': None, 'p2p': None},
        'Z': {'peak': None, 'p2p': None},
    },
    'acceleration': {
        'X': {'peak': None, 'p2p': None},
        'Y': {'peak': None, 'p2p': None},
        'Z': {'peak': None, 'p2p': None},
    },
}
kpi_lock = Lock()

def _update_latest_kpi(data_json, parsed_row, ts_str):
    """
    Populate latest_kpi from a raw ESP32 JSON payload (data_json) combined with
    the already-parsed CSV row dict (parsed_row). Must be called inside kpi_lock.

    data_json : the raw dict received from the ESP32 (contains vel_*_ms, disp_*_mm)
    parsed_row: dict from _parse_teensy_string / store_teensy_data
    ts_str    : ISO timestamp string for freshness display
    """
    global latest_kpi

    def _safe(v):
        """Return float or None; never 0 as a stand-in for missing."""
        try:
            f = float(v)
            return f if f != 0.0 else None
        except (TypeError, ValueError):
            return None

    # ---- Displacement (mm) from parsed CSV columns ----
    latest_kpi['displacement']['Z']['peak'] = _safe(parsed_row.get('disp_peak_z_mm'))
    latest_kpi['displacement']['Z']['p2p']  = _safe(parsed_row.get('disp_p2p_z_mm'))
    latest_kpi['displacement']['X']['peak'] = _safe(parsed_row.get('disp_peak_x_mm'))
    latest_kpi['displacement']['X']['p2p']  = _safe(parsed_row.get('disp_p2p_x_mm'))
    latest_kpi['displacement']['Y']['peak'] = _safe(parsed_row.get('disp_peak_y_mm'))
    latest_kpi['displacement']['Y']['p2p']  = _safe(parsed_row.get('disp_p2p_y_mm'))

    # ---- Velocity (m/s) ----
    # 1. Raw JSON if provided
    # 2. Derived from displacement & dominant FFT frequency: v_peak = 2*pi*f * (d_mm/1000)
    # 3. Fallback from acceleration: v_peak = a_peak / (2*pi*f)
    freq = _safe(parsed_row.get('fft1_freq') or parsed_row.get('FFT1_Freq'))
    if not freq or freq <= 0:
        peaks = parsed_row.get('fft_peaks', [])
        if peaks and len(peaks) > 0 and peaks[0][0] > 0:
            freq = float(peaks[0][0])
        else:
            freq = 50.0  # nominal 50 Hz industrial baseline
    omega = 2.0 * math.pi * freq

    def _vel(axis_name, raw_axis_key):
        # 1. Direct from raw JSON if available
        if data_json:
            raw = data_json.get(raw_axis_key)
            if isinstance(raw, dict):
                p = _safe(raw.get('peak'))
                p2p = _safe(raw.get('p2p'))
                if p is not None:
                    return p, p2p
            elif raw is not None:
                try:
                    f = float(raw)
                    return round(f, 6), round(f * 2.0, 6)
                except (TypeError, ValueError):
                    pass

        # 2. Derive from displacement (mm)
        d_peak = _safe(parsed_row.get(f'disp_peak_{axis_name.lower()}_mm') or parsed_row.get(f'Disp_Peak_{axis_name.upper()}_mm'))
        d_p2p  = _safe(parsed_row.get(f'disp_p2p_{axis_name.lower()}_mm') or parsed_row.get(f'Disp_P2P_{axis_name.upper()}_mm'))
        if d_peak is not None and d_peak > 0:
            v_peak = omega * (d_peak / 1000.0)
            v_p2p  = omega * (d_p2p / 1000.0) if d_p2p is not None else v_peak * 2.0
            return round(v_peak, 6), round(v_p2p, 6)

        # 3. Fallback from acceleration (m/s^2)
        max_a = _safe(parsed_row.get(f'max_a{axis_name.lower()}') or parsed_row.get(f'Max_A{axis_name.upper()}'))
        min_a = _safe(parsed_row.get(f'min_a{axis_name.lower()}') or parsed_row.get(f'Min_A{axis_name.upper()}'))
        if max_a is not None or min_a is not None:
            a_peak = max(abs(max_a or 0), abs(min_a or 0))
            if a_peak > 0:
                v_peak = a_peak / omega
                v_p2p  = v_peak * 2.0
                return round(v_peak, 6), round(v_p2p, 6)

        return None, None

    latest_kpi['velocity']['Z']['peak'], latest_kpi['velocity']['Z']['p2p'] = _vel('Z', 'vel_z_ms')
    latest_kpi['velocity']['X']['peak'], latest_kpi['velocity']['X']['p2p'] = _vel('X', 'vel_x_ms')
    latest_kpi['velocity']['Y']['peak'], latest_kpi['velocity']['Y']['p2p'] = _vel('Y', 'vel_y_ms')

    # ---- Acceleration (m/s^2) from parsed CSV columns ----
    # peak  = max(|max_acc|, |min_acc|)   <- firmware definition
    # p2p   = max_acc - min_acc           <- firmware definition
    def _acc_kpi(max_key, min_key):
        max_v = _safe(parsed_row.get(max_key))
        min_v = _safe(parsed_row.get(min_key))
        if max_v is None and min_v is None:
            return None, None
        peak = None
        p2p  = None
        if max_v is not None and min_v is not None:
            peak = max(abs(max_v), abs(min_v))
            p2p  = max_v - min_v
        elif max_v is not None:
            peak = abs(max_v)
        elif min_v is not None:
            peak = abs(min_v)
        return peak, p2p

    latest_kpi['acceleration']['Z']['peak'], latest_kpi['acceleration']['Z']['p2p'] = _acc_kpi('max_az', 'min_az')
    latest_kpi['acceleration']['X']['peak'], latest_kpi['acceleration']['X']['p2p'] = _acc_kpi('max_ax', 'min_ax')
    latest_kpi['acceleration']['Y']['peak'], latest_kpi['acceleration']['Y']['p2p'] = _acc_kpi('max_ay', 'min_ay')

    latest_kpi['timestamp'] = ts_str

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
    """Create or upgrade master CSV to match CSV_HEADERS schema without data loss"""
    if not os.path.exists(MASTER_CSV):
        try:
            with open(MASTER_CSV, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(CSV_HEADERS)
            print(f"[INIT] {MASTER_CSV} initialised with schema ({len(CSV_HEADERS)} columns)")
        except Exception as e:
            print(f"[INIT ERROR] Could not create {MASTER_CSV}: {e}")
        return

    try:
        with open(MASTER_CSV, 'r', newline='') as f:
            reader = csv.reader(f)
            rows = list(reader)

        if not rows:
            with open(MASTER_CSV, 'w', newline='') as f:
                csv.writer(f).writerow(CSV_HEADERS)
            return

        header = rows[0]
        if header == CSV_HEADERS and all(len(r) == len(CSV_HEADERS) for r in rows[1:]):
            return

        print(f"[INIT] Upgrading {MASTER_CSV} schema to {len(CSV_HEADERS)} columns...")
        header_map = {col: idx for idx, col in enumerate(header)}
        new_rows = [CSV_HEADERS]

        for r in rows[1:]:
            if not r:
                continue
            if len(r) == len(CSV_HEADERS):
                new_rows.append(r)
            else:
                new_r = [''] * len(CSV_HEADERS)
                for c_idx, c_name in enumerate(CSV_HEADERS):
                    if c_name in header_map and header_map[c_name] < len(r):
                        new_r[c_idx] = r[header_map[c_name]]
                new_rows.append(new_r)

        with open(MASTER_CSV, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(new_rows)
        print(f"[INIT] {MASTER_CSV} successfully upgraded to {len(CSV_HEADERS)} columns.")
    except Exception as e:
        print(f"[INIT WARNING] Could not upgrade {MASTER_CSV}: {e}")

initialize_master_csv()
scan_existing_dates()

# ================= AIVEN STARTUP SYNC =================
RPI_CLIENT_URL = os.environ.get('RPI_CLIENT_URL', '')  # e.g. http://192.168.1.50:5000


def _pull_from_rpi(last_local_epoch: float):
    """
    Layer 2 recovery: pull missed CSV files directly from the RPi client's
    /list-pending and /fetch-file endpoints.
    Only used when the laptop has been off so long that Aiven overflowed and
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


def sync_from_aiven():
    """
    On startup, fill local CSVs from two sources:
      Layer 1 — Aiven DB (rows since last local epoch)
      Layer 2 — RPi direct pull (for extreme gaps when Aiven overflowed)
    After writing rows locally, DELETES them from Aiven (acknowledged transit buffer).
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

        print(f"[SYNC] Last local epoch: {last_local_epoch} — fetching newer rows from Aiven...")
        rows = _db.fetch_rows_since(last_local_epoch)

        if rows:
            print(f"[SYNC] Writing {len(rows)} missed rows to local CSV files...")
            max_synced_epoch = last_local_epoch
            with csv_lock:
                for r in rows:
                    # r follows db.DB_COLUMNS / CSV_HEADERS order (33 columns):
                    #   date, time_sec,
                    #   max_az, min_az, mean_az, std_az, skewness_az, kurtosis_az,
                    #   max_ax, min_ax, mean_ax, max_ay, min_ay, mean_ay,
                    #   disp_peak/p2p/rms for z, x, y (9 cols),
                    #   fft1_freq, fft1_mag ... fft5_freq, fft5_mag
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

            print(f"[SYNC] ✅ Synced {len(rows)} rows from Aiven.")

            # ---- Update latest_status from the most recently synced row ----
            # rows are ordered by time_sec ASC, so the last one is the newest.
            try:
                last_row    = list(rows[-1])
                last_epoch  = float(last_row[1])
                last_max_az = float(last_row[2]) if last_row[2] is not None else 0.0
                last_freq   = float(last_row[23]) if last_row[23] is not None else 0.0  # fft1_freq
                violation   = check_threshold_violation(
                                  last_max_az, THRESHOLDS['z_axis']['acceleration']
                              ) if last_max_az else None
                ts_str = datetime.utcfromtimestamp(last_epoch).strftime('%Y-%m-%d %H:%M:%S')
                with status_lock:
                    latest_status['timestamp'] = ts_str
                    latest_status['magnitude'] = last_max_az
                    latest_status['frequency'] = last_freq
                    if violation:
                        latest_status['status']     = f'{violation} Threshold'
                        latest_status['alert']      = True
                        latest_status['alert_type'] = f'Z-Axis {violation} Threshold'
                        latest_status['alert_time'] = ts_str
                    else:
                        latest_status['status']     = 'Normal'
                        latest_status['alert']      = False
                        latest_status['alert_type'] = None
            except Exception as _st_err:
                print(f"[SYNC] Status update warning (non-fatal): {_st_err}")

            # ---- Acknowledged delete: purge synced rows from Aiven ----
            try:
                deleted = _db.delete_rows_before_epoch(max_synced_epoch)
                print(f"[SYNC] 🗑  Deleted {deleted} acknowledged rows from Aiven (transit buffer cleaned).")
            except Exception as del_err:
                print(f"[SYNC] Delete warning (non-fatal): {del_err}")
                
            # Tell the local dashboard to refresh its charts
            socketio.emit('data_update', {'type': 'sync', 'count': len(rows)})
        else:
            # print("[SYNC] Aiven: already up to date — no new rows.") # Muted to prevent spam
            pass

        # ---- Layer 2: RPi direct pull (catches anything Aiven may have trimmed) ----
        _pull_from_rpi(last_local_epoch)

    except Exception as e:
        print(f"[SYNC ERROR] Could not sync from Aiven: {e} — continuing in local-only mode.")

def sync_loop():
    """Background thread to poll Aiven DB for new data every 10 seconds"""
    print("[SYNC] Background sync thread started.")
    while True:
        try:
            sync_from_aiven()
        except Exception as e:
            print(f"[SYNC LOOP ERROR] {e}")
        time.sleep(10)

# Run initial sync and start background polling
synced = sync_from_aiven()
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

# ================= TEENSY / ESP32 FILE PARSING =================
def _get_empty_parsed_data():
    return {
        'max_az': None, 'min_az': None, 'mean_az': None,
        'std_az': None, 'skewness_az': None, 'kurtosis_az': None,
        'max_ax': None, 'min_ax': None, 'mean_ax': None,
        'max_ay': None, 'min_ay': None, 'mean_ay': None,
        'disp_peak_z_mm': None, 'disp_p2p_z_mm': None, 'disp_rms_z_mm': None,
        'disp_peak_x_mm': None, 'disp_p2p_x_mm': None, 'disp_rms_x_mm': None,
        'disp_peak_y_mm': None, 'disp_p2p_y_mm': None, 'disp_rms_y_mm': None,
        'fft_peaks': []   # list of (freq, mag) up to 5
    }


def parse_teensy_file(filepath):
    """
    Parse a Teensy/ESP32 CSV statistics file from disk and return a structured dict.
    Sections detected: Z-AXIS, X-AXIS, Y-AXIS, DISPLACEMENT, FFT PEAKS.
    """
    try:
        with open(filepath, 'r') as f:
            content = f.read()
        return _parse_teensy_string(content)
    except Exception as e:
        print(f"[PARSE ERROR] {filepath}: {e}")
        return _get_empty_parsed_data()


def _parse_teensy_string(csv_content: str) -> dict:
    """
    Same logic as parse_teensy_file but operates on a string (no disk I/O).
    Used by the /data endpoint to parse the in-memory CSV reconstructed from ESP32 JSON.
    """
    data = _get_empty_parsed_data()
    try:
        lines = csv_content.splitlines()
        current_section = None
        for line in lines:
            line = line.strip()
            if '=== Z-AXIS STATISTICS' in line:
                current_section = 'z_stat'; continue
            elif '=== X-AXIS STATISTICS' in line:
                current_section = 'x_stat'; continue
            elif '=== Y-AXIS STATISTICS' in line:
                current_section = 'y_stat'; continue
            elif '=== DISPLACEMENT STATISTICS' in line:
                current_section = 'disp_stat'; continue
            elif '=== FFT PEAKS' in line:
                current_section = 'fft'; continue
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
                    'Maximum Az': 'max_az', 'Minimum Az': 'min_az',
                    'Mean Az': 'mean_az', 'Std Dev Az': 'std_az',
                    'Skewness Az': 'skewness_az',
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

            elif current_section == 'y_stat' and len(parts) >= 2:
                param, val_str = parts[0], parts[1]
                try:
                    val = float(val_str)
                    mapping = {
                        'Maximum Ay': 'max_ay',
                        'Minimum Ay': 'min_ay',
                        'Mean Ay':    'mean_ay',
                    }
                    if param in mapping:
                        data[mapping[param]] = val
                except ValueError:
                    continue

            elif current_section == 'disp_stat' and len(parts) >= 2:
                param, val_str = parts[0], parts[1]
                try:
                    val = float(val_str)
                    disp_mapping = {
                        'Peak Disp Z (mm)': 'disp_peak_z_mm', 'Disp Peak Z': 'disp_peak_z_mm',
                        'P2P Disp Z (mm)':  'disp_p2p_z_mm',  'Disp P2P Z':  'disp_p2p_z_mm',
                        'RMS Disp Z (mm)':  'disp_rms_z_mm',  'Disp RMS Z':  'disp_rms_z_mm',

                        'Peak Disp X (mm)': 'disp_peak_x_mm', 'Disp Peak X': 'disp_peak_x_mm',
                        'P2P Disp X (mm)':  'disp_p2p_x_mm',  'Disp P2P X':  'disp_p2p_x_mm',
                        'RMS Disp X (mm)':  'disp_rms_x_mm',  'Disp RMS X':  'disp_rms_x_mm',

                        'Peak Disp Y (mm)': 'disp_peak_y_mm', 'Disp Peak Y': 'disp_peak_y_mm',
                        'P2P Disp Y (mm)':  'disp_p2p_y_mm',  'Disp P2P Y':  'disp_p2p_y_mm',
                        'RMS Disp Y (mm)':  'disp_rms_y_mm',  'Disp RMS Y':  'disp_rms_y_mm',
                    }
                    if param in disp_mapping:
                        data[disp_mapping[param]] = val
                except ValueError:
                    continue

            elif current_section == 'fft' and len(parts) >= 3 and len(data['fft_peaks']) < 5:
                try:
                    freq = float(parts[1])
                    mag  = float(parts[2])
                    data['fft_peaks'].append((freq, mag))
                except ValueError:
                    continue
    except Exception as e:
        print(f"[PARSE ERROR] _parse_teensy_string: {e}")
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
    """Write one row to master CSV and the date-specific CSV. Returns (alert_status, alert_axis)."""

    # Flatten FFT peaks to [freq1, mag1, freq2, mag2, …] padding to 5
    fft_flat = []
    for i in range(5):
        if i < len(parsed.get('fft_peaks', [])):
            fft_flat.extend([parsed['fft_peaks'][i][0], parsed['fft_peaks'][i][1]])
        else:
            fft_flat.extend(['', ''])

    row = [
        date_str, epoch_time,
        parsed.get('max_az') if parsed.get('max_az') is not None else '',
        parsed.get('min_az') if parsed.get('min_az') is not None else '',
        parsed.get('mean_az') if parsed.get('mean_az') is not None else '',
        parsed.get('std_az') if parsed.get('std_az') is not None else '',
        parsed.get('skewness_az') if parsed.get('skewness_az') is not None else '',
        parsed.get('kurtosis_az') if parsed.get('kurtosis_az') is not None else '',
        parsed.get('max_ax') if parsed.get('max_ax') is not None else '',
        parsed.get('min_ax') if parsed.get('min_ax') is not None else '',
        parsed.get('mean_ax') if parsed.get('mean_ax') is not None else '',
        parsed.get('max_ay') if parsed.get('max_ay') is not None else '',
        parsed.get('min_ay') if parsed.get('min_ay') is not None else '',
        parsed.get('mean_ay') if parsed.get('mean_ay') is not None else '',
        parsed.get('disp_peak_z_mm') if parsed.get('disp_peak_z_mm') is not None else '',
        parsed.get('disp_p2p_z_mm') if parsed.get('disp_p2p_z_mm') is not None else '',
        parsed.get('disp_rms_z_mm') if parsed.get('disp_rms_z_mm') is not None else '',
        parsed.get('disp_peak_x_mm') if parsed.get('disp_peak_x_mm') is not None else '',
        parsed.get('disp_p2p_x_mm') if parsed.get('disp_p2p_x_mm') is not None else '',
        parsed.get('disp_rms_x_mm') if parsed.get('disp_rms_x_mm') is not None else '',
        parsed.get('disp_peak_y_mm') if parsed.get('disp_peak_y_mm') is not None else '',
        parsed.get('disp_p2p_y_mm') if parsed.get('disp_p2p_y_mm') is not None else '',
        parsed.get('disp_rms_y_mm') if parsed.get('disp_rms_y_mm') is not None else '',
    ] + fft_flat

    # Threshold checks on Z, X, Y
    alert_status = None
    alert_axis = None
    max_az = parsed.get('max_az')
    max_ax = parsed.get('max_ax')
    max_ay = parsed.get('max_ay')

    if max_az is not None and 'z_axis' in THRESHOLDS:
        violation = check_threshold_violation(max_az, THRESHOLDS['z_axis']['acceleration'])
        if violation:
            alert_status = violation
            alert_axis = 'Z'
            print(f"[ALERT] {violation} | Max_Az={max_az:.4f} m/s^2")
        else:
            print(f"[OK] Max_Az={max_az:.4f} m/s^2")

    if not alert_status and max_ax is not None and 'x_axis' in THRESHOLDS:
        violation = check_threshold_violation(max_ax, THRESHOLDS['x_axis']['acceleration'])
        if violation:
            alert_status = violation
            alert_axis = 'X'
            print(f"[ALERT] {violation} | Max_Ax={max_ax:.4f} m/s^2")

    if not alert_status and max_ay is not None and 'y_axis' in THRESHOLDS:
        violation = check_threshold_violation(max_ay, THRESHOLDS['y_axis']['acceleration'])
        if violation:
            alert_status = violation
            alert_axis = 'Y'
            print(f"[ALERT] {violation} | Max_Ay={max_ay:.4f} m/s^2")

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
            print(f"[LOCAL DB WARN] insert_row failed: {_db_err} - data saved locally only.")

    print(f"[STORED] {filename} -> {date_str}")
    return alert_status, alert_axis

# ================= DATA QUERY =================
def get_axis_data(axis='Z', metric='acceleration', last_n_minutes=None, start_time=None, end_time=None):
    """
    Return chart-ready data for the requested axis and metric from master CSV.
    axis: 'Z', 'X', 'Y'
    metric: 'acceleration' or 'displacement'
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

        axis_upper = axis.upper()
        if metric == 'displacement':
            if axis_upper == 'Z':
                max_col, min_col, avg_col = 'Disp_Peak_Z_mm', 'Disp_P2P_Z_mm', 'Disp_RMS_Z_mm'
            elif axis_upper == 'Y':
                max_col, min_col, avg_col = 'Disp_Peak_Y_mm', 'Disp_P2P_Y_mm', 'Disp_RMS_Y_mm'
            else:  # X
                max_col, min_col, avg_col = 'Disp_Peak_X_mm', 'Disp_P2P_X_mm', 'Disp_RMS_X_mm'
        else:  # acceleration
            if axis_upper == 'Z':
                max_col, min_col, avg_col = 'Max_Az', 'Min_Az', 'Mean_Az'
            elif axis_upper == 'Y':
                max_col, min_col, avg_col = 'Max_Ay', 'Min_Ay', 'Mean_Ay'
            else:  # X
                max_col, min_col, avg_col = 'Max_Ax', 'Min_Ax', 'Mean_Ax'

        max_values = pd.to_numeric(df.get(max_col, pd.Series(dtype=float)), errors='coerce')
        min_values = pd.to_numeric(df.get(min_col, pd.Series(dtype=float)), errors='coerce')
        avg_values = pd.to_numeric(df.get(avg_col, pd.Series(dtype=float)), errors='coerce') \
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
            'axis':            axis_upper,
            'metric':          metric,
            'alerts':          {'datetimes': [], 'values': []}
        }

    except Exception as e:
        print(f"[ERROR] get_axis_data: {e}")
        import traceback; traceback.print_exc()
        return None

# ================= EVENT EXTRACTION =================
def extract_and_save_event(fault_type: str):
    try:
        safe_fault_name = "".join(c if c.isalnum() or c in " _-" else "_" for c in fault_type).strip()
        events_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'events', safe_fault_name)
        os.makedirs(events_dir, exist_ok=True)
        
        # Read from MASTER_CSV
        import pandas as pd
        if not os.path.exists(MASTER_CSV):
            return False, "No data available."
            
        df = pd.read_csv(MASTER_CSV)
        if df.empty:
            return False, "No data available."
            
        df['Time_sec'] = pd.to_numeric(df['Time_sec'], errors='coerce')
        df = df.dropna(subset=['Time_sec'])
        if df.empty:
            return False, "No data available."
            
        cutoff = df['Time_sec'].max() - (10 * 60)
        df_event = df[df['Time_sec'] >= cutoff]
        
        if df_event.empty:
            return False, "No data available in the last 10 minutes."
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"event_{timestamp}.csv"
        filepath = os.path.join(events_dir, filename)
        
        df_event.to_csv(filepath, index=False)
        return True, f"Event saved successfully to {os.path.join('events', safe_fault_name, filename)}"
    except Exception as e:
        print(f"[ERROR] extract_and_save_event: {e}")
        return False, str(e)

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
        # Direct JSON upload support
        if request.is_json:
            return receive_esp32_data()

        if "file" not in request.files:
            return jsonify({"error": "no file"}), 400

        file = request.files["file"]
        if not file.filename:
            return jsonify({"error": "empty filename"}), 400

        filepath = os.path.join(UPLOAD_DIR, file.filename)
        file.save(filepath)

        # Parse the Teensy/ESP32 statistics file
        parsed = parse_teensy_file(filepath)
        date_str, epoch_time = epoch_from_filename(file.filename)

        alert_status, alert_axis = store_teensy_data(file.filename, parsed, date_str, epoch_time)

        # Update KPI cache (no raw JSON available for file uploads, so velocity stays None)
        ts_iso = datetime.fromtimestamp(epoch_time).isoformat()
        with kpi_lock:
            _update_latest_kpi(None, parsed, ts_iso)

        # Update latest status
        top_freq = parsed['fft_peaks'][0][0] if parsed['fft_peaks'] else 0
        with status_lock:
            latest_status['timestamp'] = datetime.fromtimestamp(epoch_time).strftime('%Y-%m-%d %H:%M:%S')
            latest_status['magnitude'] = parsed.get('max_az') or 0
            latest_status['frequency'] = top_freq

            if alert_status:
                axis_name = alert_axis or 'Z'
                latest_status['status']     = f'{alert_status} Threshold'
                latest_status['alert']      = True
                latest_status['alert_type'] = f'{axis_name}-Axis {alert_status} Threshold'
                latest_status['alert_time'] = latest_status['timestamp']

                socketio.emit('alert_notification', {
                    'alert_type': f'{axis_name}-Axis {alert_status} Threshold',
                    'timestamp':  latest_status['timestamp'],
                    'magnitude':  parsed.get(f'max_a{axis_name.lower()}'),
                    'axis':       axis_name
                })
            else:
                latest_status['status']     = 'Normal'
                latest_status['alert']      = False
                latest_status['alert_type'] = None

        socketio.emit('data_update', {'type': 'file', 'filename': file.filename})
        return jsonify({"status": "received", "date": date_str, "alert": alert_status, "axis": alert_axis})

    except Exception as e:
        print(f"[ERROR] Upload: {e}")
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


def _extract_metric_dict(val):
    """Helper to extract (max, min, peak, p2p, rms) from metric dict or numeric scalar"""
    if isinstance(val, dict):
        return (
            float(val.get("max", 0.0)),
            float(val.get("min", 0.0)),
            float(val.get("peak", 0.0)),
            float(val.get("p2p", 0.0)),
            float(val.get("rms", 0.0))
        )
    p = float(val or 0.0)
    return p, -p, p, 2.0 * p, p / 1.414


@app.route("/data", methods=["POST"])
def receive_esp32_data():
    try:
        if not request.is_json:
            return jsonify({"error": "Expected JSON payload"}), 400

        data     = request.get_json()
        core_id  = data.get("core", 0)
        samples  = data.get("samples", 0)
        print(f"\n[ESP32 Core {core_id}] Direct JSON received (samples: {samples})")

        # ---- Wall-clock timestamps (RPi reception) ----
        now_utc   = datetime.now(timezone.utc)
        epoch_now = now_utc.timestamp()
        now_str   = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename  = f"ESP32_Core{core_id}_{now_str}.csv"

        record_start = data.get("record_start", 0)
        record_stop  = data.get("record_stop",  0)
        duration_s   = (record_stop - record_start) / 1000.0 if record_stop > record_start else 0.0

        epoch_field = data.get("record_start_epoch")
        if epoch_field and int(epoch_field) > 0:
            epoch_t  = float(epoch_field)
            date_str = datetime.fromtimestamp(epoch_t).strftime('%Y-%m-%d')
        else:
            epoch_t  = epoch_now
            date_str = datetime.now().strftime('%Y-%m-%d')

        freqs     = data.get("freq", [])
        mags      = data.get("mag",  [])
        fft_lines = [f"{i+1},{freqs[i]},{mags[i]}"
                     for i in range(min(len(freqs), len(mags)))]

        # Extract 3-axis displacement metrics
        disp_z_max, disp_z_min, disp_z_peak, disp_z_p2p, disp_z_rms = _extract_metric_dict(data.get("disp_z_mm"))
        disp_x_max, disp_x_min, disp_x_peak, disp_x_p2p, disp_x_rms = _extract_metric_dict(data.get("disp_x_mm"))
        disp_y_max, disp_y_min, disp_y_peak, disp_y_p2p, disp_y_rms = _extract_metric_dict(data.get("disp_y_mm"))

        # Extract 3-axis velocity metrics & diagnostics
        vel_z_max, vel_z_min, vel_z_peak, vel_z_p2p, vel_z_rms = _extract_metric_dict(data.get("vel_z_ms"))
        vel_x_max, vel_x_min, vel_x_peak, vel_x_p2p, vel_x_rms = _extract_metric_dict(data.get("vel_x_ms"))
        vel_y_max, vel_y_min, vel_y_peak, vel_y_p2p, vel_y_rms = _extract_metric_dict(data.get("vel_y_ms"))
        diag = data.get("diag", {}) if isinstance(data.get("diag"), dict) else {}

        # Reconstruct standard CSV text
        csv_lines = [
            "=== METADATA ===",
            "Parameter,Value,Unit",
            f"Device ID,ESP32_Core{core_id},-",
            "File number,0,-",
            f"File name,{filename},-",
            f"Reception time (UTC),{now_utc.isoformat()},-",
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
            f"Maximum Az,{data.get('max',      0.0)},m/s^2",
            f"Minimum Az,{data.get('min',      0.0)},m/s^2",
            f"Mean Az,{data.get('mean',        0.0)},m/s^2",
            f"Std Dev Az,{data.get('std_dev',  0.0)},m/s^2",
            f"Skewness Az,{data.get('skewness',0.0)},-",
            f"Excess Kurtosis Az,{data.get('kurtosis', 0.0)},-",
            "",
            "=== X-AXIS STATISTICS ===",
            "Parameter,Value,Unit",
            f"Maximum Ax,{data.get('max_x',  0.0)},m/s^2",
            f"Minimum Ax,{data.get('min_x',  0.0)},m/s^2",
            f"Mean Ax,{data.get('mean_x',    0.0)},m/s^2",
            "",
            "=== Y-AXIS STATISTICS ===",
            "Parameter,Value,Unit",
            f"Maximum Ay,{data.get('max_y',  0.0)},m/s^2",
            f"Minimum Ay,{data.get('min_y',  0.0)},m/s^2",
            f"Mean Ay,{data.get('mean_y',    0.0)},m/s^2",
            "",
            "=== DISPLACEMENT STATISTICS (mm) ===",
            "Parameter,Value,Unit",
            f"Peak Disp Z (mm),{disp_z_peak},mm",
            f"P2P Disp Z (mm),{disp_z_p2p},mm",
            f"RMS Disp Z (mm),{disp_z_rms},mm",
            f"Peak Disp X (mm),{disp_x_peak},mm",
            f"P2P Disp X (mm),{disp_x_p2p},mm",
            f"RMS Disp X (mm),{disp_x_rms},mm",
            f"Peak Disp Y (mm),{disp_y_peak},mm",
            f"P2P Disp Y (mm),{disp_y_p2p},mm",
            f"RMS Disp Y (mm),{disp_y_rms},mm",
            "",
            "=== VELOCITY STATISTICS (m/s) ===",
            "Parameter,Value,Unit",
            f"Peak Vel Z (m/s),{vel_z_peak},m/s",
            f"P2P Vel Z (m/s),{vel_z_p2p},m/s",
            f"RMS Vel Z (m/s),{vel_z_rms},m/s",
            f"Peak Vel X (m/s),{vel_x_peak},m/s",
            f"P2P Vel X (m/s),{vel_x_p2p},m/s",
            f"RMS Vel X (m/s),{vel_x_rms},m/s",
            f"Peak Vel Y (m/s),{vel_y_peak},m/s",
            f"P2P Vel Y (m/s),{vel_y_p2p},m/s",
            f"RMS Vel Y (m/s),{vel_y_rms},m/s",
            "",
            "=== FILTER & FFT DIAGNOSTICS ===",
            "Parameter,Value,Unit",
            f"Cutoff Frequency (Hz),{diag.get('cutoff_hz', 0.0)},Hz",
            f"Valid FFT Samples,{diag.get('valid_samples', 0)},samples",
            "",
            "=== FFT PEAKS (Z-AXIS) ===",
            "Rank,Frequency (Hz),Magnitude",
        ] + fft_lines + ["================================================="]

        csv_content = "\n".join(csv_lines)

        # 1. Save CSV locally on RPi (inside UPLOAD_DIR)
        csv_filepath = os.path.join(UPLOAD_DIR, filename)
        try:
            with open(csv_filepath, 'w') as f:
                f.write(csv_content)
            print(f"[RPi Receiver] Saved CSV locally: {csv_filepath}")
        except Exception as fe:
            print(f"[RPi Receiver WARNING] Could not write file {csv_filepath}: {fe}")

        # 2. Parse string & store in Master CSV and Date CSV
        parsed = _parse_teensy_string(csv_content)
        alert_status, alert_axis = store_teensy_data(filename, parsed, date_str, epoch_t)

        # 3a. Update KPI cache (velocity captured here from raw JSON before it is lost)
        ts_iso = datetime.fromtimestamp(epoch_t).isoformat()
        with kpi_lock:
            _update_latest_kpi(data, parsed, ts_iso)

        top_freq = parsed['fft_peaks'][0][0] if parsed['fft_peaks'] else 0
        with status_lock:
            latest_status['timestamp'] = datetime.fromtimestamp(epoch_t).strftime('%Y-%m-%d %H:%M:%S')
            latest_status['magnitude'] = parsed.get('max_az') or 0
            latest_status['frequency'] = top_freq

            if alert_status:
                axis_name = alert_axis or 'Z'
                latest_status['status']     = f'{alert_status} Threshold'
                latest_status['alert']      = True
                latest_status['alert_type'] = f'{axis_name}-Axis {alert_status} Threshold'
                latest_status['alert_time'] = latest_status['timestamp']

                socketio.emit('alert_notification', {
                    'alert_type': f'{axis_name}-Axis {alert_status} Threshold',
                    'timestamp':  latest_status['timestamp'],
                    'magnitude':  parsed.get(f'max_a{axis_name.lower()}'),
                    'axis':       axis_name
                })
            else:
                latest_status['status']     = 'Normal'
                latest_status['alert']      = False
                latest_status['alert_type'] = None

        socketio.emit('data_update', {
            'type': 'esp32_direct',
            'filename': filename,
            'core': core_id,
            'samples': samples
        })

        # 3. Optional remote forward
        if FORWARD_URL:
            try:
                import requests as _req
                files = {'file': (filename, csv_content.encode('utf-8'), 'text/csv')}
                _req.post(FORWARD_URL, files=files, timeout=5)
            except Exception as fwd_err:
                print(f"[FORWARD WARNING] Could not forward to {FORWARD_URL}: {fwd_err}")

        return jsonify({
            "status": "received",
            "filename": filename,
            "core": core_id,
            "alert": alert_status,
            "axis": alert_axis
        }), 200

    except Exception as e:
        print(f"[ERROR] /data: {e}")
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route("/api/axis_data")
def api_axis_data():
    axis        = request.args.get('axis', 'Z')
    metric      = request.args.get('metric', 'acceleration')
    minutes     = request.args.get('minutes')
    start_time  = request.args.get('start_time')
    end_time    = request.args.get('end_time')

    if start_time and end_time:
        data = get_axis_data(axis, metric=metric, start_time=start_time, end_time=end_time)
    else:
        minutes = int(minutes) if minutes else 10
        data = get_axis_data(axis, metric=metric, last_n_minutes=minutes)

    if data is None:
        return jsonify({"error": "No data available"}), 404
    return jsonify(data)

@app.route("/api/latest_status")
def api_latest_status():
    with status_lock:
        return jsonify(latest_status)

@app.route("/api/latest_kpi")
def api_latest_kpi():
    """
    Return the most recent KPI readings for all three metrics and axes.
    Values are None when unavailable (never 0 as a substitute).
    Velocity values come from the raw ESP32 JSON and are NOT stored in the CSV.
    Displacement and acceleration values are derived from stored CSV columns.
    """
    with kpi_lock:
        import copy
        return jsonify(copy.deepcopy(latest_kpi))

@app.route("/api/thresholds")
def api_thresholds():
    return jsonify(THRESHOLDS)

@app.route("/download/<filename>")
def download_file(filename):
    return send_from_directory(UPLOAD_DIR, filename, as_attachment=True)

# ================= RPi SYNC & RECOVERY ENDPOINTS =================
@app.route("/status", methods=["GET"])
def rpi_status():
    """Health check endpoint for Raspberry Pi receiver hub"""
    try:
        csv_files = [f for f in os.listdir(UPLOAD_DIR) if f.endswith('.csv')] if os.path.exists(UPLOAD_DIR) else []
        return jsonify({
            "status": "running",
            "device": "Raspberry Pi Receiver Hub",
            "time_utc": datetime.now(timezone.utc).isoformat(),
            "csv_files_count": len(csv_files),
            "latest_status": latest_status
        }), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/list-pending", methods=["GET"])
def list_pending():
    """Return list of saved CSV files and timestamps on the RPi for remote sync"""
    try:
        files = []
        if os.path.exists(UPLOAD_DIR):
            for fname in sorted(os.listdir(UPLOAD_DIR)):
                if not fname.endswith('.csv'):
                    continue
                date_str, epoch = epoch_from_filename(fname)
                fpath = os.path.join(UPLOAD_DIR, fname)
                files.append({
                    'filename': fname,
                    'date': date_str,
                    'epoch': epoch,
                    'size_bytes': os.path.getsize(fpath) if os.path.exists(fpath) else 0
                })
        return jsonify({'status': 'ok', 'count': len(files), 'files': files}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route("/fetch-file/<filename>", methods=["GET"])
def fetch_file(filename):
    """Serve a specific CSV file from RPi storage for remote sync"""
    try:
        safe_name = os.path.basename(filename)
        fpath = os.path.join(UPLOAD_DIR, safe_name)
        if not os.path.exists(fpath):
            return jsonify({'status': 'error', 'message': 'File not found'}), 404
        return send_from_directory(UPLOAD_DIR, safe_name, as_attachment=True)
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route("/acknowledge", methods=["POST"])
def acknowledge():
    """Mark files as acknowledged by moving them to acknowledged/ archive directory"""
    try:
        data = request.get_json(silent=True) or {}
        filenames = data.get('filenames', [])
        ack_dir = os.path.join(UPLOAD_DIR, 'acknowledged')
        os.makedirs(ack_dir, exist_ok=True)
        moved = 0
        for fname in filenames:
            safe_name = os.path.basename(fname)
            src = os.path.join(UPLOAD_DIR, safe_name)
            dst = os.path.join(ack_dir, safe_name)
            if os.path.exists(src):
                try:
                    os.replace(src, dst)
                    moved += 1
                except Exception:
                    pass
        return jsonify({'status': 'ok', 'acknowledged': moved}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route("/api/log_event", methods=["POST"])
def api_log_event():
    data = request.get_json()
    if not data or 'fault_type' not in data:
        return jsonify({"error": "fault_type is required"}), 400
        
    fault_type = data['fault_type']
    success, msg = extract_and_save_event(fault_type)
    
    if success:
        return jsonify({"status": "success", "message": msg}), 200
    else:
        return jsonify({"status": "error", "message": msg}), 500

@app.route("/api/events")
def api_list_events():
    events_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'events')
    if not os.path.exists(events_dir):
        return jsonify([])
        
    files_list = []
    for root, dirs, files in os.walk(events_dir):
        for file in files:
            if file.endswith('.csv'):
                fault_type = os.path.basename(root)
                rel_path = f"{fault_type}/{file}"
                files_list.append({
                    "fault_type": fault_type,
                    "filename": file,
                    "path": rel_path
                })
    return jsonify(files_list)

@app.route("/api/events/download/<path:filepath>")
def api_download_event(filepath):
    events_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'events')
    directory = os.path.join(events_dir, os.path.dirname(filepath))
    filename = os.path.basename(filepath)
    return send_from_directory(directory, filename, as_attachment=True)

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
    metric     = data.get('metric', 'acceleration')
    minutes    = data.get('minutes', 10)
    start_time = data.get('start_time')
    end_time   = data.get('end_time')

    if start_time and end_time:
        graph_data = get_axis_data(axis, metric=metric, start_time=start_time, end_time=end_time)
    else:
        graph_data = get_axis_data(axis, metric=metric, last_n_minutes=minutes)

    if graph_data:
        print(f"[SOCKET] Sending {axis} data ({metric})")
        emit('axis_data', graph_data)
    else:
        emit('axis_data', {'error': 'No data available', 'axis': axis})

# ================= MAIN =================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print("=" * 60)
    print("VIBRATION MONITORING SERVER (RPi Receiver Hub)")
    print(f"Listening on: 0.0.0.0:{port}")
    print(f"CSV schema: {len(CSV_HEADERS)} columns")
    print(f"Upload storage: {UPLOAD_DIR}/")
    print(f"Date-wise storage: {DATA_BY_DATE_DIR}/")
    print(f"Available dates: {len(available_dates_cache)}")
    if FORWARD_URL:
        print(f"Forwarding destination: {FORWARD_URL}")
    print("=" * 60)
    socketio.run(app, host="0.0.0.0", port=port, debug=False, allow_unsafe_werkzeug=True)