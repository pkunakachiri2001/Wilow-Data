"""
server.py — Local Hub / Raspberry Pi Receiver Server
Optimized for continuous 24/7 operation:
  - Pure stdlib CSV + in-memory deque ring buffer (no pandas/numpy overhead)
  - Shared parsing, 4-level threshold & KPI logic via vibration_core
  - Fast non-blocking local ingestion (/data, /upload) with async Aiven DB & FORWARD_URL push
  - Bidirectional Web <-> Local sync via Aiven PostgreSQL:
      * Pulls remote telemetry while enforcing retention window (never blanks web charts)
      * Bidirectional 4-level threshold sync (app_thresholds + thresholds.json)
      * Bidirectional event CSV snapshot sync (vibration_events + local events/)
  - Automatic uploads/ retention cleanup for unbounded uptime
"""

from collections import deque
import csv
from datetime import datetime, timedelta, timezone
import json
import os
from threading import Lock
import threading
import time

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from flask import Flask, jsonify, render_template, request, send_from_directory
from flask_cors import CORS
from flask_socketio import SocketIO, emit

import db as _db
import vibration_core as vc

# ================= CONFIGURATION =================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
MASTER_CSV = os.path.join(BASE_DIR, "master_data.csv")
DATA_BY_DATE_DIR = os.path.join(BASE_DIR, "data_by_date")
STATIC_DIR = os.path.join(BASE_DIR, "static")
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")
EVENTS_DIR = os.path.join(BASE_DIR, "events")
THRESHOLDS_FILE = os.path.join(BASE_DIR, "thresholds.json")

FORWARD_URL = os.environ.get('FORWARD_URL') or os.environ.get('REMOTE_SERVER_URL', '')
ESP32_API_KEY = os.environ.get('ESP32_API_KEY', '')
RPI_CLIENT_URL = os.environ.get('RPI_CLIENT_URL', '')
AIVEN_RETENTION_MINUTES = int(os.environ.get('AIVEN_RETENTION_MINUTES', '60'))
UPLOAD_RETENTION_DAYS = int(os.environ.get('UPLOAD_RETENTION_DAYS', '7'))

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)
os.makedirs(TEMPLATE_DIR, exist_ok=True)
os.makedirs(DATA_BY_DATE_DIR, exist_ok=True)
os.makedirs(EVENTS_DIR, exist_ok=True)

CSV_HEADERS = vc.CSV_HEADERS

# ================= THRESHOLDS & STATE =================
THRESHOLDS = vc.get_default_thresholds()
_thresholds_updated_at = 0.0
thresh_lock = Lock()

DB_ENABLED = False
_db_reconnect_ts = 0.0

app = Flask(__name__, static_folder=STATIC_DIR, template_folder=TEMPLATE_DIR)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'vibration-monitoring-updated')
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

csv_lock = Lock()
data_lock = Lock()
RECENT_ROWS = deque(maxlen=20_000)  # ~24h+ ring buffer of normalized 33-col lists
_last_local_epoch = 0.0
_last_data_received_ts = 0.0
_last_synced_event_ts = 0.0

available_dates_cache = set()
status_lock = Lock()
latest_status = {
    'timestamp': None,
    'status': 'Waiting for data...',
    'alert': None,
    'alert_type': None,
    'alert_time': None,
    'magnitude': 0.0,
    'frequency': 0.0,
    'ai_confidence': None,
    'ai_status': None,
    'server_mode': 'local',
    'db_connected': False,
}

kpi_lock = Lock()
latest_kpi = vc.build_kpi_snapshot(None, {}, None)

_last_auto_event_time = {}
_auto_event_lock = Lock()


# ================= LOCAL THRESHOLDS PERSISTENCE =================
def _load_local_thresholds_file():
    global THRESHOLDS, _thresholds_updated_at
    if not os.path.exists(THRESHOLDS_FILE):
        return
    try:
        with open(THRESHOLDS_FILE, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        if isinstance(payload, dict) and 'thresholds' in payload:
            with thresh_lock:
                vc.apply_threshold_payload(THRESHOLDS, payload['thresholds'])
                _thresholds_updated_at = float(payload.get('updated_at', 0.0))
        elif isinstance(payload, dict):
            with thresh_lock:
                vc.apply_threshold_payload(THRESHOLDS, payload)
    except Exception as e:
        print(f"[THRESHOLDS WARN] Could not load {THRESHOLDS_FILE}: {e}")


def _save_local_thresholds_file(thresholds_dict: dict, updated_at: float):
    try:
        with open(THRESHOLDS_FILE, 'w', encoding='utf-8') as f:
            json.dump({'thresholds': thresholds_dict, 'updated_at': updated_at}, f, indent=2)
    except Exception as e:
        print(f"[THRESHOLDS WARN] Could not save {THRESHOLDS_FILE}: {e}")


# ================= DATE & CSV HELPERS =================
def get_date_csv(date_str: str) -> str:
    date_folder = os.path.join(DATA_BY_DATE_DIR, date_str)
    os.makedirs(date_folder, exist_ok=True)
    available_dates_cache.add(date_str)
    return os.path.join(date_folder, f"data_{date_str}.csv")


def init_date_csv(csv_path: str):
    if not os.path.exists(csv_path):
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(CSV_HEADERS)


def scan_existing_dates():
    try:
        if os.path.exists(DATA_BY_DATE_DIR):
            for date_folder in os.listdir(DATA_BY_DATE_DIR):
                if os.path.isdir(os.path.join(DATA_BY_DATE_DIR, date_folder)):
                    available_dates_cache.add(date_folder)
        print(f"[DATES] Found {len(available_dates_cache)} existing dates")
    except Exception as e:
        print(f"[DATES] Scan error: {e}")


def initialize_master_csv():
    """Create or upgrade master CSV to 33-column CSV_HEADERS schema without data loss."""
    if not os.path.exists(MASTER_CSV):
        try:
            with open(MASTER_CSV, 'w', newline='', encoding='utf-8') as f:
                csv.writer(f).writerow(CSV_HEADERS)
            print(f"[INIT] {MASTER_CSV} initialised with schema ({len(CSV_HEADERS)} columns)")
        except Exception as e:
            print(f"[INIT ERROR] Could not create {MASTER_CSV}: {e}")
        return

    try:
        with open(MASTER_CSV, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header == CSV_HEADERS:
                return
            rows = [r for r in reader if r]

        if not header:
            with open(MASTER_CSV, 'w', newline='', encoding='utf-8') as f:
                csv.writer(f).writerow(CSV_HEADERS)
            return

        print(f"[INIT] Upgrading {MASTER_CSV} schema to {len(CSV_HEADERS)} columns...")
        header_map = {col: idx for idx, col in enumerate(header)}
        new_rows = [CSV_HEADERS]
        for r in rows:
            if len(r) == len(CSV_HEADERS):
                new_rows.append(r)
            else:
                new_r = [''] * len(CSV_HEADERS)
                for c_idx, c_name in enumerate(CSV_HEADERS):
                    if c_name in header_map and header_map[c_name] < len(r):
                        new_r[c_idx] = r[header_map[c_name]]
                new_rows.append(new_r)

        with open(MASTER_CSV, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerows(new_rows)
        print(f"[INIT] {MASTER_CSV} upgraded to {len(CSV_HEADERS)} columns.")
    except Exception as e:
        print(f"[INIT WARNING] Could not upgrade {MASTER_CSV}: {e}")


def _hydrate_from_master_csv():
    """Stream master_data.csv once on startup into RECENT_ROWS and latest state."""
    global _last_local_epoch
    if not os.path.exists(MASTER_CSV):
        return

    newest_norm = None
    newest_epoch = 0.0
    try:
        with csv_lock, open(MASTER_CSV, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            _ = next(reader, None)  # header
            with data_lock:
                RECENT_ROWS.clear()
                for raw in reader:
                    if not raw:
                        continue
                    norm = vc.normalize_row_tuple(raw)
                    epoch_t = norm[1]
                    if epoch_t is None:
                        continue
                    RECENT_ROWS.append(norm)
                    if epoch_t >= newest_epoch:
                        newest_epoch = epoch_t
                        newest_norm = norm
                _last_local_epoch = newest_epoch

        if newest_norm is not None:
            parsed = vc.row_tuple_to_parsed_dict(newest_norm)
            _apply_reading_state(parsed, newest_epoch, data_json=None, emit_sockets=False)
            print(f"[INIT] Hydrated {len(RECENT_ROWS)} rows from master CSV (latest={vc.utc_iso(newest_epoch)}).")
    except Exception as e:
        print(f"[INIT WARN] Failed hydrating master CSV: {e}")


# ================= THRESHOLD EVALUATION & AUTO EVENTS =================
def trigger_auto_event_creation(fault_type: str, cooldown_secs: int = 30):
    """Automatically snapshot last 10 min on threshold breach (debounced per fault_type)."""
    now = time.time()
    with _auto_event_lock:
        last_time = _last_auto_event_time.get(fault_type, 0)
        if now - last_time < cooldown_secs:
            return False, "Cooldown active"
        _last_auto_event_time[fault_type] = now

    def _worker():
        try:
            success, msg = extract_and_save_event(fault_type)
            if success:
                print(f"[AUTO EVENT] Saved: {msg}")
                socketio.emit('event_logged', {
                    'fault_type': fault_type,
                    'message': msg,
                    'timestamp': vc.utc_iso(time.time()),
                })
            else:
                print(f"[AUTO EVENT] Notice: {msg}")
        except Exception as err:
            print(f"[AUTO EVENT ERROR] {err}")

    threading.Thread(target=_worker, daemon=True).start()
    return True, "Triggered"


def check_threshold_violation(max_val, min_val_or_thresh, maybe_thresh=None):
    return vc.check_threshold_violation(max_val, min_val_or_thresh, maybe_thresh)


def _apply_reading_state(parsed: dict, epoch_t: float, data_json=None, emit_sockets: bool = True):
    """Update latest_kpi + latest_status, check all axes, emit alert & trigger event if breached."""
    global _last_data_received_ts
    ts_iso = vc.utc_iso(epoch_t)
    kpi_snap = vc.build_kpi_snapshot(data_json, parsed, ts_iso)
    with kpi_lock:
        latest_kpi.update(kpi_snap)

    with thresh_lock:
        thresh_snap = dict(THRESHOLDS)

    alert_status, alert_axis, breach_val = vc.check_all_axes_violations(parsed, thresh_snap)

    peaks = parsed.get('fft_peaks') or []
    top_freq = peaks[0][0] if peaks else (vc.to_float(parsed.get('fft1_freq')) or 0.0)
    max_az = vc.to_float(parsed.get('max_az'))
    _last_data_received_ts = time.time()

    with status_lock:
        latest_status['timestamp'] = ts_iso
        latest_status['magnitude'] = max_az if max_az is not None else 0.0
        latest_status['frequency'] = float(top_freq) if top_freq is not None else 0.0
        latest_status['db_connected'] = DB_ENABLED

        if alert_status:
            axis_name = alert_axis or 'Z'
            latest_status['status'] = f'{alert_status} Threshold'
            latest_status['alert'] = True
            latest_status['alert_type'] = f'{axis_name}-Axis {alert_status} Threshold'
            latest_status['alert_time'] = ts_iso
        else:
            latest_status['status'] = 'Normal'
            latest_status['alert'] = False
            latest_status['alert_type'] = None

    if alert_status and emit_sockets:
        axis_name = alert_axis or 'Z'
        mag_val = breach_val if breach_val is not None else (max_az or 0.0)
        socketio.emit('alert_notification', {
            'alert_type': f'{axis_name}-Axis {alert_status} Threshold',
            'timestamp': ts_iso,
            'magnitude': mag_val,
            'axis': axis_name,
            'ai_confidence': None,
        })
        trigger_auto_event_creation(f"{axis_name}-Axis {alert_status} Threshold")

    return alert_status, alert_axis


def _append_row_locally(norm_row: list, csv_row: list, date_str: str):
    """Append one row to master CSV, date CSV, and RECENT_ROWS ring buffer."""
    global _last_local_epoch
    epoch_t = float(norm_row[1])
    with csv_lock:
        with open(MASTER_CSV, 'a', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(csv_row)
        if date_str:
            date_csv = get_date_csv(date_str)
            init_date_csv(date_csv)
            with open(date_csv, 'a', newline='', encoding='utf-8') as f:
                csv.writer(f).writerow(csv_row)

    with data_lock:
        RECENT_ROWS.append(norm_row)
        if epoch_t > _last_local_epoch:
            _last_local_epoch = epoch_t


def _push_remote_async(parsed: dict, date_str: str, epoch_time: float, filename: str, csv_content: str | None = None):
    """Non-blocking background push to Aiven DB and optional FORWARD_URL."""
    def _worker():
        if DB_ENABLED:
            try:
                row_dict = _db.parsed_to_row_dict(parsed, date_str, epoch_time)
                _db.insert_row(row_dict)
            except Exception as _db_err:
                print(f"[LOCAL DB WARN] insert_row failed: {_db_err}")

        if FORWARD_URL and csv_content:
            try:
                import requests as _req
                headers = {'X-API-Key': ESP32_API_KEY} if ESP32_API_KEY else {}
                files = {'file': (filename, csv_content.encode('utf-8'), 'text/csv')}
                _req.post(FORWARD_URL, files=files, headers=headers, timeout=5)
            except Exception as fwd_err:
                print(f"[FORWARD WARNING] Could not forward to {FORWARD_URL}: {fwd_err}")

    threading.Thread(target=_worker, daemon=True).start()


def store_teensy_data(filename: str, parsed: dict, date_str: str, epoch_time: float, csv_content: str | None = None):
    """Write row locally synchronously, push remotely asynchronously. Returns (alert_status, alert_axis)."""
    csv_row = vc.parsed_to_csv_row(parsed, date_str, epoch_time)
    norm_row = vc.normalize_row_tuple(csv_row)
    _append_row_locally(norm_row, csv_row, date_str)
    _push_remote_async(parsed, date_str, epoch_time, filename, csv_content=csv_content)
    print(f"[STORED] {filename} -> {date_str}")
    with thresh_lock:
        alert_status, alert_axis, _ = vc.check_all_axes_violations(parsed, THRESHOLDS)
    return alert_status, alert_axis


# ================= DATA QUERY (IN-MEMORY + DATE CSVs) =================
def _read_csv_rows(filepath: str) -> list:
    out = []
    if not os.path.exists(filepath):
        return out
    try:
        with open(filepath, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            _ = next(reader, None)  # header
            for raw in reader:
                if not raw:
                    continue
                norm = vc.normalize_row_tuple(raw)
                if norm[1] is not None:
                    out.append(norm)
    except Exception as e:
        print(f"[READ CSV WARN] {filepath}: {e}")
    return out


def _date_strings_for_range(start_t: float, end_t: float) -> list:
    """Return sorted YYYY-MM-DD strings covering [start_t, end_t] +/- 1 day margin."""
    d0 = (datetime.fromtimestamp(start_t, tz=timezone.utc) - timedelta(days=1)).date()
    d1 = (datetime.fromtimestamp(end_t, tz=timezone.utc) + timedelta(days=1)).date()
    dates = []
    cur = d0
    while cur <= d1:
        dates.append(cur.strftime('%Y-%m-%d'))
        cur += timedelta(days=1)
    return dates


def get_axis_data(axis='Z', metric='acceleration', last_n_minutes=None, start_time=None, end_time=None):
    """
    Serve live requests in <1ms from RECENT_ROWS ring buffer, and historical queries
    from date-partitioned CSVs (falling back to master_data.csv if needed).
    """
    try:
        axis_upper = (axis or 'Z').upper()
        metric_lower = (metric or 'acceleration').lower()
        rows = []

        if start_time is not None and end_time is not None:
            st = float(start_time)
            et = float(end_time)
            loaded = []
            found_date_file = False
            for d_str in _date_strings_for_range(st, et):
                p = os.path.join(DATA_BY_DATE_DIR, d_str, f"data_{d_str}.csv")
                if os.path.exists(p):
                    found_date_file = True
                    loaded.extend(_read_csv_rows(p))
            if not found_date_file:
                with csv_lock:
                    loaded = _read_csv_rows(MASTER_CSV)
            rows = [r for r in loaded if st <= r[1] <= et]
        else:
            mins = float(last_n_minutes) if last_n_minutes is not None else 10.0
            with data_lock:
                buf = list(RECENT_ROWS)
            if not buf:
                return None
            max_t = max(r[1] for r in buf)
            cutoff = max_t - (mins * 60.0)
            oldest_buf = min(r[1] for r in buf)

            if cutoff >= oldest_buf or len(buf) < 20_000:
                rows = [r for r in buf if r[1] >= cutoff]
            else:
                loaded = []
                for d_str in _date_strings_for_range(cutoff, max_t):
                    p = os.path.join(DATA_BY_DATE_DIR, d_str, f"data_{d_str}.csv")
                    if os.path.exists(p):
                        loaded.extend(_read_csv_rows(p))
                if not loaded:
                    loaded = buf
                rows = [r for r in loaded if r[1] >= cutoff]

        if not rows:
            return None

        rows.sort(key=lambda r: r[1])

        if metric_lower == 'displacement':
            if axis_upper == 'Z':
                max_idx = vc.COL_INDEX['Disp_Peak_Z_mm']
                min_idx = vc.COL_INDEX['Disp_P2P_Z_mm']
                avg_idx = vc.COL_INDEX['Disp_RMS_Z_mm']
            elif axis_upper == 'Y':
                max_idx = vc.COL_INDEX['Disp_Peak_Y_mm']
                min_idx = vc.COL_INDEX['Disp_P2P_Y_mm']
                avg_idx = vc.COL_INDEX['Disp_RMS_Y_mm']
            else:
                max_idx = vc.COL_INDEX['Disp_Peak_X_mm']
                min_idx = vc.COL_INDEX['Disp_P2P_X_mm']
                avg_idx = vc.COL_INDEX['Disp_RMS_X_mm']
        else:
            if axis_upper == 'Z':
                max_idx = vc.COL_INDEX['Max_Az']
                min_idx = vc.COL_INDEX['Min_Az']
                avg_idx = vc.COL_INDEX['Mean_Az']
            elif axis_upper == 'Y':
                max_idx = vc.COL_INDEX['Max_Ay']
                min_idx = vc.COL_INDEX['Min_Ay']
                avg_idx = vc.COL_INDEX['Mean_Ay']
            else:
                max_idx = vc.COL_INDEX['Max_Ax']
                min_idx = vc.COL_INDEX['Min_Ax']
                avg_idx = vc.COL_INDEX['Mean_Ax']

        fft_idx = vc.COL_INDEX['FFT1_Freq']

        timestamps = []
        max_values = []
        min_values = []
        avg_values = []
        frequencies = []

        for r in rows:
            mx = r[max_idx]
            mn = r[min_idx]
            if mx is None or mn is None:
                continue
            av = r[avg_idx] if r[avg_idx] is not None else (mx + mn) / 2.0
            timestamps.append(r[1])
            max_values.append(mx)
            min_values.append(mn)
            avg_values.append(av)
            if r[fft_idx] is not None:
                frequencies.append(r[fft_idx])

        if not timestamps:
            return None

        datetime_labels = [vc.utc_iso(ts) for ts in timestamps]
        avg_freq = sum(frequencies) / len(frequencies) if frequencies else None

        with thresh_lock:
            axis_thresh = THRESHOLDS.get(f'{axis_upper.lower()}_axis', {}).get(metric_lower)
        alerts = vc.compute_chart_alerts(datetime_labels, max_values, min_values, axis_thresh)

        return {
            'timestamps': timestamps,
            'datetime_labels': datetime_labels,
            'max': max_values,
            'min': min_values,
            'avg': avg_values,
            'frequency': float(avg_freq) if avg_freq is not None else None,
            'axis': axis_upper,
            'metric': metric_lower,
            'alerts': alerts,
        }
    except Exception as e:
        print(f"[ERROR] get_axis_data: {e}")
        return None


# ================= EVENT EXTRACTION (LOCAL + CLOUD SYNC) =================
def extract_and_save_event(fault_type: str):
    """Save last 10 minutes as a canonical 33-col event CSV locally and push to Aiven."""
    try:
        safe_fault_name = vc.sanitize_fault_name(fault_type)
        events_subdir = os.path.join(EVENTS_DIR, safe_fault_name)
        os.makedirs(events_subdir, exist_ok=True)

        with data_lock:
            buf = list(RECENT_ROWS)
        if not buf and os.path.exists(MASTER_CSV):
            with csv_lock:
                buf = _read_csv_rows(MASTER_CSV)
        if not buf:
            return False, "No data available."

        max_t = max(r[1] for r in buf)
        cutoff = max_t - (10 * 60)
        event_rows = sorted((r for r in buf if r[1] >= cutoff), key=lambda r: r[1])
        if not event_rows:
            return False, "No data available in the last 10 minutes."

        csv_content = vc.rows_to_csv_string(event_rows)
        now_dt = datetime.now(timezone.utc)
        timestamp = f"{now_dt.strftime('%Y%m%d_%H%M%S')}_{now_dt.microsecond // 1000:03d}"
        filename = f"event_{timestamp}.csv"
        filepath = os.path.join(events_subdir, filename)

        with open(filepath, 'w', encoding='utf-8', newline='') as f:
            f.write(csv_content)

        if DB_ENABLED:
            def _push_ev():
                try:
                    _db.save_event(safe_fault_name, filename, csv_content, source='local')
                except Exception as ev_err:
                    print(f"[EVENT SYNC WARN] Could not push event to Aiven: {ev_err}")
            threading.Thread(target=_push_ev, daemon=True).start()

        return True, f"Event saved successfully to {os.path.join('events', safe_fault_name, filename)}"
    except Exception as e:
        print(f"[ERROR] extract_and_save_event: {e}")
        return False, str(e)


# ================= AIVEN SYNC, RPi PULL & HOUSEKEEPING =================
def _pull_from_rpi(last_local_epoch: float):
    """Layer 2 recovery: pull missed CSV files directly from RPi client if configured."""
    if not RPI_CLIENT_URL:
        return 0
    try:
        import requests as _req
        resp = _req.get(f"{RPI_CLIENT_URL}/list-pending", timeout=10)
        if resp.status_code != 200:
            return 0

        files = resp.json().get('files', [])
        new_files = [f for f in files if f.get('epoch', 0) > last_local_epoch]
        if not new_files:
            return 0

        pulled = 0
        for file_info in new_files:
            fname = os.path.basename(file_info['filename'])
            try:
                fr = _req.get(f"{RPI_CLIENT_URL}/fetch-file/{fname}", timeout=10)
                if fr.status_code == 200:
                    fpath = os.path.join(UPLOAD_DIR, fname)
                    with open(fpath, 'w', encoding='utf-8') as f:
                        f.write(fr.text)
                    parsed = vc.parse_teensy_string(fr.text)
                    date_str, epoch_t = vc.epoch_from_filename(fname)
                    if epoch_t > last_local_epoch:
                        store_teensy_data(fname, parsed, date_str, epoch_t)
                        pulled += 1
            except Exception as fe:
                print(f"[SYNC-RPi] Failed to fetch {fname}: {fe}")
        return pulled
    except Exception as e:
        print(f"[SYNC-RPi] Error: {e}")
        return 0


def _sync_thresholds_with_aiven():
    """Keep 4-level thresholds synchronized bidirectionally between local & Aiven DB."""
    global THRESHOLDS, _thresholds_updated_at
    if not DB_ENABLED:
        return
    try:
        db_thresh, db_ts = _db.load_thresholds()
        if db_thresh and db_ts > _thresholds_updated_at:
            with thresh_lock:
                vc.apply_threshold_payload(THRESHOLDS, db_thresh)
                _thresholds_updated_at = db_ts
                payload = json.loads(json.dumps(THRESHOLDS))
            _save_local_thresholds_file(payload, _thresholds_updated_at)
            _apply_existing_status_threshold_recheck()
            socketio.emit('thresholds_updated', payload)
        elif _thresholds_updated_at > db_ts:
            with thresh_lock:
                payload = json.loads(json.dumps(THRESHOLDS))
                ts = _thresholds_updated_at
            _db.save_thresholds(payload, ts)
    except Exception as e:
        print(f"[SYNC THRESHOLDS WARN] {e}")


def _apply_existing_status_threshold_recheck():
    """Update alert indicator immediately after synced threshold change if data exists."""
    with data_lock:
        if not RECENT_ROWS:
            return
        newest = RECENT_ROWS[-1]
    parsed = vc.row_tuple_to_parsed_dict(newest)
    _apply_reading_state(parsed, newest[1], data_json=None, emit_sockets=False)


def _sync_events_from_aiven():
    """Download any cloud-created events since _last_synced_event_ts into local events/ folder."""
    global _last_synced_event_ts
    if not DB_ENABLED:
        return
    try:
        events = _db.fetch_events_since(_last_synced_event_ts)
        for ev in events:
            safe_fault = vc.sanitize_fault_name(ev['fault_type'])
            fname = os.path.basename(ev['filename'])
            subdir = os.path.join(EVENTS_DIR, safe_fault)
            os.makedirs(subdir, exist_ok=True)
            fpath = os.path.join(subdir, fname)
            if not os.path.exists(fpath):
                with open(fpath, 'w', encoding='utf-8', newline='') as f:
                    f.write(ev['csv_content'])
                socketio.emit('event_logged', {
                    'fault_type': safe_fault,
                    'message': f"Synced from cloud: {safe_fault}/{fname}",
                    'timestamp': vc.utc_iso(ev.get('created_at') or time.time()),
                })
            if ev.get('created_at', 0.0) > _last_synced_event_ts:
                _last_synced_event_ts = ev['created_at']
    except Exception as e:
        print(f"[SYNC EVENTS WARN] {e}")


def sync_from_aiven():
    """
    Synchronize telemetry, thresholds, and events with Aiven PostgreSQL:
      1. Pull rows newer than _last_local_epoch and store locally (CSV + RECENT_ROWS)
      2. Update local KPI + status + threshold check from newest synced row
      3. Purge ONLY rows older than AIVEN_RETENTION_MINUTES so web charts keep live history
      4. Sync 4-level thresholds bidirectionally & download cloud-created event CSVs
    """
    global _last_local_epoch
    if not DB_ENABLED:
        return

    try:
        with data_lock:
            last_epoch = _last_local_epoch

        rows = _db.fetch_rows_since(last_epoch)
        max_synced_epoch = last_epoch

        if rows:
            print(f"[SYNC] Writing {len(rows)} missed rows to local CSV files...")
            newest_norm = None
            for r in rows:
                norm = vc.normalize_row_tuple(r)
                epoch = norm[1]
                if epoch is None or epoch <= _last_local_epoch:
                    continue
                date_str = norm[0] or datetime.fromtimestamp(epoch, tz=timezone.utc).strftime('%Y-%m-%d')
                parsed = vc.row_tuple_to_parsed_dict(norm)
                csv_row = vc.parsed_to_csv_row(parsed, date_str, epoch)
                _append_row_locally(norm, csv_row, date_str)
                if epoch > max_synced_epoch:
                    max_synced_epoch = epoch
                    newest_norm = norm

            if newest_norm is not None:
                parsed = vc.row_tuple_to_parsed_dict(newest_norm)
                _apply_reading_state(parsed, newest_norm[1], data_json=None, emit_sockets=True)
                socketio.emit('data_update', {'type': 'sync', 'count': len(rows)})

        # Retention purge: keep recent AIVEN_RETENTION_MINUTES in Aiven for web dashboard!
        if max_synced_epoch > 0:
            retention_cutoff = time.time() - (AIVEN_RETENTION_MINUTES * 60.0)
            purge_before = min(max_synced_epoch, retention_cutoff)
            if purge_before > 0:
                try:
                    _db.delete_rows_before_epoch(purge_before)
                except Exception as del_err:
                    print(f"[SYNC] Purge warning (non-fatal): {del_err}")

        _sync_thresholds_with_aiven()
        _sync_events_from_aiven()
        _pull_from_rpi(last_epoch)
    except Exception as e:
        print(f"[SYNC ERROR] Could not sync from Aiven: {e} — continuing locally.")


def cleanup_old_uploads():
    """Prune raw uploaded CSVs in uploads/ older than UPLOAD_RETENTION_DAYS."""
    if UPLOAD_RETENTION_DAYS <= 0 or not os.path.exists(UPLOAD_DIR):
        return 0
    cutoff = time.time() - (UPLOAD_RETENTION_DAYS * 86400.0)
    removed = 0
    for root, _, files in os.walk(UPLOAD_DIR):
        for fname in files:
            if not fname.endswith('.csv'):
                continue
            fpath = os.path.join(root, fname)
            try:
                if os.path.getmtime(fpath) < cutoff:
                    os.remove(fpath)
                    removed += 1
            except OSError:
                pass
    if removed:
        print(f"[CLEANUP] Removed {removed} old raw CSV(s) from {UPLOAD_DIR}")
    return removed


def sync_loop():
    """Continuous 24/7 background loop for Aiven sync, DB reconnect, and disk cleanup."""
    global DB_ENABLED, _db_reconnect_ts
    print("[SYNC] Background sync thread started.")
    last_cleanup = 0.0
    while True:
        try:
            now = time.time()
            if not DB_ENABLED and _db.is_configured() and (now - _db_reconnect_ts >= 30.0):
                _db_reconnect_ts = now
                try:
                    _db.init_db()
                    DB_ENABLED = True
                    with status_lock:
                        latest_status['db_connected'] = True
                    print("[LOCAL] Reconnected to Aiven DB.")
                except Exception:
                    pass

            if DB_ENABLED:
                sync_from_aiven()

            if now - last_cleanup >= 3600.0:
                last_cleanup = now
                cleanup_old_uploads()
        except Exception as e:
            print(f"[SYNC LOOP ERROR] {e}")
        time.sleep(10)


def start_background_tasks():
    """Initialise DB connection cleanly and launch the background sync daemon."""
    global DB_ENABLED
    if _db.is_configured():
        try:
            _db.init_db()
            DB_ENABLED = True
            with status_lock:
                latest_status['db_connected'] = True
            print("[LOCAL] Aiven DB enabled.")
        except Exception as err:
            DB_ENABLED = False
            print(f"[LOCAL WARNING] Aiven DB init failed: {err} — will retry automatically.")
    else:
        print("[LOCAL] DATABASE_URL not set — running in local-only mode.")

    if DB_ENABLED:
        sync_from_aiven()
    threading.Thread(target=sync_loop, daemon=True).start()


# Startup local preparation (safe during imports & tests)
_load_local_thresholds_file()
initialize_master_csv()
scan_existing_dates()
_hydrate_from_master_csv()


# ================= ROUTES =================
@app.route("/")
def home():
    return render_template("dashboard.html", is_cloud=False)


@app.route("/health")
def health():
    age_sec = round(time.time() - _last_data_received_ts, 1) if _last_data_received_ts > 0 else None
    return jsonify({
        "status": "ok",
        "server": "local",
        "db_connected": DB_ENABLED,
        "last_data_age_sec": age_sec,
        "buffer_rows": len(RECENT_ROWS),
    }), 200


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
        if request.is_json:
            return receive_esp32_data()

        if "file" not in request.files:
            return jsonify({"error": "no file"}), 400

        file = request.files["file"]
        if not file.filename:
            return jsonify({"error": "empty filename"}), 400

        safe_name = os.path.basename(file.filename)
        filepath = os.path.join(UPLOAD_DIR, safe_name)
        file.save(filepath)

        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            csv_content = f.read()

        parsed = vc.parse_teensy_string(csv_content)
        date_str, epoch_time = vc.epoch_from_filename(safe_name)

        store_teensy_data(safe_name, parsed, date_str, epoch_time, csv_content=csv_content)
        alert_status, alert_axis = _apply_reading_state(parsed, epoch_time, data_json=None, emit_sockets=True)

        socketio.emit('data_update', {'type': 'file', 'filename': safe_name})
        return jsonify({"status": "received", "date": date_str, "alert": alert_status, "axis": alert_axis})
    except Exception as e:
        print(f"[ERROR] Upload: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/data", methods=["POST"])
def receive_esp32_data():
    try:
        if not request.is_json:
            return jsonify({"error": "Expected JSON payload"}), 400

        data = request.get_json()
        core_id = data.get("core", 0)
        samples = data.get("samples", 0)

        filename, csv_content, date_str, epoch_t = vc.reconstruct_csv_from_esp32_json(data)

        csv_filepath = os.path.join(UPLOAD_DIR, filename)
        try:
            with open(csv_filepath, 'w', encoding='utf-8') as f:
                f.write(csv_content)
        except Exception as fe:
            print(f"[RPi Receiver WARNING] Could not write file {csv_filepath}: {fe}")

        parsed = vc.parse_teensy_string(csv_content)
        store_teensy_data(filename, parsed, date_str, epoch_t, csv_content=csv_content)
        alert_status, alert_axis = _apply_reading_state(parsed, epoch_t, data_json=data, emit_sockets=True)

        socketio.emit('data_update', {
            'type': 'esp32_direct',
            'filename': filename,
            'core': core_id,
            'samples': samples,
        })

        return jsonify({
            "status": "received",
            "filename": filename,
            "core": core_id,
            "alert": alert_status,
            "axis": alert_axis,
        }), 200
    except Exception as e:
        print(f"[ERROR] /data: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/axis_data")
def api_axis_data():
    axis = request.args.get('axis', 'Z')
    metric = request.args.get('metric', 'acceleration')
    minutes = request.args.get('minutes')
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')

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
        snap = dict(latest_status)
        snap['db_connected'] = DB_ENABLED
        return jsonify(snap)


@app.route("/api/latest_kpi")
def api_latest_kpi():
    with kpi_lock:
        return jsonify(json.loads(json.dumps(latest_kpi)))


@app.route("/api/thresholds", methods=["GET", "POST"])
def api_thresholds():
    global THRESHOLDS, _thresholds_updated_at
    if request.method == "POST":
        try:
            data = request.get_json(silent=True) or {}
            with thresh_lock:
                vc.apply_threshold_payload(THRESHOLDS, data)
                payload = json.loads(json.dumps(THRESHOLDS))
                _thresholds_updated_at = time.time()
                ts = _thresholds_updated_at

            _save_local_thresholds_file(payload, ts)
            if DB_ENABLED:
                try:
                    _db.save_thresholds(payload, ts)
                except Exception as db_err:
                    print(f"[THRESHOLDS SYNC WARN] {db_err}")

            _apply_existing_status_threshold_recheck()
            socketio.emit('thresholds_updated', payload)
            return jsonify({"status": "success", "thresholds": payload})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 400

    with thresh_lock:
        return jsonify(json.loads(json.dumps(THRESHOLDS)))


@app.route("/download/<filename>")
def download_file(filename):
    return send_from_directory(UPLOAD_DIR, os.path.basename(filename), as_attachment=True)


# ================= RPi SYNC & RECOVERY ENDPOINTS =================
@app.route("/status", methods=["GET"])
def rpi_status():
    try:
        csv_files = [f for f in os.listdir(UPLOAD_DIR) if f.endswith('.csv')] if os.path.exists(UPLOAD_DIR) else []
        return jsonify({
            "status": "running",
            "device": "Raspberry Pi Receiver Hub",
            "time_utc": datetime.now(timezone.utc).isoformat(),
            "csv_files_count": len(csv_files),
            "latest_status": latest_status,
        }), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/list-pending", methods=["GET"])
def list_pending():
    try:
        files = []
        if os.path.exists(UPLOAD_DIR):
            for fname in sorted(os.listdir(UPLOAD_DIR)):
                if not fname.endswith('.csv'):
                    continue
                date_str, epoch = vc.epoch_from_filename(fname)
                fpath = os.path.join(UPLOAD_DIR, fname)
                files.append({
                    'filename': fname,
                    'date': date_str,
                    'epoch': epoch,
                    'size_bytes': os.path.getsize(fpath) if os.path.exists(fpath) else 0,
                })
        return jsonify({'status': 'ok', 'count': len(files), 'files': files}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route("/fetch-file/<filename>", methods=["GET"])
def fetch_file(filename):
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
    data = request.get_json(silent=True) or {}
    if 'fault_type' not in data:
        return jsonify({"error": "fault_type is required"}), 400

    fault_type = data['fault_type']
    success, msg = extract_and_save_event(fault_type)
    if success:
        return jsonify({"status": "success", "message": msg}), 200
    return jsonify({"status": "error", "message": msg}), 500


@app.route("/api/events")
def api_list_events():
    seen = {}
    if os.path.exists(EVENTS_DIR):
        for root, _, files in os.walk(EVENTS_DIR):
            for fname in files:
                if fname.endswith('.csv'):
                    fault_type = os.path.basename(root)
                    seen[fname] = {
                        "fault_type": fault_type,
                        "filename": fname,
                        "path": f"{fault_type}/{fname}",
                    }
    if DB_ENABLED:
        for item in _db.list_events(limit=200):
            if item['filename'] not in seen:
                seen[item['filename']] = item
    files_list = sorted(seen.values(), key=lambda x: x.get('filename', ''), reverse=True)
    return jsonify(files_list)


@app.route("/api/events/download/<path:filepath>")
def api_download_event(filepath):
    safe_fault = vc.sanitize_fault_name(os.path.dirname(filepath))
    safe_name = os.path.basename(filepath)
    directory = os.path.join(EVENTS_DIR, safe_fault)
    if os.path.exists(os.path.join(directory, safe_name)):
        return send_from_directory(directory, safe_name, as_attachment=True)
    if DB_ENABLED:
        csv_content = _db.get_event_csv(safe_name)
        if csv_content is not None:
            from flask import Response
            return Response(
                csv_content,
                mimetype='text/csv',
                headers={'Content-Disposition': f'attachment; filename="{safe_name}"'},
            )
    return jsonify({"error": "Event file not found"}), 404


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
    axis = data.get('axis', 'Z')
    metric = data.get('metric', 'acceleration')
    minutes = data.get('minutes', 10)
    start_time = data.get('start_time')
    end_time = data.get('end_time')

    if start_time and end_time:
        graph_data = get_axis_data(axis, metric=metric, start_time=start_time, end_time=end_time)
    else:
        graph_data = get_axis_data(axis, metric=metric, last_n_minutes=minutes)

    if graph_data:
        emit('axis_data', graph_data)
    else:
        emit('axis_data', {'error': 'No data available', 'axis': axis})


# ================= MAIN =================
if __name__ == "__main__":
    start_background_tasks()
    port = int(os.environ.get("PORT", 5000))
    print("=" * 60)
    print("VIBRATION MONITORING SERVER (Local Hub)")
    print(f"Listening on: 0.0.0.0:{port}")
    print(f"CSV schema: {len(CSV_HEADERS)} columns | Buffer: {len(RECENT_ROWS)} rows")
    print(f"Aiven DB: {'Connected' if DB_ENABLED else 'Offline/Disabled'}")
    print(f"Retention: Aiven={AIVEN_RETENTION_MINUTES}m | Uploads={UPLOAD_RETENTION_DAYS}d")
    if FORWARD_URL:
        print(f"Forwarding destination: {FORWARD_URL}")
    print("=" * 60)
    socketio.run(app, host="0.0.0.0", port=port, debug=False, allow_unsafe_werkzeug=True)
