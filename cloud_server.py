"""
cloud_server.py — Render (Cloud) Deployment
Optimized for continuous 24/7 operation:
  - Gevent async mode under gunicorn on Render, clean threading fallback locally
  - Shared core parsing/threshold/KPI logic via vibration_core
  - 33-column storage & queries (Z/X/Y acceleration + displacement + 5 FFT peaks)
  - Bidirectional Web <-> Local Hub sync via Aiven PostgreSQL:
      * Live telemetry & threshold violations -> real-time Socket.IO updates
      * Shared thresholds (app_thresholds table) -> instant Web <-> Local propagation
      * Shared fault event CSVs (vibration_events table) -> survive ephemeral Render restarts
"""

try:
    from gevent import monkey
    monkey.patch_all()  # MUST be first import when gevent is available
    import gevent
    try:
        from psycogreen.gevent import patch_psycopg
        patch_psycopg()
    except ImportError:
        print("[CLOUD WARNING] psycogreen not installed — DB calls may block event loop.", flush=True)
    GEVENT_AVAILABLE = True
except ImportError:
    gevent = None
    GEVENT_AVAILABLE = False

import copy
import os
import threading
import time
from datetime import datetime, timezone
from threading import Lock

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from flask import Flask, Response, jsonify, render_template, request, send_from_directory
from flask_cors import CORS
from flask_socketio import SocketIO, emit

import db
import vibration_core as vc

# ================= CONFIGURATION =================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
TEMPLATE_DIR = os.path.join(BASE_DIR, "templates")
EVENTS_DIR = os.path.join(BASE_DIR, "events")

os.makedirs(STATIC_DIR, exist_ok=True)
os.makedirs(TEMPLATE_DIR, exist_ok=True)

# ================= THRESHOLDS =================
THRESHOLDS = vc.get_default_thresholds()
_thresholds_updated_at = 0.0
thresh_lock = Lock()

# ================= FLASK SETUP =================
app = Flask(__name__, static_folder=STATIC_DIR, template_folder=TEMPLATE_DIR)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'vibration-cloud-key')
CORS(app)
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode='gevent' if GEVENT_AVAILABLE else 'threading',
    ping_timeout=60,
    ping_interval=25,
)

ESP32_API_KEY = os.environ.get('ESP32_API_KEY', '')

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
    'server_mode': 'cloud',
    'db_connected': False,
}

kpi_lock = Lock()
latest_kpi = vc.build_kpi_snapshot(None, {}, None)

MAX_AIVEN_ROWS = 500_000
OVERFLOW_DELETE_N = 10_000

_db_connected = False
_last_seen_epoch = 0.0
_last_data_received_ts = 0.0
_last_db_sync_time = 0.0
_connected_clients = 0
_clients_lock = Lock()

_axis_data_cache = {}
_cache_lock = Lock()

_last_auto_event_time = {}
_auto_event_lock = Lock()


def _spawn_bg(fn, *args, **kwargs):
    """Spawn a background greenlet under gevent or daemon thread otherwise."""
    if GEVENT_AVAILABLE and gevent is not None:
        return gevent.spawn(fn, *args, **kwargs)
    t = threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True)
    t.start()
    return t


def _clear_axis_cache():
    with _cache_lock:
        _axis_data_cache.clear()


# ================= THRESHOLD EVALUATION & AUTO EVENTS =================
def trigger_auto_event_creation(fault_type: str, cooldown_secs: int = 30):
    """Automatically snapshot last 10 min into Aiven + local backup on threshold breach."""
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
                print(f"[AUTO EVENT] Saved: {msg}", flush=True)
                socketio.emit('event_logged', {
                    'fault_type': fault_type,
                    'message': msg,
                    'timestamp': vc.utc_iso(time.time()),
                })
            else:
                print(f"[AUTO EVENT] Notice: {msg}", flush=True)
        except Exception as err:
            print(f"[AUTO EVENT ERROR] {err}", flush=True)

    _spawn_bg(_worker)
    return True, "Triggered"


def check_threshold_violation(max_val, min_val_or_thresh, maybe_thresh=None):
    return vc.check_threshold_violation(max_val, min_val_or_thresh, maybe_thresh)


def _apply_reading_state(parsed: dict, epoch_t: float, data_json=None, emit_sockets: bool = True):
    """
    Update latest_kpi and latest_status atomically from a parsed telemetry row,
    evaluate 4-level thresholds across axes, emit Socket.IO notifications, and
    trigger automatic event snapshot creation when breached.
    Returns (alert_status, alert_axis).
    """
    global _last_seen_epoch, _last_data_received_ts, _db_connected

    ts_iso = vc.utc_iso(epoch_t)
    kpi_snap = vc.build_kpi_snapshot(data_json, parsed, ts_iso)
    with kpi_lock:
        latest_kpi.update(kpi_snap)

    with thresh_lock:
        thresh_snap = copy.deepcopy(THRESHOLDS)

    alert_status, alert_axis, breach_val = vc.check_all_axes_violations(parsed, thresh_snap)

    peaks = parsed.get('fft_peaks') or []
    top_freq = peaks[0][0] if peaks else (vc.to_float(parsed.get('fft1_freq')) or 0.0)
    max_az = vc.to_float(parsed.get('max_az'))

    if epoch_t > _last_seen_epoch:
        _last_seen_epoch = epoch_t
    _last_data_received_ts = time.time()
    _clear_axis_cache()

    with status_lock:
        latest_status['timestamp'] = ts_iso
        latest_status['magnitude'] = max_az if max_az is not None else 0.0
        latest_status['frequency'] = float(top_freq) if top_freq is not None else 0.0
        latest_status['db_connected'] = _db_connected

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


# ================= DB SYNC & BACKGROUND WATCHER =================
def _load_thresholds_from_db():
    """Sync shared thresholds from Aiven app_thresholds if newer than local copy."""
    global THRESHOLDS, _thresholds_updated_at, _db_connected
    if not db.is_configured():
        return False
    db_thresh, db_ts = db.load_thresholds()
    _db_connected = True
    if db_thresh and db_ts > _thresholds_updated_at:
        with thresh_lock:
            vc.apply_threshold_payload(THRESHOLDS, db_thresh)
            _thresholds_updated_at = db_ts
        _clear_axis_cache()
        return True
    return False


def _sync_latest_from_db(force: bool = False, emit_sockets: bool = True):
    """
    Refresh latest_kpi, latest_status, and thresholds from Aiven DB.
    Throttled to at most once per 2.0s unless force=True.
    Returns True if a newer telemetry row was observed.
    """
    global _last_db_sync_time, _db_connected, _last_seen_epoch
    now_mono = time.time()
    if not force and (now_mono - _last_db_sync_time < 2.0):
        return False
    if not db.is_configured():
        return False

    _last_db_sync_time = now_mono
    try:
        row = db.get_latest_row_dict()
        _db_connected = True
        with status_lock:
            latest_status['db_connected'] = True

        if not row or not row.get('time_sec'):
            return False

        epoch_t = float(row['time_sec'])
        if epoch_t <= _last_seen_epoch and not force:
            return False

        is_new_row = epoch_t > _last_seen_epoch
        peaks = []
        for n in range(1, 6):
            f = vc.to_float(row.get(f'fft{n}_freq'))
            m = vc.to_float(row.get(f'fft{n}_mag'))
            if f is not None and m is not None:
                peaks.append((f, m))
        row_parsed = dict(row)
        row_parsed['fft_peaks'] = peaks

        _apply_reading_state(row_parsed, epoch_t, data_json=None, emit_sockets=(is_new_row and emit_sockets))
        return is_new_row
    except Exception as e:
        _db_connected = False
        with status_lock:
            latest_status['db_connected'] = False
        print(f"[CLOUD SYNC WARN] Could not sync latest from DB: {e}", flush=True)
        return False


def _init_db_and_start_watcher():
    global _db_connected
    if not db.is_configured():
        print("[CLOUD WARNING] DATABASE_URL not set — running without Aiven DB.", flush=True)
        return

    try:
        db.init_db()
        _db_connected = True
        with status_lock:
            latest_status['db_connected'] = True
        print("[CLOUD] Aiven DB initialised.", flush=True)
    except Exception as e:
        _db_connected = False
        print(f"[CLOUD WARNING] DB init failed: {e} — watcher will retry.", flush=True)

    try:
        _load_thresholds_from_db()
    except Exception as e:
        print(f"[CLOUD WARNING] Initial threshold load failed: {e}", flush=True)

    _sync_latest_from_db(force=True, emit_sockets=False)

    last_overflow_check = 0.0
    while True:
        with _clients_lock:
            has_clients = _connected_clients > 0
        interval = 5.0 if has_clients else 15.0
        if GEVENT_AVAILABLE and gevent is not None:
            gevent.sleep(interval)
        else:
            time.sleep(interval)

        try:
            if not _db_connected:
                try:
                    db.init_db()
                    _db_connected = True
                    with status_lock:
                        latest_status['db_connected'] = True
                except Exception:
                    continue

            new_data = _sync_latest_from_db(force=True)
            if new_data:
                socketio.emit('data_update', {'type': 'db_sync'})

            thresh_changed = _load_thresholds_from_db()
            if thresh_changed:
                with thresh_lock:
                    payload = copy.deepcopy(THRESHOLDS)
                socketio.emit('thresholds_updated', payload)

            now = time.time()
            if now - last_overflow_check >= 600.0:
                last_overflow_check = now
                count = db.get_row_count()
                if count > MAX_AIVEN_ROWS:
                    trimmed = db.delete_oldest_rows(OVERFLOW_DELETE_N)
                    print(f"[OVERFLOW] Trimmed {trimmed} oldest rows (was {count}).", flush=True)
        except Exception as err:
            print(f"[CLOUD WATCHER WARN] {err}", flush=True)


if os.environ.get('TESTING') != '1':
    _spawn_bg(_init_db_and_start_watcher)


# ================= DATA QUERY (FROM AIVEN) =================
def get_axis_data(axis='Z', metric='acceleration', last_n_minutes=None, start_time=None, end_time=None):
    is_live_query = (start_time is None and end_time is None)
    axis_upper = (axis or 'Z').upper()
    metric_lower = (metric or 'acceleration').lower()
    cache_key = (axis_upper, metric_lower, last_n_minutes or 10)
    now = time.time()

    if is_live_query:
        with _cache_lock:
            if cache_key in _axis_data_cache:
                cached_res, cache_t = _axis_data_cache[cache_key]
                if now - cache_t < 3.0:
                    return cached_res

    try:
        rows = db.fetch_rows_for_chart(
            axis=axis_upper,
            start_time=start_time,
            end_time=end_time,
            last_n_minutes=last_n_minutes,
        )
        if not rows:
            return None

        col_idx = {col: i for i, col in enumerate(db.DB_COLUMNS)}

        if metric_lower == 'displacement':
            if axis_upper == 'Z':
                max_idx = col_idx['disp_peak_z_mm']
                min_idx = col_idx['disp_p2p_z_mm']
                avg_idx = col_idx['disp_rms_z_mm']
            elif axis_upper == 'Y':
                max_idx = col_idx['disp_peak_y_mm']
                min_idx = col_idx['disp_p2p_y_mm']
                avg_idx = col_idx['disp_rms_y_mm']
            else:
                max_idx = col_idx['disp_peak_x_mm']
                min_idx = col_idx['disp_p2p_x_mm']
                avg_idx = col_idx['disp_rms_x_mm']
        else:
            if axis_upper == 'Z':
                max_idx = col_idx['max_az']
                min_idx = col_idx['min_az']
                avg_idx = col_idx['mean_az']
            elif axis_upper == 'Y':
                max_idx = col_idx['max_ay']
                min_idx = col_idx['min_ay']
                avg_idx = col_idx['mean_ay']
            else:
                max_idx = col_idx['max_ax']
                min_idx = col_idx['min_ax']
                avg_idx = col_idx['mean_ax']

        fft_idx = col_idx['fft1_freq']

        timestamps = [r[1] for r in rows]
        max_values = [r[max_idx] for r in rows]
        min_values = [r[min_idx] for r in rows]
        avg_values = [r[avg_idx] for r in rows]
        frequencies = [r[fft_idx] for r in rows]

        datetime_labels = [vc.utc_iso(ts) for ts in timestamps]
        valid_freqs = [float(f) for f in frequencies if f is not None]
        avg_freq = sum(valid_freqs) / len(valid_freqs) if valid_freqs else None

        with thresh_lock:
            axis_thresh = THRESHOLDS.get(f'{axis_upper.lower()}_axis', {}).get(metric_lower)
        alerts = vc.compute_chart_alerts(datetime_labels, max_values, min_values, axis_thresh)

        result = {
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

        if is_live_query:
            with _cache_lock:
                if len(_axis_data_cache) > 32:
                    _axis_data_cache.clear()
                _axis_data_cache[cache_key] = (result, now)

        return result
    except Exception as e:
        print(f"[ERROR] get_axis_data (cloud): {e}", flush=True)
        return None


# ================= EVENT EXTRACTION (AIVEN DB + LOCAL BACKUP) =================
def extract_and_save_event(fault_type: str):
    """Snapshot the last 10 minutes as a canonical 33-col CSV in Aiven DB and local backup."""
    try:
        safe_fault_name = vc.sanitize_fault_name(fault_type)
        rows = db.fetch_rows_for_chart(last_n_minutes=10)
        if not rows:
            return False, "No data available in the last 10 minutes."

        csv_content = vc.rows_to_csv_string(rows)
        now_dt = datetime.now(timezone.utc)
        timestamp = f"{now_dt.strftime('%Y%m%d_%H%M%S')}_{now_dt.microsecond // 1000:03d}"
        filename = f"event_{timestamp}.csv"

        db_ok = False
        if db.is_configured():
            db_ok = db.save_event(safe_fault_name, filename, csv_content, source='cloud')

        try:
            events_dir = os.path.join(EVENTS_DIR, safe_fault_name)
            os.makedirs(events_dir, exist_ok=True)
            filepath = os.path.join(events_dir, filename)
            with open(filepath, 'w', encoding='utf-8', newline='') as f:
                f.write(csv_content)
        except Exception as fs_err:
            if not db_ok:
                return False, f"Failed saving event: {fs_err}"

        return True, f"Event saved successfully ({safe_fault_name}/{filename})"
    except Exception as e:
        print(f"[ERROR] extract_and_save_event: {e}", flush=True)
        return False, str(e)


# ================= ROUTES =================
@app.route("/")
def home():
    return render_template("dashboard.html", is_cloud=True)


@app.route("/health")
def health():
    """Keep-alive and DB diagnostics endpoint."""
    age_sec = round(time.time() - _last_data_received_ts, 1) if _last_data_received_ts > 0 else None
    return jsonify({
        "status": "ok",
        "server": "cloud",
        "db_connected": _db_connected,
        "last_data_age_sec": age_sec,
    }), 200


@app.route("/api/available-dates", methods=['GET'])
def get_available_dates():
    try:
        dates = db.get_available_dates()
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
        filename = file.filename
        if not filename:
            return jsonify({"error": "empty filename"}), 400

        csv_content = file.read().decode('utf-8', errors='replace')
        parsed = vc.parse_teensy_string(csv_content)
        date_str, epoch_t = vc.epoch_from_filename(filename)

        row_dict = db.parsed_to_row_dict(parsed, date_str, epoch_t)
        if db.is_configured():
            try:
                db.insert_row(row_dict)
            except Exception as db_err:
                print(f"[CLOUD DB WARN] insert_row: {db_err}", flush=True)

        alert_status, alert_axis = _apply_reading_state(parsed, epoch_t, data_json=None, emit_sockets=True)
        socketio.emit('data_update', {'type': 'file', 'filename': filename})
        return jsonify({"status": "received", "date": date_str, "alert": alert_status, "axis": alert_axis})
    except Exception as e:
        print(f"[ERROR] Upload: {e}", flush=True)
        return jsonify({"error": str(e)}), 500


@app.route("/data", methods=["POST"])
def receive_esp32_data():
    if ESP32_API_KEY:
        incoming_key = request.headers.get('X-API-Key', '')
        if incoming_key != ESP32_API_KEY:
            print(f"[SECURITY] /data blocked — wrong X-API-Key from {request.remote_addr}", flush=True)
            return jsonify({"error": "Unauthorized"}), 401
    try:
        if not request.is_json:
            return jsonify({"error": "Expected JSON payload"}), 400

        data = request.get_json()
        filename, csv_content, date_str, epoch_t = vc.reconstruct_csv_from_esp32_json(data)
        parsed = vc.parse_teensy_string(csv_content)

        row_dict = db.parsed_to_row_dict(parsed, date_str, epoch_t)
        if db.is_configured():
            try:
                db.insert_row(row_dict)
            except Exception as db_err:
                print(f"[ESP32 DB WARN] insert_row: {db_err}", flush=True)

        alert_status, alert_axis = _apply_reading_state(parsed, epoch_t, data_json=data, emit_sockets=True)
        socketio.emit('data_update', {'type': 'esp32_direct', 'filename': filename})
        return jsonify({"status": "received", "filename": filename, "alert": alert_status, "axis": alert_axis}), 200
    except Exception as e:
        print(f"[ERROR] /data: {e}", flush=True)
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
        return jsonify({"error": "No data available", "axis": axis, "metric": metric}), 404
    return jsonify(data)


@app.route("/api/latest_status")
def api_latest_status():
    _sync_latest_from_db()
    with status_lock:
        snap = dict(latest_status)
        snap['db_connected'] = _db_connected
        return jsonify(snap)


@app.route("/favicon.ico")
def favicon():
    return '', 204


@app.route("/api/latest_kpi")
def api_latest_kpi():
    _sync_latest_from_db()
    with kpi_lock:
        return jsonify(copy.deepcopy(latest_kpi))


@app.route("/api/thresholds", methods=["GET", "POST"])
def api_thresholds():
    global THRESHOLDS, _thresholds_updated_at
    if request.method == "POST":
        try:
            data = request.get_json(silent=True) or {}
            with thresh_lock:
                vc.apply_threshold_payload(THRESHOLDS, data)
                payload = copy.deepcopy(THRESHOLDS)
                _thresholds_updated_at = time.time()

            if db.is_configured():
                saved_ts = db.save_thresholds(payload, _thresholds_updated_at)
                if saved_ts:
                    _thresholds_updated_at = saved_ts

            _clear_axis_cache()
            socketio.emit('thresholds_updated', payload)
            return jsonify({"status": "success", "thresholds": payload})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 400

    with thresh_lock:
        return jsonify(copy.deepcopy(THRESHOLDS))


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
    if db.is_configured():
        for item in db.list_events(limit=200):
            seen[item['filename']] = item

    if os.path.exists(EVENTS_DIR):
        for root, _, files in os.walk(EVENTS_DIR):
            for fname in files:
                if fname.endswith('.csv') and fname not in seen:
                    fault_type = os.path.basename(root)
                    seen[fname] = {
                        "fault_type": fault_type,
                        "filename": fname,
                        "path": f"{fault_type}/{fname}",
                    }

    events_list = sorted(seen.values(), key=lambda x: x.get('filename', ''), reverse=True)
    return jsonify(events_list)


@app.route("/api/events/download/<path:filepath>")
def api_download_event(filepath):
    safe_name = os.path.basename(filepath)
    if db.is_configured():
        csv_content = db.get_event_csv(safe_name)
        if csv_content is not None:
            return Response(
                csv_content,
                mimetype='text/csv',
                headers={'Content-Disposition': f'attachment; filename="{safe_name}"'},
            )

    safe_sub = vc.sanitize_fault_name(os.path.dirname(filepath))
    directory = os.path.join(EVENTS_DIR, safe_sub)
    if os.path.exists(os.path.join(directory, safe_name)):
        return send_from_directory(directory, safe_name, as_attachment=True)
    return jsonify({"error": "Event file not found"}), 404


# ================= SOCKETIO =================
@socketio.on('connect')
def handle_connect():
    global _connected_clients
    with _clients_lock:
        _connected_clients += 1
    emit('connection_response', {'status': 'connected'})


@socketio.on('disconnect')
def handle_disconnect():
    global _connected_clients
    with _clients_lock:
        _connected_clients = max(0, _connected_clients - 1)


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
        emit('axis_data', {'error': 'No data available', 'axis': axis, 'metric': metric})


# ================= MAIN =================
if __name__ == "__main__":
    print("=" * 60)
    print("CLOUD VIBRATION MONITORING SERVER (Render)")
    print("=" * 60)
    socketio.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 5003)), debug=False)
