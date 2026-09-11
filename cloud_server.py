"""
cloud_server.py — Render (Cloud) Deployment
Identical logic to server.py but:
  - No local CSV writes (Render filesystem is ephemeral)
  - All storage goes to Neon DB via db.py
  - Reads data from Neon DB for /api/axis_data
  - Uses gevent async_mode for Render compatibility
  - /health endpoint prevents Render sleep (ping via UptimeRobot)
"""

from gevent import monkey
monkey.patch_all()  # MUST be first import — patches stdlib for async

from flask import Flask, request, jsonify, render_template
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import os
import io
import random
from datetime import datetime
from threading import Lock

import db  # shared Neon module
import joblib
import numpy as np

# ================= CONFIGURATION =================
STATIC_DIR   = "static"
TEMPLATE_DIR = "templates"

os.makedirs(STATIC_DIR,   exist_ok=True)
os.makedirs(TEMPLATE_DIR, exist_ok=True)

# ================= THRESHOLDS =================
THRESHOLDS = {
    'z_axis': {
        'acceleration': {
            'over_positive':  45,
            'under_positive':  6.5,
            'under_negative': -6.5,
            'over_negative':  -20
        }
    }
}

# ================= FLASK SETUP =================
app = Flask(__name__, static_folder=STATIC_DIR, template_folder=TEMPLATE_DIR)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'vibration-cloud-key')
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='gevent')

# ---- ESP32 API Key (optional but recommended) ----
# Set ESP32_API_KEY in Render environment variables.
# The ESP32 must send: http.addHeader("X-API-Key", "<same-key>");
# Leave empty string to disable auth (useful during initial testing).
ESP32_API_KEY = os.environ.get('ESP32_API_KEY', '')

status_lock = Lock()
latest_status = {
    'timestamp':  None,
    'status':     'Waiting for data...',
    'alert':      None,
    'alert_time': None,
    'magnitude':  0,
    'frequency':  0,
    'ai_confidence': None,
    'ai_status': None
}

last_ai_auto_event_time = 0  # Timestamp of last auto-extraction

# ---- Neon transit buffer overflow protection ----
# At ~392 bytes/row, 500k rows = ~196 MB, well within 512 MB Neon free limit.
# At 15k rows/day, this covers ~33 days of local server being offline.
# When exceeded, oldest OVERFLOW_DELETE_N rows are trimmed automatically.
MAX_NEON_ROWS     = 500_000

# ================= AI MODEL LOADING =================
AI_MODEL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'anomaly_detector')
scaler_path = os.path.join(AI_MODEL_DIR, 'scaler.joblib')
model_path = os.path.join(AI_MODEL_DIR, 'isolation_forest_model.joblib')

try:
    ai_scaler = joblib.load(scaler_path)
    ai_model = joblib.load(model_path)
    print(f"[AI MODEL] Successfully loaded Isolation Forest and Scaler from {AI_MODEL_DIR}")
except Exception as e:
    ai_scaler = None
    ai_model = None
    print(f"[AI MODEL ERROR] Failed to load models: {e}. AI anomaly detection disabled.")

def evaluate_ai_anomaly(parsed):
    if ai_model is None or ai_scaler is None:
        return None, None
        
    try:
        # Extract the exact 19 features the model was trained on
        features = [
            parsed.get('max_az', 0.0), parsed.get('min_az', 0.0), parsed.get('mean_az', 0.0),
            parsed.get('std_az', 0.0), parsed.get('skewness_az', 0.0), parsed.get('kurtosis_az', 0.0),
            parsed.get('max_ax', 0.0), parsed.get('min_ax', 0.0), parsed.get('mean_ax', 0.0)
        ]
        
        # Add 5 FFT peaks
        fft_peaks = parsed.get('fft_peaks', [])
        for i in range(5):
            if i < len(fft_peaks):
                features.append(fft_peaks[i][0]) # Frequency
                features.append(fft_peaks[i][1]) # Magnitude
            else:
                features.extend([0.0, 0.0])
                
        # Scale
        X = np.array(features).reshape(1, -1)
        X_scaled = ai_scaler.transform(X)
        
        # Evaluate
        score = ai_model.decision_function(X_scaled)[0]
        
        # Convert raw score (approx -0.5 to 0.5) to confidence percentage 0-100%
        # using a sigmoid function where score=0 is 50%.
        confidence = 100 * (1 / (1 + np.exp(20 * score)))
        
        status = 'Anomaly' if confidence > 90 else 'Normal'
        return float(confidence), status
        
    except Exception as e:
        print(f"[AI EVAL ERROR] {e}")
        return None, None
OVERFLOW_DELETE_N = 10_000

# ================= DB INIT =================
try:
    db.init_db()
    print("[CLOUD] Neon DB initialised.")
except Exception as e:
    print(f"[CLOUD WARNING] DB init failed: {e} — will retry on first request.")


# ================= THRESHOLD CHECK =================
def check_threshold_violation(value, thresholds):
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
    data = {
        'max_az': None, 'min_az': None, 'mean_az': None,
        'std_az': None, 'skewness_az': None, 'kurtosis_az': None,
        'max_ax': None, 'min_ax': None, 'mean_ax': None,
        'fft_peaks': []
    }
    try:
        with open(filepath, 'r') as f:
            lines = f.readlines()

        current_section = None
        for line in lines:
            line = line.strip()
            if '=== Z-AXIS STATISTICS' in line:
                current_section = 'z_stat'; continue
            elif '=== X-AXIS STATISTICS' in line:
                current_section = 'x_stat'; continue
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


def _parse_teensy_string(csv_content: str) -> dict:
    """
    Same logic as parse_teensy_file but operates on a string (no disk I/O).
    Used by the /data endpoint to parse the in-memory CSV reconstructed from ESP32 JSON.
    """
    data = {
        'max_az': None, 'min_az': None, 'mean_az': None,
        'std_az': None, 'skewness_az': None, 'kurtosis_az': None,
        'max_ax': None, 'min_ax': None, 'mean_ax': None,
        'fft_peaks': []
    }
    try:
        lines = csv_content.splitlines()
        current_section = None
        for line in lines:
            line = line.strip()
            if '=== Z-AXIS STATISTICS' in line:
                current_section = 'z_stat'; continue
            elif '=== X-AXIS STATISTICS' in line:
                current_section = 'x_stat'; continue
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
    try:
        base  = os.path.splitext(os.path.basename(filename))[0]
        parts = base.split('_')
        if len(parts) >= 2:
            date_str = parts[-2]
            time_str = parts[-1]
            dt = datetime.strptime(date_str + time_str, '%Y%m%d%H%M%S')
            return dt.strftime('%Y-%m-%d'), dt.timestamp()
        else:
            raise ValueError("Filename format not recognized")
    except Exception as e:
        print(f"[PARSE WARNING] Could not parse date from {filename}: {e}")
        now = datetime.now()
        return now.strftime('%Y-%m-%d'), now.timestamp()


# ================= DATA QUERY (FROM NEON) =================
def get_axis_data(axis='Z', last_n_minutes=None, start_time=None, end_time=None):
    try:
        rows = db.fetch_rows_for_chart(
            axis=axis,
            start_time=start_time,
            end_time=end_time,
            last_n_minutes=last_n_minutes
        )
        if not rows:
            return None

        # Columns from fetch_rows_for_chart:
        # 0=date, 1=time_sec, 2=max_az, 3=min_az, 4=mean_az, 5=std_az,
        # 6=skewness_az, 7=kurtosis_az, 8=max_ax, 9=min_ax, 10=mean_ax,
        # 11=fft1_freq, 12=fft1_mag, ...

        if axis == 'Z':
            max_idx, min_idx, avg_idx = 2, 3, 4
        else:
            max_idx, min_idx, avg_idx = 8, 9, 10

        timestamps      = [r[1] for r in rows]
        max_values      = [r[max_idx] for r in rows]
        min_values      = [r[min_idx] for r in rows]
        avg_values      = [r[avg_idx] for r in rows]
        frequencies     = [r[11] for r in rows]  # fft1_freq

        datetime_labels = [
            datetime.fromtimestamp(ts).astimezone().isoformat() for ts in timestamps
        ]

        valid_freqs = [f for f in frequencies if f is not None]
        avg_freq    = sum(valid_freqs) / len(valid_freqs) if valid_freqs else None

        return {
            'timestamps':      timestamps,
            'datetime_labels': datetime_labels,
            'max':             max_values,
            'min':             min_values,
            'avg':             avg_values,
            'frequency':       float(avg_freq) if avg_freq is not None else None,
            'axis':            axis,
            'metric':          'acceleration',
            'alerts':          {'datetimes': [], 'values': []}
        }
    except Exception as e:
        print(f"[ERROR] get_axis_data (cloud): {e}")
        import traceback; traceback.print_exc()
        return None


# ================= EVENT EXTRACTION =================
def extract_and_save_event(fault_type: str):
    try:
        # Create folder structure
        # Safe folder name
        safe_fault_name = "".join(c if c.isalnum() or c in " _-" else "_" for c in fault_type).strip()
        events_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'events', safe_fault_name)
        os.makedirs(events_dir, exist_ok=True)
        
        # Fetch last 10 minutes of raw data
        rows = db.fetch_rows_for_chart(last_n_minutes=10)
        if not rows:
            return False, "No data available in the last 10 minutes."
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"event_{timestamp}.csv"
        filepath = os.path.join(events_dir, filename)
        
        # Columns from db.fetch_rows_for_chart:
        # (date, time_sec, max_az, min_az, mean_az, std_az, skewness_az, kurtosis_az,
        #  max_ax, min_ax, mean_ax, fft1_freq, fft1_mag, ...)
        
        import csv
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            header = [
                'Date', 'Time_sec', 
                'Max_Az', 'Min_Az', 'Mean_Az', 'Std_Az', 'Skewness_Az', 'Kurtosis_Az',
                'Max_Ax', 'Min_Ax', 'Mean_Ax',
                'FFT1_Freq', 'FFT1_Mag', 'FFT2_Freq', 'FFT2_Mag', 'FFT3_Freq', 'FFT3_Mag',
                'FFT4_Freq', 'FFT4_Mag', 'FFT5_Freq', 'FFT5_Mag'
            ]
            writer.writerow(header)
            writer.writerows(rows)
            
        return True, f"Event saved successfully to {os.path.join('events', safe_fault_name, filename)}"
    except Exception as e:
        print(f"[ERROR] extract_and_save_event: {e}")
        return False, str(e)


# ================= ROUTES =================
@app.route("/")
def home():
    return render_template("dashboard.html", is_cloud=True)


@app.route("/health")
def health():
    """UptimeRobot / Render keep-alive ping endpoint."""
    return jsonify({"status": "ok", "server": "cloud"}), 200


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
        if "file" not in request.files:
            return jsonify({"error": "no file"}), 400

        file     = request.files["file"]
        filename = file.filename

        # Save to a temp location (Render has ephemeral /tmp)
        tmp_path = f"/tmp/{filename}"
        file.save(tmp_path)

        parsed             = parse_teensy_file(tmp_path)
        date_str, epoch_t  = epoch_from_filename(filename)

        # Remove temp file
        try:
            os.remove(tmp_path)
        except OSError:
            pass

        # Build row dict and insert into Neon
        row_dict = db.parsed_to_row_dict(parsed, date_str, epoch_t)
        try:
            db.insert_row(row_dict)
        except Exception as db_err:
            print(f"[CLOUD DB WARN] insert_row: {db_err}")

        # ---- Overflow guard: keep Neon lean (ring buffer) ----
        # Only run the check every ~100 uploads to avoid hammering the DB
        if random.randint(1, 100) == 1:
            try:
                count = db.get_row_count()
                if count > MAX_NEON_ROWS:
                    trimmed = db.delete_oldest_rows(OVERFLOW_DELETE_N)
                    print(f"[OVERFLOW] Neon had {count} rows (>{MAX_NEON_ROWS}). "
                          f"Trimmed {trimmed} oldest rows. Local server may have been offline too long.")
            except Exception as ov_err:
                print(f"[OVERFLOW CHECK ERROR] {ov_err}")

        # Threshold check on Max_Az
        alert_status = None
        max_az = parsed.get('max_az')
        if max_az is not None:
            violation = check_threshold_violation(max_az, THRESHOLDS['z_axis']['acceleration'])
            if violation:
                alert_status = violation
                print(f"[ALERT] {violation} | Max_Az={max_az:.4f}")
            else:
                print(f"[OK] Max_Az={max_az:.4f}")

        top_freq = parsed['fft_peaks'][0][0] if parsed['fft_peaks'] else 0
        with status_lock:
            latest_status['timestamp'] = datetime.fromtimestamp(epoch_t).strftime('%Y-%m-%d %H:%M:%S')
            latest_status['magnitude'] = max_az or 0
            latest_status['frequency'] = top_freq

            if alert_status:
                latest_status['status']     = f'{alert_status} Threshold'
                latest_status['alert']      = True
                latest_status['alert_type'] = f'Z-Axis {alert_status} Threshold'
                latest_status['alert_time'] = latest_status['timestamp']
                socketio.emit('alert_notification', {
                    'alert_type': f'Z-Axis {alert_status} Threshold',
                    'timestamp':  latest_status['timestamp'],
                    'magnitude':  max_az,
                    'axis':       'Z'
                })
            else:
                latest_status['status']     = 'Normal'
                latest_status['alert']      = False
                latest_status['alert_type'] = None

        socketio.emit('data_update', {'type': 'file', 'filename': filename})
        return jsonify({"status": "received", "date": date_str, "alert": alert_status})

    except Exception as e:
        print(f"[ERROR] Upload: {e}")
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ================= /data — ESP32 S3 DIRECT JSON ENDPOINT =================
# Replaces the RPi middleman. Accepts the same JSON the ESP32 already sends,
# reconstructs the CSV in memory, then runs the existing parse→DB pipeline.
@app.route("/data", methods=["POST"])
def receive_esp32_data():
    # ---- API key check (set ESP32_API_KEY env var on Render to enable) ----
    if ESP32_API_KEY:
        incoming_key = request.headers.get('X-API-Key', '')
        if incoming_key != ESP32_API_KEY:
            print(f"[SECURITY] /data blocked — wrong or missing X-API-Key from {request.remote_addr}")
            return jsonify({"error": "Unauthorized"}), 401
    try:
        if not request.is_json:
            return jsonify({"error": "Expected JSON payload"}), 400

        data     = request.get_json()
        core_id  = data.get("core", 0)
        samples  = data.get("samples", 0)
        print(f"\n[ESP32 Core {core_id}] Direct JSON received (samples: {samples})")

        # ---- Reconstruct Teensy-format CSV in memory (same as rpi_client.py) ----
        now_str      = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename     = f"ESP32_Core{core_id}_{now_str}.csv"
        record_start = data.get("record_start", 0)
        record_stop  = data.get("record_stop",  0)
        duration_s   = (record_stop - record_start) / 1000.0 \
                       if record_stop > record_start else 0.0

        freqs     = data.get("freq", [])
        mags      = data.get("mag",  [])
        fft_lines = [f"{i+1},{freqs[i]},{mags[i]}"
                     for i in range(min(len(freqs), len(mags)))]

        csv_lines = [
            "=== METADATA ===",
            "Parameter,Value,Unit",
            f"Device ID,ESP32_Core{core_id},-",
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
            "=== FFT PEAKS (Z-AXIS) ===",
            "Rank,Frequency (Hz),Magnitude",
        ] + fft_lines + ["================================================="]

        csv_content = "\n".join(csv_lines)

        # ---- Parse the reconstructed CSV (using StringIO — no disk I/O) ----
        parsed = _parse_teensy_string(csv_content)
        date_str, epoch_t = datetime.now().strftime('%Y-%m-%d'), datetime.now().timestamp()

        # ---- Insert into Neon DB ----
        row_dict = db.parsed_to_row_dict(parsed, date_str, epoch_t)
        try:
            db.insert_row(row_dict)
        except Exception as db_err:
            print(f"[ESP32 DB WARN] insert_row: {db_err}")

        # ---- Overflow guard (same as /upload) ----
        if random.randint(1, 100) == 1:
            try:
                count = db.get_row_count()
                if count > MAX_NEON_ROWS:
                    trimmed = db.delete_oldest_rows(OVERFLOW_DELETE_N)
                    print(f"[OVERFLOW] Trimmed {trimmed} oldest rows (was {count}).")
            except Exception as ov_err:
                print(f"[OVERFLOW CHECK ERROR] {ov_err}")

        # ---- Threshold check & socket emit ----
        alert_status = None
        max_az = parsed.get('max_az')
        if max_az is not None:
            violation = check_threshold_violation(max_az, THRESHOLDS['z_axis']['acceleration'])
            if violation:
                alert_status = violation
                print(f"[ALERT] {violation} | Max_Az={max_az:.4f}")
            else:
                print(f"[OK] Max_Az={max_az:.4f}")

        top_freq = parsed['fft_peaks'][0][0] if parsed['fft_peaks'] else 0
        # ---- AI Model Check & Auto-Trigger ----
        global last_ai_auto_event_time
        ai_confidence, ai_status = evaluate_ai_anomaly(parsed)
        
        if ai_status == 'Anomaly' and ai_confidence > 90.0:
            now_ts = datetime.now().timestamp()
            # 10 minute cooldown (600 seconds)
            if (now_ts - last_ai_auto_event_time) > 600:
                print(f"[AI AUTO-TRIGGER] Anomaly detected with {ai_confidence:.1f}% confidence. Extracting event...")
                success, msg = extract_and_save_event("Unknown Event Detected")
                if success:
                    last_ai_auto_event_time = now_ts
                    print(f"[AI AUTO-TRIGGER] {msg}")
                else:
                    print(f"[AI AUTO-TRIGGER] Failed: {msg}")
        
        with status_lock:
            latest_status['timestamp'] = datetime.fromtimestamp(epoch_t).strftime('%Y-%m-%d %H:%M:%S')
            latest_status['magnitude'] = max_az or 0
            latest_status['frequency'] = top_freq
            latest_status['ai_confidence'] = ai_confidence
            latest_status['ai_status'] = ai_status

            if alert_status or (ai_status == 'Anomaly'):
                cause = alert_status if alert_status else 'AI Model'
                latest_status['status']     = f'{cause} Threshold'
                latest_status['alert']      = True
                latest_status['alert_type'] = f'Z-Axis {cause} Threshold'
                latest_status['alert_time'] = latest_status['timestamp']
                socketio.emit('alert_notification', {
                    'alert_type': f'Z-Axis {cause} Threshold',
                    'timestamp':  latest_status['timestamp'],
                    'magnitude':  max_az,
                    'axis':       'Z',
                    'ai_confidence': ai_confidence
                })
            else:
                latest_status['status']     = 'Normal'
                latest_status['alert']      = False
                latest_status['alert_type'] = None

        socketio.emit('data_update', {'type': 'esp32_direct', 'filename': filename})
        return jsonify({"status": "received", "filename": filename, "alert": alert_status}), 200

    except Exception as e:
        print(f"[ERROR] /data: {e}")
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/axis_data")
def api_axis_data():
    axis       = request.args.get('axis', 'Z')
    minutes    = request.args.get('minutes')
    start_time = request.args.get('start_time')
    end_time   = request.args.get('end_time')

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
    from flask import send_from_directory
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
    minutes    = data.get('minutes', 10)
    start_time = data.get('start_time')
    end_time   = data.get('end_time')

    if start_time and end_time:
        graph_data = get_axis_data(axis, start_time=start_time, end_time=end_time)
    else:
        graph_data = get_axis_data(axis, last_n_minutes=minutes)

    if graph_data:
        emit('axis_data', graph_data)
    else:
        emit('axis_data', {'error': 'No data available', 'axis': axis})


# ================= MAIN =================
if __name__ == "__main__":
    print("=" * 60)
    print("CLOUD VIBRATION MONITORING SERVER (Render)")
    print("=" * 60)
    socketio.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 5003)), debug=False)
