"""
vibration_core.py — Shared core logic for Vibration Monitoring (Cloud + Local Hub)
Stdlib-only module providing:
  - Canonical 33-column CSV/DB schema and row conversion helpers
  - Default 4-level threshold schema (Max Max, Min Max, Max Min, Min Min)
  - Teensy/ESP32 CSV parsing and JSON->CSV reconstruction (all 3 axes + displacement + FFT)
  - KPI snapshot calculation (Displacement, Velocity, Acceleration)
  - Threshold violation checking and chart alert extraction
"""

import copy
import csv
import io
import math
import os
from datetime import datetime, timezone

# ================= CSV / DB SCHEMA (33 COLUMNS) =================
# Exact column order shared by server.py CSVs and db.DB_COLUMNS.
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
    'FFT5_Freq', 'FFT5_Mag',
]

# Lowercase parsed/DB keys corresponding 1-to-1 with CSV_HEADERS
PARSED_KEYS = [
    'date', 'time_sec',
    'max_az', 'min_az', 'mean_az', 'std_az', 'skewness_az', 'kurtosis_az',
    'max_ax', 'min_ax', 'mean_ax',
    'max_ay', 'min_ay', 'mean_ay',
    'disp_peak_z_mm', 'disp_p2p_z_mm', 'disp_rms_z_mm',
    'disp_peak_x_mm', 'disp_p2p_x_mm', 'disp_rms_x_mm',
    'disp_peak_y_mm', 'disp_p2p_y_mm', 'disp_rms_y_mm',
    'fft1_freq', 'fft1_mag',
    'fft2_freq', 'fft2_mag',
    'fft3_freq', 'fft3_mag',
    'fft4_freq', 'fft4_mag',
    'fft5_freq', 'fft5_mag',
]

COL_INDEX = {col: idx for idx, col in enumerate(CSV_HEADERS)}

# ================= FRONTEND-ONLY 4-LEVEL THRESHOLDS =================
# Zero hardcoded active numeric thresholds: lines start unset (None) until
# configured by the user in the dashboard UI (and persisted in Neon DB).
EMPTY_THRESHOLDS = {
    'z_axis': {
        'acceleration': {
            'over_positive': None,     # Max Max (Upper Critical)
            'under_positive': None,    # Min Max (Upper Warning)
            'under_negative': None,    # Max Min (Lower Warning)
            'over_negative': None,     # Min Min (Lower Critical)
        },
        'displacement': {
            'over_positive': None,
            'under_positive': None,
            'under_negative': None,
            'over_negative': None,
        },
    },
    'x_axis': {
        'acceleration': {
            'over_positive': None,
            'under_positive': None,
            'under_negative': None,
            'over_negative': None,
        },
        'displacement': {
            'over_positive': None,
            'under_positive': None,
            'under_negative': None,
            'over_negative': None,
        },
    },
    'y_axis': {
        'acceleration': {
            'over_positive': None,
            'under_positive': None,
            'under_negative': None,
            'over_negative': None,
        },
        'displacement': {
            'over_positive': None,
            'under_positive': None,
            'under_negative': None,
            'over_negative': None,
        },
    },
}

DEFAULT_THRESHOLDS = EMPTY_THRESHOLDS


def get_empty_thresholds() -> dict:
    """Return a deep copy of the unset (all-None) 4-level threshold structure."""
    return copy.deepcopy(EMPTY_THRESHOLDS)


def get_default_thresholds() -> dict:
    """Return unset (all-None) thresholds so only frontend-configured lines are active."""
    return get_empty_thresholds()


def has_any_active_threshold(thresholds: dict) -> bool:
    """Return True if at least one numeric threshold line is set."""
    if not isinstance(thresholds, dict):
        return False
    for axis_key in ('z_axis', 'x_axis', 'y_axis'):
        metrics = thresholds.get(axis_key)
        if not isinstance(metrics, dict):
            continue
        for metric_key in ('acceleration', 'displacement'):
            vals = metrics.get(metric_key)
            if not isinstance(vals, dict):
                continue
            for lvl in ('over_positive', 'under_positive', 'under_negative', 'over_negative'):
                if to_float(vals.get(lvl)) is not None:
                    return True
    return False


def to_float(val):
    """Safely convert a value to finite float, or return None."""
    if val is None or val == '':
        return None
    try:
        f = float(val)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def utc_iso(epoch_t: float) -> str:
    """Format an epoch timestamp as a canonical UTC ISO-8601 string ending with Z."""
    return datetime.fromtimestamp(float(epoch_t), tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def utc_display(epoch_t: float) -> str:
    """Format an epoch timestamp as a readable UTC YYYY-MM-DD HH:MM:SS string."""
    return datetime.fromtimestamp(float(epoch_t), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


def sanitize_fault_name(fault_type: str) -> str:
    """Sanitize a fault/event label for safe filesystem folder naming."""
    cleaned = "".join(c if c.isalnum() or c in " _-" else "_" for c in str(fault_type)).strip()
    return cleaned or "Unknown_Fault"


# ================= THRESHOLD EVALUATION & UPDATES =================
def check_threshold_violation_detail(max_val, min_val_or_thresh, maybe_thresh=None):
    """
    Evaluate 4 frontend-configured threshold levels and return
    (level, measured_value, threshold_value) or (None, None, None):
      - Max Max: max_val > over_positive (Upper Critical Peak)
      - Min Min: min_val < over_negative (Lower Critical Peak)
      - Min Max: 0 < max_val < under_positive (Upper Warning / Low Positive)
      - Max Min: under_negative < min_val < 0 (Lower Warning / Low Negative)
    Supports both (max_val, min_val, thresholds) and legacy (max_val, thresholds).
    """
    if maybe_thresh is not None:
        min_val = to_float(min_val_or_thresh)
        thresholds = maybe_thresh
    else:
        thresholds = min_val_or_thresh
        min_val = None

    max_val = to_float(max_val)

    if not thresholds or not isinstance(thresholds, dict):
        return None, None, None

    over_pos = to_float(thresholds.get('over_positive'))
    under_pos = to_float(thresholds.get('under_positive'))
    under_neg = to_float(thresholds.get('under_negative'))
    over_neg = to_float(thresholds.get('over_negative'))

    # 1. Critical High (Max Max)
    if max_val is not None and over_pos is not None and max_val > over_pos:
        return 'Max Max', max_val, over_pos

    # 2. Critical Low (Min Min)
    if min_val is not None and over_neg is not None and min_val < over_neg:
        return 'Min Min', min_val, over_neg

    # 3. Warning High (Min Max)
    if max_val is not None and under_pos is not None and under_pos > 0 and 0 < max_val < under_pos:
        return 'Min Max', max_val, under_pos

    # 4. Warning Low (Max Min)
    if min_val is not None and under_neg is not None and under_neg < 0 and under_neg < min_val < 0:
        return 'Max Min', min_val, under_neg

    return None, None, None


def check_threshold_violation(max_val, min_val_or_thresh, maybe_thresh=None):
    """Backward-compatible wrapper returning only the violated threshold level string or None."""
    level, _, _ = check_threshold_violation_detail(max_val, min_val_or_thresh, maybe_thresh)
    return level


def check_all_axes_violations_detailed(parsed: dict, thresholds: dict):
    """
    Evaluate Z, X, Y acceleration and displacement ONLY against non-None
    thresholds set on the frontend chart lines.
    Returns dict with {level, axis, metric, measured_value, threshold_value, fault_type} or None.
    """
    if not parsed or not thresholds:
        return None

    for axis_key, axis_letter in (('z_axis', 'Z'), ('x_axis', 'X'), ('y_axis', 'Y')):
        axis_thresh = thresholds.get(axis_key, {}).get('acceleration')
        if not axis_thresh:
            continue
        max_v = to_float(parsed.get(f'max_a{axis_letter.lower()}'))
        min_v = to_float(parsed.get(f'min_a{axis_letter.lower()}'))
        if max_v is None and min_v is None:
            continue
        level, measured_val, thresh_val = check_threshold_violation_detail(max_v, min_v, axis_thresh)
        if level:
            return {
                'level': level,
                'axis': axis_letter,
                'metric': 'acceleration',
                'measured_value': measured_val,
                'threshold_value': thresh_val,
                'fault_type': f"{axis_letter}-Axis {level} Threshold",
            }

    for axis_key, axis_letter, col_suffix in (
        ('z_axis', 'Z-Disp', 'z'),
        ('x_axis', 'X-Disp', 'x'),
        ('y_axis', 'Y-Disp', 'y'),
    ):
        disp_thresh = thresholds.get(axis_key, {}).get('displacement')
        if not disp_thresh:
            continue
        disp_peak = to_float(parsed.get(f'disp_peak_{col_suffix}_mm'))
        if disp_peak is None:
            continue
        level, measured_val, thresh_val = check_threshold_violation_detail(disp_peak, disp_peak, disp_thresh)
        if level:
            return {
                'level': level,
                'axis': axis_letter,
                'metric': 'displacement',
                'measured_value': measured_val,
                'threshold_value': thresh_val,
                'fault_type': f"{axis_letter} {level} Threshold",
            }

    return None


def check_all_axes_violations(parsed: dict, thresholds: dict):
    """
    Evaluate Z, X, Y acceleration & displacement against frontend thresholds.
    Returns (violation_label, axis_label, breach_value) or (None, None, None).
    """
    detail = check_all_axes_violations_detailed(parsed, thresholds)
    if not detail:
        return None, None, None
    return detail['level'], detail['axis'], detail['measured_value']


def apply_threshold_payload(thresholds: dict, data: dict) -> dict:
    """
    Apply either nested axis/metric dicts or flat chart updates
    (max_max, min_max, max_min, min_min + legacy aliases) onto `thresholds` in place.
    Explicit None or '' clears a line back to unset (None).
    """
    if not isinstance(data, dict):
        return thresholds

    valid_keys = {'over_positive', 'under_positive', 'under_negative', 'over_negative'}
    for axis, metrics in data.items():
        if axis in ('configured', 'updated_at'):
            continue
        if axis in thresholds and isinstance(metrics, dict):
            for metric, vals in metrics.items():
                if metric in thresholds[axis] and isinstance(vals, dict):
                    for k, v in vals.items():
                        if k in valid_keys:
                            if v is None or v == '':
                                thresholds[axis][metric][k] = None
                            else:
                                f = to_float(v)
                                if f is not None:
                                    thresholds[axis][metric][k] = f

    if 'axis' in data:
        axis = data['axis']
        metric = data.get('metric', 'acceleration')
        if axis in thresholds and metric in thresholds[axis]:
            target = thresholds[axis][metric]
            key_aliases = (
                ('over_positive', ('max_max', 'over_positive', 'max')),
                ('under_positive', ('min_max', 'under_positive')),
                ('under_negative', ('max_min', 'under_negative')),
                ('over_negative', ('min_min', 'over_negative', 'min')),
            )
            for canonical_key, aliases in key_aliases:
                for alias in aliases:
                    if alias in data:
                        raw_val = data[alias]
                        if raw_val is None or raw_val == '':
                            target[canonical_key] = None
                        else:
                            f = to_float(raw_val)
                            if f is not None:
                                target[canonical_key] = f
                        break

    return thresholds


# ================= PARSING & CSV RECONSTRUCTION =================
def get_empty_parsed_data() -> dict:
    return {
        'max_az': None, 'min_az': None, 'mean_az': None,
        'std_az': None, 'skewness_az': None, 'kurtosis_az': None,
        'max_ax': None, 'min_ax': None, 'mean_ax': None,
        'max_ay': None, 'min_ay': None, 'mean_ay': None,
        'disp_peak_z_mm': None, 'disp_p2p_z_mm': None, 'disp_rms_z_mm': None,
        'disp_peak_x_mm': None, 'disp_p2p_x_mm': None, 'disp_rms_x_mm': None,
        'disp_peak_y_mm': None, 'disp_p2p_y_mm': None, 'disp_rms_y_mm': None,
        'fft_peaks': [],  # list of (freq, mag) up to 5
    }


def parse_teensy_string(csv_content: str) -> dict:
    """
    Parse Teensy/ESP32 statistics CSV text (in memory) across all 5 sections:
    Z-AXIS, X-AXIS, Y-AXIS, DISPLACEMENT, and FFT PEAKS.
    """
    data = get_empty_parsed_data()
    try:
        current_section = None
        for raw_line in csv_content.splitlines():
            line = raw_line.strip()
            if '=== Z-AXIS STATISTICS' in line:
                current_section = 'z_stat'
                continue
            elif '=== X-AXIS STATISTICS' in line:
                current_section = 'x_stat'
                continue
            elif '=== Y-AXIS STATISTICS' in line:
                current_section = 'y_stat'
                continue
            elif '=== DISPLACEMENT STATISTICS' in line:
                current_section = 'disp_stat'
                continue
            elif '=== FFT PEAKS' in line:
                current_section = 'fft'
                continue
            elif line.startswith('===') or line.startswith('Parameter') or line.startswith('Rank') or not line:
                continue

            parts = [p.strip() for p in line.split(',')]

            if current_section == 'z_stat' and len(parts) >= 2:
                val = to_float(parts[1])
                if val is None:
                    continue
                mapping = {
                    'Maximum Az': 'max_az',
                    'Minimum Az': 'min_az',
                    'Mean Az': 'mean_az',
                    'Std Dev Az': 'std_az',
                    'Skewness Az': 'skewness_az',
                    'Excess Kurtosis Az': 'kurtosis_az',
                }
                if parts[0] in mapping:
                    data[mapping[parts[0]]] = val

            elif current_section == 'x_stat' and len(parts) >= 2:
                val = to_float(parts[1])
                if val is None:
                    continue
                mapping = {
                    'Maximum Ax': 'max_ax',
                    'Minimum Ax': 'min_ax',
                    'Mean Ax': 'mean_ax',
                }
                if parts[0] in mapping:
                    data[mapping[parts[0]]] = val

            elif current_section == 'y_stat' and len(parts) >= 2:
                val = to_float(parts[1])
                if val is None:
                    continue
                mapping = {
                    'Maximum Ay': 'max_ay',
                    'Minimum Ay': 'min_ay',
                    'Mean Ay': 'mean_ay',
                }
                if parts[0] in mapping:
                    data[mapping[parts[0]]] = val

            elif current_section == 'disp_stat' and len(parts) >= 2:
                val = to_float(parts[1])
                if val is None:
                    continue
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
                if parts[0] in disp_mapping:
                    data[disp_mapping[parts[0]]] = val

            elif current_section == 'fft' and len(parts) >= 3 and len(data['fft_peaks']) < 5:
                freq = to_float(parts[1])
                mag = to_float(parts[2])
                if freq is not None and mag is not None:
                    data['fft_peaks'].append((freq, mag))
    except Exception as e:
        print(f"[PARSE ERROR] parse_teensy_string: {e}")
    return data


def parse_teensy_file(filepath: str) -> dict:
    """Parse a Teensy/ESP32 CSV statistics file from disk."""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            return parse_teensy_string(f.read())
    except Exception as e:
        print(f"[PARSE ERROR] {filepath}: {e}")
        return get_empty_parsed_data()


def extract_metric_dict(val):
    """Extract (max, min, peak, p2p, rms) from a nested metric dict or scalar."""
    if isinstance(val, dict):
        return (
            float(val.get("max", 0.0) or 0.0),
            float(val.get("min", 0.0) or 0.0),
            float(val.get("peak", 0.0) or 0.0),
            float(val.get("p2p", 0.0) or 0.0),
            float(val.get("rms", 0.0) or 0.0),
        )
    p = float(val or 0.0)
    return p, -p, p, 2.0 * p, p / 1.414


def reconstruct_csv_from_esp32_json(data: dict, now_utc: datetime | None = None):
    """
    Build standard Teensy CSV text from an ESP32 JSON payload.
    Returns (filename, csv_content, date_str, epoch_t).
    """
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)

    core_id = data.get("core", 0)
    samples = data.get("samples", 0)
    record_start = data.get("record_start", 0)
    record_stop = data.get("record_stop", 0)
    duration_s = (record_stop - record_start) / 1000.0 if record_stop > record_start else 0.0

    epoch_field = to_float(data.get("record_start_epoch"))
    if epoch_field is not None and epoch_field > 1_700_000_000:
        epoch_t = float(epoch_field)
    else:
        epoch_t = now_utc.timestamp()

    dt_utc = datetime.fromtimestamp(epoch_t, tz=timezone.utc)
    date_str = dt_utc.strftime('%Y-%m-%d')
    stamp_str = dt_utc.strftime('%Y%m%d_%H%M%S')
    filename = f"ESP32_Core{core_id}_{stamp_str}.csv"

    freqs = data.get("freq", []) or []
    mags = data.get("mag", []) or []
    fft_lines = [
        f"{i+1},{freqs[i]},{mags[i]}"
        for i in range(min(len(freqs), len(mags)))
    ]

    _, _, disp_z_peak, disp_z_p2p, disp_z_rms = extract_metric_dict(data.get("disp_z_mm"))
    _, _, disp_x_peak, disp_x_p2p, disp_x_rms = extract_metric_dict(data.get("disp_x_mm"))
    _, _, disp_y_peak, disp_y_p2p, disp_y_rms = extract_metric_dict(data.get("disp_y_mm"))

    _, _, vel_z_peak, vel_z_p2p, vel_z_rms = extract_metric_dict(data.get("vel_z_ms"))
    _, _, vel_x_peak, vel_x_p2p, vel_x_rms = extract_metric_dict(data.get("vel_x_ms"))
    _, _, vel_y_peak, vel_y_p2p, vel_y_rms = extract_metric_dict(data.get("vel_y_ms"))

    diag = data.get("diag", {}) if isinstance(data.get("diag"), dict) else {}

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

    return filename, "\n".join(csv_lines), date_str, epoch_t


def epoch_from_filename(filename: str):
    """
    Extract (date_str, epoch) from filenames like Teensy_Core1_YYYYMMDD_HHMMSS.csv.
    Falls back cleanly to current UTC time if format differs.
    """
    try:
        base = os.path.splitext(os.path.basename(filename))[0]
        parts = base.split('_')
        if len(parts) >= 2:
            date_part = parts[-2]
            time_part = parts[-1]
            dt = datetime.strptime(date_part + time_part, '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc)
            return dt.strftime('%Y-%m-%d'), dt.timestamp()
        raise ValueError("Filename format not recognized")
    except Exception as e:
        print(f"[PARSE WARNING] Could not parse date from {filename}, using current UTC time. Error: {e}")
        now = datetime.now(timezone.utc)
        return now.strftime('%Y-%m-%d'), now.timestamp()


# ================= ROW CONVERSION & KPI =================
def parsed_to_csv_row(parsed: dict, date_str: str, epoch_time: float) -> list:
    """Build a 33-element list aligned strictly with CSV_HEADERS."""
    peaks = parsed.get('fft_peaks', [])
    fft_flat = []
    for i in range(5):
        if i < len(peaks):
            fft_flat.extend([peaks[i][0], peaks[i][1]])
        else:
            fft_flat.extend(['', ''])

    scalar_keys = PARSED_KEYS[2:23]  # 21 scalar float keys between time_sec and fft1_freq
    row = [date_str, epoch_time]
    for k in scalar_keys:
        val = parsed.get(k)
        row.append(val if val is not None else '')
    row.extend(fft_flat)
    return row


def normalize_row_tuple(raw_row) -> list:
    """Normalize a raw CSV row list or DB tuple into a 33-element typed list."""
    out = [None] * len(CSV_HEADERS)
    if not raw_row:
        return out
    n = min(len(raw_row), len(CSV_HEADERS))
    out[0] = str(raw_row[0]) if raw_row[0] not in (None, '') else ''
    for i in range(1, n):
        out[i] = to_float(raw_row[i])
    return out


def row_tuple_to_parsed_dict(row_tuple) -> dict:
    """
    Convert a 33-column DB tuple or CSV row list into a parsed dict compatible
    with build_kpi_snapshot(), check_all_axes_violations(), etc.
    """
    norm = normalize_row_tuple(row_tuple)
    d = {}
    for i, key in enumerate(PARSED_KEYS):
        d[key] = norm[i]

    fft_peaks = []
    for n in range(5):
        f = d.get(f'fft{n+1}_freq')
        m = d.get(f'fft{n+1}_mag')
        if f is not None and m is not None:
            fft_peaks.append((f, m))
    d['fft_peaks'] = fft_peaks
    return d


def build_kpi_snapshot(data_json, parsed_row: dict, ts_iso: str) -> dict:
    """
    Compute KPI snapshot dict across Displacement (mm), Velocity (m/s), and
    Acceleration (m/s^2) for X, Y, Z axes.
    """
    def _nonzero(v):
        f = to_float(v)
        return f if (f is not None and f != 0.0) else None

    freq = _nonzero(parsed_row.get('fft1_freq') or parsed_row.get('FFT1_Freq'))
    if not freq or freq <= 0:
        peaks = parsed_row.get('fft_peaks', [])
        if peaks and len(peaks) > 0 and peaks[0][0] > 0:
            freq = float(peaks[0][0])
        else:
            freq = 50.0
    omega = 2.0 * math.pi * freq

    def _vel(axis_name, raw_axis_key):
        if data_json:
            raw = data_json.get(raw_axis_key)
            if isinstance(raw, dict):
                p = _nonzero(raw.get('peak'))
                p2p = _nonzero(raw.get('p2p'))
                if p is not None:
                    return p, p2p
            elif raw is not None:
                f = _nonzero(raw)
                if f is not None:
                    return round(f, 6), round(f * 2.0, 6)

        d_peak = _nonzero(
            parsed_row.get(f'disp_peak_{axis_name.lower()}_mm')
            or parsed_row.get(f'Disp_Peak_{axis_name.upper()}_mm')
        )
        d_p2p = _nonzero(
            parsed_row.get(f'disp_p2p_{axis_name.lower()}_mm')
            or parsed_row.get(f'Disp_P2P_{axis_name.upper()}_mm')
        )
        if d_peak is not None and d_peak > 0:
            v_peak = omega * (d_peak / 1000.0)
            v_p2p = omega * (d_p2p / 1000.0) if d_p2p is not None else v_peak * 2.0
            return round(v_peak, 6), round(v_p2p, 6)

        max_a = _nonzero(
            parsed_row.get(f'max_a{axis_name.lower()}')
            or parsed_row.get(f'Max_A{axis_name.upper()}')
        )
        min_a = _nonzero(
            parsed_row.get(f'min_a{axis_name.lower()}')
            or parsed_row.get(f'Min_A{axis_name.upper()}')
        )
        if max_a is not None or min_a is not None:
            a_peak = max(abs(max_a or 0.0), abs(min_a or 0.0))
            if a_peak > 0:
                v_peak = a_peak / omega
                v_p2p = v_peak * 2.0
                return round(v_peak, 6), round(v_p2p, 6)

        return None, None

    def _acc_kpi(max_key, min_key):
        max_v = _nonzero(parsed_row.get(max_key))
        min_v = _nonzero(parsed_row.get(min_key))
        if max_v is None and min_v is None:
            return None, None
        peak = None
        p2p = None
        if max_v is not None and min_v is not None:
            peak = max(abs(max_v), abs(min_v))
            p2p = max_v - min_v
        elif max_v is not None:
            peak = abs(max_v)
        elif min_v is not None:
            peak = abs(min_v)
        return peak, p2p

    vz_p, vz_p2p = _vel('Z', 'vel_z_ms')
    vx_p, vx_p2p = _vel('X', 'vel_x_ms')
    vy_p, vy_p2p = _vel('Y', 'vel_y_ms')

    az_p, az_p2p = _acc_kpi('max_az', 'min_az')
    ax_p, ax_p2p = _acc_kpi('max_ax', 'min_ax')
    ay_p, ay_p2p = _acc_kpi('max_ay', 'min_ay')

    return {
        'timestamp': ts_iso,
        'displacement': {
            'Z': {'peak': _nonzero(parsed_row.get('disp_peak_z_mm')), 'p2p': _nonzero(parsed_row.get('disp_p2p_z_mm'))},
            'X': {'peak': _nonzero(parsed_row.get('disp_peak_x_mm')), 'p2p': _nonzero(parsed_row.get('disp_p2p_x_mm'))},
            'Y': {'peak': _nonzero(parsed_row.get('disp_peak_y_mm')), 'p2p': _nonzero(parsed_row.get('disp_p2p_y_mm'))},
        },
        'velocity': {
            'Z': {'peak': vz_p, 'p2p': vz_p2p},
            'X': {'peak': vx_p, 'p2p': vx_p2p},
            'Y': {'peak': vy_p, 'p2p': vy_p2p},
        },
        'acceleration': {
            'Z': {'peak': az_p, 'p2p': az_p2p},
            'X': {'peak': ax_p, 'p2p': ax_p2p},
            'Y': {'peak': ay_p, 'p2p': ay_p2p},
        },
    }


def rows_to_csv_string(rows: list) -> str:
    """Serialize 33-column row tuples/lists with CSV_HEADERS into CSV text."""
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator='\n')
    writer.writerow(CSV_HEADERS)
    for r in rows:
        row_out = []
        r_len = len(r)
        for i in range(len(CSV_HEADERS)):
            val = r[i] if i < r_len else ''
            row_out.append('' if val is None else val)
        writer.writerow(row_out)
    return buf.getvalue()


def compute_chart_alerts(datetime_labels: list, max_vals: list, min_vals: list, thresh_dict: dict) -> dict:
    """
    Identify chart points that breach any frontend-configured threshold line
    (Max Max, Min Min, Min Max, Max Min) so red alert markers can be plotted.
    """
    alert_dt = []
    alert_val = []
    if not thresh_dict or not datetime_labels:
        return {'datetimes': alert_dt, 'values': alert_val}

    for i, dt_str in enumerate(datetime_labels):
        mx = to_float(max_vals[i]) if i < len(max_vals) else None
        mn = to_float(min_vals[i]) if i < len(min_vals) else None
        level, measured_val, _ = check_threshold_violation_detail(mx, mn, thresh_dict)
        if level is not None and measured_val is not None:
            alert_dt.append(dt_str)
            alert_val.append(measured_val)

    return {'datetimes': alert_dt, 'values': alert_val}
