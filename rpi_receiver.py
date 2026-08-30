 #!/usr/bin/env python3
"""
rpi_receiver.py  –  Raspberry Pi  ← Teensy/ESP32  HTTP Listener
================================================================
Receives a processed JSON payload from a Teensy (or ESP32), saves a
formatted CSV file locally, then forwards both a stats summary and the
raw CSV file to a remote ngrok dashboard server.

TIMESTAMP STRATEGY
------------------
All timestamps on the RPi side are generated using timezone-aware UTC
datetimes (datetime.now(timezone.utc)).  This removes the ambiguity that
caused an apparent 8-hour offset when the RPi's local clock happened to
be set to a different timezone than the dashboard server.

The formatted timestamp written to the CSV uses ISO-8601 with the UTC
offset appended (e.g. "2026-08-18 07:30:00+00:00") so the dashboard can
always parse it unambiguously.

ESP32 REAL-TIME TIMESTAMP SUGGESTIONS  (see bottom of this file)
-----------------------------------------------------------------
The ESP32 sends `record_start` / `record_stop` as milliseconds since
boot (millis()).  To convert those into real wall-clock times on the RPi
you need either:
  Option A  – NTP sync on the ESP32 (recommended)
  Option B  – GPS module on the ESP32
  Option C  – RPi injects a wall-clock anchor at reception time (done here)

Requirements:
    pip install flask requests

Run:
    python3 rpi_receiver.py
"""

import os
import time
import requests
from datetime import datetime, timezone, timedelta
from flask import Flask, request, jsonify

# ============================================================
# CONFIGURATION
# ============================================================

# URL of your remote ngrok dashboard server  (keep /upload at the end)
REMOTE_SERVER_URL = "https://tidings-custard-parking.ngrok-free.dev/upload"

# Directory where CSVs are saved locally on the Raspberry Pi
LOCAL_SAVE_DIR = "downloaded_csvs"

# Port the RPi listens on for incoming data from the Teensy / ESP32
LOCAL_PORT = 5000

# Expected sender IP  (set to None to accept from any IP during testing)
TEENSY_IP = "192.168.100.198"

# Your local timezone offset from UTC expressed as hours.
# Example: UTC+5:30 → LOCAL_UTC_OFFSET_HOURS = 5.5
# This is ONLY used for display in the CSV "RECORDING INFO" section so a
# human reading the file sees local time.  All machine-readable fields stay UTC.
LOCAL_UTC_OFFSET_HOURS = 5.5          # ← Change to your timezone

# ============================================================
# BOOT-TIME ANCHOR
# ============================================================
# When the RPi boots the Teensy can have been running for many seconds
# already.  We record the wall-clock UTC time at the moment we START
# this script and the monotonic counter at the same instant.  Every
# millis() value the Teensy sends can then be converted to a real UTC
# wall-clock time by:
#
#   wall_utc = SCRIPT_START_UTC + (teensy_millis_ms - 0) / 1000 (approx)
#
# For Option C (anchor at first reception) we also record the RPi's
# millis at each request arrival.  See receive_data() below.
SCRIPT_START_UTC   = datetime.now(timezone.utc)
SCRIPT_START_MONO  = time.monotonic()   # seconds, float

def millis_to_utc(teensy_millis: int) -> datetime:
    """
    Convert a Teensy/ESP32 millis()-since-boot value to a UTC datetime.

    We assume the Teensy booted at approximately the same time as this
    script started (SCRIPT_START_MONO ~ 0 on the Teensy at that moment).
    If you restart this script while the Teensy is already running you
    must account for the elapsed millis() at startup.

    A better approach is Option A or B (NTP / GPS on the device itself).
    """
    elapsed_s = teensy_millis / 1000.0
    return SCRIPT_START_UTC + timedelta(seconds=elapsed_s)


# ============================================================
# INIT
# ============================================================
os.makedirs(LOCAL_SAVE_DIR, exist_ok=True)

app = Flask(__name__)

LOCAL_TZ = timezone(timedelta(hours=LOCAL_UTC_OFFSET_HOURS))


# ============================================================
# /data  — POST endpoint
# ============================================================
@app.route("/data", methods=["POST"])
def receive_data():
    # ----------------------------------------------------------
    # 1. Security: validate sender IP
    # ----------------------------------------------------------
    if TEENSY_IP and request.remote_addr != TEENSY_IP:
        print(
            f"[Security] Blocked request from unauthorized IP: "
            f"{request.remote_addr}  (Expected: {TEENSY_IP})"
        )
        return jsonify({
            "status":  "error",
            "message": f"Unauthorized sender IP: {request.remote_addr}"
        }), 403

    # ----------------------------------------------------------
    # 2. Parse JSON
    # ----------------------------------------------------------
    if not request.is_json:
        return jsonify({"status": "error", "message": "Expected JSON payload"}), 400

    data = request.get_json()

    core_id  = data.get("core", 0)
    samples  = data.get("samples", 0)
    print(f"\n[Teensy Core {core_id}] Received payload  (samples={samples})")

    # ----------------------------------------------------------
    # Wall-clock timestamps  (UTC, no ambiguity)
    # ----------------------------------------------------------
    # reception_utc  – the moment this HTTP request arrived on the RPi.
    reception_utc   = datetime.now(timezone.utc)
    reception_local = reception_utc.astimezone(LOCAL_TZ)

    record_start_ms = data.get("record_start", 0)
    record_stop_ms  = data.get("record_stop",  0)
    duration_ms     = max(0, record_stop_ms - record_start_ms)
    duration_s      = duration_ms / 1000.0

    # --- Option A: use NTP epoch sent directly from the ESP32 -----
    # record_start_epoch is a Unix UTC epoch (seconds) captured by
    # the ESP32 via time() immediately after configTime() synced.
    epoch_field = data.get("record_start_epoch", None)
    if epoch_field and int(epoch_field) > 0:
        estimated_start_utc = datetime.fromtimestamp(int(epoch_field), tz=timezone.utc)
        estimated_stop_utc  = estimated_start_utc + timedelta(milliseconds=duration_ms)
        time_source = "NTP (ESP32)"
    else:
        # --- Fallback Option C: back-calculate from reception time --
        estimated_stop_utc  = reception_utc
        estimated_start_utc = reception_utc - timedelta(milliseconds=duration_ms)
        time_source = "RPi-anchored estimate (no epoch in payload)"

    print(f"[Time]  Source : {time_source}")
    print(f"[Time]  Start  : {estimated_start_utc.isoformat()}")
    print(f"[Time]  Stop   : {estimated_stop_utc.isoformat()}")

    # Format duration as H:MM:SS.mmm
    h  = int(duration_ms // 3_600_000)
    m  = int((duration_ms % 3_600_000) // 60_000)
    s  = int((duration_ms % 60_000) // 1_000)
    ms = int(duration_ms % 1_000)
    duration_str = f"{h}:{m:02d}:{s:02d}.{ms:03d}"

    # ----------------------------------------------------------
    # 4. File name and local display timestamp
    # ----------------------------------------------------------
    # Use local time for the human-readable filename (easier to find on disk)
    filename_ts = reception_local.strftime("%Y%m%d_%H%M%S")
    filename    = f"Teensy_Core{core_id}_{filename_ts}.csv"

    # ISO-8601 UTC string stored in CSV metadata  ← no offset confusion
    timestamp_iso_utc = reception_utc.strftime("%Y-%m-%d %H:%M:%S+00:00")
    # Human-friendly local time for the METADATA section
    timestamp_local   = reception_local.strftime("%Y-%m-%d %H:%M:%S %Z")

    # ----------------------------------------------------------
    # 5. Extract stats
    # ----------------------------------------------------------
    freq_list = data.get("freq", [])
    mag_list  = data.get("mag",  [])
    freq_z    = float(freq_list[0]) if freq_list else 0.0

    summary_data = {
        "filename":   filename,
        # Always send UTC ISO-8601 to the dashboard – unambiguous!
        "timestamp":  timestamp_iso_utc,
        "max_x":      float(data.get("max_x",   0.0)),
        "min_x":      float(data.get("min_x",   0.0)),
        "avg_x":      float(data.get("mean_x",  0.0)),
        "freq_x":     0.0,
        "max_y":      0.0,
        "min_y":      0.0,
        "avg_y":      0.0,
        "freq_y":     0.0,
        "max_z":      float(data.get("max",     0.0)),
        "min_z":      float(data.get("min",     0.0)),
        "avg_z":      float(data.get("mean",    0.0)),
        "freq_z":     freq_z,
        "samples":    int(samples),
    }

    # ----------------------------------------------------------
    # 6. Build CSV content
    # ----------------------------------------------------------
    csv_lines = [
        "=== METADATA ===",
        "Parameter,Value,Unit",
        f"Device ID,Teensy_Core{core_id},-",
        "File number,0,-",
        f"File name,{filename},-",
        f"Reception time (UTC),{timestamp_iso_utc},-",
        f"Reception time (Local),{timestamp_local},-",
        "",
        "=== RECORDING INFO ===",
        "Parameter,Value,Unit",
        f"Record start (ms from boot),{record_start_ms},ms",
        f"Record stop  (ms from boot),{record_stop_ms},ms",
        f"Estimated start (UTC),{estimated_start_utc.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]+'+00:00'},-",
        f"Estimated stop  (UTC),{estimated_stop_utc.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]+'+00:00'},-",
        f"Duration,{duration_s:.3f},s",
        f"Duration (H:MM:SS.mmm),{duration_str},-",
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
    ]

    for i in range(min(len(freq_list), len(mag_list))):
        csv_lines.append(f"{i+1},{freq_list[i]},{mag_list[i]}")

    csv_lines.append("=" * 49)
    csv_content = "\n".join(csv_lines)

    # ----------------------------------------------------------
    # 7. Save locally
    # ----------------------------------------------------------
    local_path = os.path.join(LOCAL_SAVE_DIR, filename)
    try:
        with open(local_path, "w") as f:
            f.write(csv_content)
        print(f"[Local] Saved → {local_path}")
    except IOError as e:
        print(f"[Local] ERROR: Could not save CSV: {e}")

    # ----------------------------------------------------------
    # 8. Forward JSON stats to dashboard
    # ----------------------------------------------------------
    print("[Remote] Sending stats JSON to dashboard…")
    try:
        resp = requests.post(REMOTE_SERVER_URL, json=summary_data, timeout=10)
        if resp.status_code == 200:
            print("[Remote] Dashboard stats updated ✓")
        else:
            print(f"[Remote] ERROR: HTTP {resp.status_code} when posting stats")
    except requests.exceptions.RequestException as e:
        print(f"[Remote] ERROR: Could not reach dashboard (stats): {e}")

    # ----------------------------------------------------------
    # 9. Forward raw CSV file to dashboard
    # ----------------------------------------------------------
    print("[Remote] Forwarding raw CSV file to dashboard…")
    try:
        files    = {"file": (filename, csv_content.encode(), "text/csv")}
        resp_csv = requests.post(REMOTE_SERVER_URL, files=files, timeout=10)
        if resp_csv.status_code == 200:
            print("[Remote] CSV file forwarded ✓")
        else:
            print(f"[Remote] ERROR: HTTP {resp_csv.status_code} when forwarding CSV")
    except requests.exceptions.RequestException as e:
        print(f"[Remote] ERROR: Could not reach dashboard (CSV): {e}")

    return jsonify({"status": "received", "filename": filename}), 200


# ============================================================
# /status  — quick health-check endpoint
# ============================================================
@app.route("/status", methods=["GET"])
def status():
    return jsonify({
        "status":         "running",
        "local_save_dir": LOCAL_SAVE_DIR,
        "script_start_utc": SCRIPT_START_UTC.isoformat(),
    }), 200


# ============================================================
# Entry point
# ============================================================
if __name__ == "__main__":
    print("=" * 60)
    print("  RPi Teensy Receiver  –  HTTP Listener")
    print("=" * 60)
    print(f"  Listening on  : http://0.0.0.0:{LOCAL_PORT}")
    print(f"  Dashboard URL : {REMOTE_SERVER_URL}")
    print(f"  Local CSV dir : {LOCAL_SAVE_DIR}/")
    print(f"  Script start  : {SCRIPT_START_UTC.isoformat()} UTC")
    print(f"  Local TZ      : UTC{'+' if LOCAL_UTC_OFFSET_HOURS >= 0 else ''}{LOCAL_UTC_OFFSET_HOURS:g}h")
    print("=" * 60)
    print()
    print("  ── ESP32 / Teensy real-time timestamp options ──────────")
    print("  Option A  NTP sync via WiFi  (recommended for ESP32)")
    print("  Option B  GPS module (uBlox NEO / u-blox M10) attached to Teensy")
    print("  Option C  RPi-anchored estimation  ← this script does this now")
    print("=" * 60)
    print()

    try:
        app.run(host="0.0.0.0", port=LOCAL_PORT, debug=False, threaded=True)
    except KeyboardInterrupt:
        print("\nStopped by user.")


# ================================================================
# ESP32 REAL-TIME TIMESTAMP SUGGESTIONS
# ================================================================
#
# PROBLEM
# -------
# The ESP32/Teensy sends `record_start` and `record_stop` as raw
# millis()-since-boot.  The RPi has no way to know when the Teensy
# actually booted, so it cannot convert those milliseconds to wall-clock
# times without help.  This was the root cause of the 8-hour offset seen
# in earlier testing (the RPi clock was in UTC but the dashboard assumed
# local time, or vice-versa).
#
# ─────────────────────────────────────────────────────────────────────
# OPTION A  –  NTP Sync on the ESP32  (RECOMMENDED for ESP32)
# ─────────────────────────────────────────────────────────────────────
# The ESP32 has built-in WiFi and the Arduino SDK ships with <time.h> /
# configTime().  Add to your ESP32 sketch:
#
#   #include <time.h>
#   // In setup(), after WiFi.begin() / WiFi.waitForConnectResult():
#   configTime(0, 0, "pool.ntp.org", "time.google.com");  // UTC, no DST
#   struct tm timeinfo;
#   while (!getLocalTime(&timeinfo)) { delay(200); }      // blocks until synced
#
# Then, at the moment you START a recording window:
#   time_t now_epoch;
#   time(&now_epoch);                      // seconds since Unix epoch (UTC)
#   uint32_t start_ms = millis();
#   data["record_start_epoch"] = (long)now_epoch;   // add to JSON payload
#
# On the RPi you can then do:
#   record_start_utc = datetime.fromtimestamp(data["record_start_epoch"],
#                                             tz=timezone.utc)
#
# IMPORTANT: always use UTC offset = 0, 0 in configTime() and always
# parse with tz=timezone.utc on the Python side.  Never rely on implicit
# local-time conversion – that is what caused the 8-hour drift before.
#
# ─────────────────────────────────────────────────────────────────────
# OPTION B  –  GPS Module  (best accuracy, works offline)
# ─────────────────────────────────────────────────────────────────────
# Attach a GPS module (e.g. uBlox NEO-6M / NEO-M10) to the Teensy's
# UART (Serial1).  Use the TinyGPS++ library:
#
#   #include <TinyGPS++.h>
#   TinyGPSPlus gps;
#   // In loop() feed gps.encode(Serial1.read())
#   // When you start recording:
#   if (gps.time.isValid() && gps.date.isValid()) {
#     // Build an ISO-8601 string: "2026-08-18T07:30:00Z"
#     char iso[25];
#     snprintf(iso, sizeof(iso), "%04d-%02d-%02dT%02d:%02d:%02dZ",
#              gps.date.year(), gps.date.month(),  gps.date.day(),
#              gps.time.hour(), gps.time.minute(), gps.time.second());
#     payload["record_start_iso"] = iso;   // UTC, ends with Z
#   }
#
# Parse on the Python side:
#   from datetime import datetime, timezone
#   dt = datetime.fromisoformat(
#         data["record_start_iso"].replace("Z", "+00:00"))
#   # dt is now a timezone-aware UTC datetime – no offset issues.
#
# ─────────────────────────────────────────────────────────────────────
# OPTION C  –  RPi-anchored estimation  (what this script does now)
# ─────────────────────────────────────────────────────────────────────
# If you cannot change the ESP32/Teensy firmware:
#  • The RPi timestamps every incoming request with reception_utc.
#  • The recording STOP time ≈ reception time (data is sent immediately).
#  • The recording START time = reception_utc - duration_ms.
# Accuracy: ±network_latency (~1–5 ms on LAN) – good enough for most
# vibration-analysis use cases.
#
# AVOIDING TIMEZONE BUGS (applies to all options)
# ------------------------------------------------
# Rule 1 – Always store and transmit UTC.  Use datetime.now(timezone.utc)
#           on the RPi, not datetime.now() (naive/local).
# Rule 2 – Use ISO-8601 with explicit +00:00 or Z suffix in JSON and CSV.
# Rule 3 – configTime(0, 0, ...) on the ESP32 – UTC only, no DST offset.
# Rule 4 – On the dashboard server, parse timestamps with an explicit UTC
#           timezone (e.g. dateutil.parser.parse(ts).astimezone(timezone.utc)).
# ================================================================
