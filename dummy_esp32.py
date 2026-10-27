"""
dummy_esp32.py  —  Simulates esp32_one (Core 1) and esp32_num2 (Core 2)
sending vibration data to the local server for dashboard testing.

Usage:
    python dummy_esp32.py                  # sends to http://127.0.0.1:5000
    python dummy_esp32.py --url http://10.25.1.38:5000
    python dummy_esp32.py --interval 5    # send every 5 seconds (default: 8)
    python dummy_esp32.py --core 1        # only simulate Core 1
    python dummy_esp32.py --core 2        # only simulate Core 2
    python dummy_esp32.py --burst 20      # send 20 packets then stop (default: infinite)

The script alternates between Core 1 (Z primary) and Core 2 (secondary sync)
to mimic the real SYNC-pin handshake behaviour of the firmware.
"""

import argparse
import math
import random
import time
import requests
from datetime import datetime, timezone

# ──────────────────────────────────────────────────────────────
#  Default configuration
# ──────────────────────────────────────────────────────────────
DEFAULT_URL      = "http://127.0.0.1:5000"
DEFAULT_INTERVAL = 8          # seconds between sends
SAMPLE_RATE      = 3200.0     # Hz  (matches firmware)
RECORD_DURATION  = 5.0        # seconds per recording window
NUM_SAMPLES      = int(SAMPLE_RATE * RECORD_DURATION)

# Realistic base vibration values (industrial motor — clearly visible on charts)
BASE_ACC_Z  = 5.2    # m/s²  — primary axis (well within ±45 scale)
BASE_ACC_X  = 2.8    # m/s²
BASE_ACC_Y  = 1.9    # m/s²
NOISE       = 0.4    # ±random jitter per packet (~8%)

# ──────────────────────────────────────────────────────────────
#  Helpers
# ──────────────────────────────────────────────────────────────
def _jitter(base: float, noise: float = NOISE) -> float:
    """Return base ± small random noise."""
    return round(base + random.uniform(-noise, noise), 5)


def _disp_dict(acc_peak: float) -> dict:
    """
    Approximate displacement from acceleration peak using:
        d = a / (2*pi*f)^2
    at a representative frequency of 50 Hz.
    """
    freq = 50.0  # Hz
    omega = 2 * math.pi * freq
    d_peak = abs(acc_peak) / (omega ** 2) * 1000.0   # → mm
    d_p2p  = d_peak * 2.0
    d_rms  = d_peak / math.sqrt(2)
    return {
        "max":  round( d_peak, 6),
        "min":  round(-d_peak, 6),
        "peak": round( d_peak, 6),
        "p2p":  round( d_p2p,  6),
        "rms":  round( d_rms,  6),
    }


def _vel_dict(acc_peak: float) -> dict:
    """
    Approximate velocity from acceleration peak:
        v = a / (2*pi*f)
    """
    freq  = 50.0
    omega = 2 * math.pi * freq
    v_peak = abs(acc_peak) / omega
    v_p2p  = v_peak * 2.0
    v_rms  = v_peak / math.sqrt(2)
    return {
        "max":  round( v_peak, 6),
        "min":  round(-v_peak, 6),
        "peak": round( v_peak, 6),
        "p2p":  round( v_p2p,  6),
        "rms":  round( v_rms,  6),
    }


def _fft_peaks(base_freq: float = 50.0, n: int = 5) -> tuple[list, list]:
    """Generate n harmonic FFT peaks (freq, mag)."""
    freqs, mags = [], []
    for i in range(1, n + 1):
        f = round(base_freq * i + random.uniform(-0.5, 0.5), 2)
        m = round(random.uniform(0.05, 0.3) / i, 5)   # harmonics decay
        freqs.append(f)
        mags.append(m)
    return freqs, mags


def build_payload(core_id: int, packet_index: int) -> dict:
    """
    Build a full JSON payload that matches the schema expected by
    server.py  receive_esp32_data()  (POST /data).
    """
    # Slowly drift the acceleration upward every 10 packets to show trend
    drift = (packet_index // 10) * 0.15

    max_az  = _jitter(BASE_ACC_Z + drift)
    min_az  = _jitter(-BASE_ACC_Z - drift)
    mean_az = _jitter(0.0, 0.01)
    std_az  = _jitter(0.25, 0.02)

    max_ax  = _jitter(BASE_ACC_X)
    min_ax  = _jitter(-BASE_ACC_X)
    mean_ax = _jitter(0.0, 0.005)

    max_ay  = _jitter(BASE_ACC_Y)
    min_ay  = _jitter(-BASE_ACC_Y)
    mean_ay = _jitter(0.0, 0.005)

    freqs, mags = _fft_peaks(base_freq=50.0)

    boot_ms = int(time.time() * 1000) % (2**31)   # millis-from-boot spoof
    record_start = boot_ms
    record_stop  = boot_ms + int(RECORD_DURATION * 1000)

    now_epoch = int(datetime.now(timezone.utc).timestamp())

    payload = {
        # ── Identity ──────────────────────────────────
        "core":               core_id,
        "samples":            NUM_SAMPLES,
        "sample_rate":        SAMPLE_RATE,
        "record_start":       record_start,
        "record_stop":        record_stop,
        "record_start_epoch": now_epoch,

        # ── Z-axis statistics ─────────────────────────
        "max":      round(max_az,  5),
        "min":      round(min_az,  5),
        "mean":     round(mean_az, 5),
        "std_dev":  round(std_az,  5),
        "skewness": round(random.uniform(-0.3, 0.3), 4),
        "kurtosis": round(random.uniform(-0.5, 0.5), 4),

        # ── X-axis statistics ─────────────────────────
        "max_x":  round(max_ax,  5),
        "min_x":  round(min_ax,  5),
        "mean_x": round(mean_ax, 5),

        # ── Y-axis statistics ─────────────────────────
        "max_y":  round(max_ay,  5),
        "min_y":  round(min_ay,  5),
        "mean_y": round(mean_ay, 5),

        # ── Displacement (mm) ─────────────────────────
        "disp_z_mm": _disp_dict(max_az),
        "disp_x_mm": _disp_dict(max_ax),
        "disp_y_mm": _disp_dict(max_ay),

        # ── Velocity (m/s) ────────────────────────────
        "vel_z_ms": _vel_dict(max_az),
        "vel_x_ms": _vel_dict(max_ax),
        "vel_y_ms": _vel_dict(max_ay),

        # ── FFT peaks ─────────────────────────────────
        "freq": freqs,
        "mag":  mags,

        # ── Diagnostics ───────────────────────────────
        "diag": {
            "wifi_rssi":    random.randint(-75, -45),
            "free_heap":    random.randint(180000, 280000),
            "uptime_ms":    boot_ms,
        },
    }
    return payload


# ──────────────────────────────────────────────────────────────
#  Main send loop
# ──────────────────────────────────────────────────────────────
def send_packet(url: str, core_id: int, packet_index: int, verbose: bool = True) -> bool:
    payload = build_payload(core_id, packet_index)
    endpoint = f"{url.rstrip('/')}/data"
    esp_name = f"esp32_one (Core 1)" if core_id == 1 else f"esp32_num2 (Core 2)"

    try:
        resp = requests.post(
            endpoint,
            json=payload,
            timeout=10,
            headers={"Content-Type": "application/json"},
        )
        ts = datetime.now().strftime("%H:%M:%S")
        if resp.status_code == 200:
            result = resp.json()
            alert  = result.get("alert") or "None"
            if verbose:
                print(
                    f"[{ts}] [OK] [{esp_name}] #{packet_index:04d}  "
                    f"Az={payload['max']:.4f} m/s2  "
                    f"Disp_Z={payload['disp_z_mm']['peak']:.6f} mm  "
                    f"Vel_Z={payload['vel_z_ms']['peak']:.6f} m/s  "
                    f"Alert={alert}"
                )
            return True
        else:
            print(f"[{ts}] [ERR] [{esp_name}] HTTP {resp.status_code}: {resp.text[:120]}")
            return False
    except requests.exceptions.ConnectionError:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] [ERR] Cannot connect to {endpoint}  -- is the server running?")
        return False
    except Exception as exc:
        print(f"[ERROR] {esp_name}: {exc}")
        return False


def run(url: str, interval: float, cores: list[int], burst: int | None):
    print("=" * 65)
    print("  Cyrus Vibration -- ESP32 Dummy Data Simulator")
    print("=" * 65)
    print(f"  Target  : {url}")
    print(f"  Interval: {interval}s per packet")
    print(f"  Cores   : {cores}")
    print(f"  Burst   : {'infinite' if burst is None else burst} packets")
    print("=" * 65)
    print("  Press Ctrl+C to stop.\n")

    packet_index = 0
    try:
        while burst is None or packet_index < burst:
            # Alternate: Core 1 sends first, Core 2 follows 1 s later
            # (matches firmware SYNC-pin handshake: Core 1 records first)
            for core_id in cores:
                if core_id == 2 and len(cores) > 1:
                    time.sleep(1.0)   # simulate 1 s sync delay
                send_packet(url, core_id, packet_index)

            packet_index += 1
            if burst is None or packet_index < burst:
                time.sleep(max(0.0, interval - (1.0 if len(cores) > 1 else 0.0)))

    except KeyboardInterrupt:
        print(f"\n[STOP] Sent {packet_index} round(s). Goodbye.")


# ──────────────────────────────────────────────────────────────
#  CLI
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Simulate esp32_one + esp32_num2 sending vibration data to the server."
    )
    parser.add_argument(
        "--url", default=DEFAULT_URL,
        help=f"Server base URL (default: {DEFAULT_URL})"
    )
    parser.add_argument(
        "--interval", type=float, default=DEFAULT_INTERVAL,
        help=f"Seconds between sends per core (default: {DEFAULT_INTERVAL})"
    )
    parser.add_argument(
        "--core", type=int, choices=[0, 1, 2], default=0,
        help="Which core to simulate: 0=both (default), 1=esp32_one only, 2=esp32_num2 only"
    )
    parser.add_argument(
        "--burst", type=int, default=None,
        help="Send this many packets then stop (default: run forever)"
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Suppress per-packet output"
    )
    args = parser.parse_args()

    cores = [1, 2] if args.core == 0 else [args.core]
    run(
        url      = args.url,
        interval = args.interval,
        cores    = cores,
        burst    = args.burst,
    )
