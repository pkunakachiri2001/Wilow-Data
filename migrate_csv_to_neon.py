#!/usr/bin/env python3
"""
migrate_csv_to_neon.py — One-time migration script
Imports all existing master_data.csv rows into the Neon PostgreSQL database.

Usage:
    DATABASE_URL="postgresql://..." python migrate_csv_to_neon.py

Safe to run multiple times — uses ON CONFLICT (time_sec) DO NOTHING.
Handles NUL bytes and other corrupt rows gracefully (skips bad rows).
"""

import os
import sys
import csv
from datetime import datetime

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import db

MASTER_CSV = "master_data.csv"
BATCH_SIZE = 500  # Insert in batches of 500 rows


def _sanitize(value):
    """Strip NUL bytes and surrounding whitespace from string values."""
    if isinstance(value, str):
        return value.replace('\x00', '').strip()
    return value


def _sanitize_row(row_dict):
    """Apply _sanitize to all string values in a row dict."""
    return {k: (_sanitize(v) if isinstance(v, str) else v)
            for k, v in row_dict.items()}


def _insert_batch_with_fallback(batch, batch_num):
    """
    Try a bulk batch insert. If that fails (e.g. NUL bytes in one row),
    fall back to inserting rows one at a time and skip only the bad ones.
    Returns (inserted_count, skipped_count).
    """
    try:
        db.insert_many_rows(batch)
        return len(batch), 0
    except Exception as bulk_err:
        print(f"\n[MIGRATE] Batch {batch_num} bulk insert failed ({bulk_err}) — "
              f"falling back to row-by-row insert...")
        inserted = 0
        skipped  = 0
        for row_dict in batch:
            try:
                db.insert_row(row_dict)
                inserted += 1
            except Exception as row_err:
                print(f"[MIGRATE] ⚠  Skipping row time_sec={row_dict.get('time_sec')}: {row_err}")
                skipped += 1
        return inserted, skipped


def run_migration():
    if not os.path.exists(MASTER_CSV):
        print(f"[MIGRATE] ERROR: {MASTER_CSV} not found in current directory.")
        sys.exit(1)

    print(f"[MIGRATE] Connecting to Neon DB...")
    db.init_db()
    print(f"[MIGRATE] Table ready.")

    print(f"[MIGRATE] Reading {MASTER_CSV} (stripping NUL bytes)...")
    rows_to_insert = []
    skipped = 0
    total   = 0

    # Open with errors='replace' to handle any binary corruption at the file level
    with open(MASTER_CSV, 'r', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            try:
                time_sec_raw = _sanitize(row.get('Time_sec', '') or '')
                time_sec = float(time_sec_raw) if time_sec_raw else 0.0
                if time_sec == 0:
                    skipped += 1
                    continue

                date_str = _sanitize(row.get('Date', ''))
                if not date_str:
                    date_str = datetime.fromtimestamp(time_sec).strftime('%Y-%m-%d')

                def _float(key):
                    v = _sanitize(row.get(key, '') or '')
                    try:
                        return float(v) if v else None
                    except (ValueError, TypeError):
                        return None

                row_dict = _sanitize_row({
                    'date':        date_str,
                    'time_sec':    time_sec,
                    'max_az':      _float('Max_Az'),
                    'min_az':      _float('Min_Az'),
                    'mean_az':     _float('Mean_Az'),
                    'std_az':      _float('Std_Az'),
                    'skewness_az': _float('Skewness_Az'),
                    'kurtosis_az': _float('Kurtosis_Az'),
                    'max_ax':      _float('Max_Ax'),
                    'min_ax':      _float('Min_Ax'),
                    'mean_ax':     _float('Mean_Ax'),
                    'fft1_freq':   _float('FFT1_Freq'),
                    'fft1_mag':    _float('FFT1_Mag'),
                    'fft2_freq':   _float('FFT2_Freq'),
                    'fft2_mag':    _float('FFT2_Mag'),
                    'fft3_freq':   _float('FFT3_Freq'),
                    'fft3_mag':    _float('FFT3_Mag'),
                    'fft4_freq':   _float('FFT4_Freq'),
                    'fft4_mag':    _float('FFT4_Mag'),
                    'fft5_freq':   _float('FFT5_Freq'),
                    'fft5_mag':    _float('FFT5_Mag'),
                })
                rows_to_insert.append(row_dict)

            except Exception as e:
                print(f"[MIGRATE] Skipping row {total}: {e}")
                skipped += 1
                continue

    print(f"[MIGRATE] CSV read: {total} total rows, {skipped} skipped, "
          f"{len(rows_to_insert)} to insert.")

    if not rows_to_insert:
        print("[MIGRATE] Nothing to insert. Done.")
        return

    # Batch insert with per-row fallback on failure
    total_inserted = 0
    total_skipped  = 0
    batch_num = 0
    for i in range(0, len(rows_to_insert), BATCH_SIZE):
        batch     = rows_to_insert[i:i + BATCH_SIZE]
        batch_num += 1
        ins, skp  = _insert_batch_with_fallback(batch, batch_num)
        total_inserted += ins
        total_skipped  += skp
        progress = min(i + BATCH_SIZE, len(rows_to_insert))
        print(f"[MIGRATE] Progress: {progress}/{len(rows_to_insert)} rows processed...", end='\r')

    print(f"\n[MIGRATE] ✅ Migration complete!")
    print(f"[MIGRATE]    Inserted : {total_inserted} rows")
    print(f"[MIGRATE]    Skipped  : {total_skipped} bad rows")

    # Verify
    latest = db.get_latest_timestamp()
    if latest:
        latest_dt = datetime.fromtimestamp(latest).strftime('%Y-%m-%d %H:%M:%S')
        print(f"[MIGRATE]    Latest row in DB: {latest_dt}")


if __name__ == "__main__":
    if not os.environ.get("DATABASE_URL"):
        print("[MIGRATE] ERROR: DATABASE_URL environment variable not set.")
        print("  Set it with:  export DATABASE_URL='postgresql://...'")
        sys.exit(1)
    run_migration()
