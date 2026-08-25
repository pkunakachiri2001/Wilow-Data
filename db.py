"""
db.py — Shared Neon PostgreSQL module
Used by both cloud_server.py (Render) and server.py (local)
All data is stored in the `vibration_data` table.
Set DATABASE_URL in environment or .env before importing.
"""

import os
import psycopg2
import psycopg2.pool
from datetime import datetime

DATABASE_URL = os.environ.get('DATABASE_URL', '')

_pool = None


def _get_pool():
    global _pool
    if _pool is None or _pool.closed:
        if not DATABASE_URL:
            raise RuntimeError("[DB] DATABASE_URL env var is not set.")
        _pool = psycopg2.pool.ThreadedConnectionPool(
            1, 10, DATABASE_URL
        )
    return _pool


def _get_conn():
    return _get_pool().getconn()


def _put_conn(conn):
    _get_pool().putconn(conn)


# ================= INIT =================
def init_db():
    """Create the vibration_data table if it does not exist."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS vibration_data (
                    id           SERIAL PRIMARY KEY,
                    date         TEXT,
                    time_sec     DOUBLE PRECISION UNIQUE NOT NULL,
                    max_az       DOUBLE PRECISION,
                    min_az       DOUBLE PRECISION,
                    mean_az      DOUBLE PRECISION,
                    std_az       DOUBLE PRECISION,
                    skewness_az  DOUBLE PRECISION,
                    kurtosis_az  DOUBLE PRECISION,
                    max_ax       DOUBLE PRECISION,
                    min_ax       DOUBLE PRECISION,
                    mean_ax      DOUBLE PRECISION,
                    fft1_freq    DOUBLE PRECISION,
                    fft1_mag     DOUBLE PRECISION,
                    fft2_freq    DOUBLE PRECISION,
                    fft2_mag     DOUBLE PRECISION,
                    fft3_freq    DOUBLE PRECISION,
                    fft3_mag     DOUBLE PRECISION,
                    fft4_freq    DOUBLE PRECISION,
                    fft4_mag     DOUBLE PRECISION,
                    fft5_freq    DOUBLE PRECISION,
                    fft5_mag     DOUBLE PRECISION,
                    created_at   TIMESTAMP DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_time_sec ON vibration_data(time_sec);
                CREATE INDEX IF NOT EXISTS idx_date     ON vibration_data(date);
            """)
            conn.commit()
        print("[DB] vibration_data table ready.")
    except Exception as e:
        conn.rollback()
        print(f"[DB ERROR] init_db: {e}")
        raise
    finally:
        _put_conn(conn)


# ================= WRITE =================
def insert_row(row_dict: dict):
    """
    Insert one parsed row into Neon.
    Silently skips duplicate time_sec values (ON CONFLICT DO NOTHING).

    Expected keys:
        date, time_sec,
        max_az, min_az, mean_az, std_az, skewness_az, kurtosis_az,
        max_ax, min_ax, mean_ax,
        fft1_freq, fft1_mag, fft2_freq, fft2_mag,
        fft3_freq, fft3_mag, fft4_freq, fft4_mag, fft5_freq, fft5_mag
    """
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO vibration_data
                    (date, time_sec,
                     max_az,  min_az,  mean_az,  std_az,  skewness_az,  kurtosis_az,
                     max_ax,  min_ax,  mean_ax,
                     fft1_freq, fft1_mag, fft2_freq, fft2_mag,
                     fft3_freq, fft3_mag, fft4_freq, fft4_mag,
                     fft5_freq, fft5_mag)
                VALUES
                    (%(date)s,     %(time_sec)s,
                     %(max_az)s,   %(min_az)s,   %(mean_az)s,  %(std_az)s,
                     %(skewness_az)s, %(kurtosis_az)s,
                     %(max_ax)s,   %(min_ax)s,   %(mean_ax)s,
                     %(fft1_freq)s, %(fft1_mag)s,
                     %(fft2_freq)s, %(fft2_mag)s,
                     %(fft3_freq)s, %(fft3_mag)s,
                     %(fft4_freq)s, %(fft4_mag)s,
                     %(fft5_freq)s, %(fft5_mag)s)
                ON CONFLICT (time_sec) DO NOTHING
            """, row_dict)
            conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[DB ERROR] insert_row: {e}")
        raise
    finally:
        _put_conn(conn)


def insert_many_rows(rows: list):
    """Bulk insert a list of row_dicts. Useful for sync / migration."""
    if not rows:
        return
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            for row_dict in rows:
                cur.execute("""
                    INSERT INTO vibration_data
                        (date, time_sec,
                         max_az,  min_az,  mean_az,  std_az,  skewness_az,  kurtosis_az,
                         max_ax,  min_ax,  mean_ax,
                         fft1_freq, fft1_mag, fft2_freq, fft2_mag,
                         fft3_freq, fft3_mag, fft4_freq, fft4_mag,
                         fft5_freq, fft5_mag)
                    VALUES
                        (%(date)s,     %(time_sec)s,
                         %(max_az)s,   %(min_az)s,   %(mean_az)s,  %(std_az)s,
                         %(skewness_az)s, %(kurtosis_az)s,
                         %(max_ax)s,   %(min_ax)s,   %(mean_ax)s,
                         %(fft1_freq)s, %(fft1_mag)s,
                         %(fft2_freq)s, %(fft2_mag)s,
                         %(fft3_freq)s, %(fft3_mag)s,
                         %(fft4_freq)s, %(fft4_mag)s,
                         %(fft5_freq)s, %(fft5_mag)s)
                    ON CONFLICT (time_sec) DO NOTHING
                """, row_dict)
        conn.commit()
        print(f"[DB] Bulk inserted {len(rows)} rows.")
    except Exception as e:
        conn.rollback()
        print(f"[DB ERROR] insert_many_rows: {e}")
        raise
    finally:
        _put_conn(conn)


# ================= READ =================
def get_latest_timestamp() -> float:
    """Returns the max time_sec stored in Neon, or 0.0 if empty."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(time_sec) FROM vibration_data")
            result = cur.fetchone()
            return float(result[0]) if result and result[0] is not None else 0.0
    except Exception as e:
        print(f"[DB ERROR] get_latest_timestamp: {e}")
        return 0.0
    finally:
        _put_conn(conn)


def fetch_rows_since(epoch_time: float) -> list:
    """
    Fetch all rows with time_sec > epoch_time.
    Used by local server on startup to sync missed data from Neon.
    Returns list of tuples in CSV column order.
    """
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT date, time_sec,
                       max_az,  min_az,  mean_az,  std_az,  skewness_az,  kurtosis_az,
                       max_ax,  min_ax,  mean_ax,
                       fft1_freq, fft1_mag, fft2_freq, fft2_mag,
                       fft3_freq, fft3_mag, fft4_freq, fft4_mag,
                       fft5_freq, fft5_mag
                FROM vibration_data
                WHERE time_sec > %s
                ORDER BY time_sec ASC
            """, (epoch_time,))
            return cur.fetchall()
    except Exception as e:
        print(f"[DB ERROR] fetch_rows_since: {e}")
        return []
    finally:
        _put_conn(conn)


def fetch_rows_for_chart(axis='Z', start_time=None, end_time=None, last_n_minutes=None) -> list:
    """
    Fetch rows for chart rendering.
    Returns list of tuples:
      (date, time_sec, max_az, min_az, mean_az, std_az, skewness_az, kurtosis_az,
       max_ax, min_ax, mean_ax, fft1_freq, fft1_mag, ...)
    """
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            if start_time and end_time:
                cur.execute("""
                    SELECT date, time_sec,
                           max_az, min_az, mean_az, std_az, skewness_az, kurtosis_az,
                           max_ax, min_ax, mean_ax,
                           fft1_freq, fft1_mag, fft2_freq, fft2_mag,
                           fft3_freq, fft3_mag, fft4_freq, fft4_mag,
                           fft5_freq, fft5_mag
                    FROM vibration_data
                    WHERE time_sec >= %s AND time_sec <= %s
                    ORDER BY time_sec ASC
                """, (float(start_time), float(end_time)))
            elif last_n_minutes is not None:
                cur.execute("""
                    SELECT date, time_sec,
                           max_az, min_az, mean_az, std_az, skewness_az, kurtosis_az,
                           max_ax, min_ax, mean_ax,
                           fft1_freq, fft1_mag, fft2_freq, fft2_mag,
                           fft3_freq, fft3_mag, fft4_freq, fft4_mag,
                           fft5_freq, fft5_mag
                    FROM vibration_data
                    WHERE time_sec >= EXTRACT(EPOCH FROM NOW()) - %s
                    ORDER BY time_sec ASC
                """, (float(last_n_minutes) * 60,))
            else:
                cur.execute("""
                    SELECT date, time_sec,
                           max_az, min_az, mean_az, std_az, skewness_az, kurtosis_az,
                           max_ax, min_ax, mean_ax,
                           fft1_freq, fft1_mag, fft2_freq, fft2_mag,
                           fft3_freq, fft3_mag, fft4_freq, fft4_mag,
                           fft5_freq, fft5_mag
                    FROM vibration_data
                    ORDER BY time_sec DESC LIMIT 600
                """)
            return cur.fetchall()
    except Exception as e:
        print(f"[DB ERROR] fetch_rows_for_chart: {e}")
        return []
    finally:
        _put_conn(conn)


def get_available_dates() -> list:
    """Return sorted list of distinct date strings (newest first)."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT date FROM vibration_data
                WHERE date IS NOT NULL
                ORDER BY date DESC
            """)
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"[DB ERROR] get_available_dates: {e}")
        return []
    finally:
        _put_conn(conn)


# ================= DELETE (transit buffer management) =================
def delete_rows_before_epoch(epoch_time: float) -> int:
    """
    Delete all rows with time_sec <= epoch_time.
    Called by local server after a successful sync to acknowledge and
    purge rows from Neon (keeping it as a lean transit buffer).
    Returns number of rows deleted.
    """
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM vibration_data WHERE time_sec <= %s",
                (epoch_time,)
            )
            deleted = cur.rowcount
            conn.commit()
        print(f"[DB] Acknowledged delete: {deleted} rows removed from Neon (epoch <= {epoch_time:.0f}).")
        return deleted
    except Exception as e:
        conn.rollback()
        print(f"[DB ERROR] delete_rows_before_epoch: {e}")
        return 0
    finally:
        _put_conn(conn)


def get_row_count() -> int:
    """Fast COUNT(*) — used by cloud server overflow guard."""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM vibration_data")
            result = cur.fetchone()
            return int(result[0]) if result else 0
    except Exception as e:
        print(f"[DB ERROR] get_row_count: {e}")
        return 0
    finally:
        _put_conn(conn)


def delete_oldest_rows(n: int) -> int:
    """
    Delete the n oldest rows by time_sec.
    Called by cloud server overflow guard when row count exceeds MAX_NEON_ROWS.
    Returns number of rows deleted.
    """
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM vibration_data
                WHERE id IN (
                    SELECT id FROM vibration_data
                    ORDER BY time_sec ASC
                    LIMIT %s
                )
            """, (n,))
            deleted = cur.rowcount
            conn.commit()
        print(f"[DB] Overflow trim: deleted {deleted} oldest rows.")
        return deleted
    except Exception as e:
        conn.rollback()
        print(f"[DB ERROR] delete_oldest_rows: {e}")
        return 0
    finally:
        _put_conn(conn)


# ================= HELPERS =================
def parsed_to_row_dict(parsed: dict, date_str: str, epoch_time: float) -> dict:
    """Convert parse_teensy_file() output → row_dict for insert_row()."""
    peaks = parsed.get('fft_peaks', [])

    def fv(i, j):
        return peaks[i][j] if i < len(peaks) else None

    return {
        'date':        date_str,
        'time_sec':    epoch_time,
        'max_az':      parsed.get('max_az'),
        'min_az':      parsed.get('min_az'),
        'mean_az':     parsed.get('mean_az'),
        'std_az':      parsed.get('std_az'),
        'skewness_az': parsed.get('skewness_az'),
        'kurtosis_az': parsed.get('kurtosis_az'),
        'max_ax':      parsed.get('max_ax'),
        'min_ax':      parsed.get('min_ax'),
        'mean_ax':     parsed.get('mean_ax'),
        'fft1_freq': fv(0, 0), 'fft1_mag': fv(0, 1),
        'fft2_freq': fv(1, 0), 'fft2_mag': fv(1, 1),
        'fft3_freq': fv(2, 0), 'fft3_mag': fv(2, 1),
        'fft4_freq': fv(3, 0), 'fft4_mag': fv(3, 1),
        'fft5_freq': fv(4, 0), 'fft5_mag': fv(4, 1),
    }
