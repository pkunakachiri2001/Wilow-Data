"""
db.py — Shared Aiven PostgreSQL module
Used by server.py (RPi / local receiver).
All data is stored in the `vibration_data` table.
Set DATABASE_URL in environment or .env before importing.

Schema (33 data columns, same order as CSV_HEADERS in server.py):
    date, time_sec,
    max_az, min_az, mean_az, std_az, skewness_az, kurtosis_az,     (Z accel)
    max_ax, min_ax, mean_ax,                                       (X accel)
    max_ay, min_ay, mean_ay,                                       (Y accel)
    disp_peak/p2p/rms _z/_x/_y _mm,                                (displacement)
    fft1_freq, fft1_mag ... fft5_freq, fft5_mag                    (Z FFT peaks)

Tables created with the older 21-column layout are upgraded automatically by
init_db() (ALTER TABLE ... ADD COLUMN IF NOT EXISTS) — no data is lost.
"""

import os
import psycopg2
import psycopg2.pool
from datetime import datetime

DATABASE_URL = os.environ.get('DATABASE_URL', '')

_pool = None

# ================= SCHEMA =================
# Order MUST match CSV_HEADERS in server.py (fetch_rows_since() rows are written
# straight into the local CSV files).
TEXT_COLUMNS = ['date']
KEY_COLUMN = 'time_sec'
FLOAT_COLUMNS = [
    'max_az', 'min_az', 'mean_az', 'std_az', 'skewness_az', 'kurtosis_az',
    'max_ax', 'min_ax', 'mean_ax',
    'max_ay', 'min_ay', 'mean_ay',
    'disp_peak_z_mm', 'disp_p2p_z_mm', 'disp_rms_z_mm',
    'disp_peak_x_mm', 'disp_p2p_x_mm', 'disp_rms_x_mm',
    'disp_peak_y_mm', 'disp_p2p_y_mm', 'disp_rms_y_mm',
    'fft1_freq', 'fft1_mag', 'fft2_freq', 'fft2_mag',
    'fft3_freq', 'fft3_mag', 'fft4_freq', 'fft4_mag',
    'fft5_freq', 'fft5_mag',
]
DB_COLUMNS = TEXT_COLUMNS + [KEY_COLUMN] + FLOAT_COLUMNS   # 33 columns

_COL_LIST    = ', '.join(DB_COLUMNS)
_PARAM_LIST  = ', '.join(f'%({c})s' for c in DB_COLUMNS)
_INSERT_SQL  = f"""
    INSERT INTO vibration_data ({_COL_LIST})
    VALUES ({_PARAM_LIST})
    ON CONFLICT (time_sec) DO NOTHING
"""


def _make_dsn() -> str:
    """
    Return DATABASE_URL for Aiven DB.
    (Removed aggressive keepalives used for Aiven serverless).
    """
    return DATABASE_URL


def _get_pool():
    global _pool
    if _pool is None or _pool.closed:
        if not DATABASE_URL:
            raise RuntimeError("[DB] DATABASE_URL env var is not set.")
        _pool = psycopg2.pool.ThreadedConnectionPool(
            1, 10, _make_dsn()
        )
    return _pool


def _get_conn():
    """
    Fetch a connection from the pool.
    Pings the connection with SELECT 1 to ensure it's alive.
    If the pool is poisoned by a stale Aiven connection, reset it entirely
    so the next request gets a fresh healthy connection instead of failing.
    """
    global _pool
    max_retries = 3
    for attempt in range(max_retries):
        try:
            conn = _get_pool().getconn()
            # Ping the connection to ensure it is alive
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return conn
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            print(f"[DB] Stale connection detected on getconn (attempt {attempt+1}): {e}. Resetting pool.")
            try:
                if _pool and not _pool.closed:
                    _pool.closeall()
            except Exception:
                pass
            _pool = None

    # Fallback to returning a fresh connection on the last try
    return _get_pool().getconn()


def _put_conn(conn):
    _get_pool().putconn(conn)


# ================= INIT =================
def init_db():
    """
    Create the vibration_data table if it does not exist, and add any columns
    that are missing from an older (21-column) table.
    """
    float_defs = ',\n                    '.join(f'{c} DOUBLE PRECISION' for c in FLOAT_COLUMNS)
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS vibration_data (
                    id           SERIAL PRIMARY KEY,
                    date         TEXT,
                    time_sec     DOUBLE PRECISION UNIQUE NOT NULL,
                    {float_defs},
                    created_at   TIMESTAMP DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_time_sec ON vibration_data(time_sec);
                CREATE INDEX IF NOT EXISTS idx_date     ON vibration_data(date);
            """)
            # Migration: upgrade tables created with the old 21-column layout
            for col in FLOAT_COLUMNS:
                cur.execute(
                    f"ALTER TABLE vibration_data ADD COLUMN IF NOT EXISTS {col} DOUBLE PRECISION"
                )
            conn.commit()
        print(f"[DB] vibration_data table ready ({len(DB_COLUMNS)} data columns).")
    except Exception as e:
        conn.rollback()
        print(f"[DB ERROR] init_db: {e}")
        raise
    finally:
        _put_conn(conn)


# ================= WRITE =================
def _complete_row(row_dict: dict) -> dict:
    """Return a dict with every DB column present (missing -> None/NULL)."""
    return {c: row_dict.get(c) for c in DB_COLUMNS}


def insert_row(row_dict: dict):
    """
    Insert one parsed row into Aiven.
    Silently skips duplicate time_sec values (ON CONFLICT DO NOTHING).

    Expected keys: see DB_COLUMNS (date, time_sec, max_az ... fft5_mag).
    Missing keys are stored as NULL.
    """
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(_INSERT_SQL, _complete_row(row_dict))
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
                cur.execute(_INSERT_SQL, _complete_row(row_dict))
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
    """Returns the max time_sec stored in Aiven, or 0.0 if empty."""
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


def get_latest_row_dict() -> dict:
    """
    Fetch the single newest row by time_sec from vibration_data as a dictionary.
    Used by cloud server to hydrate latest_kpi on startup or after restarts.
    """
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT {_COL_LIST}
                FROM vibration_data
                ORDER BY time_sec DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                return {}
            return dict(zip(DB_COLUMNS, row))
    except Exception as e:
        print(f"[DB ERROR] get_latest_row_dict: {e}")
        return {}
    finally:
        _put_conn(conn)


def fetch_rows_since(epoch_time: float) -> list:
    """
    Fetch all rows with time_sec > epoch_time.
    Used by local server on startup to sync missed data from Aiven.
    Returns list of tuples in CSV column order (see DB_COLUMNS).
    """
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT {_COL_LIST}
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
    Returns list of tuples in DB_COLUMNS order:
      (date, time_sec, max_az, min_az, mean_az, std_az, skewness_az, kurtosis_az,
       max_ax, min_ax, mean_ax, max_ay, min_ay, mean_ay, disp_*..., fft1_freq, ...)
    """
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            if start_time and end_time:
                cur.execute(f"""
                    SELECT {_COL_LIST}
                    FROM vibration_data
                    WHERE time_sec >= %s AND time_sec <= %s
                    ORDER BY time_sec ASC
                """, (float(start_time), float(end_time)))
            elif last_n_minutes is not None:
                cur.execute(f"""
                    SELECT {_COL_LIST}
                    FROM vibration_data
                    WHERE time_sec >= EXTRACT(EPOCH FROM NOW()) - %s
                    ORDER BY time_sec ASC
                """, (float(last_n_minutes) * 60,))
            else:
                cur.execute(f"""
                    SELECT {_COL_LIST}
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
    purge rows from Aiven (keeping it as a lean transit buffer).
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
        print(f"[DB] Acknowledged delete: {deleted} rows removed from Aiven (epoch <= {epoch_time:.0f}).")
        return deleted
    except Exception as e:
        conn.rollback()
        print(f"[DB ERROR] delete_rows_before_epoch: {e}")
        return 0
    finally:
        _put_conn(conn)


def get_row_count() -> int:
    """Fast COUNT(*) — used by overflow guard."""
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
    Used by an overflow guard when row count exceeds a maximum.
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

    row = {'date': date_str, 'time_sec': epoch_time}

    # Scalar stats (max/min/mean Az/Ax/Ay, std/skew/kurt, displacement metrics)
    for col in FLOAT_COLUMNS:
        if not col.startswith('fft'):
            row[col] = parsed.get(col)

    # FFT peaks (up to 5)
    for n in range(5):
        row[f'fft{n+1}_freq'] = fv(n, 0)
        row[f'fft{n+1}_mag']  = fv(n, 1)

    return row
