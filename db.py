"""
db.py — Shared Neon PostgreSQL module
Used by both cloud_server.py (Render) and server.py (local hub / RPi).
Tables managed automatically by init_db():
  1. vibration_data   (33 data columns + id, created_at)
  2. app_thresholds   (single-row JSON config + updated_at epoch for cloud<->local sync)
  3. vibration_events (saved threshold/manual fault event snapshots + metadata, bounded to max 200)
"""

from datetime import datetime, timezone
import json
import os
import time
import urllib.parse
import psycopg2
import psycopg2.pool
import psycopg2.extensions

DATABASE_URL = os.environ.get('DATABASE_URL', '')


def _get_database_url() -> str:
    """Resolve DATABASE_URL dynamically so .env loaded after import works reliably."""
    return os.environ.get('DATABASE_URL', '').strip() or DATABASE_URL.strip()


def is_configured() -> bool:
    """Return True if a PostgreSQL DATABASE_URL is currently configured."""
    return bool(_get_database_url())


# ================= SCHEMA =================
# Order MUST match vibration_core.CSV_HEADERS (33 columns).
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
DB_COLUMNS = TEXT_COLUMNS + [KEY_COLUMN] + FLOAT_COLUMNS  # 33 columns

_COL_LIST = ', '.join(DB_COLUMNS)
_PARAM_LIST = ', '.join(f'%({c})s' for c in DB_COLUMNS)
_INSERT_SQL = f"""
    INSERT INTO vibration_data ({_COL_LIST})
    VALUES ({_PARAM_LIST})
    ON CONFLICT (time_sec) DO NOTHING
"""


def _make_dsn() -> str:
    """
    Return DATABASE_URL with SSL + TCP keepalives so orphaned sessions from
    restarts/deploys are freed rapidly on Neon.
    """
    dsn = _get_database_url()
    if not dsn:
        return ''
    sep = '&' if '?' in dsn else '?'
    params = [
        'sslmode=require',
        'keepalives=1',
        'keepalives_idle=20',
        'keepalives_interval=10',
        'keepalives_count=3',
    ]
    for p in params:
        k = p.split('=')[0]
        if f"{k}=" not in dsn:
            dsn = f"{dsn}{sep}{p}"
            sep = '&'
    return dsn


def _strip_channel_binding(dsn: str) -> str:
    """Fallback helper for older libpq builds that reject channel_binding query param."""
    parts = urllib.parse.urlsplit(dsn)
    qs = [
        (k, v)
        for k, v in urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
        if k.lower() != 'channel_binding'
    ]
    return urllib.parse.urlunsplit((
        parts.scheme,
        parts.netloc,
        parts.path,
        urllib.parse.urlencode(qs),
        parts.fragment,
    ))


POOL_MAX_CONN = 4
IDLE_PING_SECONDS = 30.0
_pool = None


class DBConnectionPool:
    """
    Thread-safe & greenlet-safe self-healing connection pool.
    Uses a LifoQueue storing (conn, last_used_ts) with strict cap of max_conn (4).
    Dead/stale connections decrement _created atomically and are replaced cleanly
    so transient network drops never leak pool slots.
    """

    def __init__(self, dsn_func, max_conn=POOL_MAX_CONN, timeout=12):
        self.dsn_func = dsn_func
        self.max_conn = max_conn
        self.timeout = timeout
        try:
            from gevent.queue import LifoQueue, Empty
            from gevent.lock import Semaphore
            self._queue = LifoQueue()
            self._empty_exc = Empty
            self._lock = Semaphore()
        except ImportError:
            import queue
            import threading
            self._queue = queue.LifoQueue()
            self._empty_exc = queue.Empty
            self._lock = threading.Lock()
        self._created = 0

    def _connect_fresh(self):
        dsn = self.dsn_func()
        try:
            return psycopg2.connect(dsn, connect_timeout=15)
        except Exception as exc:
            if 'channel_binding' in str(exc).lower():
                fallback_dsn = _strip_channel_binding(dsn)
                return psycopg2.connect(fallback_dsn, connect_timeout=15)
            raise

    def _safe_close(self, conn):
        try:
            conn.close()
        except Exception:
            pass

    def _is_usable(self, conn, last_ts: float) -> bool:
        if conn is None or getattr(conn, 'closed', 1):
            return False
        if (time.time() - last_ts) <= IDLE_PING_SECONDS:
            return True
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            conn.rollback()
            return True
        except Exception:
            return False

    def getconn(self, timeout=None):
        to = timeout if timeout is not None else self.timeout

        # 1. Drain any idle connections from the pool, discarding dead ones cleanly
        while True:
            try:
                item = self._queue.get_nowait()
            except self._empty_exc:
                break
            conn, last_ts = item
            if self._is_usable(conn, last_ts):
                return conn
            self._safe_close(conn)
            with self._lock:
                self._created = max(0, self._created - 1)

        # 2. If under pool cap, reserve a slot and open a fresh connection
        reserved = False
        with self._lock:
            if self._created < self.max_conn:
                self._created += 1
                reserved = True
        if reserved:
            try:
                return self._connect_fresh()
            except Exception:
                with self._lock:
                    self._created = max(0, self._created - 1)
                raise

        # 3. Pool is at capacity; wait cooperatively for an in-flight connection
        try:
            item = self._queue.get(timeout=to)
        except self._empty_exc:
            raise psycopg2.pool.PoolError(f"Database connection pool exhausted (waited {to}s)")

        conn, last_ts = item
        if self._is_usable(conn, last_ts):
            return conn

        # Returned connection is dead; close & replace within the same reserved slot
        self._safe_close(conn)
        try:
            return self._connect_fresh()
        except Exception:
            with self._lock:
                self._created = max(0, self._created - 1)
            raise

    def putconn(self, conn, close=False):
        if conn is None:
            return
        if close or getattr(conn, 'closed', 1):
            self._safe_close(conn)
            with self._lock:
                self._created = max(0, self._created - 1)
            return

        try:
            if conn.status == psycopg2.extensions.STATUS_IN_TRANSACTION:
                conn.rollback()
            self._queue.put_nowait((conn, time.time()))
        except Exception:
            self._safe_close(conn)
            with self._lock:
                self._created = max(0, self._created - 1)

    def closeall(self):
        while True:
            try:
                conn, _ = self._queue.get_nowait()
                self._safe_close(conn)
            except self._empty_exc:
                break
        with self._lock:
            self._created = 0


def _get_pool():
    global _pool
    if _pool is None:
        if not is_configured():
            raise RuntimeError("[DB] DATABASE_URL env var is not set.")
        _pool = DBConnectionPool(_make_dsn, max_conn=POOL_MAX_CONN, timeout=12)
    return _pool


def _get_conn():
    return _get_pool().getconn()


def _put_conn(conn, close=False):
    global _pool
    if _pool is not None and conn is not None:
        _pool.putconn(conn, close=close)


def health_check() -> bool:
    """Return True if Neon PostgreSQL is reachable and responds to SELECT 1."""
    if not is_configured():
        return False
    conn = None
    conn_broken = False
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.rollback()
        return True
    except Exception:
        conn_broken = True
        return False
    finally:
        _put_conn(conn, close=conn_broken)


# ================= INIT =================
def init_db():
    """
    Create vibration_data, app_thresholds, and vibration_events tables if absent,
    and migrate older vibration_data / vibration_events schemas automatically.
    """
    float_defs = ',\n                    '.join(f'{c} DOUBLE PRECISION' for c in FLOAT_COLUMNS)
    conn = _get_conn()
    conn_broken = False
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
            for col in FLOAT_COLUMNS:
                cur.execute(
                    f"ALTER TABLE vibration_data ADD COLUMN IF NOT EXISTS {col} DOUBLE PRECISION"
                )
            cur.execute("""
                CREATE TABLE IF NOT EXISTS app_thresholds (
                    id         INTEGER PRIMARY KEY CHECK (id = 1),
                    config     TEXT NOT NULL,
                    updated_at DOUBLE PRECISION NOT NULL
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS vibration_events (
                    id              SERIAL PRIMARY KEY,
                    fault_type      TEXT NOT NULL,
                    filename        TEXT UNIQUE NOT NULL,
                    created_at      DOUBLE PRECISION NOT NULL,
                    event_time      TEXT,
                    source          TEXT DEFAULT 'cloud',
                    axis            TEXT,
                    metric          TEXT,
                    threshold_level TEXT,
                    threshold_value DOUBLE PRECISION,
                    measured_value  DOUBLE PRECISION,
                    csv_content     TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_events_created ON vibration_events(created_at DESC);
            """)
            event_extra_cols = (
                ("event_time", "TEXT"),
                ("source", "TEXT DEFAULT 'cloud'"),
                ("axis", "TEXT"),
                ("metric", "TEXT"),
                ("threshold_level", "TEXT"),
                ("threshold_value", "DOUBLE PRECISION"),
                ("measured_value", "DOUBLE PRECISION"),
            )
            for ecol, etype in event_extra_cols:
                cur.execute(
                    f"ALTER TABLE vibration_events ADD COLUMN IF NOT EXISTS {ecol} {etype}"
                )
            conn.commit()
        print(f"[DB] Neon tables ready ({len(DB_COLUMNS)} data columns + thresholds + events).")
    except Exception as e:
        conn_broken = True
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[DB ERROR] init_db: {e}")
        raise
    finally:
        _put_conn(conn, close=conn_broken)


# ================= WRITE =================
def _complete_row(row_dict: dict) -> dict:
    """Return a dict with every DB column present (missing -> None/NULL)."""
    return {c: row_dict.get(c) for c in DB_COLUMNS}


def insert_row(row_dict: dict):
    """Insert one parsed row into Neon (skips duplicate time_sec cleanly)."""
    conn = _get_conn()
    conn_broken = False
    try:
        with conn.cursor() as cur:
            cur.execute(_INSERT_SQL, _complete_row(row_dict))
            conn.commit()
    except Exception as e:
        conn_broken = True
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[DB ERROR] insert_row: {e}")
        raise
    finally:
        _put_conn(conn, close=conn_broken)


def insert_many_rows(rows: list):
    """Bulk insert a list of row_dicts."""
    if not rows:
        return
    conn = _get_conn()
    conn_broken = False
    try:
        with conn.cursor() as cur:
            for row_dict in rows:
                cur.execute(_INSERT_SQL, _complete_row(row_dict))
        conn.commit()
        print(f"[DB] Bulk inserted {len(rows)} rows.")
    except Exception as e:
        conn_broken = True
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[DB ERROR] insert_many_rows: {e}")
        raise
    finally:
        _put_conn(conn, close=conn_broken)


# ================= READ =================
def get_latest_timestamp() -> float:
    """Return the max time_sec stored in Neon, or 0.0 if empty."""
    conn = _get_conn()
    conn_broken = False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(time_sec) FROM vibration_data")
            result = cur.fetchone()
            return float(result[0]) if result and result[0] is not None else 0.0
    except Exception as e:
        conn_broken = True
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[DB ERROR] get_latest_timestamp: {e}")
        return 0.0
    finally:
        _put_conn(conn, close=conn_broken)


def get_latest_row_dict() -> dict:
    """Fetch the newest row by time_sec from vibration_data as a dictionary."""
    conn = _get_conn()
    conn_broken = False
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
        conn_broken = True
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[DB ERROR] get_latest_row_dict: {e}")
        return {}
    finally:
        _put_conn(conn, close=conn_broken)


def fetch_rows_since(epoch_time: float) -> list:
    """Fetch all rows with time_sec > epoch_time ordered ASC."""
    conn = _get_conn()
    conn_broken = False
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
        conn_broken = True
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[DB ERROR] fetch_rows_since: {e}")
        return []
    finally:
        _put_conn(conn, close=conn_broken)


def fetch_rows_for_chart(axis='Z', start_time=None, end_time=None, last_n_minutes=None) -> list:
    """Fetch 33-column tuples for chart rendering."""
    conn = _get_conn()
    conn_broken = False
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
                    WHERE time_sec >= (
                        SELECT COALESCE(MAX(time_sec), EXTRACT(EPOCH FROM NOW())) - %s
                        FROM vibration_data
                    )
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
        conn_broken = True
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[DB ERROR] fetch_rows_for_chart: {e}")
        return []
    finally:
        _put_conn(conn, close=conn_broken)


def get_available_dates() -> list:
    """Return sorted list of distinct date strings (newest first)."""
    conn = _get_conn()
    conn_broken = False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT date FROM vibration_data
                WHERE date IS NOT NULL
                ORDER BY date DESC
            """)
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        conn_broken = True
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[DB ERROR] get_available_dates: {e}")
        return []
    finally:
        _put_conn(conn, close=conn_broken)


# ================= DELETE (retention / overflow management) =================
def delete_rows_before_epoch(epoch_time: float) -> int:
    """Delete rows with time_sec <= epoch_time once local hub archives them."""
    conn = _get_conn()
    conn_broken = False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM vibration_data WHERE time_sec <= %s",
                (epoch_time,)
            )
            deleted = cur.rowcount
            conn.commit()
        if deleted:
            print(f"[DB] Retention purge: {deleted} rows removed from Neon (epoch <= {epoch_time:.0f}).")
        return deleted
    except Exception as e:
        conn_broken = True
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[DB ERROR] delete_rows_before_epoch: {e}")
        return 0
    finally:
        _put_conn(conn, close=conn_broken)


def get_row_count() -> int:
    """Fast COUNT(*) for overflow guard."""
    conn = _get_conn()
    conn_broken = False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM vibration_data")
            result = cur.fetchone()
            return int(result[0]) if result else 0
    except Exception as e:
        conn_broken = True
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[DB ERROR] get_row_count: {e}")
        return 0
    finally:
        _put_conn(conn, close=conn_broken)


def delete_oldest_rows(n: int) -> int:
    """Delete n oldest rows by time_sec when buffer exceeds MAX_DB_ROWS."""
    conn = _get_conn()
    conn_broken = False
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
        conn_broken = True
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[DB ERROR] delete_oldest_rows: {e}")
        return 0
    finally:
        _put_conn(conn, close=conn_broken)


# ================= SHARED THRESHOLDS (WEB <-> LOCAL SYNC) =================
def save_thresholds(thresholds_dict: dict, updated_at: float | None = None) -> float:
    """Upsert global thresholds JSON into Neon app_thresholds (id=1). Returns updated_at."""
    ts = float(updated_at) if updated_at is not None else time.time()
    payload = json.dumps(thresholds_dict)
    conn = _get_conn()
    conn_broken = False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO app_thresholds (id, config, updated_at)
                VALUES (1, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET config = EXCLUDED.config,
                    updated_at = EXCLUDED.updated_at
            """, (payload, ts))
            conn.commit()
        return ts
    except Exception as e:
        conn_broken = True
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[DB ERROR] save_thresholds: {e}")
        return 0.0
    finally:
        _put_conn(conn, close=conn_broken)


def load_thresholds():
    """Return (thresholds_dict, updated_at) from Neon, or (None, 0.0) if not set."""
    conn = _get_conn()
    conn_broken = False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT config, updated_at FROM app_thresholds WHERE id = 1")
            row = cur.fetchone()
            if not row:
                return None, 0.0
            return json.loads(row[0]), float(row[1])
    except Exception as e:
        conn_broken = True
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[DB ERROR] load_thresholds: {e}")
        return None, 0.0
    finally:
        _put_conn(conn, close=conn_broken)


# ================= SHARED EVENTS (CLOUD <-> LOCAL SYNC) =================
def save_event(
    fault_type: str,
    filename: str,
    csv_content: str,
    source: str = 'cloud',
    max_events: int = 200,
    *,
    axis: str | None = None,
    metric: str | None = None,
    threshold_level: str | None = None,
    threshold_value: float | None = None,
    measured_value: float | None = None,
    event_time: str | None = None,
) -> bool:
    """Store an event snapshot + metadata in Neon and cap stored rows to max_events."""
    created_at = time.time()
    iso_time = event_time or datetime.fromtimestamp(created_at, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    conn = _get_conn()
    conn_broken = False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO vibration_events (
                    fault_type, filename, created_at, event_time, source,
                    axis, metric, threshold_level, threshold_value, measured_value, csv_content
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (filename) DO NOTHING
            """, (
                fault_type, filename, created_at, iso_time, source,
                axis, metric, threshold_level, threshold_value, measured_value, csv_content,
            ))
            if max_events > 0:
                cur.execute("""
                    DELETE FROM vibration_events
                    WHERE id NOT IN (
                        SELECT id FROM vibration_events
                        ORDER BY created_at DESC, id DESC
                        LIMIT %s
                    )
                """, (int(max_events),))
            conn.commit()
        print(f"[DB] Event logged to Neon: {fault_type}/{filename} (source={source})")
        return True
    except Exception as e:
        conn_broken = True
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[DB ERROR] save_event: {e}")
        return False
    finally:
        _put_conn(conn, close=conn_broken)


def list_events(limit: int = 200) -> list:
    """Return event metadata dicts (without csv_content) ordered newest-first."""
    conn = _get_conn()
    conn_broken = False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT fault_type, filename, created_at, event_time, source,
                       axis, metric, threshold_level, threshold_value, measured_value
                FROM vibration_events
                ORDER BY created_at DESC, id DESC
                LIMIT %s
            """, (int(limit),))
            rows = cur.fetchall()
        return [
            {
                'fault_type': r[0],
                'filename': r[1],
                'created_at': float(r[2]) if r[2] is not None else 0.0,
                'event_time': r[3],
                'source': r[4] or 'cloud',
                'axis': r[5],
                'metric': r[6],
                'threshold_level': r[7],
                'threshold_value': float(r[8]) if r[8] is not None else None,
                'measured_value': float(r[9]) if r[9] is not None else None,
                'path': f"{r[0]}/{r[1]}",
            }
            for r in rows
        ]
    except Exception as e:
        conn_broken = True
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[DB ERROR] list_events: {e}")
        return []
    finally:
        _put_conn(conn, close=conn_broken)


def get_event_csv(filename: str) -> str | None:
    """Fetch stored CSV content for an event filename from Neon."""
    conn = _get_conn()
    conn_broken = False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT csv_content FROM vibration_events WHERE filename = %s LIMIT 1",
                (os.path.basename(filename),)
            )
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as e:
        conn_broken = True
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[DB ERROR] get_event_csv: {e}")
        return None
    finally:
        _put_conn(conn, close=conn_broken)


def fetch_events_since(epoch_time: float) -> list:
    """Fetch events created after epoch_time (including csv_content) ordered ASC."""
    conn = _get_conn()
    conn_broken = False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT fault_type, filename, created_at, event_time, source,
                       axis, metric, threshold_level, threshold_value, measured_value,
                       csv_content
                FROM vibration_events
                WHERE created_at > %s
                ORDER BY created_at ASC, id ASC
            """, (float(epoch_time),))
            rows = cur.fetchall()
        return [
            {
                'fault_type': r[0],
                'filename': r[1],
                'created_at': float(r[2]) if r[2] is not None else 0.0,
                'event_time': r[3],
                'source': r[4] or 'cloud',
                'axis': r[5],
                'metric': r[6],
                'threshold_level': r[7],
                'threshold_value': float(r[8]) if r[8] is not None else None,
                'measured_value': float(r[9]) if r[9] is not None else None,
                'csv_content': r[10],
            }
            for r in rows
        ]
    except Exception as e:
        conn_broken = True
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[DB ERROR] fetch_events_since: {e}")
        return []
    finally:
        _put_conn(conn, close=conn_broken)


# ================= HELPERS =================
def parsed_to_row_dict(parsed: dict, date_str: str, epoch_time: float) -> dict:
    """Convert parse_teensy_file() output -> row_dict for insert_row()."""
    peaks = parsed.get('fft_peaks', [])

    def fv(i, j):
        return peaks[i][j] if i < len(peaks) else None

    row = {'date': date_str, 'time_sec': epoch_time}

    for col in FLOAT_COLUMNS:
        if not col.startswith('fft'):
            row[col] = parsed.get(col)

    for n in range(5):
        row[f'fft{n+1}_freq'] = fv(n, 0)
        row[f'fft{n+1}_mag'] = fv(n, 1)

    return row
