# SERVER_ENGINE_APP_FUNCTIONS.py
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Coroutine, Union, Mapping, Callable

import functools
import inspect
import logging
import time
from pathlib import Path
from datetime import datetime
import threading
import asyncio
import os
import statistics

# Optional; kept for type hints / legacy awareness
import pyodbc  # type: ignore

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from contextlib import contextmanager

import traceback

# -----------------------------------------------------------------------------
# Async utils: loop-safe scheduler usable from threads or loop
# -----------------------------------------------------------------------------
try:
    _MAIN_LOOP  # type: ignore[name-defined]
except NameError:
    _MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None  # set on startup


def ASYNC_SET_MAIN_LOOP(loop: asyncio.AbstractEventLoop) -> None:
    """Capture the process' main event loop exactly once at app startup."""
    global _MAIN_LOOP
    _MAIN_LOOP = loop

def schedule_coro(coro: Coroutine[Any, Any, Any]) -> Union[asyncio.Task, Any]:
    """Schedule a coroutine from loop or thread with performance optimization."""
    start_time = time.time()
    
    try:
        # Fast path: we're in the main event loop
        loop = asyncio.get_running_loop()
        result = loop.create_task(coro)
        
        # ✅ PERFORMANCE MONITORING: Log fast path usage
        if hasattr(schedule_coro, '_fast_path_count'):
            schedule_coro._fast_path_count += 1
        else:
            schedule_coro._fast_path_count = 1
            
        return result
        
    except RuntimeError:
        # Slow path: we're in a different thread
        if _MAIN_LOOP is None:
            raise RuntimeError(
                "MAIN event loop not set. Call ASYNC_SET_MAIN_LOOP(asyncio.get_running_loop()) at startup."
            )
        
        # ✅ PERFORMANCE OPTIMIZATION: Use ThreadPoolExecutor for better performance
        # This avoids the overhead of asyncio.run_coroutine_threadsafe
        import concurrent.futures
        if not hasattr(schedule_coro, '_executor'):
            schedule_coro._executor = concurrent.futures.ThreadPoolExecutor(max_workers=20, thread_name_prefix="schedule_coro")
        
        # Submit the coroutine execution to the thread pool
        try:
            future = schedule_coro._executor.submit(asyncio.run, coro)
            
            # ✅ PERFORMANCE MONITORING: Log slow path usage
            if hasattr(schedule_coro, '_slow_path_count'):
                schedule_coro._slow_path_count += 1
            else:
                schedule_coro._slow_path_count = 1
                
            # Log if slow path is taking too long
            slow_path_time = time.time() - start_time
            if slow_path_time > 0.1:  # 100ms threshold
                LOGGER.warning(f"schedule_coro slow path took {slow_path_time*1000:.1f}ms")
                
            return future
            
        except Exception as e:
            # Fallback to the original method if thread pool fails
            LOGGER.warning(f"ThreadPoolExecutor failed, falling back to run_coroutine_threadsafe: {e}")
            return asyncio.run_coroutine_threadsafe(coro, _MAIN_LOOP)

LOGGER = logging.getLogger("app")

# -----------------------------------------------------------------------------
# Engine creation / pooling (lazy + startup/shutdown helpers)
# -----------------------------------------------------------------------------
DB_URL = os.getenv(
    "VIOLIN_DB_URL",
    "mssql+pyodbc://violin:Test123!@104.40.11.248:3341/VIOLIN?driver=ODBC+Driver+17+for+SQL+Server",
)

_DB_ENGINE: Optional[Engine] = None  # set via get_engine()

def _create_engine() -> Engine:
    return create_engine(
        DB_URL,
        future=True,
        pool_pre_ping=True,
        pool_recycle=1800,
        fast_executemany=True,
        # PERFORMANCE OPTIMIZATION: Balanced settings for stability
        pool_size=10,              # Reduced from 20 for better stability
        max_overflow=20,           # Reduced from 40 for better resource management
        pool_timeout=30,           # Reduced from 60 for faster failure detection
        pool_reset_on_return='rollback',  # Changed from 'commit' for better transaction handling
        # Additional performance optimizations
        echo=False,                 # Disable SQL logging in production
        echo_pool=False,           # Disable pool logging
        # Connection optimization
        connect_args={
            "autocommit": False,   # Explicit transaction control
            "isolation_level": "READ_COMMITTED",  # Balance between performance and consistency
        }
    )

def get_engine() -> Engine:
    global _DB_ENGINE
    if _DB_ENGINE is None:
        _DB_ENGINE = _create_engine()
    return _DB_ENGINE

def DB_ENGINE_STARTUP(warm_pool: bool = True) -> None:
    """
    Call once at FastAPI startup. Warms the pool and loads the allowlist
    for generic inserts from P_ENGINE_TABLE_INSERTS_METADATA.
    """
    eng = get_engine()
    if warm_pool:
        try:
            # begin() ensures a commit on success
            with eng.begin() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as e:
            LOGGER.exception("DB_ENGINE_STARTUP warm_pool failed: %s", e)
    
    # PERFORMANCE MONITORING: Log connection pool status
    try:
        pool = eng.pool
        pool_info = f"DB_ENGINE_STARTUP: Connection pool configured - size={pool.size()}, overflow={pool.overflow()}, timeout={pool.timeout()}s"
        LOGGER.info(pool_info)
        
        # Also log to console for visibility (ASCII-safe)
        try:
            from SERVER_ENGINE_APP_FUNCTIONS import CONSOLE_LOG as _CL  # self-import safe at runtime
        except Exception:
            _CL = None
        if _CL:
            _CL("STARTUP", pool_info)
            # Additional pool diagnostics
            _CL("STARTUP", f"Pool details: checked_in={pool.checkedin()}, checked_out={pool.checkedout()}")
        else:
            print(pool_info)
            print(f"Pool details: checked_in={pool.checkedin()}, checked_out={pool.checkedout()}")
    except Exception as e:
        error_msg = f"DB_ENGINE_STARTUP: Could not log pool status: {e}"
        LOGGER.warning(error_msg)
        try:
            from SERVER_ENGINE_APP_FUNCTIONS import CONSOLE_LOG as _CL  # self-import safe at runtime
        except Exception:
            _CL = None
        if _CL:
            _CL("STARTUP", error_msg)
        else:
            print(error_msg)
    
    try:
        DB_INIT_ALLOWED_TABLES()
        try:
            from SERVER_ENGINE_APP_FUNCTIONS import CONSOLE_LOG as _CL  # self-import safe at runtime
        except Exception:
            _CL = None
        msg_ok = "DB_ENGINE_STARTUP: Table allowlist initialized successfully"
        if _CL:
            _CL("STARTUP", msg_ok)
        else:
            print(msg_ok)
    except Exception as e:
        error_msg = f"Failed to initialize table allowlist: {e}"
        LOGGER.exception(error_msg)
        try:
            from SERVER_ENGINE_APP_FUNCTIONS import CONSOLE_LOG as _CL  # self-import safe at runtime
        except Exception:
            _CL = None
        if _CL:
            _CL("STARTUP", f"ERROR: {error_msg}")
        else:
            print(f"ERROR: {error_msg}")

def DB_ENGINE_SHUTDOWN() -> None:
    """Optional: dispose the engine at application shutdown."""
    global _DB_ENGINE
    if _DB_ENGINE is not None:
        try:
            _DB_ENGINE.dispose()
        except Exception:
            pass
        _DB_ENGINE = None

def DB_GET_POOL_STATUS() -> Dict[str, Any]:
    """Get current connection pool status for monitoring."""
    try:
        eng = get_engine()
        pool = eng.pool
        return {
            "pool_size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
            "total_connections": pool.checkedin() + pool.checkedout()
        }
    except Exception as e:
        LOGGER.warning("DB_GET_POOL_STATUS failed: %s", e)
        return {"error": str(e)}

def DB_GET_PERFORMANCE_STATS() -> Dict[str, Any]:
    """Get database performance statistics for monitoring."""
    try:
        eng = get_engine()
        pool = eng.pool
        
        # Test connection performance
        t0 = time.perf_counter()
        with eng.begin() as conn:
            conn.execute(text("SELECT 1"))
        connection_test_ms = (time.perf_counter() - t0) * 1000
        
        return {
            "pool_status": DB_GET_POOL_STATUS(),
            "connection_test_ms": round(connection_test_ms, 2),
            "pool_health": "GOOD" if connection_test_ms < 10 else "SLOW" if connection_test_ms < 100 else "VERY_SLOW"
        }
    except Exception as e:
        return {"error": str(e)}

# -----------------------------------------------------------------------------
# Generic INSERT allowlist (from P_ENGINE_TABLE_INSERTS_METADATA)
# -----------------------------------------------------------------------------
_ALLOWED_TABLE_COLUMNS: Dict[str, set] = {}

def DB_INIT_ALLOWED_TABLES() -> None:
    """
    Load whitelist of (TABLE_NAME, COLUMN_NAME) from P_ENGINE_TABLE_INSERTS_METADATA.
    Must be called before using DB_INSERT_TABLE / DB_INSERT_TABLE_BULK.
    """
    try:
        with get_engine().begin() as CONN:
            rows = CONN.execute(text("EXEC P_ENGINE_TABLE_INSERTS_METADATA")).fetchall()
        _ALLOWED_TABLE_COLUMNS.clear()
        for r in rows:
            t = str(getattr(r, "TABLE_NAME")).upper().strip()
            c = str(getattr(r, "COLUMN_NAME")).upper().strip()
            _ALLOWED_TABLE_COLUMNS.setdefault(t, set()).add(c)
        LOGGER.info("DB_INIT_ALLOWED_TABLES loaded %d tables", len(_ALLOWED_TABLE_COLUMNS))
    except Exception as e:
        LOGGER.exception("DB_INIT_ALLOWED_TABLES failed: %s", e)

def _filter_insert_payload(table: str, row: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Keep only columns allowed for this table; inject DT_ADDED=datetime.now()
    if the allowlist contains it (case-insensitive), regardless of input.
    """
    t = str(table).upper().strip()
    allowed = _ALLOWED_TABLE_COLUMNS.get(t)
    if not allowed:
        raise ValueError(f"INSERT not allowed for table '{table}' (not in allowlist).")

    out: Dict[str, Any] = {}
    provided_ci = {str(k).upper(): k for k in row.keys()}

    for col_ci in allowed:
        if col_ci == "DT_ADDED":  # case-insensitive via col_ci from DB
            out["DT_ADDED"] = datetime.now()
            continue
        k_orig = provided_ci.get(col_ci)
        if k_orig is not None:
            out[k_orig] = row[k_orig]

    if not out:
        raise ValueError(f"No allowed columns provided for table '{table}'.")
    return out

def _make_insert_sql(table: str, cols: Iterable[str]) -> str:
    t = str(table).upper().strip()
    if t not in _ALLOWED_TABLE_COLUMNS:
        raise ValueError(f"INSERT not allowed for table '{table}' (not in allowlist).")
    col_list = list(cols)
    placeholders = [f":{c}" for c in col_list]
    cols_sql = ", ".join(col_list)
    vals_sql = ", ".join(placeholders)
    return f"INSERT INTO {t} ({cols_sql}) VALUES ({vals_sql})"

def _make_bulk_insert_sql(table: str, cols: Iterable[str]) -> str:
    """Generate SQL for bulk inserts using ? placeholders for executemany."""
    t = str(table).upper().strip()
    if t not in _ALLOWED_TABLE_COLUMNS:
        raise ValueError(f"INSERT not allowed for table '{table}' (not in allowlist).")
    col_list = list(cols)
    placeholders = ["?" for _ in col_list]  # Use ? for executemany
    cols_sql = ", ".join(col_list)
    vals_sql = ", ".join(placeholders)
    return f"INSERT INTO {t} ({cols_sql}) VALUES ({vals_sql})"

# -----------------------------------------------------------------------------
# fire_and_forget helper
# -----------------------------------------------------------------------------
def _fire_and_forget(fn, *args, **kwargs):
    """Run a synchronous DB function off the event loop ASAP."""
    try:
        loop = asyncio.get_running_loop()
        # ✅ PERFORMANCE OPTIMIZATION: Use asyncio.to_thread instead of creating new threads
        loop.create_task(asyncio.to_thread(fn, *args, **kwargs))
    except RuntimeError:
        # Fallback to thread pool executor for better performance
        import concurrent.futures
        if not hasattr(_fire_and_forget, '_executor'):
            _fire_and_forget._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=10,  # Limit concurrent threads
                thread_name_prefix="db_insert"
            )
        _fire_and_forget._executor.submit(fn, *args, **kwargs)

# -----------------------------------------------------------------------------
# Insert tracing controls (env toggles)
# -----------------------------------------------------------------------------
_TRACE_INSERTS = os.getenv("VIOLIN_DB_TRACE_INSERTS", "0") == "1"
_SLOW_MS = int(os.getenv("VIOLIN_DB_SLOW_MS", "300"))

def _now_ms() -> float:
    return time.perf_counter() * 1000.0

def _log_insert_timing(table: str, ms: float, fire_and_forget: bool, extra: str = "") -> None:
    if _TRACE_INSERTS:
        LOGGER.info("DB_INSERT_TABLE[%s]%s took %.1f ms (fof=%s)", table, f" {extra}" if extra else "", ms, fire_and_forget)
    if ms >= _SLOW_MS:
        LOGGER.warning("SLOW INSERT: %s (%.1f ms, fof=%s)%s", table, ms, fire_and_forget, f" {extra}" if extra else "")

# -----------------------------------------------------------------------------
# Generic insert APIs
# -----------------------------------------------------------------------------
def DB_INSERT_TABLE(table: str, row: Mapping[str, Any], *, fire_and_forget: bool = True) -> None:
    """Insert a single row into an allowlisted table."""
    try:
        filtered = _filter_insert_payload(table, row)
        sql = _make_insert_sql(table, filtered.keys())
    except Exception as e:
        LOGGER.exception("DB_INSERT_TABLE precheck failed for %s: %s", table, e)
        return

    def _do():
        t0 = _now_ms()
        try:
            with get_engine().begin() as CONN:
                CONN.execute(text(sql), filtered)
        except Exception as e:
            LOGGER.exception("DB_INSERT_TABLE insert failed for %s: %s", table, e)
        finally:
            _log_insert_timing(table, _now_ms() - t0, fire_and_forget=False)

    if fire_and_forget:
        sched_t0 = _now_ms()
        _fire_and_forget(_do)
        sched_ms = _now_ms() - sched_t0
        if _TRACE_INSERTS:
            LOGGER.info("DB_INSERT_TABLE[%s] scheduled in %.1f ms (fof=True)", table, sched_ms)
    else:
        _do()

def DB_INSERT_TABLE_BULK(table: str, rows: List[Mapping[str, Any]], *, fire_and_forget: bool = True) -> None:
    """
    Insert multiple rows into an allowlisted table efficiently.
    Injects DT_ADDED=datetime.now() for each row if present in allowlist.
    Uses a stable column set across all rows; missing keys → NULL (except DT_ADDED).
    """
    if not rows:
        return
    try:
        t = str(table).upper().strip()
        allowed = _ALLOWED_TABLE_COLUMNS.get(t)
        if not allowed:
            raise ValueError(f"INSERT not allowed for table '{table}' (not in allowlist).")

        union_cols_ci: set = set()
        for r in rows:
            for k in r.keys():
                k_ci = str(k).upper()
                if k_ci in allowed and k_ci != "DT_ADDED":
                    union_cols_ci.add(k_ci)
        if "DT_ADDED" in allowed:
            union_cols_ci.add("DT_ADDED")

        if not union_cols_ci:
            raise ValueError(f"No allowed columns provided for table '{table}' (bulk).")

        name_map: Dict[str, str] = {}
        for r in rows:
            for k in r.keys():
                k_ci = str(k).upper()
                if k_ci in union_cols_ci and k_ci != "DT_ADDED" and k_ci not in name_map:
                    name_map[k_ci] = k
        if "DT_ADDED" in union_cols_ci:
            name_map["DT_ADDED"] = "DT_ADDED"

        col_names = [name_map[ci] for ci in union_cols_ci]
        sql = _make_bulk_insert_sql(table, col_names)  # Use ? placeholders for executemany

        now_val = datetime.now
        payload: List[Dict[str, Any]] = []
        for r in rows:
            one: Dict[str, Any] = {}
            for ci in union_cols_ci:
                if ci == "DT_ADDED":
                    one["DT_ADDED"] = now_val()
                else:
                    k_orig = name_map[ci]
                    one[k_orig] = r.get(k_orig, r.get(k_orig.upper(), None))
            payload.append(one)
    except Exception as e:
        LOGGER.exception("DB_INSERT_TABLE_BULK precheck failed for %s: %s", table, e)
        return

    def _do():
        t0 = _now_ms()
        try:
            # PERFORMANCE OPTIMIZATION: Use raw connection for bulk inserts to avoid ORM overhead
            engine = get_engine()
            raw_conn = engine.raw_connection()
            try:
                # Set autocommit for bulk operations
                raw_conn.autocommit = True
                
                # Use executemany for better performance than execute with list
                cursor = raw_conn.cursor()
                try:
                    # Convert payload to list of tuples for executemany
                    values = [[row.get(col) for col in col_names] for row in payload]
                    cursor.executemany(sql, values)
                    raw_conn.commit()
                finally:
                    cursor.close()
            finally:
                raw_conn.close()
                    
        except Exception as e:
            LOGGER.exception("DB_INSERT_TABLE_BULK insert failed for %s: %s", table, e)
        finally:
            _log_insert_timing(table + " (BULK)", _now_ms() - t0, fire_and_forget=False, extra=f"rows={len(payload)}")

    if fire_and_forget:
        sched_t0 = _now_ms()
        _fire_and_forget(_do)
        sched_ms = _now_ms() - sched_t0
        if _TRACE_INSERTS:
            LOGGER.info("DB_INSERT_TABLE_BULK[%s] scheduled in %.1f ms (fof=True, rows=%d)", table, sched_ms, len(rows))
    else:
        _do()

# -----------------------------------------------------------------------------
# Logging (ASCII-safe)
# -----------------------------------------------------------------------------
def CONSOLE_LOG(prefix: str, msg: str, obj: Any = None) -> None:
    """Print a single-line log safely even if there are odd characters."""
    try:
        if obj is None:
            print(f"{prefix} - {msg}", flush=True)
        else:
            print(f"{prefix} - {msg} {obj}", flush=True)
    except Exception:
        try:
            s = f"{prefix} - {msg} {obj}".encode("utf-8", "replace").decode("ascii", "ignore")
            print(s, flush=True)
        except Exception:
            print(f"{prefix} - {msg}", flush=True)

# -----------------------------------------------------------------------------
# pyodbc helpers — reusing the pool via raw_connection()
# -----------------------------------------------------------------------------
def DB_CONNECT():
    """Return a DBAPI (pyodbc) connection from the pooled Engine."""
    conn = get_engine().raw_connection()
    try:
        conn.autocommit = True  # type: ignore[attr-defined]
    except Exception:
        pass
    return conn

@contextmanager
def DB_CONNECT_CTX():
    """Context-manager wrapper around DB_CONNECT()."""
    conn = DB_CONNECT()
    try:
        yield conn
        try:
            conn.commit()
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass

def DB_BULK_INSERT(conn, sql: str, rows: Iterable[tuple]) -> None:
    batch = list(rows)
    if not batch:
        return
    cur = conn.cursor()
    cur.fast_executemany = True
    cur.executemany(sql, batch)
    try:
        conn.commit()
    except Exception:
        pass

# -----------------------------------------------------------------------------
# Stored-proc helpers
# -----------------------------------------------------------------------------
def _exec_sp_and_position_cursor(cur, sp_name: str, **params) -> bool:
    keys = list(params.keys())
    args = [params[k] for k in keys]
    placeholders = ",".join([f"@{k}=?" for k in keys])
    sql = f"EXEC {sp_name} {placeholders}" if placeholders else f"EXEC {sp_name}"
    cur.execute(sql, args)
    while True:
        if cur.description is not None:
            return True
        if not cur.nextset():
            return False

def DB_EXEC_SP_MULTIPLE_ROWS(conn, sp_name: str, **params) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    if not _exec_sp_and_position_cursor(cur, sp_name, **params):
        return []
    cols = [c[0] for c in cur.description]  # type: ignore[union-attr]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def DB_EXEC_SP_SINGLE_ROW(conn, sp_name: str, **params) -> Dict[str, Any]:
    cur = conn.cursor()
    if not _exec_sp_and_position_cursor(cur, sp_name, **params):
        return {}
    cols = [c[0] for c in cur.description]  # type: ignore[union-attr]
    row = cur.fetchone()
    return dict(zip(cols, row)) if row else {}

def DB_EXEC_SP_NO_RESULT(conn, sp_name: str, **params) -> Optional[int]:
    cur = conn.cursor()
    keys = list(params.keys())
    args = [params[k] for k in keys]
    placeholders = ",".join([f"@{k}=?" for k in keys])
    sql = f"EXEC {sp_name} {placeholders}" if placeholders else f"EXEC {sp_name}"
    cur.execute(sql, args)
    total = 0
    try:
        while True:
            if cur.rowcount is not None and cur.rowcount >= 0:
                total += cur.rowcount
            if not cur.nextset():
                break
    except Exception:
        pass
    try:
        conn.commit()
    except Exception:
        pass
    return total if total > 0 else None

# -----------------------------------------------------------------------------
# ENGINE_DB_LOG_FUNCTIONS (decorator-based logger)
# -----------------------------------------------------------------------------
_LOG_INSERT_SQL_TEXT = text("""
INSERT INTO ENGINE_DB_LOG_FUNCTIONS
(DT_FUNCTION_MESSAGE_QUEUED, DT_ADDED, PYTHON_FUNCTION_NAME, PYTHON_FILE_NAME,
 RECORDING_ID, AUDIO_CHUNK_NO, FRAME_NO, START_STOP_OR_ERROR_MSG)
VALUES (:DT_FUNCTION_MESSAGE_QUEUED, :DT_ADDED, :PYTHON_FUNCTION_NAME, :PYTHON_FILE_NAME,
        :RECORDING_ID, :AUDIO_CHUNK_NO, :FRAME_NO, :START_STOP_OR_ERROR_MSG)
""")

def _truncate(s: Optional[str], max_len: int) -> Optional[str]:
    if s is None:
        return None
    return s if len(s) <= max_len else s[: max_len - 1] + "…"

def _db_log_event(dt_function_message_queued: datetime,
                  function_name: str,
                  file_name: str,
                  message: str,
                  recording_id=None,
                  audio_chunk_no=None,
                  frame_no=None) -> None:
    """
    Single-row insert into ENGINE_DB_LOG_FUNCTIONS via SQLAlchemy.
    Uses local machine time. Never raises to caller.
    """
    dt_added = datetime.now()
    fn_100 = _truncate(function_name, 100)
    file_100 = _truncate(file_name, 100)

    try:
        with get_engine().begin() as CONN:
            CONN.execute(
                _LOG_INSERT_SQL_TEXT,
                {
                    "DT_FUNCTION_MESSAGE_QUEUED": dt_function_message_queued,
                    "DT_ADDED": dt_added,
                    "PYTHON_FUNCTION_NAME": fn_100,
                    "PYTHON_FILE_NAME": file_100,
                    "RECORDING_ID": recording_id,
                    "AUDIO_CHUNK_NO": audio_chunk_no,
                    "FRAME_NO": frame_no,
                    "START_STOP_OR_ERROR_MSG": message,
                },
            )
    except Exception as e:
        LOGGER.exception(
            "ENGINE_DB_LOG_FUNCTIONS insert failed: %s | fn=%s | file=%s | rid=%s | chunk=%s | frame=%s",
            e, fn_100, file_100, recording_id, audio_chunk_no, frame_no
        )

try:
    from fastapi import WebSocket as _FastAPIWebSocket  # type: ignore
except Exception:
    _FastAPIWebSocket = None  # type: ignore


# -----------------------------------------------------------------------------
# Micro benchmarking helpers (for nailing the 1s latency)
# -----------------------------------------------------------------------------
def _pct(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    idx = max(0, min(len(values) - 1, int(round((p / 100.0) * (len(values) - 1)))))
    return sorted(values)[idx]

def _stats_ms(samples: List[float]) -> Dict[str, float]:
    if not samples:
        return {"count": 0, "min": 0.0, "p50": 0.0, "p90": 0.0, "p95": 0.0, "max": 0.0, "avg": 0.0}
    return {
        "count": float(len(samples)),
        "min": min(samples),
        "p50": _pct(samples, 50),
        "p90": _pct(samples, 90),
        "p95": _pct(samples, 95),
        "max": max(samples),
        "avg": statistics.fmean(samples),
    }

def DB_PING(iterations: int = 50) -> Dict[str, float]:
    """
    Repeatedly run a trivial SELECT to measure baseline round-trip latency.
    Helps isolate network/driver latency from INSERT overhead.
    """
    times: List[float] = []
    with get_engine().connect() as CONN:
        # warm
        try:
            CONN.execute(text("SELECT 1"))
        except Exception as e:
            LOGGER.exception("DB_PING warmup failed: %s", e)
        for _ in range(max(1, iterations)):
            t0 = _now_ms()
            CONN.execute(text("SELECT 1"))
            times.append(_now_ms() - t0)
    s = _stats_ms(times)
    LOGGER.info("DB_PING: min=%.1f p50=%.1f p90=%.1f p95=%.1f max=%.1f avg=%.1f (n=%d)",
                s["min"], s["p50"], s["p90"], s["p95"], s["max"], s["avg"], int(s["count"]))
    return s

def DB_BENCHMARK_INSERT(table: str, row: Mapping[str, Any], n: int = 50, warmup: int = 5) -> Dict[str, float]:
    """
    Measure single-row insert latency using the same generic path (no fire_and_forget).
    Useful to see actual end-to-end timing including SQLAlchemy/pyodbc.
    """
    # Warmups (ignored)
    for _ in range(max(0, warmup)):
        DB_INSERT_TABLE(table, row, fire_and_forget=False)

    times: List[float] = []
    for i in range(max(1, n)):
        t0 = _now_ms()
        DB_INSERT_TABLE(table, row, fire_and_forget=False)
        times.append(_now_ms() - t0)
    s = _stats_ms(times)
    LOGGER.info("DB_BENCHMARK_INSERT[%s]: min=%.1f p50=%.1f p90=%.1f p95=%.1f max=%.1f avg=%.1f (n=%d)",
                table, s["min"], s["p50"], s["p90"], s["p95"], s["max"], s["avg"], int(s["count"]))
    return s

def DB_BENCHMARK_EXEC(sql: str, params_list: Optional[List[Mapping[str, Any]]] = None, warmup: int = 5) -> Dict[str, float]:
    """
    Benchmark arbitrary SQL text with a list of param dicts; if params_list is None,
    we just execute once per iteration without params. Uses a single transaction per run.
    """
    params_list = params_list or [{}]
    with get_engine().begin() as CONN:
        # warm
        for _ in range(max(0, warmup)):
            for p in params_list:
                CONN.execute(text(sql), p)

    times: List[float] = []
    with get_engine().begin() as CONN:
        for p in params_list:
            t0 = _now_ms()
            CONN.execute(text(sql), p)
            times.append(_now_ms() - t0)

    s = _stats_ms(times)
    LOGGER.info("DB_BENCHMARK_EXEC: %s | min=%.1f p50=%.1f p90=%.1f p95=%.1f max=%.1f avg=%.1f (n=%d)",
                sql.splitlines()[0][:120], s["min"], s["p50"], s["p90"], s["p95"], s["max"], s["avg"], int(s["count"]))
    return s

##################################################################################

_ERROR_TABLE = "ENGINE_DB_LOG_FUNCTION_ERROR"

def _extract_context(fn: Callable, args: tuple, kwargs: dict) -> Dict[str, Any]:
    """
    Safely pull useful IDs from function arguments if they exist.
    Add/adjust keys here as your project evolves.
    """
    ctx: Dict[str, Any] = {}
    try:
        sig = inspect.signature(fn)
        bound = sig.bind_partial(*args, **kwargs)
        bound.apply_defaults()
        pick = ("RECORDING_ID", "AUDIO_FRAME_NO", "AUDIO_CHUNK_NO", "START_MS", "END_MS")
        for k in pick:
            if k in bound.arguments:
                ctx[k] = bound.arguments[k]
    except Exception:
        pass
    return ctx

def _maybe_log_success(fn: Callable, args: tuple, kwargs: dict, result: Any) -> None:
    """Optional success logging; off by default to keep noise low."""
    return

def ENGINE_DB_LOG_FUNCTIONS_INS(level=logging.INFO, *, defer_ws_db_io: bool = True):
    """
    WS-safe logging decorator.

    • Writes Start/End/Error rows to ENGINE_DB_LOG_FUNCTIONS (timeline).
    • ALSO writes a detailed row to ENGINE_DB_LOG_FUNCTION_ERROR on exceptions
      (ERROR_MESSAGE_TEXT + TRACEBACK_TEXT + context IDs).
    • Captures DT_FUNCTION_MESSAGE_QUEUED when the log is queued (pre-exec).
    • For WS handlers, DB inserts can be deferred/offloaded so the handshake
      can reach `await ws.accept()` without blocking.
    """

    def decorate(func):
        is_coro = inspect.iscoroutinefunction(func)

        module = func.__module__
        qual   = func.__qualname__
        src    = inspect.getsourcefile(func) or inspect.getfile(func) or "<?>"
        file_name = Path(src).name
        func_id   = f"{module}.{qual}"

        def _log_python(kind: str, msg: str):
            try:
                if kind == "Error":
                    LOGGER.exception("[%s | %s] %s", func_id, file_name, msg)
                else:
                    LOGGER.log(level, "[%s | %s] %s", func_id, file_name, msg, stacklevel=3)
            except Exception:
                pass

        def _compose_msg(kind: str, extra_msg: str = None, elapsed: float = None) -> str:
            base = "Start" if kind == "Start" else ("End" if kind == "End" else "Error")
            if kind == "End" and elapsed is not None:
                base = f"{base} ({elapsed:.3f}s)"
            if extra_msg:
                extra = (extra_msg or "").strip()
                if len(extra) > 4000:
                    extra = extra[:4000] + "…"
                base = f"{base}: {extra}"
            return base

        # --- ENGINE_DB_LOG_FUNCTIONS insert (now uses robust context extractor) ---
        def _db_insert(kind: str, args: tuple, kwargs: dict, msg: str, queued_at: datetime):
            ctx = _extract_context(func, args, kwargs)
            _db_log_event(
                dt_function_message_queued=queued_at,
                function_name=func_id,
                file_name=file_name,
                message=msg,
                recording_id=ctx.get("RECORDING_ID"),
                audio_chunk_no=ctx.get("AUDIO_CHUNK_NO"),
                frame_no=ctx.get("AUDIO_FRAME_NO"),
            )

        def _schedule_db(kind: str, args: tuple, kwargs: dict, msg: str, queued_at: datetime, prefer_async: bool):
            try:
                if prefer_async:
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(asyncio.to_thread(_db_insert, kind, args, kwargs, msg, queued_at))
                        return
                    except RuntimeError:
                        pass
                threading.Thread(target=_db_insert, args=(kind, args, kwargs, msg, queued_at), daemon=True).start()
            except Exception:
                pass

        # --- error table insert (uses robust context extractor) -------------------
        def _db_insert_error_row(args: tuple, kwargs: dict, exc: BaseException):
            ctx = _extract_context(func, args, kwargs)
            row = {
                "DT_ADDED": datetime.now(),
                "PYTHON_FUNCTION_NAME": func_id,
                "PYTHON_FILE_NAME": file_name,
                "ERROR_MESSAGE_TEXT": f"{exc.__class__.__name__}: {exc}",
                "TRACEBACK_TEXT": traceback.format_exc(),
                # optional context (schema can ignore if not allowlisted)
                "RECORDING_ID": ctx.get("RECORDING_ID"),
                "AUDIO_CHUNK_NO": ctx.get("AUDIO_CHUNK_NO"),
                "AUDIO_FRAME_NO": ctx.get("AUDIO_FRAME_NO"),
                "START_MS": ctx.get("START_MS"),
                "END_MS": ctx.get("END_MS"),
            }
            try:
                DB_INSERT_TABLE(_ERROR_TABLE, row, fire_and_forget=True)  # type: ignore[arg-type]
            except Exception:
                try:
                    CONSOLE_LOG("ENGINE_DB_LOG_FUNCTIONS_INS", "ERROR_TABLE_INSERT_FAILED", {
                        "fn": func_id, "file": file_name, "err": str(exc)
                    })
                except Exception:
                    pass

        def _is_ws_call(args, kwargs) -> bool:
            if _FastAPIWebSocket is None:
                return False
            try:
                for a in args:
                    if isinstance(a, _FastAPIWebSocket):
                        return True
                for v in (kwargs or {}).values():
                    if isinstance(v, _FastAPIWebSocket):
                        return True
            except Exception:
                pass
            return False

        @functools.wraps(func)
        async def aw(*args, **kwargs):
            ws_mode = _is_ws_call(args, kwargs)
            t0 = time.perf_counter()

            start_msg = _compose_msg("Start")
            _log_python("Start", start_msg)
            start_queued_at = datetime.now()
            try:
                if ws_mode and defer_ws_db_io:
                    _schedule_db("Start", args, kwargs, start_msg, start_queued_at, prefer_async=True)
                else:
                    _db_insert("Start", args, kwargs, start_msg, start_queued_at)

                result = await func(*args, **kwargs)

                end_msg = _compose_msg("End", elapsed=time.perf_counter() - t0)
                _log_python("End", end_msg)
                end_queued_at = datetime.now()
                if ws_mode and defer_ws_db_io:
                    _schedule_db("End", args, kwargs, end_msg, end_queued_at, prefer_async=True)
                else:
                    _db_insert("End", args, kwargs, end_msg, end_queued_at)
                return result
            except Exception as e:
                err_msg = _compose_msg("Error", extra_msg=f"{e.__class__.__name__}: {e}")
                _log_python("Error", err_msg)
                err_queued_at = datetime.now()
                if ws_mode and defer_ws_db_io:
                    _schedule_db("Error", args, kwargs, err_msg, err_queued_at, prefer_async=True)
                else:
                    _db_insert("Error", args, kwargs, err_msg, err_queued_at)

                _db_insert_error_row(args, kwargs, e)
                raise

        @functools.wraps(func)
        def sw(*args, **kwargs):
            ws_mode = _is_ws_call(args, kwargs)
            t0 = time.perf_counter()

            start_msg = _compose_msg("Start")
            _log_python("Start", start_msg)
            start_queued_at = datetime.now()
            try:
                if ws_mode and defer_ws_db_io:
                    _schedule_db("Start", args, kwargs, start_msg, start_queued_at, prefer_async=False)
                else:
                    _db_insert("Start", args, kwargs, start_msg, start_queued_at)

                result = func(*args, **kwargs)

                end_msg = _compose_msg("End", elapsed=time.perf_counter() - t0)
                _log_python("End", end_msg)
                end_queued_at = datetime.now()
                if ws_mode and defer_ws_db_io:
                    _schedule_db("End", args, kwargs, end_msg, end_queued_at, prefer_async=False)
                else:
                    _db_insert("End", args, kwargs, end_msg, end_queued_at)
                return result
            except Exception as e:
                err_msg = _compose_msg("Error", extra_msg=f"{e.__class__.__name__}: {e}")
                _log_python("Error", err_msg)
                err_queued_at = datetime.now()
                if ws_mode and defer_ws_db_io:
                    _schedule_db("Error", args, kwargs, err_msg, err_queued_at, prefer_async=False)
                else:
                    _db_insert("Error", args, kwargs, err_msg, err_queued_at)

                _db_insert_error_row(args, kwargs, e)
                raise

        return aw if is_coro else sw

    return decorate
