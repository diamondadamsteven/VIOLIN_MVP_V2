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
from SERVER_ENGINE_APP_VARIABLES import RESULT_SET_P_ENGINE_DB_LOG_COLUMNS_BY_TABLE_NAME_GET_ARRAY
import sqlite3

# FastAPI WebSocket import for WS detection
try:
    from fastapi import WebSocket as _FastAPIWebSocket  # type: ignore
except Exception:
    _FastAPIWebSocket = None  # type: ignore

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

LOGGER = logging.getLogger("app")

# -----------------------------------------------------------------------------
# Engine creation / pooling (lazy + startup/shutdown helpers)
# -----------------------------------------------------------------------------
DB_URL = r"mssql+pyodbc://violin:Test123!@adam\MSSQLSERVER01/VIOLIN?driver=ODBC+Driver+17+for+SQL+Server"

_DB_ENGINE: Optional[Engine] = None  # set via get_engine()

def _create_engine() -> Engine:
    return create_engine(
        DB_URL,
        future=True,
        pool_pre_ping=True,
        pool_recycle=1800,
        fast_executemany=True,
        # PERFORMANCE OPTIMIZATION: Balanced settings for stability
        pool_size=5,               # Reduced from 10 for better stability
        max_overflow=5,            # Reduced from 20 for better resource management
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
        P_ENGINE_DB_LOG_COLUMNS_BY_TABLE_NAME_GET()
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
# Generic INSERT allowlist (from P_ENGINE_DB_LOG_COLUMNS_BY_TABLE_NAME_GET)
# -----------------------------------------------------------------------------
def P_ENGINE_DB_LOG_COLUMNS_BY_TABLE_NAME_GET() -> None:
    with get_engine().begin() as CONN:
        rows = CONN.execute(text("EXEC P_ENGINE_DB_LOG_COLUMNS_BY_TABLE_NAME_GET")).fetchall()
               
        # Import and clear the global variable
        RESULT_SET_P_ENGINE_DB_LOG_COLUMNS_BY_TABLE_NAME_GET_ARRAY.clear()
        
        for r in rows:
            t = str(getattr(r, "TABLE_NAME")).upper().strip()
            c = str(getattr(r, "COLUMN_NAME")).upper().strip()
            
            # Initialize table entry if it doesn't exist
            if t not in RESULT_SET_P_ENGINE_DB_LOG_COLUMNS_BY_TABLE_NAME_GET_ARRAY:
                RESULT_SET_P_ENGINE_DB_LOG_COLUMNS_BY_TABLE_NAME_GET_ARRAY[t] = []
            
            # Add column to the table's column list
            RESULT_SET_P_ENGINE_DB_LOG_COLUMNS_BY_TABLE_NAME_GET_ARRAY[t].append({
                "TABLE_NAME": t,
                "COLUMN_NAME": c
            })


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
def ENGINE_DB_LOG_TABLE_INS(table: str, row: Mapping[str, Any]) -> None:
    """Insert a single row into SQLite table using global column definitions."""
    try:
        # Get column names from the global variable
        table_upper = table.upper().strip()
        if table_upper not in RESULT_SET_P_ENGINE_DB_LOG_COLUMNS_BY_TABLE_NAME_GET_ARRAY:
            LOGGER.error("Table %s not found in allowlist", table)
            return
        
        # Extract column names for this table
        columns = []
        for col_data in RESULT_SET_P_ENGINE_DB_LOG_COLUMNS_BY_TABLE_NAME_GET_ARRAY[table_upper]:
            columns.append(col_data["COLUMN_NAME"])
        
        if not columns:
            LOGGER.error("No columns found for table %s", table)
            return
        
        # Build SQL dynamically
        cols_sql = ", ".join(columns)
        placeholders = ", ".join(["?" for _ in columns])
        sql = f"INSERT INTO {table_upper} ({cols_sql}) VALUES ({placeholders})"
        
        # Extract values in the correct order for positional parameters
        values = []
        for col in columns:
            values.append(row.get(col, None))  # Use None if key doesn't exist
        
        # Execute insert into SQLite instead of SQL Server
        with sqlite3.connect(r"C:\Users\diamo\VIOLIN_MVP_V2\SQLite_VIOLIN_MVP_2.db") as conn:
            conn.execute(sql, values)
            conn.commit()
            
    except Exception as e:
        LOGGER.exception("ENGINE_DB_LOG_TABLE_INS failed for %s: %s", table, e)

 
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

##################################################################################

_ERROR_TABLE = "ENGINE_DB_LOG_FUNCTION_ERROR"

def ENGINE_DB_LOG_FUNCTIONS_INS(level=logging.INFO, *, defer_ws_db_io: bool = True):
    """
    WS-safe logging decorator that writes to SQLite.
    • Writes Start/End/Error rows to ENGINE_DB_LOG_FUNCTIONS (timeline).
    • ALSO writes a detailed row to ENGINE_DB_LOG_FUNCTION_ERROR on exceptions
    • Captures DT_FUNCTION_MESSAGE_QUEUED when the log is queued (pre-exec).
    • For WS handlers, DB inserts can be deferred/offloaded so the handshake
      can reach `await ws.accept()` without blocking.
    """

    def decorate(func):
        is_coro = inspect.iscoroutinefunction(func)

        # Get function info
        module = func.__module__
        qual = func.__qualname__
        src = inspect.getsourcefile(func) or inspect.getfile(func) or "<?>"
        file_name = Path(src).name
        func_id = f"{module}.{qual}"

        # Helper function to extract context from function arguments
        def extract_context(args: tuple, kwargs: dict) -> Dict[str, Any]:
            ctx = {}
            try:
                sig = inspect.signature(func)
                bound = sig.bind_partial(*args, **kwargs)
                bound.apply_defaults()
                pick = ("RECORDING_ID", "AUDIO_FRAME_NO", "AUDIO_CHUNK_NO", "START_MS", "END_MS")
                for k in pick:
                    if k in bound.arguments:
                        ctx[k] = bound.arguments[k]
            except Exception:
                pass
            return ctx

        # Helper function to compose messages
        def compose_msg(kind: str, extra_msg: str = None, elapsed: float = None) -> str:
            base = "Start" if kind == "Start" else ("End" if kind == "End" else "Error")
            if kind == "End" and elapsed is not None:
                base = f"{base} ({elapsed:.3f}s)"
            if extra_msg:
                extra = (extra_msg or "").strip()
                if len(extra) > 4000:
                    extra = extra[:4000] + "…"
                base = f"{base}: {extra}"
            return base

        # Helper function to truncate strings
        def truncate(s: Optional[str], max_len: int) -> Optional[str]:
            if s is None:
                return None
            return s if len(s) <= max_len else s[: max_len - 1] + "…"

        # Helper function to check if this is a WebSocket call
        def is_ws_call(args, kwargs) -> bool:
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

        # Helper function to log to ENGINE_DB_LOG_FUNCTIONS table
        def log_function_event(kind: str, args: tuple, kwargs: dict, msg: str, queued_at: datetime):
            ctx = extract_context(args, kwargs)
            
            # Console logging
            # try:
            #     CONSOLE_LOG("ENGINE_DB_LOG_FUNCTIONS_INS", f"FUNCTION_{kind.upper()}", {
            #         "function": func_id,
            #         "file": file_name,
            #         "message": msg,
            #         "recording_id": ctx.get("RECORDING_ID"),
            #         "audio_frame_no": ctx.get("AUDIO_FRAME_NO"),
            #         "args_count": len(args),
            #         "kwargs_count": len(kwargs),
            #         "queued_at": queued_at.isoformat()
            #     })
            # except Exception:
            #     pass
            
            # Insert into SQLite ENGINE_DB_LOG_FUNCTIONS table
            try:
                dt_added = datetime.now()
                fn_100 = truncate(func_id, 100)
                file_100 = truncate(file_name, 100)
                
                sql = """
                INSERT INTO ENGINE_DB_LOG_FUNCTIONS
                (DT_FUNCTION_MESSAGE_QUEUED, DT_ADDED, PYTHON_FUNCTION_NAME, PYTHON_FILE_NAME,
                 RECORDING_ID, AUDIO_CHUNK_NO, FRAME_NO, START_STOP_OR_ERROR_MSG)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                with sqlite3.connect(r"C:\Users\diamo\VIOLIN_MVP_V2\SQLite_VIOLIN_MVP_2.db") as conn:
                    conn.execute(sql, (
                        queued_at,
                        dt_added,
                        fn_100,
                        file_100,
                        ctx.get("RECORDING_ID"),
                        ctx.get("AUDIO_CHUNK_NO"),
                        ctx.get("AUDIO_FRAME_NO"),
                        msg
                    ))
                    conn.commit()
            except Exception as e:
                LOGGER.exception("ENGINE_DB_LOG_FUNCTIONS insert failed: %s", e)

        # Helper function to log errors to ENGINE_DB_LOG_FUNCTION_ERROR table
        def log_function_error(args: tuple, kwargs: dict, exc: BaseException):
            ctx = extract_context(args, kwargs)
            
            # Console logging for errors
            try:
                CONSOLE_LOG("ENGINE_DB_LOG_FUNCTIONS_INS", "FUNCTION_ERROR", {
                    "function": func_id,
                    "file": file_name,
                    "error": f"{exc.__class__.__name__}: {exc}",
                    "recording_id": ctx.get("RECORDING_ID"),
                    "audio_frame_no": ctx.get("AUDIO_FRAME_NO"),
                    "traceback": traceback.format_exc()[:500]
                })
            except Exception:
                pass
            
            # Insert into SQLite ENGINE_DB_LOG_FUNCTION_ERROR table
            try:
                sql = """
                INSERT INTO ENGINE_DB_LOG_FUNCTION_ERROR
                (DT_ADDED, PYTHON_FUNCTION_NAME, PYTHON_FILE_NAME, ERROR_MESSAGE_TEXT, TRACEBACK_TEXT,
                 RECORDING_ID, AUDIO_CHUNK_NO, AUDIO_FRAME_NO, START_MS, END_MS)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                with sqlite3.connect(r"C:\Users\diamo\VIOLIN_MVP_V2\SQLite_VIOLIN_MVP_2.db") as conn:
                    conn.execute(sql, (
                        datetime.now(),
                        func_id,
                        file_name,
                        f"{exc.__class__.__name__}: {exc}",
                        traceback.format_exc(),
                        ctx.get("RECORDING_ID"),
                        ctx.get("AUDIO_CHUNK_NO"),
                        ctx.get("AUDIO_FRAME_NO"),
                        ctx.get("START_MS"),
                        ctx.get("END_MS")
                    ))
                    conn.commit()
            except Exception as e:
                LOGGER.exception("ENGINE_DB_LOG_FUNCTION_ERROR insert failed: %s", e)

        # Helper function to schedule database operations
        def schedule_db(kind: str, args: tuple, kwargs: dict, msg: str, queued_at: datetime, prefer_async: bool):
            try:
                if prefer_async:
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(asyncio.to_thread(log_function_event, kind, args, kwargs, msg, queued_at))
                        return
                    except RuntimeError:
                        pass
                threading.Thread(target=log_function_event, args=(kind, args, kwargs, msg, queued_at), daemon=True).start()
            except Exception:
                pass

        # Python logging helper
        def log_python(kind: str, msg: str):
            try:
                if kind == "Error":
                    LOGGER.exception("[%s | %s] %s", func_id, file_name, msg)
                # else:
                #     LOGGER.log(level, "[%s | %s] %s", func_id, file_name, msg, stacklevel=3)
            except Exception:
                pass

        # Async wrapper
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            ws_mode = is_ws_call(args, kwargs)
            t0 = time.perf_counter()

            start_msg = compose_msg("Start")
            log_python("Start", start_msg)
            start_queued_at = datetime.now()
            
            try:
                if ws_mode and defer_ws_db_io:
                    schedule_db("Start", args, kwargs, start_msg, start_queued_at, prefer_async=True)
                else:
                    log_function_event("Start", args, kwargs, start_msg, start_queued_at)

                result = await func(*args, **kwargs)

                end_msg = compose_msg("End", elapsed=time.perf_counter() - t0)
                log_python("End", end_msg)
                end_queued_at = datetime.now()
                
                if ws_mode and defer_ws_db_io:
                    schedule_db("End", args, kwargs, end_msg, end_queued_at, prefer_async=True)
                else:
                    log_function_event("End", args, kwargs, end_msg, end_queued_at)
                    
                return result
            except Exception as e:
                err_msg = compose_msg("Error", extra_msg=f"{e.__class__.__name__}: {e}")
                log_python("Error", err_msg)
                err_queued_at = datetime.now()
                
                if ws_mode and defer_ws_db_io:
                    schedule_db("Error", args, kwargs, err_msg, err_queued_at, prefer_async=True)
                else:
                    log_function_event("Error", args, kwargs, err_msg, err_queued_at)

                log_function_error(args, kwargs, e)
                raise

        # Sync wrapper
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            ws_mode = is_ws_call(args, kwargs)
            t0 = time.perf_counter()

            start_msg = compose_msg("Start")
            log_python("Start", start_msg)
            start_queued_at = datetime.now()
            
            try:
                if ws_mode and defer_ws_db_io:
                    schedule_db("Start", args, kwargs, start_msg, start_queued_at, prefer_async=False)
                else:
                    log_function_event("Start", args, kwargs, start_msg, start_queued_at)

                result = func(*args, **kwargs)

                end_msg = compose_msg("End", elapsed=time.perf_counter() - t0)
                log_python("End", end_msg)
                end_queued_at = datetime.now()
                
                if ws_mode and defer_ws_db_io:
                    schedule_db("End", args, kwargs, end_msg, end_queued_at, prefer_async=False)
                else:
                    log_function_event("End", args, kwargs, end_msg, end_queued_at)
                    
                return result
            except Exception as e:
                err_msg = compose_msg("Error", extra_msg=f"{e.__class__.__name__}: {e}")
                log_python("Error", err_msg)
                err_queued_at = datetime.now()
                
                if ws_mode and defer_ws_db_io:
                    schedule_db("Error", args, kwargs, err_msg, err_queued_at, prefer_async=False)
                else:
                    log_function_event("Error", args, kwargs, err_msg, err_queued_at)

                log_function_error(args, kwargs, e)
                raise

        return async_wrapper if is_coro else sync_wrapper

    return decorate

