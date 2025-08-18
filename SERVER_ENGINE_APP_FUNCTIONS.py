# SERVER_ENGINE_APP_FUNCTIONS.py
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Coroutine, Union

import functools
import inspect
import logging
import time
from pathlib import Path
from datetime import datetime, timedelta
import threading
import asyncio
import os
import json as _json

# Optional; kept for type hints / legacy awareness
import pyodbc  # type: ignore

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from contextlib import contextmanager

from SERVER_ENGINE_APP_VARIABLES import (
    RECORDING_AUDIO_FRAME_ARRAY,
    RECORDING_AUDIO_CHUNK_ARRAY,
    RECORDING_CONFIG_ARRAY,
    RECORDING_WEBSOCKET_CONNECTION_ARRAY,
    RECORDING_WEBSOCKET_MESSAGE_ARRAY,
)

# -----------------------------------------------------------------------------
# Async utils: loop-safe scheduler usable from threads or loop
# -----------------------------------------------------------------------------
# uvicorn reload can reimport modules; keep a single shared slot
try:
    _MAIN_LOOP  # type: ignore[name-defined]
except NameError:
    _MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None  # set on startup

def ASYNC_SET_MAIN_LOOP(loop: asyncio.AbstractEventLoop) -> None:
    """
    Capture the process' main event loop exactly once at app startup.
    Call from FastAPI's @APP.on_event("startup").
    """
    global _MAIN_LOOP
    _MAIN_LOOP = loop

def schedule_coro(coro: Coroutine[Any, Any, Any]) -> Union[asyncio.Task, Any]:
    """
    Schedule a coroutine regardless of caller context:
      • If we're already on an event loop → create_task(coro)
      • If we're in a worker thread      → run_coroutine_threadsafe(coro, _MAIN_LOOP)
    Returns a Task (in-loop) or concurrent.futures.Future (from thread).
    """
    try:
        loop = asyncio.get_running_loop()
        return loop.create_task(coro)
    except RuntimeError:
        if _MAIN_LOOP is None:
            raise RuntimeError(
                "MAIN event loop not set. Call ASYNC_SET_MAIN_LOOP(asyncio.get_running_loop()) at startup."
            )
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
    """
    Create the pooled SQLAlchemy Engine.
    """
    return create_engine(
        DB_URL,
        future=True,
        pool_pre_ping=True,   # refresh stale conns automatically
        pool_recycle=1800,    # recycle every 30 minutes
        fast_executemany=True # speed up pyodbc executemany
    )

def get_engine() -> Engine:
    """Return the global Engine, creating it on first use."""
    global _DB_ENGINE
    if _DB_ENGINE is None:
        _DB_ENGINE = _create_engine()
    return _DB_ENGINE

def DB_ENGINE_STARTUP(warm_pool: bool = True) -> None:
    """
    Call once at FastAPI startup. Lazily creates the engine and (optionally)
    warms the pool with a trivial query so first real insert is fast.
    Also ensures the background log-writer is running.
    """
    eng = get_engine()
    if warm_pool:
        try:
            with eng.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as e:
            LOGGER.exception("DB_ENGINE_STARTUP warm_pool failed: %s", e)
    # Start background log writer once
    try:
        _start_log_writer_once()
    except Exception as e:
        LOGGER.exception("Failed to start log writer: %s", e)

def DB_ENGINE_SHUTDOWN() -> None:
    """Optional: dispose the engine at application shutdown."""
    global _DB_ENGINE
    if _DB_ENGINE is not None:
        try:
            _DB_ENGINE.dispose()
        except Exception:
            pass
        _DB_ENGINE = None

# -----------------------------------------------------------------------------
# Background logging queue (shared for steps, websocket messages, audio frames)
# -----------------------------------------------------------------------------
# Data shape:
#   {"kind": "step"|"ws_msg"|"audio_frame", "params": {...}}
try:
    _LOG_QUEUE  # type: ignore[name-defined]
except NameError:
    _LOG_QUEUE: Optional[asyncio.Queue] = None

try:
    _LOG_WRITER_STARTED  # type: ignore[name-defined]
except NameError:
    _LOG_WRITER_STARTED: bool = False

def _ensure_log_queue() -> asyncio.Queue:
    global _LOG_QUEUE
    if _LOG_QUEUE is None:
        _LOG_QUEUE = asyncio.Queue()
    return _LOG_QUEUE

def _logq_put(item: Dict[str, Any]) -> None:
    """
    Put into the async queue from loop or thread.
    """
    q = _ensure_log_queue()
    try:
        loop = asyncio.get_running_loop()
        q.put_nowait(item)
    except RuntimeError:
        # thread: use main loop
        if _MAIN_LOOP is None:
            raise RuntimeError("MAIN_LOOP not set; call ASYNC_SET_MAIN_LOOP() at startup")
        asyncio.run_coroutine_threadsafe(q.put(item), _MAIN_LOOP)

def _start_log_writer_once() -> None:
    global _LOG_WRITER_STARTED
    if _LOG_WRITER_STARTED:
        return
    _LOG_WRITER_STARTED = True
    schedule_coro(_log_writer_loop())

# ----- SQL statements reused by the writer
_SQL_STEP = text("""
INSERT INTO ENGINE_DB_LOG_STEPS (
    DT_ADDED, STEP_NAME, PYTHON_FUNCTION_NAME, PYTHON_FILE_NAME,
    RECORDING_ID, AUDIO_CHUNK_NO, FRAME_NO
) VALUES (
    :DT_ADDED, :STEP_NAME, :PYTHON_FUNCTION_NAME, :PYTHON_FILE_NAME,
    :RECORDING_ID, :AUDIO_CHUNK_NO, :FRAME_NO
)
""")

_SQL_WSMSG = text("""
INSERT INTO ENGINE_DB_LOG_WEBSOCKET_MESSAGE (
    RECORDING_ID, MESSAGE_TYPE, AUDIO_FRAME_NO,
    DT_MESSAGE_RECEIVED, DT_MESSAGE_PROCESS_STARTED
) VALUES (
    :RECORDING_ID, :MESSAGE_TYPE, :AUDIO_FRAME_NO,
    :DT_MESSAGE_RECEIVED, :DT_MESSAGE_PROCESS_STARTED
)
""")

_SQL_FRAME = text("""
INSERT INTO ENGINE_DB_LOG_AUDIO_FRAME (
    RECORDING_ID, FRAME_NO, AUDIO_STREAM_FRAME_SIZE_IN_MS, DT_FRAME_RECEIVED
) VALUES (
    :RECORDING_ID, :FRAME_NO, :AUDIO_STREAM_FRAME_SIZE_IN_MS, :DT_FRAME_RECEIVED
)
""")

async def _log_writer_loop() -> None:
    """
    Background task: drain queue in small batches and insert per-kind.
    """
    BATCH_MAX = 200
    IDLE_TIMEOUT = 0.25  # seconds

    while True:
        try:
            q = _ensure_log_queue()
            # block for first item
            item = await q.get()
            batch: List[Dict[str, Any]] = [item]
            # slurp up to BATCH_MAX-1 more items without blocking
            for _ in range(BATCH_MAX - 1):
                try:
                    batch.append(q.get_nowait())
                except asyncio.QueueEmpty:
                    break

            # split by kind
            steps: List[Dict[str, Any]] = []
            wsmsgs: List[Dict[str, Any]] = []
            frames: List[Dict[str, Any]] = []

            for rec in batch:
                k = rec.get("kind")
                p = rec.get("params", {})
                if k == "step":
                    steps.append(p)
                elif k == "ws_msg":
                    wsmsgs.append(p)
                elif k == "audio_frame":
                    frames.append(p)

            # write in one DB context
            if steps or wsmsgs or frames:
                with get_engine().begin() as CONN:
                    if steps:
                        CONN.execute(_SQL_STEP, steps)
                    if wsmsgs:
                        CONN.execute(_SQL_WSMSG, wsmsgs)
                    if frames:
                        CONN.execute(_SQL_FRAME, frames)
        except Exception as e:
            LOGGER.exception("log-writer loop error: %s", e)
            # small backoff to avoid thrash
            await asyncio.sleep(IDLE_TIMEOUT)

# -----------------------------------------------------------------------------
# Fast path: lightweight per-event insert into ENGINE_DB_LOG_FUNCTIONS (unchanged)
# -----------------------------------------------------------------------------
_LOG_INSERT_SQL_TEXT = text("""
INSERT INTO ENGINE_DB_LOG_FUNCTIONS
(DT_ADDED, PYTHON_FUNCTION_NAME, PYTHON_FILE_NAME,
 RECORDING_ID, AUDIO_CHUNK_NO, FRAME_NO, START_STOP_OR_ERROR_MSG)
VALUES (:DT_ADDED, :PYTHON_FUNCTION_NAME, :PYTHON_FILE_NAME,
        :RECORDING_ID, :AUDIO_CHUNK_NO, :FRAME_NO, :START_STOP_OR_ERROR_MSG)
""")

def _truncate(s: Optional[str], max_len: int) -> Optional[str]:
    if s is None:
        return None
    return s if len(s) <= max_len else s[: max_len - 1] + "…"

def _db_log_event(function_name: str,
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
            "DB_LOG_FUNCTIONS insert failed: %s | fn=%s | file=%s | rid=%s | chunk=%s | frame=%s",
            e, fn_100, file_100, recording_id, audio_chunk_no, frame_no
        )

# -----------------------------------------------------------------------------
# WS-safe logging decorator
# -----------------------------------------------------------------------------
try:
    from fastapi import WebSocket as _FastAPIWebSocket  # type: ignore
except Exception:
    _FastAPIWebSocket = None  # type: ignore

def DB_LOG_FUNCTIONS(level=logging.INFO, *, defer_ws_db_io: bool = True):
    """
    WS-safe logging decorator:

      • Logs Start/End/Error to Python logger immediately.
      • For WebSocket handlers, DB inserts can be deferred/offloaded so
        the handshake can reach `await ws.accept()` without blocking.
      • NEVER raises out of the decorator (errors are swallowed/logged).
    """
    def decorate(func):
        is_coro = inspect.iscoroutinefunction(func)

        module = func.__module__
        qual   = func.__qualname__
        src    = inspect.getsourcefile(func) or inspect.getfile(func) or "<?>"
        file_name = Path(src).name
        func_id   = f"{module}.{qual}"

        def _extract_ctx(kwargs: dict):
            return kwargs.get("RECORDING_ID"), kwargs.get("AUDIO_CHUNK_NO"), kwargs.get("FRAME_NO")

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
                extra = extra_msg.strip()
                if len(extra) > 4000:
                    extra = extra[:4000] + "…"
                base = f"{base}: {extra}"
            return base

        def _db_insert(kind: str, kwargs: dict, msg: str):
            rid, chk, frm = _extract_ctx(kwargs)
            _db_log_event(function_name=func_id,
                          file_name=file_name,
                          message=msg,
                          recording_id=rid,
                          audio_chunk_no=chk,
                          frame_no=frm)

        def _schedule_db(kind: str, kwargs: dict, msg: str, prefer_async: bool):
            try:
                if prefer_async:
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(asyncio.to_thread(_db_insert, kind, kwargs, msg))
                        return
                    except RuntimeError:
                        pass
                threading.Thread(target=_db_insert, args=(kind, kwargs, msg), daemon=True).start()
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
            try:
                if ws_mode and defer_ws_db_io:
                    _schedule_db("Start", kwargs, start_msg, prefer_async=True)
                else:
                    _db_insert("Start", kwargs, start_msg)

                result = await func(*args, **kwargs)

                end_msg = _compose_msg("End", elapsed=time.perf_counter() - t0)
                _log_python("End", end_msg)
                if ws_mode and defer_ws_db_io:
                    _schedule_db("End", kwargs, end_msg, prefer_async=True)
                else:
                    _db_insert("End", kwargs, end_msg)
                return result
            except Exception as e:
                err_msg = _compose_msg("Error", extra_msg=f"{e.__class__.__name__}: {e}")
                _log_python("Error", err_msg)
                if ws_mode and defer_ws_db_io:
                    _schedule_db("Error", kwargs, err_msg, prefer_async=True)
                else:
                    _db_insert("Error", kwargs, err_msg)
                raise

        @functools.wraps(func)
        def sw(*args, **kwargs):
            ws_mode = _is_ws_call(args, kwargs)
            t0 = time.perf_counter()

            start_msg = _compose_msg("Start")
            _log_python("Start", start_msg)
            try:
                if ws_mode and defer_ws_db_io:
                    _schedule_db("Start", kwargs, start_msg, prefer_async=False)
                else:
                    _db_insert("Start", kwargs, start_msg)

                result = func(*args, **kwargs)

                end_msg = _compose_msg("End", elapsed=time.perf_counter() - t0)
                _log_python("End", end_msg)
                if ws_mode and defer_ws_db_io:
                    _schedule_db("End", kwargs, end_msg, prefer_async=False)
                else:
                    _db_insert("End", kwargs, end_msg)
                return result
            except Exception as e:
                err_msg = _compose_msg("Error", extra_msg=f"{e.__class__.__name__}: {e}")
                _log_python("Error", err_msg)
                if ws_mode and defer_ws_db_io:
                    _schedule_db("Error", kwargs, err_msg, prefer_async=False)
                else:
                    _db_insert("Error", kwargs, err_msg)
                raise

        return aw if is_coro else sw
    return decorate

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
    """
    Return a DBAPI (pyodbc) connection from the pooled Engine.
    NOTE: This is NOT a context manager. Close it or use DB_CONNECT_CTX().
    """
    conn = get_engine().raw_connection()
    try:
        conn.autocommit = True  # type: ignore[attr-defined]
    except Exception:
        pass
    return conn

@contextmanager
def DB_CONNECT_CTX():
    """
    Context-manager wrapper around DB_CONNECT():

        with DB_CONNECT_CTX() as conn:
            ...

    Ensures close()/commit as appropriate.
    """
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
    # Ensure commit when using raw_connection()
    try:
        conn.commit()
    except Exception:
        pass

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
    # Commit in case the SP performs writes
    try:
        conn.commit()
    except Exception:
        pass
    return total if total > 0 else None

# -----------------------------------------------------------------------------
# Queue-based logger APIs (enqueue; background writer performs the INSERTs)
# -----------------------------------------------------------------------------

# ... keep your imports and previous code above ...

def _fire_and_forget(fn, *args, **kwargs):
    """
    Run a synchronous DB function off the event loop ASAP.
    Works whether we're on the loop or in a worker thread.
    """
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(asyncio.to_thread(fn, *args, **kwargs))
    except RuntimeError:
        # we're in a non-async thread
        threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True).start()

# 1) FRAME (now fire-and-forget)
def DB_LOG_ENGINE_DB_AUDIO_FRAME(RECORDING_ID: int, FRAME_NO: int) -> None:
    cfg = RECORDING_CONFIG_ARRAY.get(RECORDING_ID, {}) or {}
    frame_ms = cfg.get("AUDIO_STREAM_FRAME_SIZE_IN_MS")  # allow NULL if missing

    frame_map = RECORDING_AUDIO_FRAME_ARRAY.get(RECORDING_ID, {}) or {}
    frame_row = frame_map.get(FRAME_NO, {}) or {}
    dt_frame_received = frame_row.get("DT_FRAME_RECEIVED")

    def _do():
        with get_engine().begin() as CONN:
            CONN.execute(
                text("""
                    INSERT INTO ENGINE_DB_LOG_AUDIO_FRAME
                    (RECORDING_ID, FRAME_NO, AUDIO_STREAM_FRAME_SIZE_IN_MS, DT_FRAME_RECEIVED)
                    VALUES (:RECORDING_ID, :FRAME_NO, :AUDIO_STREAM_FRAME_SIZE_IN_MS, :DT_FRAME_RECEIVED)
                """),
                {
                    "RECORDING_ID": RECORDING_ID,
                    "FRAME_NO": FRAME_NO,
                    "AUDIO_STREAM_FRAME_SIZE_IN_MS": frame_ms,
                    "DT_FRAME_RECEIVED": dt_frame_received,
                },
            )
    _fire_and_forget(_do)

# 2) WEBSOCKET MESSAGE (now fire-and-forget)
def DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE(MESSAGE_ID: int) -> None:
    msg = RECORDING_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID) or {}

    def _do():
        with get_engine().begin() as CONN:
            CONN.execute(
                text("""
                    INSERT INTO ENGINE_DB_LOG_WEBSOCKET_MESSAGE (
                        RECORDING_ID,
                        MESSAGE_TYPE,
                        AUDIO_FRAME_NO,
                        DT_MESSAGE_RECEIVED,
                        DT_MESSAGE_PROCESS_STARTED,
                        WEBSOCKET_CONNECTION_ID
                    )
                    VALUES (
                        :RECORDING_ID,
                        :MESSAGE_TYPE,
                        :AUDIO_FRAME_NO,
                        :DT_MESSAGE_RECEIVED,
                        :DT_MESSAGE_PROCESS_STARTED,
                        :WEBSOCKET_CONNECTION_ID
                    )
                """),
                {
                    "RECORDING_ID": msg.get("RECORDING_ID"),
                    "MESSAGE_TYPE": msg.get("MESSAGE_TYPE"),
                    "AUDIO_FRAME_NO": msg.get("AUDIO_FRAME_NO"),
                    "DT_MESSAGE_RECEIVED": msg.get("DT_MESSAGE_RECEIVED"),
                    "DT_MESSAGE_PROCESS_STARTED": msg.get("DT_MESSAGE_PROCESS_STARTED"),
                    "WEBSOCKET_CONNECTION_ID": msg.get("WEBSOCKET_CONNECTION_ID")
                },
            )
    _fire_and_forget(_do)

# 3) STEP LOGGER (unchanged signature; still uses named params)
def DB_LOG_ENGINE_DB_LOG_STEPS(
    DT_ADDED: datetime,
    STEP_NAME: str,
    PYTHON_FUNCTION_NAME: str,
    PYTHON_FILE_NAME: str,
    RECORDING_ID: Optional[int] = None,
    AUDIO_CHUNK_NO: Optional[int] = None,
    FRAME_NO: Optional[int] = None,
) -> None:
    try:
        with get_engine().begin() as CONN:
            CONN.execute(
                text("""
                    INSERT INTO ENGINE_DB_LOG_STEPS (
                        DT_ADDED,
                        STEP_NAME,
                        PYTHON_FUNCTION_NAME,
                        PYTHON_FILE_NAME,
                        RECORDING_ID,
                        AUDIO_CHUNK_NO,
                        FRAME_NO
                    )
                    VALUES (
                        :DT_ADDED,
                        :STEP_NAME,
                        :PYTHON_FUNCTION_NAME,
                        :PYTHON_FILE_NAME,
                        :RECORDING_ID,
                        :AUDIO_CHUNK_NO,
                        :FRAME_NO
                    )
                """),
                {
                    "DT_ADDED": DT_ADDED,
                    "STEP_NAME": STEP_NAME,
                    "PYTHON_FUNCTION_NAME": PYTHON_FUNCTION_NAME,
                    "PYTHON_FILE_NAME": PYTHON_FILE_NAME,
                    "RECORDING_ID": RECORDING_ID,
                    "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO,
                    "FRAME_NO": FRAME_NO,
                },
            )
    except Exception as e:
        LOGGER.exception(
            "DB_LOG_ENGINE_DB_LOG_STEPS insert failed "
            "(step=%s, fn=%s, file=%s, rid=%s, chunk=%s, frame=%s): %s",
            STEP_NAME, PYTHON_FUNCTION_NAME, PYTHON_FILE_NAME,
            RECORDING_ID, AUDIO_CHUNK_NO, FRAME_NO, e
        )

# ... keep the rest of your file as-is ...



# 2) AUDIO CHUNK (unchanged; direct write, not queued — it’s much rarer & heavier)
def DB_LOG_ENGINE_DB_RECORDING_AUDIO_CHUNK(RECORDING_ID: int, AUDIO_CHUNK_NO: int) -> None:
    ch = RECORDING_AUDIO_CHUNK_ARRAY[RECORDING_ID][AUDIO_CHUNK_NO]
    with get_engine().begin() as CONN:
        CONN.execute(
            text("""
                INSERT INTO ENGINE_DB_LOG_RECORDING_AUDIO_CHUNK (
                    RECORDING_ID, AUDIO_CHUNK_NO, AUDIO_CHUNK_DURATION_IN_MS, START_MS, END_MS,
                    MIN_AUDIO_STREAM_FRAME_NO, MAX_AUDIO_STREAM_FRAME_NO,
                    YN_RUN_FFT, YN_RUN_ONS, YN_RUN_PYIN, YN_RUN_CREPE,
                    DT_COMPLETE_FRAMES_RECEIVED, DT_START_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK,
                    DT_COMPLETE_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK,
                    DT_AUDIO_CHUNK_CONVERTED_TO_WAV, DT_AUDIO_CHUNK_WAV_SAVED_TO_FILE,
                    DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_16K, DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_22050,
                    DT_AUDIO_CHUNK_PREPARATION_COMPLETE, DT_START_AUDIO_CHUNK_PROCESS, DT_END_AUDIO_CHUNK_PROCESS,
                    DT_START_FFT, FFT_DURATION_IN_MS, FFT_RECORD_CNT,
                    DT_START_ONS, ONS_DURATION_IN_MS, ONS_RECORD_CNT,
                    DT_START_PYIN, PYIN_DURATION_IN_MS, PYIN_RECORD_CNT,
                    DT_START_CREPE, CREPE_DURATION_IN_MS, CREPE_RECORD_CNT,
                    DT_START_VOLUME, VOLUME_10_MS_DURATION_IN_MS, VOLUME_1_MS_DURATION_IN_MS,
                    VOLUME_10_MS_RECORD_CNT, VOLUME_1_MS_RECORD_CNT,
                    DT_START_P_ENGINE_ALL_MASTER, P_ENGINE_ALL_MASTER_DURATION_IN_MS,
                    DT_ADDED
                )
                VALUES (
                    :RECORDING_ID, :AUDIO_CHUNK_NO, :AUDIO_CHUNK_DURATION_IN_MS, :START_MS, :END_MS,
                    :MIN_AUDIO_STREAM_FRAME_NO, :MAX_AUDIO_STREAM_FRAME_NO,
                    :YN_RUN_FFT, :YN_RUN_ONS, :YN_RUN_PYIN, :YN_RUN_CREPE,
                    :DT_COMPLETE_FRAMES_RECEIVED, :DT_START_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK,
                    :DT_COMPLETE_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK,
                    :DT_AUDIO_CHUNK_CONVERTED_TO_WAV, :DT_AUDIO_CHUNK_WAV_SAVED_TO_FILE,
                    :DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_16K, :DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_22050,
                    :DT_AUDIO_CHUNK_PREPARATION_COMPLETE, :DT_START_AUDIO_CHUNK_PROCESS, :DT_END_AUDIO_CHUNK_PROCESS,
                    :DT_START_FFT, :FFT_DURATION_IN_MS, :FFT_RECORD_CNT,
                    :DT_START_ONS, :ONS_DURATION_IN_MS, :ONS_RECORD_CNT,
                    :DT_START_PYIN, :PYIN_DURATION_IN_MS, :PYIN_RECORD_CNT,
                    :DT_START_CREPE, :CREPE_DURATION_IN_MS, :CREPE_RECORD_CNT,
                    :DT_START_VOLUME, :VOLUME_10_MS_DURATION_IN_MS, :VOLUME_1_MS_DURATION_IN_MS,
                    :VOLUME_10_MS_RECORD_CNT, :VOLUME_1_MS_RECORD_CNT,
                    :DT_START_P_ENGINE_ALL_MASTER, :P_ENGINE_ALL_MASTER_DURATION_IN_MS,
                    :DT_ADDED
                )
            """),
            {
                "RECORDING_ID": RECORDING_ID,
                "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO,
                "AUDIO_CHUNK_DURATION_IN_MS": ch.get("AUDIO_CHUNK_DURATION_IN_MS"),
                "START_MS": ch.get("START_MS"),
                "END_MS": ch.get("END_MS"),
                "MIN_AUDIO_STREAM_FRAME_NO": ch.get("MIN_AUDIO_STREAM_FRAME_NO"),
                "MAX_AUDIO_STREAM_FRAME_NO": ch.get("MAX_AUDIO_STREAM_FRAME_NO"),
                "YN_RUN_FFT":   ch.get("YN_RUN_FFT", "N"),
                "YN_RUN_ONS":   ch.get("YN_RUN_ONS", "N"),
                "YN_RUN_PYIN":  ch.get("YN_RUN_PYIN", "N"),
                "YN_RUN_CREPE": ch.get("YN_RUN_CREPE", "N"),
                "DT_COMPLETE_FRAMES_RECEIVED":                      ch.get("DT_COMPLETE_FRAMES_RECEIVED"),
                "DT_START_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK":    ch.get("DT_START_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK"),
                "DT_COMPLETE_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK": ch.get("DT_COMPLETE_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK"),
                "DT_AUDIO_CHUNK_CONVERTED_TO_WAV":                  ch.get("DT_AUDIO_CHUNK_CONVERTED_TO_WAV"),
                "DT_AUDIO_CHUNK_WAV_SAVED_TO_FILE":                 ch.get("DT_AUDIO_CHUNK_WAV_SAVED_TO_FILE"),
                "DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_16K":      ch.get("DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_16K"),
                "DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_22050":    ch.get("DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_22050"),
                "DT_AUDIO_CHUNK_PREPARATION_COMPLETE":              ch.get("DT_AUDIO_CHUNK_PREPARATION_COMPLETE"),
                "DT_START_AUDIO_CHUNK_PROCESS":                     ch.get("DT_START_AUDIO_CHUNK_PROCESS"),
                "DT_END_AUDIO_CHUNK_PROCESS":                       ch.get("DT_END_AUDIO_CHUNK_PROCESS"),
                "DT_START_FFT":                                     ch.get("DT_START_FFT"),
                "FFT_DURATION_IN_MS":                               ch.get("FFT_DURATION_IN_MS"),
                "FFT_RECORD_CNT":                                   ch.get("FFT_RECORD_CNT"),
                "DT_START_ONS":                                     ch.get("DT_START_ONS"),
                "ONS_DURATION_IN_MS":                               ch.get("ONS_DURATION_IN_MS"),
                "ONS_RECORD_CNT":                                   ch.get("ONS_RECORD_CNT"),
                "DT_START_PYIN":                                    ch.get("DT_START_PYIN"),
                "PYIN_DURATION_IN_MS":                              ch.get("PYIN_DURATION_IN_MS"),
                "PYIN_RECORD_CNT":                                  ch.get("PYIN_RECORD_CNT"),
                "DT_START_CREPE":                                   ch.get("DT_START_CREPE"),
                "CREPE_DURATION_IN_MS":                             ch.get("CREPE_DURATION_IN_MS"),
                "CREPE_RECORD_CNT":                                 ch.get("CREPE_RECORD_CNT"),
                "DT_START_VOLUME":                                  ch.get("DT_START_VOLUME"),
                "VOLUME_10_MS_DURATION_IN_MS":                      ch.get("VOLUME_10_MS_DURATION_IN_MS"),
                "VOLUME_1_MS_DURATION_IN_MS":                       ch.get("VOLUME_1_MS_DURATION_IN_MS"),
                "VOLUME_10_MS_RECORD_CNT":                          ch.get("VOLUME_10_MS_RECORD_CNT"),
                "VOLUME_1_MS_RECORD_CNT":                           ch.get("VOLUME_1_MS_RECORD_CNT"),
                "DT_START_P_ENGINE_ALL_MASTER":                     ch.get("DT_START_P_ENGINE_ALL_MASTER"),

                "P_ENGINE_ALL_MASTER_DURATION_IN_MS":               ch.get("P_ENGINE_ALL_MASTER_DURATION_IN_MS"),
                "DT_ADDED": datetime.now(),
            },
        )

# 3) RECORDING CONFIG (unchanged)
def DB_LOG_ENGINE_DB_RECORDING_CONFIG(RECORDING_ID: int) -> None:
    cfg = RECORDING_CONFIG_ARRAY[RECORDING_ID]
    with get_engine().begin() as CONN:
        CONN.execute(
            text("""
                INSERT INTO ENGINE_DB_LOG_RECORDING_CONFIG (
                    RECORDING_ID, DT_RECORDING_START, VIOLINIST_ID, COMPOSE_PLAY_OR_PRACTICE,
                    AUDIO_STREAM_FILE_NAME, AUDIO_STREAM_FRAME_SIZE_IN_MS,
                    AUDIO_CHUNK_DURATION_IN_MS, CNT_FRAMES_PER_AUDIO_CHUNK, YN_RUN_FFT,
                    DT_ADDED, WEBSOCKET_CONNECTION_ID
                )
                VALUES (
                    :RECORDING_ID, :DT_RECORDING_START, :VIOLINIST_ID, :COMPOSE_PLAY_OR_PRACTICE,
                    :AUDIO_STREAM_FILE_NAME, :AUDIO_STREAM_FRAME_SIZE_IN_MS,
                    :AUDIO_CHUNK_DURATION_IN_MS, :CNT_FRAMES_PER_AUDIO_CHUNK, :YN_RUN_FFT,
                    :DT_ADDED, :WEBSOCKET_CONNECTION_ID
                )
            """),
            {
                "RECORDING_ID": RECORDING_ID,
                "DT_RECORDING_START": cfg.get("DT_RECORDING_START"),
                "VIOLINIST_ID": cfg.get("VIOLINIST_ID"),
                "COMPOSE_PLAY_OR_PRACTICE": cfg.get("COMPOSE_PLAY_OR_PRACTICE"),
                "AUDIO_STREAM_FILE_NAME": cfg.get("AUDIO_STREAM_FILE_NAME"),
                "AUDIO_STREAM_FRAME_SIZE_IN_MS": cfg.get("AUDIO_STREAM_FRAME_SIZE_IN_MS"),
                "AUDIO_CHUNK_DURATION_IN_MS": cfg.get("AUDIO_CHUNK_DURATION_IN_MS"),
                "CNT_FRAMES_PER_AUDIO_CHUNK": cfg.get("CNT_FRAMES_PER_AUDIO_CHUNK"),
                "YN_RUN_FFT": cfg.get("YN_RUN_FFT", "N"),
                "DT_ADDED": datetime.now(),
                "WEBSOCKET_CONNECTION_ID": cfg.get("WEBSOCKET_CONNECTION_ID"),
            },
        )

# 4) WEBSOCKET CONNECTION (unchanged)
MAX_VARCHAR_SAFE = 4000  # conservative cap if target column isn't MAX

def _truncate_text(val: Optional[str], n: int = MAX_VARCHAR_SAFE) -> Optional[str]:
    if val is None:
        return None
    return val if len(val) <= n else val[: n - 1] + "…"

def DB_LOG_ENGINE_DB_WEBSOCKET_CONNECTION(WEBSOCKET_CONNECTION_ID: int) -> None:
    row = RECORDING_WEBSOCKET_CONNECTION_ARRAY[WEBSOCKET_CONNECTION_ID]

    # Headers -> JSON string (ASCII-safe), then cap to avoid NVARCHAR length errors
    client_headers = row.get("CLIENT_HEADERS")
    try:
        if isinstance(client_headers, (dict, list)):
            client_headers = _json.dumps(client_headers, ensure_ascii=False, separators=(",", ":"))
        elif client_headers is None:
            client_headers = "{}"
        else:
            client_headers = str(client_headers)
    except Exception as e:
        LOGGER.exception("WS_CONN: headers serialization failed: %s", e)
        client_headers = "{}"

    client_headers = _truncate_text(client_headers, MAX_VARCHAR_SAFE)

    try:
        with get_engine().begin() as CONN:
            CONN.execute(
                text("""
                    INSERT INTO ENGINE_DB_LOG_WEBSOCKET_CONNECTION (
                        WEBSOCKET_CONNECTION_ID,
                        CLIENT_HOST_IP_ADDRESS,
                        CLIENT_PORT,
                        CLIENT_HEADERS,
                        DT_CONNECTION_REQUEST,
                        DT_CONNECTION_ACCEPTED,
                        DT_CONNECTION_CLOSED
                    )
                    VALUES (
                        :WEBSOCKET_CONNECTION_ID,
                        :CLIENT_HOST_IP_ADDRESS,
                        :CLIENT_PORT,
                        :CLIENT_HEADERS,
                        :DT_CONNECTION_REQUEST,
                        :DT_CONNECTION_ACCEPTED,
                        :DT_CONNECTION_CLOSED
                    )
                """),
                {
                    "WEBSOCKET_CONNECTION_ID": WEBSOCKET_CONNECTION_ID,
                    "CLIENT_HOST_IP_ADDRESS": row.get("CLIENT_HOST_IP_ADDRESS"),
                    "CLIENT_PORT": row.get("CLIENT_PORT"),
                    "CLIENT_HEADERS": client_headers,
                    "DT_CONNECTION_REQUEST": row.get("DT_CONNECTION_REQUEST"),
                    "DT_CONNECTION_ACCEPTED": row.get("DT_CONNECTION_ACCEPTED"),
                    "DT_CONNECTION_CLOSED": row.get("DT_CONNECTION_CLOSED"),
                },
            )
    except Exception as e:
        LOGGER.exception("DB_LOG_WEBSOCKET_CONNECTION insert failed (id=%s): %s",
                         WEBSOCKET_CONNECTION_ID, e)

