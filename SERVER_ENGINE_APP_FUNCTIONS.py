# SERVER_ENGINE_APP_FUNCTIONS.py
from typing import Any, Dict, Iterable, List, Optional

import pyodbc  # type: ignore

from SERVER_ENGINE_APP_VARIABLES import (
    RECORDING_AUDIO_FRAME_ARRAY,
    RECORDING_AUDIO_CHUNK_ARRAY,
    RECORDING_CONFIG_ARRAY,
    RECORDING_WEBSOCKET_CONNECTION_ARRAY,
    RECORDING_WEBSOCKET_MESSAGE_ARRAY,
)

import functools
import inspect
import logging
import time
from pathlib import Path
from datetime import datetime
import threading
import asyncio
from sqlalchemy import create_engine, text


DB_ENGINE = create_engine(
    "mssql+pyodbc://violin:Test123!@104.40.11.248:3341/VIOLIN?driver=ODBC+Driver+17+for+SQL+Server",
    fast_executemany=True,   # enables pyodbc bulk optimization
    future=True,
    pool_pre_ping=True,      # keeps pooled conns fresh
    pool_recycle=1800,       # recycle every 30 min (optional)
)

LOGGER = logging.getLogger("app")

# ─────────────────────────────────────────────────────────────
# Fast path: lightweight per-event insert into ENGINE_DB_LOG_FUNCTIONS
# ─────────────────────────────────────────────────────────────
__LOG_CONN = None
__LOG_CURSOR = None
__LOG_LOCK = threading.Lock()

_LOG_INSERT_SQL = """
INSERT INTO ENGINE_DB_LOG_FUNCTIONS
(DT_ADDED, PYTHON_FUNCTION_NAME, PYTHON_FILE_NAME, RECORDING_ID, AUDIO_CHUNK_NO, FRAME_NO, START_STOP_OR_ERROR_MSG)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""

def _truncate(s: Optional[str], max_len: int) -> Optional[str]:
    if s is None:
        return None
    return s if len(s) <= max_len else s[: max_len - 1] + "…"

def _ensure_log_cursor():
    """
    Reuse a module-level connection/cursor for speed. Falls back to None on failure.
    Uses DB_CONNECT() defined later in this file (safe at call time).
    """
    global __LOG_CONN, __LOG_CURSOR
    with __LOG_LOCK:
        try:
            if __LOG_CONN is None or __LOG_CURSOR is None:
                __LOG_CONN = DB_CONNECT()
                __LOG_CURSOR = __LOG_CONN.cursor()
            else:
                # Ping the cursor/connection; if broken, recreate
                try:
                    __LOG_CURSOR.execute("SELECT 1")
                except Exception:
                    __LOG_CONN.close()
                    __LOG_CONN = DB_CONNECT()
                    __LOG_CURSOR = __LOG_CONN.cursor()
        except Exception as e:
            LOGGER.exception("DB_LOG_FUNCTIONS: failed to open/reopen SQL connection: %s", e)
            __LOG_CONN = None
            __LOG_CURSOR = None
    return __LOG_CURSOR

def _db_log_event(function_name: str,
                  file_name: str,
                  message: str,
                  recording_id=None,
                  audio_chunk_no=None,
                  frame_no=None) -> None:
    """
    Single-row insert into ENGINE_DB_LOG_FUNCTIONS. Uses local machine time.
    Never raises to caller; logs failures to LOGGER.
    """
    cur = _ensure_log_cursor()
    dt_added = datetime.now()  # local machine time (per requirements)
    fn_100 = _truncate(function_name, 100)
    file_100 = _truncate(file_name, 100)

    try:
        if cur is not None:
            cur.execute(
                _LOG_INSERT_SQL,
                dt_added,
                fn_100,
                file_100,
                recording_id,
                audio_chunk_no,
                frame_no,
                message,
            )
        else:
            LOGGER.warning(
                "DB_LOG_FUNCTIONS: no DB cursor; would insert: %s | %s | %s | rid=%s | chunk=%s | frame=%s",
                fn_100, file_100, message, recording_id, audio_chunk_no, frame_no
            )
    except Exception as e:
        LOGGER.exception("DB_LOG_FUNCTIONS: insert failed: %s", e)

# --- WS-safe logging decorator ---
try:
    from fastapi import WebSocket as _FastAPIWebSocket  # type: ignore
except Exception:  # if FastAPI isn't imported here yet
    _FastAPIWebSocket = None  # type: ignore

def DB_LOG_FUNCTIONS(level=logging.INFO, *, defer_ws_db_io: bool = True):
    """
    WS-safe logging decorator:
      • Logs Start/End/Error to Python logger immediately.
      • For WebSocket handlers, DB inserts are deferred & offloaded to a thread
        so the handshake can reach `await ws.accept()` without blocking.
      • Never serializes WebSocket objects. Never swallows exceptions.

    Args:
      level: Python log level for Start/End.
      defer_ws_db_io: If True (default), DB inserts for WS routes are scheduled
        with `asyncio.create_task(asyncio.to_thread(...))` to avoid blocking.
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
            if kind == "Error":
                LOGGER.exception("[%s | %s] %s", func_id, file_name, msg)
            else:
                try:
                    LOGGER.log(level, "[%s | %s] %s", func_id, file_name, msg, stacklevel=3)
                except TypeError:
                    LOGGER.log(level, "[%s | %s] %s", func_id, file_name, msg)

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

        # Synchronous DB log call
        def _db_insert(kind: str, kwargs: dict, msg: str):
            rid, chk, frm = _extract_ctx(kwargs)
            _db_log_event(function_name=func_id,
                          file_name=file_name,
                          message=msg,
                          recording_id=rid,
                          audio_chunk_no=chk,
                          frame_no=frm)

        # Async helper that defers DB I/O (for WS)
        async def _db_insert_async(kind: str, kwargs: dict, msg: str):
            await asyncio.to_thread(_db_insert, kind, kwargs, msg)

        def _is_ws_call(args, kwargs) -> bool:
            if _FastAPIWebSocket is None:
                return False
            for a in args:
                if isinstance(a, _FastAPIWebSocket):
                    return True
            for v in (kwargs or {}).values():
                if isinstance(v, _FastAPIWebSocket):
                    return True
            return False

        @functools.wraps(func)
        async def aw(*args, **kwargs):
            ws_mode = _is_ws_call(args, kwargs)
            t0 = time.perf_counter()

            start_msg = _compose_msg("Start")
            _log_python("Start", start_msg)
            try:
                if ws_mode and defer_ws_db_io:
                    asyncio.create_task(_db_insert_async("Start", kwargs, start_msg))
                else:
                    _db_insert("Start", kwargs, start_msg)

                result = await func(*args, **kwargs)

                end_msg = _compose_msg("End", elapsed=time.perf_counter() - t0)
                _log_python("End", end_msg)
                if ws_mode and defer_ws_db_io:
                    asyncio.create_task(_db_insert_async("End", kwargs, end_msg))
                else:
                    _db_insert("End", kwargs, end_msg)
                return result
            except Exception as e:
                err_msg = _compose_msg("Error", extra_msg=f"{e.__class__.__name__}: {e}")
                _log_python("Error", err_msg)
                if ws_mode and defer_ws_db_io:
                    asyncio.create_task(_db_insert_async("Error", kwargs, err_msg))
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
                    asyncio.create_task(_db_insert_async("Start", kwargs, start_msg))
                else:
                    _db_insert("Start", kwargs, start_msg)

                result = func(*args, **kwargs)

                end_msg = _compose_msg("End", elapsed=time.perf_counter() - t0)
                _log_python("End", end_msg)
                if ws_mode and defer_ws_db_io:
                    asyncio.create_task(_db_insert_async("End", kwargs, end_msg))
                else:
                    _db_insert("End", kwargs, end_msg)
                return result
            except Exception as e:
                err_msg = _compose_msg("Error", extra_msg=f"{e.__class__.__name__}: {e}")
                _log_python("Error", err_msg)
                if ws_mode and defer_ws_db_io:
                    asyncio.create_task(_db_insert_async("Error", kwargs, err_msg))
                else:
                    _db_insert("Error", kwargs, err_msg)
                raise

        return aw if is_coro else sw
    return decorate

# ─────────────────────────────────────────────────────────────
# Logging (ASCII-safe)
# ─────────────────────────────────────────────────────────────
def CONSOLE_LOG(prefix: str, msg: str, obj: Any = None) -> None:
    """Prints a single-line log safely even if there are odd characters."""
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

def DB_CONNECT():
    CONNECTION_STRING = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=104.40.11.248,3341;"
        "DATABASE=VIOLIN;"
        "UID=violin;"
        "PWD=Test123!"
    )
    return pyodbc.connect(CONNECTION_STRING, autocommit=True)


def DB_BULK_INSERT(conn, sql: str, rows: Iterable[tuple]) -> None:
    batch = list(rows)
    if not batch:
        return
    cur = conn.cursor()
    cur.fast_executemany = True
    cur.executemany(sql, batch)

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
    return total if total > 0 else None

# 1) FRAME
def DB_LOG_ENGINE_DB_AUDIO_FRAME(RECORDING_ID: int, FRAME_NO: int) -> None:
    with DB_ENGINE.begin() as CONN:
        CONN.execute(
            text("""
                INSERT INTO ENGINE_DB_LOG_AUDIO_FRAME
                (RECORDING_ID, FRAME_NO, AUDIO_STREAM_FRAME_SIZE_IN_MS, DT_FRAME_RECEIVED)
                VALUES (:RECORDING_ID, :FRAME_NO, :AUDIO_STREAM_FRAME_SIZE_IN_MS, :DT_FRAME_RECEIVED)
            """),
            {
                "RECORDING_ID": RECORDING_ID,
                "FRAME_NO": FRAME_NO,
                "AUDIO_STREAM_FRAME_SIZE_IN_MS": RECORDING_CONFIG_ARRAY[RECORDING_ID]["AUDIO_STREAM_FRAME_SIZE_IN_MS"],
                "DT_FRAME_RECEIVED": RECORDING_AUDIO_FRAME_ARRAY[RECORDING_ID][FRAME_NO]["DT_FRAME_RECEIVED"],
            },
        )

# 2) AUDIO CHUNK
def DB_LOG_ENGINE_DB_RECORDING_AUDIO_CHUNK(RECORDING_ID: int, AUDIO_CHUNK_NO: int) -> None:
    ch = RECORDING_AUDIO_CHUNK_ARRAY[RECORDING_ID][AUDIO_CHUNK_NO]
    with DB_ENGINE.begin() as CONN:
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
                "AUDIO_CHUNK_DURATION_IN_MS": ch["AUDIO_CHUNK_DURATION_IN_MS"],
                "START_MS": ch["START_MS"],
                "END_MS": ch["END_MS"],
                "MIN_AUDIO_STREAM_FRAME_NO": ch["MIN_AUDIO_STREAM_FRAME_NO"],
                "MAX_AUDIO_STREAM_FRAME_NO": ch["MAX_AUDIO_STREAM_FRAME_NO"],
                "YN_RUN_FFT": ch["YN_RUN_FFT"],
                "YN_RUN_ONS": ch["YN_RUN_ONS"],
                "YN_RUN_PYIN": ch["YN_RUN_PYIN"],
                "YN_RUN_CREPE": ch["YN_RUN_CREPE"],
                "DT_COMPLETE_FRAMES_RECEIVED": ch["DT_COMPLETE_FRAMES_RECEIVED"],
                "DT_START_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK": ch["DT_START_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK"],
                "DT_COMPLETE_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK": ch["DT_COMPLETE_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK"],
                "DT_AUDIO_CHUNK_CONVERTED_TO_WAV": ch["DT_AUDIO_CHUNK_CONVERTED_TO_WAV"],
                "DT_AUDIO_CHUNK_WAV_SAVED_TO_FILE": ch["DT_AUDIO_CHUNK_WAV_SAVED_TO_FILE"],
                "DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_16K": ch["DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_16K"],
                "DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_22050": ch["DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_22050"],
                "DT_AUDIO_CHUNK_PREPARATION_COMPLETE": ch["DT_AUDIO_CHUNK_PREPARATION_COMPLETE"],
                "DT_START_AUDIO_CHUNK_PROCESS": ch["DT_START_AUDIO_CHUNK_PROCESS"],
                "DT_END_AUDIO_CHUNK_PROCESS": ch["DT_END_AUDIO_CHUNK_PROCESS"],
                "DT_START_FFT": ch["DT_START_FFT"],
                "FFT_DURATION_IN_MS": ch["FFT_DURATION_IN_MS"],
                "FFT_RECORD_CNT": ch["FFT_RECORD_CNT"],
                "DT_START_ONS": ch["DT_START_ONS"],
                "ONS_DURATION_IN_MS": ch["ONS_DURATION_IN_MS"],
                "ONS_RECORD_CNT": ch["ONS_RECORD_CNT"],
                "DT_START_PYIN": ch["DT_START_PYIN"],
                "PYIN_DURATION_IN_MS": ch["PYIN_DURATION_IN_MS"],
                "PYIN_RECORD_CNT": ch["PYIN_RECORD_CNT"],
                "DT_START_CREPE": ch["DT_START_CREPE"],
                "CREPE_DURATION_IN_MS": ch["CREPE_DURATION_IN_MS"],
                "CREPE_RECORD_CNT": ch["CREPE_RECORD_CNT"],
                "DT_START_VOLUME": ch["DT_START_VOLUME"],
                "VOLUME_10_MS_DURATION_IN_MS": ch["VOLUME_10_MS_DURATION_IN_MS"],
                "VOLUME_1_MS_DURATION_IN_MS": ch["VOLUME_1_MS_DURATION_IN_MS"],
                "VOLUME_10_MS_RECORD_CNT": ch["VOLUME_10_MS_RECORD_CNT"],
                "VOLUME_1_MS_RECORD_CNT": ch["VOLUME_1_MS_RECORD_CNT"],
                "DT_START_P_ENGINE_ALL_MASTER": ch["DT_START_P_ENGINE_ALL_MASTER"],
                "P_ENGINE_ALL_MASTER_DURATION_IN_MS": ch["P_ENGINE_ALL_MASTER_DURATION_IN_MS"],
                "DT_ADDED": datetime.now()
            },
        )

# 3) RECORDING CONFIG
def DB_LOG_ENGINE_DB_RECORDING_CONFIG(RECORDING_ID: int) -> None:
    cfg = RECORDING_CONFIG_ARRAY[RECORDING_ID]
    with DB_ENGINE.begin() as CONN:
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
                "DT_RECORDING_START": cfg["DT_RECORDING_START"],
                "VIOLINIST_ID": cfg["VIOLINIST_ID"],
                "COMPOSE_PLAY_OR_PRACTICE": cfg["COMPOSE_PLAY_OR_PRACTICE"],
                "AUDIO_STREAM_FILE_NAME": cfg["AUDIO_STREAM_FILE_NAME"],
                "AUDIO_STREAM_FRAME_SIZE_IN_MS": cfg["AUDIO_STREAM_FRAME_SIZE_IN_MS"],
                "AUDIO_CHUNK_DURATION_IN_MS": cfg["AUDIO_CHUNK_DURATION_IN_MS"],
                "CNT_FRAMES_PER_AUDIO_CHUNK": cfg["CNT_FRAMES_PER_AUDIO_CHUNK"],
                "YN_RUN_FFT": cfg["YN_RUN_FFT"],
                "DT_ADDED": datetime.now(),
                "WEBSOCKET_CONNECTION_ID": cfg["WEBSOCKET_CONNECTION_ID"],
            },
        )

# 4) WEBSOCKET CONNECTION
def DB_LOG_ENGINE_DB_WEBSOCKET_CONNECTION(WEBSOCKET_CONNECTION_ID: int) -> None:
    row = RECORDING_WEBSOCKET_CONNECTION_ARRAY[WEBSOCKET_CONNECTION_ID]
    with DB_ENGINE.begin() as CONN:
        CONN.execute(
            text("""
                INSERT INTO ENGINE_DB_LOG_WEBSOCKET_CONNECTION (
                    WEBSOCKET_CONNECTION_ID, CLIENT_HOST_IP_ADDRESS, CLIENT_PORT, CLIENT_HEADERS,
                    DT_CONNECTION_REQUEST, DT_CONNECTION_ACCEPTED, DT_CONNECTION_CLOSED
                )
                VALUES (
                    :WEBSOCKET_CONNECTION_ID, :CLIENT_HOST_IP_ADDRESS, :CLIENT_PORT, :CLIENT_HEADERS,
                    :DT_CONNECTION_REQUEST, :DT_CONNECTION_ACCEPTED, :DT_CONNECTION_CLOSED
                )
            """),
            {
                "WEBSOCKET_CONNECTION_ID": WEBSOCKET_CONNECTION_ID,
                "CLIENT_HOST_IP_ADDRESS": row["CLIENT_HOST_IP_ADDRESS"],
                "CLIENT_PORT": row["CLIENT_PORT"],
                "CLIENT_HEADERS": row["CLIENT_HEADERS"],
                "DT_CONNECTION_REQUEST": row["DT_CONNECTION_REQUEST"],
                "DT_CONNECTION_ACCEPTED": row["DT_CONNECTION_ACCEPTED"],
                "DT_CONNECTION_CLOSED": row["DT_CONNECTION_CLOSED"],
            },
        )

# 5) WEBSOCKET MESSAGE (by in-memory message id)
def DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE(MESSAGE_ID: int) -> None:
    msg = RECORDING_WEBSOCKET_MESSAGE_ARRAY[MESSAGE_ID]
    with DB_ENGINE.begin() as CONN:
        CONN.execute(
            text("""
                INSERT INTO ENGINE_DB_LOG_WEBSOCKET_MESSAGE (
                    RECORDING_ID, MESSAGE_TYPE, AUDIO_FRAME_NO, DT_MESSAGE_RECEIVED, DT_MESSAGE_PROCESS_STARTED
                )
                VALUES (
                    :RECORDING_ID, :MESSAGE_TYPE, :AUDIO_FRAME_NO, :DT_MESSAGE_RECEIVED, :DT_MESSAGE_PROCESS_STARTED
                )
            """),
            {
                "RECORDING_ID": msg["RECORDING_ID"],
                "MESSAGE_TYPE": msg["MESSAGE_TYPE"],
                "AUDIO_FRAME_NO": msg["AUDIO_FRAME_NO"],
                "DT_MESSAGE_RECEIVED": msg["DT_MESSAGE_RECEIVED"],
                "DT_MESSAGE_PROCESS_STARTED": msg["DT_MESSAGE_PROCESS_STARTED"],
            },
        )