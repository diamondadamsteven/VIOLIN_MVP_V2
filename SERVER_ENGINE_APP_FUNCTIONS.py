# SERVER_ENGINE_APP_FUNCTIONS.py
from typing import Any, Dict, Iterable, List, Optional

import pyodbc  # type: ignore

from SERVER_ENGINE_APP_VARIABLES import (
    RECORDING_CONFIG_ARRAY,
    RECORDING_AUDIO_CHUNK_ARRAY,
    RECORDING_AUDIO_FRAME_ARRAY,
)

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
    """
    Fast executemany bulk insert. No-op for empty iterable.
    """
    batch = list(rows)
    if not batch:
        return
    cur = conn.cursor()
    cur.fast_executemany = True
    cur.executemany(sql, batch)

def _exec_sp_and_position_cursor(cur, sp_name: str, **params) -> bool:
    """
    Execute the stored procedure and advance the cursor to the first
    actual result set (skipping update counts when NOCOUNT is OFF).
    Returns True if a result set is positioned; False otherwise.
    """
    keys = list(params.keys())
    args = [params[k] for k in keys]
    placeholders = ",".join([f"@{k}=?" for k in keys])
    sql = f"EXEC {sp_name} {placeholders}" if placeholders else f"EXEC {sp_name}"
    cur.execute(sql, args)

    # Move to the first result set (description not None)
    while True:
        if cur.description is not None:
            return True
        if not cur.nextset():
            return False

def DB_EXEC_SP_MULTIPLE_ROWS(conn, sp_name: str, **params) -> List[Dict[str, Any]]:
    """
    Execute a stored procedure and return all rows from the FIRST result set.
    Returns [] if no result set was produced.
    """
    cur = conn.cursor()
    if not _exec_sp_and_position_cursor(cur, sp_name, **params):
        return []
    cols = [c[0] for c in cur.description]  # type: ignore[union-attr]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def DB_EXEC_SP_SINGLE_ROW(conn, sp_name: str, **params) -> Dict[str, Any]:
    """
    Execute a stored procedure and return the first row of the FIRST result set.
    Returns {} if no result set or the result set is empty.
    """
    cur = conn.cursor()
    if not _exec_sp_and_position_cursor(cur, sp_name, **params):
        return {}
    cols = [c[0] for c in cur.description]  # type: ignore[union-attr]
    row = cur.fetchone()
    return dict(zip(cols, row)) if row else {}

def DB_EXEC_SP_NO_RESULT(conn, sp_name: str, **params) -> Optional[int]:
    """
    Execute a stored procedure that is not expected to return a result set.
    Returns a best-effort rows-affected total (None if unknown/0).
    """
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

# ─────────────────────────────────────────────────────────────
# Public logging helpers (exact signatures requested)
# ─────────────────────────────────────────────────────────────
def DB_LOG_RECORDING_CONFIG(RECORDING_ID: int) -> Optional[int]:
    """
    Calls P_ENGINE_DB_LOG_RECORDING_CONFIG_INS using values
    from RECORDING_CONFIG_ARRAY[RECORDING_ID].
    """
    cfg = RECORDING_CONFIG_ARRAY.get(RECORDING_ID)
    if not cfg:
        CONSOLE_LOG("DB_LOG_RECORDING_CONFIG", "no config in memory; skipping", {"RECORDING_ID": RECORDING_ID})
        return None

    def _nz(v):
        # Convert empty-string-ish to None for nullable SP params
        if isinstance(v, str) and v.strip() == "":
            return None
        return v

    params: Dict[str, Any] = {
        "RECORDING_ID": int(RECORDING_ID),
        "DT_RECORDING_START": cfg.get("DT_RECORDING_START"),
        "VIOLINIST_ID": cfg.get("VIOLINIST_ID"),
        "COMPOSE_PLAY_OR_PRACTICE": _nz(cfg.get("COMPOSE_PLAY_OR_PRACTICE")),
        "AUDIO_STREAM_FILE_NAME": _nz(cfg.get("AUDIO_STREAM_FILE_NAME")),
        "AUDIO_STREAM_FRAME_SIZE_IN_MS": cfg.get("AUDIO_STREAM_FRAME_SIZE_IN_MS"),
        "AUDIO_CHUNK_DURATION_IN_MS": cfg.get("AUDIO_CHUNK_DURATION_IN_MS"),
        "CNT_FRAMES_PER_AUDIO_CHUNK": cfg.get("CNT_FRAMES_PER_AUDIO_CHUNK"),
        "YN_RUN_FFT": _nz(cfg.get("YN_RUN_FFT")),
    }

    try:
        with DB_CONNECT() as conn:
            return DB_EXEC_SP_NO_RESULT(conn, "P_ENGINE_DB_LOG_RECORDING_CONFIG_INS", **params)
    except Exception as e:
        CONSOLE_LOG("DB_LOG_RECORDING_CONFIG", "error", {"RECORDING_ID": RECORDING_ID, "err": str(e)})
        return None

def DB_LOG_RECORDING_AUDIO_CHUNK(RECORDING_ID: int, AUDIO_CHUNK_NO: int) -> Optional[int]:
    """
    Calls P_ENGINE_DB_LOG_RECORDING_AUDIO_CHUNK_INS using values
    from RECORDING_AUDIO_CHUNK_ARRAY[RECORDING_ID][AUDIO_CHUNK_NO].
    """
    chunks = RECORDING_AUDIO_CHUNK_ARRAY.get(RECORDING_ID, {})
    ch = chunks.get(AUDIO_CHUNK_NO)
    if not ch:
        CONSOLE_LOG("DB_LOG_RECORDING_AUDIO_CHUNK", "chunk not found; skipping", {
            "RECORDING_ID": RECORDING_ID, "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO
        })
        return None

    # Derive CNT_AUDIO_FRAMES if possible
    min_no = ch.get("MIN_AUDIO_STREAM_FRAME_NO")
    max_no = ch.get("MAX_AUDIO_STREAM_FRAME_NO")
    cnt_frames = (max_no - min_no + 1) if isinstance(min_no, int) and isinstance(max_no, int) and max_no >= min_no else None

    def _nz(v):
        if isinstance(v, str) and v.strip() == "":
            return None
        return v

    params: Dict[str, Any] = {
        "RECORDING_ID": int(RECORDING_ID),
        "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),

        "AUDIO_CHUNK_DURATION_IN_MS": ch.get("AUDIO_CHUNK_DURATION_IN_MS"),
        "CNT_AUDIO_FRAMES": cnt_frames,
        "TOTAL_PROCESSING_DURATION_IN_MS": ch.get("TOTAL_PROCESSING_DURATION_IN_MS"),

        "YN_RUN_FFT": _nz(ch.get("YN_RUN_FFT")),
        "YN_RUN_ONS": _nz(ch.get("YN_RUN_ONS")),
        "YN_RUN_CREPE": _nz(ch.get("YN_RUN_CREPE")),
        "YN_RUN_PYIN": _nz(ch.get("YN_RUN_PYIN")),

        "DT_COMPLETE_FRAMES_RECEIVED": ch.get("DT_COMPLETE_FRAMES_RECEIVED"),

        "DT_START_FFT": ch.get("DT_START_FFT"),
        "FFT_DURATION_IN_MS": ch.get("FFT_DURATION_IN_MS"),
        "FFT_RECORD_CNT": ch.get("FFT_RECORD_CNT"),

        "DT_START_ONS": ch.get("DT_START_ONS"),
        "ONS_DURATION_IN_MS": ch.get("ONS_DURATION_IN_MS"),
        "ONS_RECORD_CNT": ch.get("ONS_RECORD_CNT"),

        "DT_START_PYIN": ch.get("DT_START_PYIN"),
        "PYIN_DURATION_IN_MS": ch.get("PYIN_DURATION_IN_MS"),
        "PYIN_RECORD_CNT": ch.get("PYIN_RECORD_CNT"),

        "DT_START_CREPE": ch.get("DT_START_CREPE"),
        "CREPE_DURATION_IN_MS": ch.get("CREPE_DURATION_IN_MS"),
        "CREPE_RECORD_CNT": ch.get("CREPE_RECORD_CNT"),

        "DT_START_VOLUME": ch.get("DT_START_VOLUME"),
        "VOLUME_10_MS_DURATION_IN_MS": ch.get("VOLUME_10_MS_DURATION_IN_MS"),
        "VOLUME_1_MS_DURATION_IN_MS": ch.get("VOLUME_1_MS_DURATION_IN_MS"),
        "VOLUME_10_MS_RECORD_CNT": ch.get("VOLUME_10_MS_RECORD_CNT"),
        "VOLUME_1_MS_RECORD_CNT": ch.get("VOLUME_1_MS_RECORD_CNT"),

        "DT_START_P_ENGINE_ALL_MASTER": ch.get("DT_START_P_ENGINE_ALL_MASTER"),
        "P_ENGINE_ALL_MASTER_DURATION_IN_MS": ch.get("P_ENGINE_ALL_MASTER_DURATION_IN_MS"),
    }

    try:
        with DB_CONNECT() as conn:
            return DB_EXEC_SP_NO_RESULT(conn, "P_ENGINE_DB_LOG_RECORDING_AUDIO_CHUNK_INS", **params)
    except Exception as e:
        CONSOLE_LOG("DB_LOG_RECORDING_AUDIO_CHUNK", "error", {
            "RECORDING_ID": RECORDING_ID, "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO, "err": str(e)
        })
        return None

def DB_LOG_ENGINE_DB_AUDIO_FRAME_TRANSFER(RECORDING_ID: int, FRAME_NO: int) -> Optional[int]:
    """
    Calls P_ENGINE_DB_LOG_AUDIO_FRAME_TRANSFER_INS using values
    from RECORDING_AUDIO_FRAME_ARRAY[RECORDING_ID][FRAME_NO].
    """
    frames = RECORDING_AUDIO_FRAME_ARRAY.get(RECORDING_ID, {})
    fr = frames.get(FRAME_NO)
    if not fr:
        CONSOLE_LOG("DB_LOG_AUDIO_FRAME_TRANSFER", "frame not found; skipping", {
            "RECORDING_ID": RECORDING_ID, "FRAME_NO": FRAME_NO
        })
        return None

    params: Dict[str, Any] = {
        "RECORDING_ID": int(RECORDING_ID),
        "FRAME_NO": int(FRAME_NO),
        "DT_FRAME_RECEIVED": fr.get("DT_FRAME_RECEIVED"),
        "DT_FRAME_CONCATENATED_TO_AUDIO_CHUNK": fr.get("DT_FRAME_CONCATENATED_TO_AUDIO_CHUNK"),
        "AUDIO_CHUNK_NO": fr.get("AUDIO_CHUNK_NO"),
        "DT_FRAME_REMOVED_FROM_MEMORY": None,  # add to array later if you want to track it
    }

    try:
        with DB_CONNECT() as conn:
            return DB_EXEC_SP_NO_RESULT(conn, "P_ENGINE_DB_LOG_AUDIO_FRAME_TRANSFER_INS", **params)
    except Exception as e:
        CONSOLE_LOG("DB_LOG_AUDIO_FRAME_TRANSFER", "error", {
            "RECORDING_ID": RECORDING_ID, "FRAME_NO": FRAME_NO, "err": str(e)
        })
        return None
