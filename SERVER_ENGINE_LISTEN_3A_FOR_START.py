# SERVER_ENGINE_LISTEN_3A_FOR_START.py
from __future__ import annotations
from datetime import datetime
import asyncio

from SERVER_ENGINE_APP_VARIABLES import (
    ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY,
    ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY,
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY,  # metadata-only (no bytes)
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    DB_LOG_FUNCTIONS,
    DB_CONNECT_CTX,
    DB_EXEC_SP_SINGLE_ROW,
    DB_EXEC_SP_MULTIPLE_ROWS,
    DB_INSERT_TABLE,
    CONSOLE_LOG,
)

def SERVER_ENGINE_LISTEN_3A_FOR_START() -> None:
    """
    Scan for unprocessed START messages and queue async processing.
    """
    to_launch = []
    for mid, msg in list(ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.items()):
        if msg.get("DT_MESSAGE_PROCESS_STARTED") is None and str(msg.get("MESSAGE_TYPE", "")).upper() == "START":
            to_launch.append(mid)

    for mid in to_launch:
        asyncio.create_task(PROCESS_WEBSOCKET_MESSAGE_TYPE_START(mid))


@DB_LOG_FUNCTIONS()
async def PROCESS_WEBSOCKET_MESSAGE_TYPE_START(MESSAGE_ID: int) -> None:
    """
    PROCESS START:
      1) Mark DT_MESSAGE_PROCESS_STARTED
      2) Persist message (best-effort)
      3) Seed ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY:
           - RECORDING_ID
           - optional WEBSOCKET_CONNECTION_ID
           - DT_RECORDING_START (now)
           - COMPOSE_CURRENT_AUDIO_CHUNK_NO = 1
      4) Load base params via P_ENGINE_ALL_RECORDING_PARAMETERS_GET
      5) If PLAY/PRACTICE: P_ENGINE_SONG_AUDIO_FRAME_FOR_PLAY_AND_PRACTICE_GET → seed ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY (metadata only)
      6) Persist recording config (best-effort)
    """
    msg = ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID)
    if not msg:
        return

    # 1) mark started
    msg["DT_MESSAGE_PROCESS_STARTED"] = datetime.now()

    # 2) persist message (allowlisted insert; safe no-op if unchanged columns)
    try:
        DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_MESSAGE", msg, fire_and_forget=True)
    except Exception as e:
        CONSOLE_LOG("DB_INSERT_MESSAGE", "schedule_failed", {"mid": MESSAGE_ID, "err": str(e)})

    rid = int(msg.get("RECORDING_ID") or 0)

    # 3) seed config
    cfg = ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY.get(rid, {"RECORDING_ID": rid})
    if "WEBSOCKET_CONNECTION_ID" in msg:
        cfg["WEBSOCKET_CONNECTION_ID"] = msg["WEBSOCKET_CONNECTION_ID"]
    cfg.setdefault("DT_RECORDING_START", datetime.now())
    ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[rid] = cfg

    # 4) load base parameters
    with DB_CONNECT_CTX() as conn:
        row = DB_EXEC_SP_SINGLE_ROW(conn, "P_ENGINE_ALL_RECORDING_PARAMETERS_GET", RECORDING_ID=rid) or {}
    for k in ("COMPOSE_PLAY_OR_PRACTICE", "AUDIO_STREAM_FILE_NAME", "COMPOSE_YN_RUN_FFT"):
        if k in row:
            cfg[k] = row[k]
    ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[rid] = cfg

    mode = str(cfg.get("COMPOSE_PLAY_OR_PRACTICE") or "").upper()

    # 5) play/practice: pre-seed per-frame metadata (no bytes)
    if mode in ("PLAY", "PRACTICE"):
        with DB_CONNECT_CTX() as conn:
            rows = DB_EXEC_SP_MULTIPLE_ROWS(conn, "P_ENGINE_SONG_AUDIO_FRAME_FOR_PLAY_AND_PRACTICE_GET", RECORDING_ID=rid) or []
        if rows:
            rec_map = ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY.setdefault(rid, {})
            for rr in rows:
                try:
                    frame_no = int(rr["AUDIO_FRAME_NO"])
                except Exception:
                    continue
                rec_map[frame_no] = {
                    "RECORDING_ID": rid,
                    "AUDIO_FRAME_NO": frame_no,
                    "START_MS": rr.get("START_MS"),
                    "END_MS": rr.get("END_MS"),
                    "YN_RUN_FFT": rr.get("YN_RUN_FFT"),
                    "YN_RUN_ONS": rr.get("YN_RUN_ONS"),
                    "YN_RUN_PYIN": rr.get("YN_RUN_PYIN"),
                    "YN_RUN_CREPE": rr.get("YN_RUN_CREPE"),
                    # timestamps/size/hash/encoding are filled later when actual bytes arrive
                }

    # 6) persist recording config
    try:
        DB_INSERT_TABLE("ENGINE_DB_LOG_RECORDING_CONFIG", cfg, fire_and_forget=True)
    except Exception as e:
        CONSOLE_LOG("DB_INSERT_RECORDING_CONFIG", "schedule_failed", {"rid": rid, "err": str(e)})
