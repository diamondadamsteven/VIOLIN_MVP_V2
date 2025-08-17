# SERVER_ENGINE_LISTEN_3A_FOR_START.py
from __future__ import annotations
from datetime import datetime
import asyncio

from SERVER_ENGINE_APP_VARIABLES import (
    RECORDING_WEBSOCKET_MESSAGE_ARRAY,
    RECORDING_CONFIG_ARRAY,
    RECORDING_AUDIO_CHUNK_ARRAY,
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    DB_LOG_FUNCTIONS,
    DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE,
    DB_CONNECT,
    DB_EXEC_SP_SINGLE_ROW,
    DB_EXEC_SP_MULTIPLE_ROWS,
    DB_LOG_ENGINE_DB_RECORDING_CONFIG
)

def SERVER_ENGINE_LISTEN_3A_FOR_START() -> None:
    """
    Scan for unprocessed START messages and queue async processing.
    """
    to_launch = []
    for mid, msg in list(RECORDING_WEBSOCKET_MESSAGE_ARRAY.items()):
        if msg.get("DT_MESSAGE_PROCESS_STARTED") is None and str(msg.get("MESSAGE_TYPE", "")).upper() == "START":
            to_launch.append(mid)

    for mid in to_launch:
        asyncio.create_task(PROCESS_WEBSOCKET_MESSAGE_TYPE_START(mid))


@DB_LOG_FUNCTIONS()
async def PROCESS_WEBSOCKET_MESSAGE_TYPE_START(MESSAGE_ID: int) -> None:
    """
    PROCESS START:
      1) Mark DT_MESSAGE_PROCESS_STARTED
      2) Log message
      3) Seed RECORDING_CONFIG_ARRAY (RECORDING_ID, CONNECTION_ID?, DT_RECORDING_START, COMPOSE_CURRENT_AUDIO_CHUNK_NO=1)
      4) Load base params via P_ENGINE_ALL_RECORDING_PARAMETERS_GET
      5) If COMPOSE: P_ENGINE_SONG_AUDIO_CHUNK_FOR_COMPOSE_GET → update cfg
      6) If PLAY/PRACTICE: P_ENGINE_SONG_AUDIO_CHUNK_FOR_PLAY_AND_PRACTICE_GET → seed RECORDING_AUDIO_CHUNK_ARRAY
    """
    msg = RECORDING_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID)
    if not msg:
        return

    # 1) mark started
    msg["DT_MESSAGE_PROCESS_STARTED"] = datetime.now()

    # 2) db log receipt/start
    DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE(MESSAGE_ID)

    rid = int(msg.get("RECORDING_ID") or 0)

    # 3) seed config
    cfg = RECORDING_CONFIG_ARRAY.get(rid, {"RECORDING_ID": rid})
    # Connection id is optional — set by upstream if known
    if "WEBSOCKET_CONNECTION_ID" in msg:
        cfg["WEBSOCKET_CONNECTION_ID"] = msg["WEBSOCKET_CONNECTION_ID"]
    cfg.setdefault("DT_RECORDING_START", datetime.now())
    cfg["COMPOSE_CURRENT_AUDIO_CHUNK_NO"] = 1
    RECORDING_CONFIG_ARRAY[rid] = cfg

    # 4) load base parameters
    with DB_CONNECT() as conn:
        row = DB_EXEC_SP_SINGLE_ROW(conn, "P_ENGINE_ALL_RECORDING_PARAMETERS_GET", RECORDING_ID=rid) or {}
    for k in ("VIOLINIST_ID", "COMPOSE_PLAY_OR_PRACTICE", "AUDIO_STREAM_FILE_NAME", "AUDIO_STREAM_FRAME_SIZE_IN_MS"):
        if k in row:
            cfg[k] = row[k]
    RECORDING_CONFIG_ARRAY[rid] = cfg

    mode = str(cfg.get("COMPOSE_PLAY_OR_PRACTICE") or "").upper()

    # 5) compose configuration
    if mode == "COMPOSE":
        with DB_CONNECT() as conn:
            r = DB_EXEC_SP_SINGLE_ROW(conn, "P_ENGINE_SONG_AUDIO_CHUNK_FOR_COMPOSE_GET", RECORDING_ID=rid) or {}
        for k in ("AUDIO_CHUNK_DURATION_IN_MS", "CNT_FRAMES_PER_AUDIO_CHUNK", "YN_RUN_FFT"):
            if k in r:
                cfg[k] = r[k]
        RECORDING_CONFIG_ARRAY[rid] = cfg

    # 6) play/practice chunk plan
    elif mode in ("PLAY", "PRACTICE"):
        with DB_CONNECT() as conn:
            rows = DB_EXEC_SP_MULTIPLE_ROWS(conn, "P_ENGINE_SONG_AUDIO_CHUNK_FOR_PLAY_AND_PRACTICE_GET", RECORDING_ID=rid) or []
        if rows:
            RECORDING_AUDIO_CHUNK_ARRAY.setdefault(rid, {})
            for rr in rows:
                chno = int(rr["AUDIO_CHUNK_NO"])
                RECORDING_AUDIO_CHUNK_ARRAY[rid][chno] = {
                    "RECORDING_ID": rid,
                    "AUDIO_CHUNK_NO": chno,
                    "AUDIO_CHUNK_DURATION_IN_MS": rr["AUDIO_CHUNK_DURATION_IN_MS"],
                    "START_MS": rr["START_MS"],
                    "END_MS": rr["END_MS"],
                    "MIN_AUDIO_STREAM_FRAME_NO": rr["MIN_AUDIO_STREAM_FRAME_NO"],
                    "MAX_AUDIO_STREAM_FRAME_NO": rr["MAX_AUDIO_STREAM_FRAME_NO"],
                    "YN_RUN_FFT": rr.get("YN_RUN_FFT"),
                    "YN_RUN_ONS": rr.get("YN_RUN_ONS"),
                    "YN_RUN_PYIN": rr.get("YN_RUN_PYIN"),
                    "YN_RUN_CREPE": rr.get("YN_RUN_CREPE"),
                }
    DB_LOG_ENGINE_DB_RECORDING_CONFIG (RECORDING_ID=rid)