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
    DB_CONNECT,
    DB_EXEC_SP_SINGLE_ROW,
    DB_EXEC_SP_MULTIPLE_ROWS,
    DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE,
    DB_LOG_ENGINE_DB_RECORDING_CONFIG,
    CONSOLE_LOG,
)

@DB_LOG_FUNCTIONS()
async def SERVER_ENGINE_LISTEN_3A_FOR_START() -> None:
    """
    Step 1) Scan RECORDING_WEBSOCKET_MESSAGE_ARRAY where DT_MESSAGE_PROCESS_STARTED is null and MESSAGE_TYPE='START'
            and kick off PROCESS_WEBSOCKET_MESSAGE_TYPE_START for each.
    """
    to_launch = []
    for mid, msg in list(RECORDING_WEBSOCKET_MESSAGE_ARRAY.items()):
        if msg.get("DT_MESSAGE_PROCESS_STARTED") is None and str(msg.get("MESSAGE_TYPE", "")).upper() == "START":
            to_launch.append(mid)

    for mid in to_launch:
        asyncio.create_task(PROCESS_WEBSOCKET_MESSAGE_TYPE_START(MESSAGE_ID=mid))

@DB_LOG_FUNCTIONS()
async def PROCESS_WEBSOCKET_MESSAGE_TYPE_START(MESSAGE_ID: int) -> None:
    """
    Step 1) Update DT_MESSAGE_PROCESS_STARTED
    Step 2) Call DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE
    Step 3) Insert into RECORDING_CONFIG_ARRAY: RECORDING_ID, WEBSOCKET_CONNECTION_ID(optional), DT_RECORDING_START, COMPOSE_CURRENT_AUDIO_CHUNK_NO = 1
    Step 4) Call P_ENGINE_ALL_RECORDING_PARAMETERS_GET -> update VIOLINIST_ID, COMPOSE_PLAY_OR_PRACTICE, AUDIO_STREAM_FILE_NAME, AUDIO_STREAM_FRAME_SIZE_IN_MS
    Step 5) If ... = "COMPOSE":
        a) P_ENGINE_SONG_AUDIO_CHUNK_FOR_COMPOSE_GET -> update AUDIO_CHUNK_DURATION_IN_MS, CNT_FRAMES_PER_AUDIO_CHUNK, YN_RUN_FFT
    Step 6) If ... in ("PLAY","PRACTICE"):
        a) P_ENGINE_SONG_AUDIO_CHUNK_FOR_PLAY_AND_PRACTICE_GET -> seed RECORDING_AUDIO_CHUNK_ARRAY rows
    """
    msg = RECORDING_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID)
    if not msg:
        return

    msg["DT_MESSAGE_PROCESS_STARTED"] = datetime.now()
    DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE(MESSAGE_ID)

    RECORDING_ID = int(msg["RECORDING_ID"])
    # Step 3: seed config
    cfg = RECORDING_CONFIG_ARRAY.get(RECORDING_ID, {"RECORDING_ID": RECORDING_ID})
    cfg.setdefault("DT_RECORDING_START", datetime.now())
    cfg.setdefault("COMPOSE_CURRENT_AUDIO_CHUNK_NO", 1)
    # WEBSOCKET_CONNECTION_ID can be set by caller elsewhere if known
    RECORDING_CONFIG_ARRAY[RECORDING_ID] = cfg

    # Step 4: load base parameters
    with DB_CONNECT() as conn:
        row = DB_EXEC_SP_SINGLE_ROW(conn, "P_ENGINE_ALL_RECORDING_PARAMETERS_GET", RECORDING_ID=RECORDING_ID) or {}
    for k in ("VIOLINIST_ID", "COMPOSE_PLAY_OR_PRACTICE", "AUDIO_STREAM_FILE_NAME", "AUDIO_STREAM_FRAME_SIZE_IN_MS"):
        if k in row:
            cfg[k] = row[k]
    RECORDING_CONFIG_ARRAY[RECORDING_ID] = cfg

    # Step 5: compose seeding
    mode = str(cfg.get("COMPOSE_PLAY_OR_PRACTICE") or "").upper()
    if mode == "COMPOSE":
        with DB_CONNECT() as conn:
            r = DB_EXEC_SP_SINGLE_ROW(conn, "P_ENGINE_SONG_AUDIO_CHUNK_FOR_COMPOSE_GET", RECORDING_ID=RECORDING_ID) or {}
        for k in ("AUDIO_CHUNK_DURATION_IN_MS", "CNT_FRAMES_PER_AUDIO_CHUNK", "YN_RUN_FFT"):
            if k in r:
                cfg[k] = r[k]
        RECORDING_CONFIG_ARRAY[RECORDING_ID] = cfg

    # Step 6: play/practice chunk map seeding
    elif mode in ("PLAY", "PRACTICE"):
        with DB_CONNECT() as conn:
            rows = DB_EXEC_SP_MULTIPLE_ROWS(conn, "P_ENGINE_SONG_AUDIO_CHUNK_FOR_PLAY_AND_PRACTICE_GET", RECORDING_ID=RECORDING_ID) or []
        if rows:
            RECORDING_AUDIO_CHUNK_ARRAY.setdefault(RECORDING_ID, {})
            for rr in rows:
                chno = int(rr["AUDIO_CHUNK_NO"])
                RECORDING_AUDIO_CHUNK_ARRAY[RECORDING_ID][chno] = {
                    "RECORDING_ID": RECORDING_ID,
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

    # Optional: log initial config snapshot
    try:
        DB_LOG_ENGINE_DB_RECORDING_CONFIG(RECORDING_ID)
    except Exception:
        pass
