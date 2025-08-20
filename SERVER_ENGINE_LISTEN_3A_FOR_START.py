# SERVER_ENGINE_LISTEN_3A_FOR_START.py
from __future__ import annotations
from datetime import datetime

from SERVER_ENGINE_APP_VARIABLES import (
    ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY,
    ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY,
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY,  # metadata-only (no bytes)
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    ENGINE_DB_LOG_FUNCTIONS_INS,
    DB_CONNECT_CTX,
    DB_EXEC_SP_SINGLE_ROW,
    DB_EXEC_SP_MULTIPLE_ROWS,
    DB_INSERT_TABLE,
    schedule_coro,  # loop/thread-safe scheduler
)


def SERVER_ENGINE_LISTEN_3A_FOR_START() -> None:
    """
    Scan for unprocessed START messages and queue async processing.
    Marks DT_MESSAGE_PROCESS_QUEDED_TO_START to avoid double-queueing.
    """
    to_launch = []
    for MESSAGE_ID, ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD in list(ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.items()):
        if ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD.get("DT_MESSAGE_PROCESS_QUEDED_TO_START") is None and str(ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD.get("MESSAGE_TYPE", "")).upper() == "START":
            to_launch.append(MESSAGE_ID)

    for MESSAGE_ID in to_launch:
        ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD = ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID)
        if ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD is None:
            continue
        ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD["DT_MESSAGE_PROCESS_QUEDED_TO_START"] = datetime.now()
        schedule_coro(PROCESS_WEBSOCKET_MESSAGE_TYPE_START(MESSAGE_ID))


@ENGINE_DB_LOG_FUNCTIONS_INS()
async def PROCESS_WEBSOCKET_MESSAGE_TYPE_START(MESSAGE_ID: int) -> None:
    """
    PROCESS START:
      1) Mark DT_MESSAGE_PROCESS_STARTED
      2) Persist the message row
      3) Seed ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY:
           - RECORDING_ID
           - optional WEBSOCKET_CONNECTION_ID
           - DT_RECORDING_START (now if not present)
           - COMPOSE_CURRENT_AUDIO_CHUNK_NO = 1 (default)
      4) Load base params via P_ENGINE_ALL_RECORDING_PARAMETERS_GET
      5) If PLAY/PRACTICE:
           P_ENGINE_SONG_AUDIO_FRAME_FOR_PLAY_AND_PRACTICE_GET → seed
           ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY (metadata only; no bytes)
      6) Persist recording config
    """
    ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD = ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID)
    if ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD is None:
        return

    # 1) mark started
    ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD["DT_MESSAGE_PROCESS_STARTED"] = datetime.now()

    # 2) persist message (allowlisted insert)
    DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_MESSAGE", ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD, fire_and_forget=True)

    # 3) seed config (in-memory)
    RECORDING_ID = int(ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD.get("RECORDING_ID") or 0)
    ENGINE_DB_LOG_RECORDING_CONFIG_RECORD = ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY.get(RECORDING_ID, {"RECORDING_ID": RECORDING_ID})

    if "WEBSOCKET_CONNECTION_ID" in ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD:
        ENGINE_DB_LOG_RECORDING_CONFIG_RECORD["WEBSOCKET_CONNECTION_ID"] = ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD["WEBSOCKET_CONNECTION_ID"]

    ENGINE_DB_LOG_RECORDING_CONFIG_RECORD.setdefault("DT_RECORDING_START", datetime.now())
    ENGINE_DB_LOG_RECORDING_CONFIG_RECORD.setdefault("COMPOSE_CURRENT_AUDIO_CHUNK_NO", 1)
    ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[RECORDING_ID] = ENGINE_DB_LOG_RECORDING_CONFIG_RECORD

    # 4) load base parameters
    with DB_CONNECT_CTX() as CONN:
        ROW = DB_EXEC_SP_SINGLE_ROW(CONN, "P_ENGINE_ALL_RECORDING_PARAMETERS_GET", RECORDING_ID=RECORDING_ID) or {}

    # Copy selected keys (extend as needed)
    for K in ("COMPOSE_PLAY_OR_PRACTICE", "AUDIO_STREAM_FILE_NAME", "COMPOSE_YN_RUN_FFT"):
        if K in ROW:
            ENGINE_DB_LOG_RECORDING_CONFIG_RECORD[K] = ROW[K]
    ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[RECORDING_ID] = ENGINE_DB_LOG_RECORDING_CONFIG_RECORD

    COMPOSE_PLAY_OR_PRACTICE = str(ENGINE_DB_LOG_RECORDING_CONFIG_RECORD.get("COMPOSE_PLAY_OR_PRACTICE") or "").upper()

    # 5) play/practice: pre-seed per-frame metadata (no bytes)
    if COMPOSE_PLAY_OR_PRACTICE in ("PLAY", "PRACTICE"):
        with DB_CONNECT_CTX() as CONN:
            ROWS = DB_EXEC_SP_MULTIPLE_ROWS(CONN, "P_ENGINE_SONG_AUDIO_FRAME_FOR_PLAY_AND_PRACTICE_GET", RECORDING_ID=RECORDING_ID) or []

        if ROWS:
            FRAMES_BY_NO = ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY.setdefault(RECORDING_ID, {})
            for RR in ROWS:
                AUDIO_FRAME_NO = int(RR.get("AUDIO_FRAME_NO") or 0)
                if AUDIO_FRAME_NO <= 0:
                    continue
                FRAMES_BY_NO[AUDIO_FRAME_NO] = {
                    "RECORDING_ID": RECORDING_ID,
                    "AUDIO_FRAME_NO": AUDIO_FRAME_NO,
                    "START_MS": RR.get("START_MS"),
                    "END_MS": RR.get("END_MS"),
                    "YN_RUN_FFT": RR.get("YN_RUN_FFT"),
                    "YN_RUN_ONS": RR.get("YN_RUN_ONS"),
                    "YN_RUN_PYIN": RR.get("YN_RUN_PYIN"),
                    "YN_RUN_CREPE": RR.get("YN_RUN_CREPE"),
                    # timestamps/size/hash/encoding are filled later when bytes arrive
                }

    # 6) persist recording config
    DB_INSERT_TABLE("ENGINE_DB_LOG_RECORDING_CONFIG", ENGINE_DB_LOG_RECORDING_CONFIG_RECORD, fire_and_forget=True)
