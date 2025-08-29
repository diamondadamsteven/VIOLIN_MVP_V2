# SERVER_ENGINE_LISTEN_3C_FOR_STOP.py
from __future__ import annotations
import asyncio

from datetime import datetime
from pathlib import Path
from hashlib import sha256

from SERVER_ENGINE_APP_VARIABLES import (
    ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY,
    ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY,
    ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY,
    AUDIO_BYTES_PER_FRAME,                       # frame size constants
    AUDIO_SAMPLES_PER_FRAME,                     # samples per frame
    AUDIO_SAMPLE_RATE,                           # sample rate
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    ENGINE_DB_LOG_FUNCTIONS_INS,
    ENGINE_DB_LOG_TABLE_INS,
    CONSOLE_LOG
)

# ─────────────────────────────────────────────────────────────
# Scanner: queue unprocessed STOP messages
# ─────────────────────────────────────────────────────────────
async def SERVER_ENGINE_LISTEN_3C_FOR_STOP() -> None:
    """
    Find STOP messages not yet queued, stamp queue time, and schedule processing.
    """
    CONSOLE_LOG("SCANNER", "=== 3C_FOR_STOP scanner starting ===")
    MESSAGE_ID_ARRAY = []
    while True:
        MESSAGE_ID_ARRAY.clear()
        for MESSAGE_ID, ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD in list(ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.items()):
            if (ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD.get("DT_MESSAGE_PROCESS_QUEUED_TO_START") is None and 
                str(ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD.get("MESSAGE_TYPE", "")).upper() == "STOP"):
                MESSAGE_ID_ARRAY.append(MESSAGE_ID)

        if MESSAGE_ID_ARRAY:
            CONSOLE_LOG("SCANNER", f"3C_FOR_STOP: found {len(MESSAGE_ID_ARRAY)} STOP messages to process")
        
        for MESSAGE_ID in MESSAGE_ID_ARRAY:
            ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD = ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID)
            CONSOLE_LOG("SCANNER", f"3C_FOR_STOP: processing STOP message {MESSAGE_ID}")
            ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD["DT_MESSAGE_PROCESS_QUEUED_TO_START"] = datetime.now()
            await PROCESS_WEBSOCKET_STOP_MESSAGE(MESSAGE_ID=MESSAGE_ID)
        
        # Sleep to prevent excessive CPU usage
        await asyncio.sleep(0.1)  # 100ms delay between scans

# ─────────────────────────────────────────────────────────────
# Worker: process a single STOP message
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def PROCESS_WEBSOCKET_STOP_MESSAGE(MESSAGE_ID: int) -> None:
    CONSOLE_LOG("SCANNER", f"PROCESS_WEBSOCKET_STOP_MESSAGE: {MESSAGE_ID}")
    ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD = ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID)

    ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD["DT_MESSAGE_PROCESS_STARTED"] = datetime.now()

    ENGINE_DB_LOG_TABLE_INS("ENGINE_DB_LOG_WEBSOCKET_MESSAGE", ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD)

    RECORDING_ID = ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD["RECORDING_ID"]
    ENGINE_DB_LOG_RECORDING_CONFIG_RECORD = ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY.get(RECORDING_ID, {})
    WEBSOCKET_CONNECTION_ID = (ENGINE_DB_LOG_RECORDING_CONFIG_RECORD.get("WEBSOCKET_CONNECTION_ID") or 
                              ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD.get("WEBSOCKET_CONNECTION_ID"))

    ENGINE_DB_LOG_WEBSOCKET_CONNECTION_RECORD = ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY[WEBSOCKET_CONNECTION_ID]
    ENGINE_DB_LOG_WEBSOCKET_CONNECTION_RECORD["DT_CONNECTION_CLOSED"] = datetime.now()
    ENGINE_DB_LOG_TABLE_INS("ENGINE_DB_LOG_WEBSOCKET_CONNECTION", ENGINE_DB_LOG_WEBSOCKET_CONNECTION_RECORD)

    ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[RECORDING_ID]["DT_RECORDING_END"] = datetime.now()
    ENGINE_DB_LOG_TABLE_INS("ENGINE_DB_LOG_RECORDING_CONFIG", ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[RECORDING_ID])

    ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.pop(MESSAGE_ID, None)
