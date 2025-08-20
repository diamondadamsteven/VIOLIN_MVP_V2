# SERVER_ENGINE_LISTEN_7_FOR_FINISHED_RECORDINGS.py
from __future__ import annotations
from datetime import datetime, timedelta

from SERVER_ENGINE_APP_VARIABLES import (
    ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY,
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY,
    ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY,
    ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY,
    WEBSOCKET_AUDIO_FRAME_ARRAY
)

from SERVER_ENGINE_APP_FUNCTIONS import (
    ENGINE_DB_LOG_FUNCTIONS_INS,
    DB_INSERT_TABLE,
    CONSOLE_LOG,
    schedule_coro,  # <— use loop-safe scheduler (works from threads)
)

def SERVER_ENGINE_LISTEN_7_FOR_FINISHED_RECORDINGS() -> None:
    """
    Find recordings where:
      • DT_RECORDING_END is not null, and
      • there are no frames for that RECORDING_ID with DT_PROCESSING_END == null
    Then queue RECORDING_FINISHED for cleanup/purge.
    """
    for RECORDING_ID, ENGINE_DB_LOG_RECORDING_CONFIG_RECORD in list(ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY.items()):
        DT_RECORDING_END = ENGINE_DB_LOG_RECORDING_CONFIG_RECORD.get("DT_RECORDING_END")
        if not DT_RECORDING_END:
            continue
        # Skip if already purged
        if ENGINE_DB_LOG_RECORDING_CONFIG_RECORD.get("DT_RECORDING_DATA_QUEDED_FOR_PURGING"):
            continue

        ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY_2 = ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY.get(RECORDING_ID, {})
        ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY_3 = any(
            fr.get("DT_PROCESSING_END") is None for fr in ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY_2.values()
        ) if ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY_2 else False

        if not ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY_3:
            # SAFE from worker threads; schedules on main loop
            ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[RECORDING_ID]["DT_RECORDING_DATA_QUEDED_FOR_PURGING"] = datetime.now()
            schedule_coro(RECORDING_FINISHED(RECORDING_ID=int(RECORDING_ID)))

@ENGINE_DB_LOG_FUNCTIONS_INS()
async def RECORDING_FINISHED(RECORDING_ID: int) -> None:
    """
    Step 1) Delete from RECORDING_AUDIO_FRAME_ARRAY, RECORDING_CONFIG_ARRAY,
            RECORDING_WEBSOCKET_CONNECTION_ARRAY, and RECORDING_WEBSOCKET_MESSAGE_ARRAY for this recording
    """
    ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[RECORDING_ID]["DT_RECORDING_DATA_PURGED"] = datetime.now()
    DB_INSERT_TABLE("ENGINE_DB_LOG_RECORDING_CONFIG", ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[RECORDING_ID], fire_and_forget=True)

    # Remove durable per-frame metadata and volatile audio arrays
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY.pop(RECORDING_ID, None)
    WEBSOCKET_AUDIO_FRAME_ARRAY.pop(RECORDING_ID, None)

    # Remove messages for this recording (messages keyed by MESSAGE_ID)
    ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY_2 = [
        MESSAGE_ID for MESSAGE_ID, ROW in list(ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.items())
        if int(ROW.get("RECORDING_ID") or 0) == int(RECORDING_ID)
    ]
    for MESSAGE_ID in ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY_2:
        ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.pop(MESSAGE_ID, None)

    # Remove the websocket connection row using the connection id from the config, if present
    WEBSOCKET_CONNECTION_ID = ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY.get(RECORDING_ID, {}).get("WEBSOCKET_CONNECTION_ID")
    if WEBSOCKET_CONNECTION_ID is not None:
        ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY.pop(WEBSOCKET_CONNECTION_ID, None)

    # Finally remove the config row itself
    ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY.pop(RECORDING_ID, None)

    CONSOLE_LOG("LISTEN_7", "recording_finished_cleanup_done", {"rid": int(RECORDING_ID)})
