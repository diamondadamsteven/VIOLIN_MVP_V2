# SERVER_ENGINE_LISTEN_3B_FOR_FRAMES.py
from __future__ import annotations
from datetime import datetime
import asyncio

from SERVER_ENGINE_APP_VARIABLES import (
    RECORDING_WEBSOCKET_MESSAGE_ARRAY,
    RECORDING_AUDIO_FRAME_ARRAY,
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    DB_LOG_FUNCTIONS,
    DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE,
    DB_LOG_ENGINE_DB_AUDIO_FRAME,
)

@DB_LOG_FUNCTIONS()
async def SERVER_ENGINE_LISTEN_3B_FOR_FRAMES() -> None:
    """
    Step 1) Scan for unprocessed FRAME messages and process them
    """
    to_launch = []
    for mid, msg in list(RECORDING_WEBSOCKET_MESSAGE_ARRAY.items()):
        if msg.get("DT_MESSAGE_PROCESS_STARTED") is None and str(msg.get("MESSAGE_TYPE", "")).upper() in ("FRAME", "FRAME_BYTES"):
            to_launch.append(mid)
    for mid in to_launch:
        asyncio.create_task(PROCESS_WEBSOCKET_MESSAGE_TYPE_FRAME(MESSAGE_ID=mid))

@DB_LOG_FUNCTIONS()
async def PROCESS_WEBSOCKET_MESSAGE_TYPE_FRAME(MESSAGE_ID: int) -> None:
    """
    Step 1) Update DT_MESSAGE_PROCESS_STARTED
    Step 2) DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE
    Step 3) Insert into RECORDING_AUDIO_FRAME_ARRAY (RECORDING_ID, FRAME_NO, DT_FRAME_RECEIVED, AUDIO_FRAME_DATA)
    Step 4) DB_LOG_ENGINE_DB_AUDIO_FRAME
    Step 5) Delete RECORDING_WEBSOCKET_MESSAGE_ARRAY entry
    """
    msg = RECORDING_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID)
    if not msg:
        return

    msg["DT_MESSAGE_PROCESS_STARTED"] = datetime.now()
    DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE(MESSAGE_ID)

    rid = int(msg.get("RECORDING_ID") or 0)
    fno = int(msg.get("AUDIO_FRAME_NO") or 0)
    dt_received = msg.get("DT_MESSAGE_RECEIVED") or datetime.now()
    frame_bytes = msg.get("AUDIO_FRAME_BYTES")  # optional (may be None)

    # Insert into frame array
    RECORDING_AUDIO_FRAME_ARRAY.setdefault(rid, {})
    RECORDING_AUDIO_FRAME_ARRAY[rid][fno] = {
        "RECORDING_ID": rid,
        "FRAME_NO": fno,
        "DT_FRAME_RECEIVED": dt_received,
        "AUDIO_FRAME_DATA": frame_bytes,
    }
    # DB log (uses DT_FRAME_RECEIVED from array)
    DB_LOG_ENGINE_DB_AUDIO_FRAME(rid, fno)

    # Remove message
    try:
        del RECORDING_WEBSOCKET_MESSAGE_ARRAY[MESSAGE_ID]
    except KeyError:
        pass
