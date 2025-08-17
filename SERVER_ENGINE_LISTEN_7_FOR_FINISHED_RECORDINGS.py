# SERVER_ENGINE_LISTEN_7_FOR_FINISHED_RECORDINGS.py
from __future__ import annotations
from datetime import datetime, timedelta
import asyncio

from SERVER_ENGINE_APP_VARIABLES import (
    RECORDING_CONFIG_ARRAY,
    RECORDING_AUDIO_CHUNK_ARRAY,
    RECORDING_AUDIO_FRAME_ARRAY,
    RECORDING_WEBSOCKET_MESSAGE_ARRAY,
    RECORDING_WEBSOCKET_CONNECTION_ARRAY,
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    DB_LOG_FUNCTIONS,
    CONSOLE_LOG
)

def SERVER_ENGINE_LISTEN_7_FOR_FINISHED_RECORDINGS() -> None:
    """
    Step 1) Find recordings where DT_RECORDING_STOP < 5-seconds-ago and there
            are no chunks remaining for that RECORDING_ID; then launch RECORDING_FINISHED.
    """
    cutoff = datetime.now() - timedelta(seconds=5)
    for rid, cfg in list(RECORDING_CONFIG_ARRAY.items()):
        dt_stop = cfg.get("DT_RECORDING_STOP")
        if not dt_stop or dt_stop >= cutoff:
            continue
        has_chunks = bool(RECORDING_AUDIO_CHUNK_ARRAY.get(rid))
        if not has_chunks:
            asyncio.create_task(RECORDING_FINISHED(RECORDING_ID=rid))

@DB_LOG_FUNCTIONS()
async def RECORDING_FINISHED(RECORDING_ID: int) -> None:
    """
    Step 1) Delete from RECORDING_AUDIO_FRAME_ARRAY, RECORDING_CONFIG_ARRAY,
            RECORDING_WEBSOCKET_CONNECTION_ARRAY, and RECORDING_WEBSOCKET_MESSAGE_ARRAY for this recording
    """
    # Frames
    RECORDING_AUDIO_FRAME_ARRAY.pop(RECORDING_ID, None)

    # Messages for this recording
    to_del = [mid for mid, m in RECORDING_WEBSOCKET_MESSAGE_ARRAY.items() if int(m.get("RECORDING_ID") or 0) == RECORDING_ID]
    for mid in to_del:
        RECORDING_WEBSOCKET_MESSAGE_ARRAY.pop(mid, None)

    # Connection (via config mapping)
    cfg = RECORDING_CONFIG_ARRAY.pop(RECORDING_ID, {})
    cid = cfg.get("WEBSOCKET_CONNECTION_ID")
    if cid:
        RECORDING_WEBSOCKET_CONNECTION_ARRAY.pop(cid, None)
