from __future__ import annotations
from datetime import datetime
import os
import inspect
import base64  # for Android RN base64 payloads

from SERVER_ENGINE_APP_VARIABLES import (
    RECORDING_WEBSOCKET_MESSAGE_ARRAY,
    RECORDING_AUDIO_FRAME_ARRAY,
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    DB_LOG_FUNCTIONS,
    DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE,   # enqueues persist
    DB_LOG_ENGINE_DB_AUDIO_FRAME,         # enqueues persist
    DB_LOG_ENGINE_DB_LOG_STEPS,           # accepts DT_ADDED
    schedule_coro,                        # loop/thread-safe scheduler
)

def _log_step(step: str, *, rid: int | None = None, fno: int | None = None) -> None:
    """
    Stamp DT_ADDED at call-time for precise step timing and enqueue the insert.
    """
    fn_name = "PROCESS_WEBSOCKET_MESSAGE_TYPE_FRAME"
    try:
        frame = inspect.currentframe()
        if frame and frame.f_back:
            fn_name = frame.f_back.f_code.co_name
    except Exception:
        pass

    DB_LOG_ENGINE_DB_LOG_STEPS(
        DT_ADDED=datetime.now(),
        STEP_NAME=step,
        PYTHON_FUNCTION_NAME=fn_name,
        PYTHON_FILE_NAME=os.path.basename(__file__),
        RECORDING_ID=rid,
        AUDIO_CHUNK_NO=None,
        FRAME_NO=fno,
    )

def SERVER_ENGINE_LISTEN_3B_FOR_FRAMES() -> None:
    """
    Scan for unprocessed FRAME messages and queue async processing.
    """
    to_launch = []
    for mid, msg in list(RECORDING_WEBSOCKET_MESSAGE_ARRAY.items()):
        # Expect MESSAGE_TYPE (renamed from 'type')
        if msg.get("DT_MESSAGE_PROCESS_STARTED") is None and str(msg.get("MESSAGE_TYPE", "")).upper() == "FRAME":
            to_launch.append(mid)

    for mid in to_launch:
        schedule_coro(PROCESS_WEBSOCKET_MESSAGE_TYPE_FRAME(mid))

@DB_LOG_FUNCTIONS()
async def PROCESS_WEBSOCKET_MESSAGE_TYPE_FRAME(MESSAGE_ID: int) -> None:
    """
    PROCESS FRAME:
      1) Mark DT_MESSAGE_PROCESS_STARTED
      2) Log the message (queued, non-blocking)
      3) Decode Android base64 if needed; insert into RECORDING_AUDIO_FRAME_ARRAY
      4) Log AUDIO_FRAME (queued, non-blocking)
      5) Delete message entry
    """
    _log_step("Begin", rid=MESSAGE_ID, fno=None)

    msg = RECORDING_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID)
    if not msg:
        return

    # 1) mark started
    msg["DT_MESSAGE_PROCESS_STARTED"] = datetime.now()

    # 2) websocket-message log
    DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE(MESSAGE_ID)

    # 3) persist frame (in-memory), decoding base64 string if present (Android RN)
    rid = int(msg.get("RECORDING_ID") or 0)
    fno = int(msg.get("AUDIO_FRAME_NO") or 0)
    dt_received = msg.get("DT_MESSAGE_RECEIVED")
    audio_bytes = msg.get("AUDIO_FRAME_BYTES")  # may be bytes OR base64 str

    # Normalize to raw bytes:
    if isinstance(audio_bytes, str):
        try:
            # Some RN Android stacks send raw binary frames as base64 text.
            audio_bytes = base64.b64decode(audio_bytes, validate=False)
        except Exception:
            # If decode fails, leave as-is; downstream will handle/skip safely.
            pass
    elif isinstance(audio_bytes, memoryview):
        audio_bytes = audio_bytes.tobytes()
    elif isinstance(audio_bytes, bytearray):
        audio_bytes = bytes(audio_bytes)

    _log_step("Here 2", rid=rid, fno=fno)

    RECORDING_AUDIO_FRAME_ARRAY.setdefault(rid, {})
    RECORDING_AUDIO_FRAME_ARRAY[rid][fno] = {
        "RECORDING_ID": rid,
        "FRAME_NO": fno,
        "DT_FRAME_RECEIVED": dt_received if isinstance(dt_received, datetime) else datetime.now(),
        "AUDIO_FRAME_DATA": audio_bytes,  # always bytes when possible
    }

    _log_step("Here 3", rid=rid, fno=fno)

    # 4) audio-frame log
    DB_LOG_ENGINE_DB_AUDIO_FRAME(rid, fno)

    # 5) remove message
    try:
        del RECORDING_WEBSOCKET_MESSAGE_ARRAY[MESSAGE_ID]
        _log_step("Here 4", rid=rid, fno=fno)
    except KeyError:
        pass
