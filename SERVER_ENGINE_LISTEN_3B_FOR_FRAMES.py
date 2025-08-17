# SERVER_ENGINE_LISTEN_3B_FOR_FRAMES.py
from __future__ import annotations
from datetime import datetime
import asyncio
import os
import inspect

from SERVER_ENGINE_APP_VARIABLES import (
    RECORDING_WEBSOCKET_MESSAGE_ARRAY,
    RECORDING_AUDIO_FRAME_ARRAY,
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    DB_LOG_FUNCTIONS,
    DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE,
    DB_LOG_ENGINE_DB_AUDIO_FRAME,
    DB_LOG_ENGINE_DB_LOG_STEPS
)

def SERVER_ENGINE_LISTEN_3B_FOR_FRAMES() -> None:
    """
    Scan for unprocessed FRAME messages and queue async processing.
    """
    to_launch = []
    for mid, msg in list(RECORDING_WEBSOCKET_MESSAGE_ARRAY.items()):
        if msg.get("DT_MESSAGE_PROCESS_STARTED") is None and str(msg.get("MESSAGE_TYPE", "")).upper() == "FRAME":
            to_launch.append(mid)

    for mid in to_launch:
        asyncio.create_task(PROCESS_WEBSOCKET_MESSAGE_TYPE_FRAME(mid))

@DB_LOG_FUNCTIONS()
async def PROCESS_WEBSOCKET_MESSAGE_TYPE_FRAME(MESSAGE_ID: int) -> None:
    """
    PROCESS FRAME:
      1) Mark DT_MESSAGE_PROCESS_STARTED
      2) Log the message
      3) Insert into RECORDING_AUDIO_FRAME_ARRAY (RECORDING_ID, FRAME_NO, DT_FRAME_RECEIVED, AUDIO_FRAME_DATA)
      4) Log AUDIO_FRAME
      5) Delete message entry
    """
    DB_LOG_ENGINE_DB_LOG_STEPS(
            STEP_NAME="Begin",
            PYTHON_FUNCTION_NAME=inspect.currentframe().f_code.co_name,
            PYTHON_FILE_NAME=os.path.basename(__file__),
            RECORDING_ID=MESSAGE_ID,
            AUDIO_CHUNK_NO=None,
            FRAME_NO=None,
        )


    msg = RECORDING_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID)
    if not msg:
        return

    # 1) mark started
    msg["DT_MESSAGE_PROCESS_STARTED"] = datetime.now()

    # 2) log message
    DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE(MESSAGE_ID)

    # 3) persist frame
    rid = int(msg.get("RECORDING_ID") or 0)
    fno = int(msg.get("AUDIO_FRAME_NO") or 0)
    dt_received = msg.get("DT_MESSAGE_RECEIVED") 
    audio_bytes = msg.get("AUDIO_FRAME_BYTES")  # may be None if not provided

    DB_LOG_ENGINE_DB_LOG_STEPS(
            STEP_NAME="Here 2",
            PYTHON_FUNCTION_NAME=inspect.currentframe().f_code.co_name,
            PYTHON_FILE_NAME=os.path.basename(__file__),
            RECORDING_ID=int(msg.get("RECORDING_ID") or 0),
            AUDIO_CHUNK_NO=None,
            FRAME_NO=int(msg.get("AUDIO_FRAME_NO") or 0),
        )

    RECORDING_AUDIO_FRAME_ARRAY.setdefault(rid, {})
    RECORDING_AUDIO_FRAME_ARRAY[rid][fno] = {
        "RECORDING_ID": rid,
        "FRAME_NO": fno,
        "DT_FRAME_RECEIVED": dt_received,
        "AUDIO_FRAME_DATA": audio_bytes,
    }

    DB_LOG_ENGINE_DB_LOG_STEPS(
            STEP_NAME="Here 3",
            PYTHON_FUNCTION_NAME=inspect.currentframe().f_code.co_name,
            PYTHON_FILE_NAME=os.path.basename(__file__),
            RECORDING_ID=int(msg.get("RECORDING_ID") or 0),
            AUDIO_CHUNK_NO=None,
            FRAME_NO=int(msg.get("AUDIO_FRAME_NO") or 0),
        )
    # 4) db log audio frame
    DB_LOG_ENGINE_DB_AUDIO_FRAME(rid, fno)

    # 5) remove message
    try:
        del RECORDING_WEBSOCKET_MESSAGE_ARRAY[MESSAGE_ID]
        DB_LOG_ENGINE_DB_LOG_STEPS(
        STEP_NAME="Here 4",
        PYTHON_FUNCTION_NAME=inspect.currentframe().f_code.co_name,
        PYTHON_FILE_NAME=os.path.basename(__file__),
        RECORDING_ID=int(msg.get("RECORDING_ID") or 0),
        AUDIO_CHUNK_NO=None,
        FRAME_NO=int(msg.get("AUDIO_FRAME_NO") or 0),
        )

    except KeyError:
        pass
