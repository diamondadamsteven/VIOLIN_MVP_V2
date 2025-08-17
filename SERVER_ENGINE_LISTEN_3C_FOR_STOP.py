# SERVER_ENGINE_LISTEN_3C_FOR_STOP.py
from __future__ import annotations
from datetime import datetime
import asyncio

from SERVER_ENGINE_APP_VARIABLES import (
    RECORDING_WEBSOCKET_MESSAGE_ARRAY,
    RECORDING_CONFIG_ARRAY,
    RECORDING_WEBSOCKET_CONNECTION_ARRAY,
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    DB_LOG_FUNCTIONS,
    DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE,
)

def SERVER_ENGINE_LISTEN_3C_FOR_STOP() -> None:
    """
    Scan for unprocessed STOP messages and queue async processing.
    """
    to_launch = []
    for mid, msg in list(RECORDING_WEBSOCKET_MESSAGE_ARRAY.items()):
        if msg.get("DT_MESSAGE_PROCESS_STARTED") is None and str(msg.get("MESSAGE_TYPE", "")).upper() == "STOP":
            to_launch.append(mid)

    for mid in to_launch:
        asyncio.create_task(PROCESS_WEBSOCKET_MESSAGE_TYPE_STOP(mid))


@DB_LOG_FUNCTIONS()
async def PROCESS_WEBSOCKET_MESSAGE_TYPE_STOP(MESSAGE_ID: int) -> None:
    """
    PROCESS STOP:
      1) Mark DT_MESSAGE_PROCESS_STARTED
      2) Log message
      3) End the websocket connection (mark closed if we can map it)
      4) Update RECORDING_CONFIG_ARRAY.DT_RECORDING_STOP
    """
    msg = RECORDING_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID)
    if not msg:
        return

    # 1) mark started
    msg["DT_MESSAGE_PROCESS_STARTED"] = datetime.now()

    # 2) db log
    DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE(MESSAGE_ID)

    # 3) mark connection closed if we know the connection id
    rid = int(msg.get("RECORDING_ID") or 0)
    cfg = RECORDING_CONFIG_ARRAY.get(rid, {})
    conn_id = cfg.get("CONNECTION_ID") or msg.get("WEBSOCKET_CONNECTION_ID")
    if conn_id and conn_id in RECORDING_WEBSOCKET_CONNECTION_ARRAY:
        RECORDING_WEBSOCKET_CONNECTION_ARRAY[conn_id]["DT_CONNECTION_CLOSED"] = datetime.now()

    # 4) set recording stop
    if rid in RECORDING_CONFIG_ARRAY:
        RECORDING_CONFIG_ARRAY[rid]["DT_RECORDING_STOP"] = datetime.now()
