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
    CONSOLE_LOG,
)

@DB_LOG_FUNCTIONS()
async def SERVER_ENGINE_LISTEN_3C_FOR_STOP() -> None:
    """
    Step 1) Find unprocessed STOP messages and process
    """
    to_launch = []
    for mid, msg in list(RECORDING_WEBSOCKET_MESSAGE_ARRAY.items()):
        if msg.get("DT_MESSAGE_PROCESS_STARTED") is None and str(msg.get("MESSAGE_TYPE", "")).upper() == "STOP":
            to_launch.append(mid)
    for mid in to_launch:
        asyncio.create_task(PROCESS_WEBSOCKET_MESSAGE_TYPE_STOP(MESSAGE_ID=mid))

@DB_LOG_FUNCTIONS()
async def PROCESS_WEBSOCKET_MESSAGE_TYPE_STOP(MESSAGE_ID: int) -> None:
    """
    Step 1) Mark DT_MESSAGE_PROCESS_STARTED
    Step 2) DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE
    Step 3) End the websocket connection (handled by listener at receipt; here we mark DT_CONNECTION_CLOSED if known)
    Step 4) Update RECORDING_CONFIG_ARRAY.DT_RECORDING_STOP
    """
    msg = RECORDING_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID)
    if not msg:
        return
    msg["DT_MESSAGE_PROCESS_STARTED"] = datetime.now()
    DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE(MESSAGE_ID)

    rid = int(msg.get("RECORDING_ID") or 0)
    if rid in RECORDING_CONFIG_ARRAY:
        RECORDING_CONFIG_ARRAY[rid]["DT_RECORDING_STOP"] = datetime.now()

    # (Optional) If WEBSOCKET_CONNECTION_ID stored in cfg, mark connection closed
    cfg = RECORDING_CONFIG_ARRAY.get(rid, {})
    cid = cfg.get("WEBSOCKET_CONNECTION_ID")
    if cid and cid in RECORDING_WEBSOCKET_CONNECTION_ARRAY:
        RECORDING_WEBSOCKET_CONNECTION_ARRAY[cid]["DT_CONNECTION_CLOSED"] = datetime.now()
