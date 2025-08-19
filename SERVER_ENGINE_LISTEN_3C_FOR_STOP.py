# SERVER_ENGINE_LISTEN_3C_FOR_STOP.py
from __future__ import annotations
from datetime import datetime
import asyncio

from SERVER_ENGINE_APP_VARIABLES import (
    ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY,
    ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY,
    ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY,
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    DB_LOG_FUNCTIONS,
    DB_INSERT_TABLE,
    CONSOLE_LOG,
)

def SERVER_ENGINE_LISTEN_3C_FOR_STOP() -> None:
    """
    Scan for unprocessed STOP messages and queue async processing.
    """
    to_launch = []
    for mid, msg in list(ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.items()):
        if msg.get("DT_MESSAGE_PROCESS_STARTED") is None and str(msg.get("MESSAGE_TYPE", "")).upper() == "STOP":
            to_launch.append(mid)

    for mid in to_launch:
        asyncio.create_task(PROCESS_WEBSOCKET_MESSAGE_TYPE_STOP(mid))


@DB_LOG_FUNCTIONS()
async def PROCESS_WEBSOCKET_MESSAGE_TYPE_STOP(MESSAGE_ID: int) -> None:
    """
    PROCESS STOP:
      1) Mark DT_MESSAGE_PROCESS_STARTED
      2) Persist message (best-effort)
      3) Mark websocket connection closed (if we can map it)
      4) Update ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY.DT_RECORDING_STOP and persist (best-effort)
    """
    msg = ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID)
    if not msg:
        return

    # 1) mark started (idempotent if re-run)
    if msg.get("DT_MESSAGE_PROCESS_STARTED") is None:
        msg["DT_MESSAGE_PROCESS_STARTED"] = datetime.now()

    # 2) persist message (allowlisted insert)
    try:
        DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_MESSAGE", msg, fire_and_forget=True)
    except Exception as e:
        CONSOLE_LOG("DB_INSERT_MESSAGE", "schedule_failed", {"mid": MESSAGE_ID, "err": str(e)})

    # Resolve recording/connection
    rid = int(msg.get("RECORDING_ID") or 0)
    cfg = ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY.get(rid, {})
    conn_id = cfg.get("WEBSOCKET_CONNECTION_ID") or msg.get("WEBSOCKET_CONNECTION_ID")

    # 3) mark connection closed if we know it
    if conn_id and conn_id in ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY:
        conn_row = ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY[conn_id]
        conn_row["DT_CONNECTION_CLOSED"] = datetime.now()
        try:
            DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_CONNECTION", conn_row, fire_and_forget=True)
        except Exception as e:
            CONSOLE_LOG("WS_CONN_DB", "close_insert_failed", {"conn_id": conn_id, "err": str(e)})

    # 4) set recording stop + persist
    if rid in ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY:
        ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[rid]["DT_RECORDING_STOP"] = datetime.now()
        try:
            DB_INSERT_TABLE("ENGINE_DB_LOG_RECORDING_CONFIG", ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[rid], fire_and_forget=True)
        except Exception as e:
            CONSOLE_LOG("DB_INSERT_RECORDING_CONFIG", "schedule_failed", {"rid": rid, "err": str(e)})
