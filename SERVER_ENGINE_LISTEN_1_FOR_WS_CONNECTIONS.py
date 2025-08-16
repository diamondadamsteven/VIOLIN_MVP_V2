# SERVER_ENGINE_LISTEN_1_FOR_WS_CONNECTIONS.py
from __future__ import annotations
from datetime import datetime
from typing import Optional, Dict
from fastapi import WebSocket

from SERVER_ENGINE_APP_VARIABLES import (
    RECORDING_WEBSOCKET_CONNECTION_ARRAY,
    RECORDING_CONFIG_ARRAY,
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    DB_LOG_FUNCTIONS,
    DB_LOG_ENGINE_DB_WEBSOCKET_CONNECTION,
    CONSOLE_LOG,
)

# Simple monotonic allocator for connection ids (int)
_next_conn_id = 1

def _alloc_connection_id() -> int:
    global _next_conn_id
    cid = _next_conn_id
    _next_conn_id += 1
    return cid

@DB_LOG_FUNCTIONS()
async def SERVER_ENGINE_LISTEN_1_FOR_WS_CONNECTIONS(ws: WebSocket) -> int:
    """
    Step 1) Receive and accept the open-WebSocket-request from the client
    Step 2) Insert into app-variable array RECORDING_WEBSOCKET_CONNECTION_ARRAY
    Step 3) Call DB_LOG_ENGINE_DB_WEBSOCKET_CONNECTION
    Returns: WEBSOCKET_CONNECTION_ID
    """
    # Step 1: accept
    await ws.accept()

    # Step 2: insert to connection array
    conn_id = _alloc_connection_id()
    try:
        client_host: Optional[str] = getattr(getattr(ws, "client", None), "host", None)
        client_port: Optional[int]  = getattr(getattr(ws, "client", None), "port", None)
    except Exception:
        client_host, client_port = None, None

    headers: Dict[str, str] = {}
    try:
        if hasattr(ws, "headers") and ws.headers is not None:
            headers = {k.decode() if isinstance(k, (bytes, bytearray)) else k:
                       v.decode() if isinstance(v, (bytes, bytearray)) else v
                       for k, v in ws.headers.raw}
    except Exception:
        headers = {}

    now = datetime.now()
    RECORDING_WEBSOCKET_CONNECTION_ARRAY[conn_id] = {
        "WEBSOCKET_CONNECTION_ID": conn_id,
        "CLIENT_HOST_IP_ADDRESS": client_host,
        "CLIENT_PORT": client_port,
        "CLIENT_HEADERS": headers,
        "DT_CONNECTION_REQUEST": now,       # first seen
        "DT_CONNECTION_ACCEPTED": now,      # accepted now
        "DT_CONNECTION_CLOSED": None,
    }

    # (Optional) if you already know the mapping to a RECORDING_ID, you could
    # stash it later inside RECORDING_CONFIG_ARRAY[RECORDING_ID]["WEBSOCKET_CONNECTION_ID"] = conn_id

    # Step 3: DB log
    DB_LOG_ENGINE_DB_WEBSOCKET_CONNECTION(conn_id)

    CONSOLE_LOG("SERVER_ENGINE_LISTEN_1_FOR_WS_CONNECTIONS", f"Accepted WS; conn_id={conn_id}")
    return conn_id
