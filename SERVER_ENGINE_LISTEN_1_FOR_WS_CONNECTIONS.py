# SERVER_ENGINE_LISTEN_1_FOR_WS_CONNECTIONS.py
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import WebSocket

from SERVER_ENGINE_APP_VARIABLES import RECORDING_WEBSOCKET_CONNECTION_ARRAY
from SERVER_ENGINE_APP_FUNCTIONS import (
    DB_LOG_FUNCTIONS,
    DB_LOG_ENGINE_DB_WEBSOCKET_CONNECTION,
    CONSOLE_LOG,
)

_NEXT_CONN_ID = 1

def _alloc_conn_id() -> int:
    """Monotonic in-process connection id."""
    global _NEXT_CONN_ID
    cid = _NEXT_CONN_ID
    _NEXT_CONN_ID += 1
    return cid

def _requested_subprotocols(ws: WebSocket) -> List[str]:
    """Parse Sec-WebSocket-Protocol request header, if any."""
    try:
        h = dict(ws.headers) if ws.headers else {}
        if "sec-websocket-protocol" in h:
            return [s.strip() for s in h["sec-websocket-protocol"].split(",") if s.strip()]
    except Exception:
        pass
    return []

def _choose_subprotocol(requested: List[str]) -> Optional[str]:
    """
    Very simple policy: echo back the first requested protocol.
    Adjust here if you want to enforce a whitelist.
    """
    return requested[0] if requested else None

async def SERVER_ENGINE_LISTEN_1_FOR_WS_CONNECTIONS(ws: WebSocket) -> int:
    """
    Step 1: Accept the WS (with negotiated subprotocol if present)
    Step 2: Store a connection row in the in-memory array
    Step 3: Fire-and-forget DB log (errors captured on the row and logged)
    Returns: WEBSOCKET_CONNECTION_ID
    """
    # --- Negotiate/accept ASAP
    requested = _requested_subprotocols(ws)
    chosen = _choose_subprotocol(requested)
    if chosen:
        await ws.accept(subprotocol=chosen)
    else:
        await ws.accept()

    # --- Gather metadata (best effort; avoid raising)
    try:
        client_host = getattr(ws.client, "host", None)
        client_port = getattr(ws.client, "port", None)
    except Exception:
        client_host = client_port = None

    try:
        headers_dict: Dict[str, str] = dict(ws.headers) if ws.headers else {}
    except Exception:
        headers_dict = {}

    scope = getattr(ws, "scope", {}) or {}
    path = scope.get("path")
    scheme = scope.get("scheme")
    qsb = scope.get("query_string", b"")
    try:
        query_string = qsb.decode("latin1") if isinstance(qsb, (bytes, bytearray)) else str(qsb)
    except Exception:
        query_string = ""

    # --- Allocate id & store
    conn_id = _alloc_conn_id()
    row: Dict[str, Any] = {
        "WEBSOCKET_CONNECTION_ID": conn_id,
        "CLIENT_HOST_IP_ADDRESS": client_host,
        "CLIENT_PORT": client_port,
        "CLIENT_HEADERS": headers_dict,
        "CLIENT_REQUESTED_SUBPROTOCOLS": requested,
        "SERVER_ACCEPTED_SUBPROTOCOL": chosen,
        "URL_SCHEME": scheme,
        "URL_PATH": path,
        "URL_QUERY_STRING": query_string,
        "DT_CONNECTION_REQUEST": datetime.now(),
        "DT_CONNECTION_ACCEPTED": datetime.now(),
        "DT_CONNECTION_CLOSED": None,
    }
    RECORDING_WEBSOCKET_CONNECTION_ARRAY[conn_id] = row

    # --- DB log (don’t let logging kill the WS): offload to a thread
    async def _persist():
        try:
            await asyncio.to_thread(DB_LOG_ENGINE_DB_WEBSOCKET_CONNECTION, conn_id)
        except Exception as e:
            row["DB_LOG_ERROR"] = str(e)
            CONSOLE_LOG("WS_CONN_DB", "Insert failed", {"conn_id": conn_id, "error": str(e)})

    try:
        asyncio.create_task(_persist())
    except Exception as e:
        row["DB_LOG_ERROR"] = f"schedule_failed: {e}"
        CONSOLE_LOG("WS_CONN_DB", "Schedule failed", {"conn_id": conn_id, "error": str(e)})

    return conn_id
