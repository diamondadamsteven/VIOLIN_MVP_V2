# SERVER_ENGINE_LISTEN_1_FOR_WS_CONNECTIONS.py
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import WebSocket

from SERVER_ENGINE_APP_VARIABLES import (
    ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY,
)  # in-memory store
from SERVER_ENGINE_APP_FUNCTIONS import (
    DB_INSERT_TABLE,   # generic, allowlisted insert
    CONSOLE_LOG,
    ENGINE_DB_LOG_FUNCTIONS_INS,  # centralized Start/End/Error logging
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
    h = dict(ws.headers) if ws.headers else {}
    if "sec-websocket-protocol" in h:
        return [s.strip() for s in h["sec-websocket-protocol"].split(",") if s.strip()]
    return []


def _choose_subprotocol(requested: List[str]) -> Optional[str]:
    """
    Very simple policy: echo back the first requested protocol.
    Adjust here if you want to enforce a whitelist.
    """
    return requested[0] if requested else None


@ENGINE_DB_LOG_FUNCTIONS_INS()
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

    # --- Gather metadata
    client_host = getattr(ws.client, "host", None)
    client_port_raw = getattr(ws.client, "port", None)
    client_port = str(client_port_raw) if client_port_raw is not None else None  # TypedDict expects str

    headers_dict: Dict[str, str] = dict(ws.headers) if ws.headers else {}
    import json as _json
    headers_str = _json.dumps(headers_dict, ensure_ascii=True)

    scope = getattr(ws, "scope", {}) or {}
    path = scope.get("path")
    scheme = scope.get("scheme")
    qsb = scope.get("query_string", b"")
    query_string = qsb.decode("latin1") if isinstance(qsb, (bytes, bytearray)) else str(qsb)

    # --- Allocate id & store
    conn_id = _alloc_conn_id()
    now = datetime.now()
    row: Dict[str, Any] = {
        # Columns defined for ENGINE_DB_LOG_WEBSOCKET_CONNECTION
        "WEBSOCKET_CONNECTION_ID": conn_id,
        "CLIENT_HOST_IP_ADDRESS": client_host,
        "CLIENT_PORT": client_port,
        "CLIENT_HEADERS": headers_str,
        "DT_CONNECTION_REQUEST": now,
        "DT_CONNECTION_ACCEPTED": now,
        "DT_CONNECTION_CLOSED": None,
        # Helpful extras (kept in memory only; filtered out by DB_INSERT_TABLE)
        "CLIENT_REQUESTED_SUBPROTOCOLS": requested,
        "SERVER_ACCEPTED_SUBPROTOCOL": chosen,
        "URL_SCHEME": scheme,
        "URL_PATH": path,
        "URL_QUERY_STRING": query_string,
    }

    # Save full row in the in-memory array
    ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY[conn_id] = row

    # --- Persist to DB using the generic allowlisted path (fire-and-forget)
    DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_CONNECTION", row, fire_and_forget=True)

    return conn_id
