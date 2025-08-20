# SERVER_ENGINE_ORCHESTRATOR.py
from __future__ import annotations
import asyncio
import inspect
import json
import os
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

# ── WS connection + message handlers ─────────────────────────
from SERVER_ENGINE_LISTEN_1_FOR_WS_CONNECTIONS import SERVER_ENGINE_LISTEN_1_FOR_WS_CONNECTIONS
from SERVER_ENGINE_LISTEN_2_FOR_WS_MESSAGES    import SERVER_ENGINE_LISTEN_2_FOR_WS_MESSAGES

# ── Engine scanners (can be sync or async; we handle both) ───
from SERVER_ENGINE_LISTEN_3A_FOR_START import SERVER_ENGINE_LISTEN_3A_FOR_START
from SERVER_ENGINE_LISTEN_3B_FOR_FRAMES import SERVER_ENGINE_LISTEN_3B_FOR_FRAMES
from SERVER_ENGINE_LISTEN_3C_FOR_STOP import SERVER_ENGINE_LISTEN_3C_FOR_STOP
from SERVER_ENGINE_LISTEN_6_FOR_AUDIO_FRAMES_TO_PROCESS import SERVER_ENGINE_LISTEN_6_FOR_AUDIO_FRAMES_TO_PROCESS
from SERVER_ENGINE_LISTEN_7_FOR_FINISHED_RECORDINGS import SERVER_ENGINE_LISTEN_7_FOR_FINISHED_RECORDINGS

from SERVER_ENGINE_APP_FUNCTIONS import (
    ENGINE_DB_LOG_FUNCTIONS_INS,
    CONSOLE_LOG,
    DB_ENGINE_STARTUP,
    DB_ENGINE_SHUTDOWN,
    schedule_coro,         # ← loop-safe scheduler (now in APP_FUNCTIONS)
    ASYNC_SET_MAIN_LOOP,   # ← setter to capture the main event loop
)

OAF_PORT = int(os.getenv("OAF_PORT", "9077"))

APP = FastAPI(title="VIOLIN_MVP Audio Stream WS Orchestrator", version="1.1.1")

# -----------------------------
# Helpers
# -----------------------------
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def websocket_send_json(ws: WebSocket, payload: Dict[str, Any]):
    """Safe JSON send that won’t raise if the client has gone away."""
    if ws.client_state == WebSocketState.CONNECTED:
        await ws.send_text(json.dumps(payload))

def _ws_peer(ws: WebSocket):
    return {"host": getattr(ws.client, "host", None), "port": getattr(ws.client, "port", None)}

def _requested_subprotocols(ws: WebSocket) -> List[str]:
    h = dict(ws.headers) if ws.headers else {}
    if "sec-websocket-protocol" in h:
        return [s.strip() for s in h["sec-websocket-protocol"].split(",") if s.strip()]
    return []

def _choose_subprotocol(requested: List[str]) -> Optional[str]:
    return requested[0] if requested else None

# -----------------------------
# Small HTTP utilities
# -----------------------------
@APP.get("/health")
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def health():
    return {"ok": True, "mode": "websocket", "oaf_port": OAF_PORT}

@APP.get("/routes")
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def list_routes():
    """Quick visibility into registered routes, including websocket ones."""
    out = []
    for r in APP.router.routes:
        kind = r.__class__.__name__
        path = getattr(r, "path", None)
        methods = list(getattr(r, "methods", []) or [])
        name = getattr(r, "name", None)
        out.append({"kind": kind, "path": path, "methods": methods, "name": name})
    return out

# -----------------------------
# WS: /ws/stream
# -----------------------------
@APP.websocket("/ws/stream")
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def ws_stream(ws: WebSocket):
    # Print BEFORE any DB work so we know the route fired
    CONSOLE_LOG("WS/STREAM", "incoming", {"peer": _ws_peer(ws)})

    # Accept & register
    conn_id = await SERVER_ENGINE_LISTEN_1_FOR_WS_CONNECTIONS(ws)
    await SERVER_ENGINE_LISTEN_2_FOR_WS_MESSAGES(ws, WEBSOCKET_CONNECTION_ID=conn_id)

# -----------------------------
# WS: /ws/echo
# -----------------------------
@APP.websocket("/ws/echo")
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def ws_echo(ws: WebSocket):
    CONSOLE_LOG("WS/ECHO", "incoming", {"peer": _ws_peer(ws)})
    req = _requested_subprotocols(ws)
    chosen = _choose_subprotocol(req)

    if chosen:
        await ws.accept(subprotocol=chosen)
    else:
        await ws.accept()

    await ws.send_text("echo-server: connected")

    while True:
        msg = await ws.receive_text()
        await ws.send_text(f"echo:{msg}")

@APP.websocket("/ws/stream_raw")
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def ws_stream_raw(ws: WebSocket):
    # No DB, no imports—just accept and keep open
    await ws.accept()
    # tiny ping so client knows it opened
    await ws.send_text("raw-ok")
    # echo loop
    while True:
        msg = await ws.receive()
        if msg.get("type") == "websocket.disconnect":
            break
        if msg.get("text") is not None:
            await ws.send_text(f"echo:{msg['text']}")
        elif msg.get("bytes") is not None:
            await ws.send_bytes(msg["bytes"])

# ── Orchestrator: tick all scanners in-process so arrays are shared ─────────────
TICK_MS = 50  # ~20Hz

async def _maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result

# Safe spawner that works for sync/async workers and prevents duplicates
_RUNNING_FLAGS: Dict[str, bool] = {}

def _spawn_non_blocking(fn, name: str):
    """
    Run `fn` once in the background:
      - if `fn` is async -> schedule fn()
      - if `fn` is sync  -> schedule in a worker thread
    Prevents duplicate concurrent runs by name.
    """
    if _RUNNING_FLAGS.get(name):
        return  # already running

    _RUNNING_FLAGS[name] = True

    async def _runner():
        try:
            if inspect.iscoroutinefunction(fn):
                # async worker: call to get coroutine then await it
                await fn()
            else:
                # sync worker: run off the event loop
                await asyncio.to_thread(fn)
        finally:
            _RUNNING_FLAGS[name] = False

    # Use the loop-safe scheduler from APP_FUNCTIONS
    schedule_coro(_runner())

@ENGINE_DB_LOG_FUNCTIONS_INS()
async def _engine_orchestrator():
    while True:
        # Small, non-blocking passes inline
        await _maybe_await(SERVER_ENGINE_LISTEN_3A_FOR_START())   # O(1)
        await _maybe_await(SERVER_ENGINE_LISTEN_3B_FOR_FRAMES())  # O(1)
        await _maybe_await(SERVER_ENGINE_LISTEN_3C_FOR_STOP())    # O(1)

        # Heavy stages -> spawn safely (handles sync/async, deduped)
        _spawn_non_blocking(SERVER_ENGINE_LISTEN_6_FOR_AUDIO_FRAMES_TO_PROCESS, "finish")
        _spawn_non_blocking(SERVER_ENGINE_LISTEN_7_FOR_FINISHED_RECORDINGS,     "finish")

        await asyncio.sleep(TICK_MS / 1000)

@APP.on_event("shutdown")
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def _shutdown():
    DB_ENGINE_SHUTDOWN()

@APP.on_event("startup")
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def _startup():
    # capture and share the main loop for schedule_coro
    ASYNC_SET_MAIN_LOOP(asyncio.get_running_loop())

    DB_ENGINE_STARTUP(warm_pool=True)
    # Show what FastAPI actually registered (helps catch 403-from-missing-route)
    CONSOLE_LOG("STARTUP", "Registered routes", [
        {"type": r.__class__.__name__, "path": getattr(r, "path", None), "methods": list(getattr(r, "methods", []) or [])}
        for r in APP.router.routes
    ])
    schedule_coro(_engine_orchestrator())

# Dev entry
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("SERVER_ENGINE_ORCHESTRATOR:APP", host="0.0.0.0", port=7070, reload=True)
