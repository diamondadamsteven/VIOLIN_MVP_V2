# SERVER_ENGINE_STARTUP_WEBSOCKET.py
from __future__ import annotations

import sys
import json
import asyncio
import subprocess
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

import builtins as _bi
import traceback
import os

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.websockets import WebSocketState
from starlette.types import ASGIApp, Scope, Receive, Send  # for logging WS Origin


from SERVER_ENGINE_APP_FUNCTIONS import (
    CONSOLE_LOG,
    DB_LOG_FUNCTIONS,  # <<< logging decorator
)

PREFIX = "SERVER_ENGINE_STARTUP_WEBSOCKET"

APP = FastAPI(title="VIOLIN_MVP Audio Stream WS Listener", version="1.4.0")

# Middleware to log WebSocket Origin/Host during the handshake
class LogWsOrigin:
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope.get("type") == "websocket":
            hdrs = dict(scope.get("headers") or [])
            origin = hdrs.get(b"origin", b"").decode("latin1", "ignore")
            host   = hdrs.get(b"host",   b"").decode("latin1", "ignore")
            path   = scope.get("path")
            print(f"WS_ORIGIN={origin}  WS_HOST={host}  PATH={path}", flush=True)
        await self.app(scope, receive, send)

# Add the origin-logging middleware BEFORE CORS
APP.add_middleware(LogWsOrigin)

# Allow explicit dev origins + React Native's Origin:null
ALLOWED_ORIGINS = [
    "http://localhost",
    "http://localhost:19000",       # Expo/Metro
    "http://localhost:19006",       # Expo web preview
    "http://192.168.1.27",
    "http://192.168.1.27:19000",
    "http://192.168.1.27:19006",
]
APP.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_origin_regex=r"^null$",   # accept RN’s Origin: null
    allow_credentials=False,        # keep False when using wildcard/regex
    allow_methods=["*"],
    allow_headers=["*"],
)

@APP.on_event("startup")
@DB_LOG_FUNCTIONS()
async def _log_routes_on_startup():
    CONSOLE_LOG(PREFIX, "ROUTES_REGISTERED", {"count": len(APP.router.routes)})
    for r in APP.router.routes:
        try:
            CONSOLE_LOG(PREFIX, "ROUTE", {"path": r.path, "methods": list(getattr(r, "methods", ["WS"]))})
        except Exception:
            CONSOLE_LOG(PREFIX, "ROUTE", {"path": r.path})

# ─────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────
@APP.get("/health")
@DB_LOG_FUNCTIONS()
async def STEP_2_HEALTH():
    CONSOLE_LOG(PREFIX, "GET /health")
    return {"ok": True, "mode": "websocket", "oaf_port": OAF_PORT}

@APP.post("/CLIENT_LOG")
@DB_LOG_FUNCTIONS()
async def client_log(request: Request):
    try:
        body = await request.json()
        entries = body.get("entries", [])
        for e in entries:
            CONSOLE_LOG(PREFIX, "PHONELOG", e)
        return {"ok": True, "count": len(entries)}
    except Exception as exc:
        CONSOLE_LOG(PREFIX, "CLIENT_LOG_ERROR", _bi.str(exc))
        return {"ok": False, "error": _bi.str(exc)}

@DB_LOG_FUNCTIONS()
async def WEBSOCKET_SEND_JSON(ws: WebSocket, payload: Dict[str, Any]):
    if ws.client_state == WebSocketState.CONNECTED:
        await ws.send_text(json.dumps(payload))

@APP.websocket("/ws/stream")
@DB_LOG_FUNCTIONS()
async def WEBSOCKET_RECEIVE_INCOMING_DATA(ws: WebSocket):
    peer = getattr(ws.client, "__dict__", None)
    CONSOLE_LOG(PREFIX, "WS_CONNECT /ws/stream", {"peer": peer})
    await ws.accept()
    CONSOLE_LOG(PREFIX, "WS_ACCEPTED", {"subprotocol": getattr(ws, "subprotocol", None)})
    # PENDING_META_QUEUE[ws] = []

# ─────────────────────────────────────────────────────────────
# Echo test endpoint (bound directly on APP)
# ─────────────────────────────────────────────────────────────
@APP.websocket("/ws/echo")
@DB_LOG_FUNCTIONS()
async def ws_echo(ws: WebSocket):
    await ws.accept()
    await ws.send_text("echo-server: connected")
    try:
        while True:
            msg = await ws.receive_text()
            await ws.send_text(f"echo:{msg}")
    except WebSocketDisconnect:
        return
