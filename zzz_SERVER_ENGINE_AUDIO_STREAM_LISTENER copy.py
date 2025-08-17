# zzz_SERVER_ENGINE_AUDIO_STREAM_LISTENER.py
# ------------------------------------------------------------
# FastAPI WebSocket listener for VIOLIN_MVP audio streaming.
# Writes incoming .m4a frames to per-recording temp folders.
# On START: initializes recording plan.
# On first BINARY: starts Step-2 pipeline (concatenate).
# On FRAME: queues TEXT meta, pairs next BINARY by arrival order, saves <FRAME_NO>.m4a.
# On FLUSH_CHUNK: writes a flush marker (partial chunk boundary).
# On STOP: drops a STOP marker; the pipeline will finish remaining chunks and finalize.
# ------------------------------------------------------------

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

# --- Force UTF-8 console on Windows
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

PREFIX = "SERVER_ENGINE_AUDIO_STREAM_LISTENER"

# ─────────────────────────────────────────────────────────────
# App + CORS
# ─────────────────────────────────────────────────────────────
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
# Onsets & Frames Docker management (kept here; called at startup)
# ─────────────────────────────────────────────────────────────
OAF_IMAGE = os.getenv("OAF_IMAGE", "violin/oaf:latest")
OAF_CONTAINER = os.getenv("OAF_CONTAINER", "violin_oaf_server")
# Host exposes microservice at 127.0.0.1:OAF_PORT -> container:9077
OAF_PORT = int(os.getenv("OAF_PORT", "9077"))

# use the shared project root from app-variables
PROJECT_ROOT = PROJECT_ROOT_DIR
CHECKPOINT_DIR = PROJECT_ROOT_DIR / "onsets-frames"   # local default; no env dependency

@DB_LOG_FUNCTIONS()
def STEP_1_ENSURE_OAF_CONTAINER_RUNNING():
    """Ensures the O&F Docker container is up and serving DOCKER_ONSETS_AND_FRAMES_SERVER.py on 9077."""
    CONSOLE_LOG(PREFIX, "STEP_1_ENSURE_OAF_CONTAINER_RUNNING")
    try:
        res = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", OAF_CONTAINER],
            capture_output=True, text=True
        )
        if res.returncode == 0 and "true" in (res.stdout or "").strip():
            CONSOLE_LOG(PREFIX, "OAF_CONTAINER_ALREADY_RUNNING", {"container": OAF_CONTAINER})
            return
    except Exception as e:
        CONSOLE_LOG(PREFIX, "DOCKER_INSPECT_FAILED_WILL_RUN", _bi.str(e))

    CONSOLE_LOG(PREFIX, "STARTING_OAF_CONTAINER", {"image": OAF_IMAGE, "name": OAF_CONTAINER})
    cmd = [
        "docker", "run", "-d", "--rm",
        "--name", OAF_CONTAINER,
        "-p", f"127.0.0.1:{OAF_PORT}:9077",
        "-v", f"{PROJECT_ROOT}:/data",
        "-v", f"{CHECKPOINT_DIR}:/model",
        "-w", "/data",
        OAF_IMAGE,
        "python", "DOCKER_ONSETS_AND_FRAMES_SERVER.py"
    ]
    subprocess.run(cmd, check=True)

@APP.on_event("startup")
@DB_LOG_FUNCTIONS()
def STEP_0_ON_STARTUP():
    CONSOLE_LOG(PREFIX, "STEP_0_ON_STARTUP")
    try:
        STEP_1_ENSURE_OAF_CONTAINER_RUNNING()
    except Exception as exc:
        CONSOLE_LOG(PREFIX, "OAF_CONTAINER_START_FAILED_NON_FATAL", _bi.str(exc))

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
    PENDING_META_QUEUE[ws] = []

    current_recording_id: Optional[int] = None  # one recording per socket

    try:
        while True:
            msg = await ws.receive()

            # If Starlette delivered an explicit disconnect message, stop immediately.
            if msg and isinstance(msg, dict) and msg.get("type") == "websocket.disconnect":
                CONSOLE_LOG(PREFIX, "WS_DISCONNECT_MSG")
                break

            # ── TEXT FRAMES ─────────────────────────────────────
            if "text" in msg and msg["text"] is not None:
                raw_text = msg["text"]
                CONSOLE_LOG(PREFIX, "WS_TEXT <-", raw_text)
                try:
                    meta = json.loads(raw_text)
                except Exception as exc:
                    await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": f"Bad JSON: {exc}"})
                    CONSOLE_LOG(PREFIX, "WS_TEXT_PARSE_ERROR", _bi.str(exc))
                    continue

                mtype = meta.get("type")
                if mtype == "START":
                    rec_id_raw = meta.get("RECORDING_ID")
                    try:
                        rec_id = int(rec_id_raw)
                    except Exception:
                        await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": "RECORDING_ID must be an integer"})
                        CONSOLE_LOG(PREFIX, "START_INVALID_RECORDING_ID", {"value": rec_id_raw})
                        continue

                    audio_name = meta.get("AUDIO_STREAM_FILE_NAME") or None

                    current_recording_id = rec_id
                    rec_dir = CREATE_TEMP_AUDIO_DIR(rec_id)
                    ACTIVE_RECORDINGS[rec_id] = {
                        "TEMP_AUDIO_STREAM_DIRECTORY": _bi.str(rec_dir),
                        "FIRST_FRAME_RECEIVED": False,
                        "PIPELINE_DONE": False,
                        "AUDIO_STREAM_FILE_NAME": audio_name,  # canonical source will be DB/Step-1
                        "CONFIG_TASK": None,
                        "PIPELINE_TASK": None,
                    }
                    NEXT_EXPECTED.setdefault(rec_id, 1)

                    # Seed/refresh minimal in-memory config (NO DB log here)
                    cfg = RECORDING_CONFIG_ARRAY.setdefault(rec_id, {"RECORDING_ID": rec_id})
                    if audio_name and not cfg.get("AUDIO_STREAM_FILE_NAME"):
                        cfg["AUDIO_STREAM_FILE_NAME"] = audio_name
                    if not cfg.get("DT_RECORDING_START"):
                        cfg["DT_RECORDING_START"] = datetime.utcnow()

                    # Kick only Step-1 (config load) now; Step-2 starts after the first binary frame arrives
                    if ACTIVE_RECORDINGS[rec_id]["CONFIG_TASK"] is None:
                        ACTIVE_RECORDINGS[rec_id]["CONFIG_TASK"] = asyncio.create_task(STEP_1_NEW_RECORDING_STARTED(rec_id))

                    CONSOLE_LOG(PREFIX, "START_ACK ->", {"RECORDING_ID": rec_id, "dir": _bi.str(rec_dir), "audio_file": audio_name})
                    await WEBSOCKET_SEND_JSON(ws, {"type": "START_ACK", "RECORDING_ID": rec_id})

                elif mtype == "FRAME":
                    rec_id_raw = meta.get("RECORDING_ID")
                    frame_no_raw = meta.get("FRAME_NO")

                    try:
                        rec_id = int(rec_id_raw)
                        frame_no = int(frame_no_raw)
                    except Exception:
                        await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": "FRAME meta requires integer RECORDING_ID and FRAME_NO"})
                        CONSOLE_LOG(PREFIX, "FRAME_META_BAD_TYPES", meta)
                        continue

                    if current_recording_id is None:
                        current_recording_id = rec_id

                    if rec_id != current_recording_id:
                        await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": "Multiple RECORDING_IDs on one WebSocket not supported"})
                        CONSOLE_LOG(PREFIX, "FRAME_WRONG_RECORDING_ID", {"expected": current_recording_id, "got": rec_id})
                        continue

                    if rec_id not in ACTIVE_RECORDINGS:
                        rec_dir = CREATE_TEMP_AUDIO_DIR(rec_id)
                        ACTIVE_RECORDINGS[rec_id] = {
                            "TEMP_AUDIO_STREAM_DIRECTORY": _bi.str(rec_dir),
                            "FIRST_FRAME_RECEIVED": False,
                            "PIPELINE_DONE": False,
                            "AUDIO_STREAM_FILE_NAME": None,
                            "CONFIG_TASK": None,
                            "PIPELINE_TASK": None,
                        }
                        NEXT_EXPECTED.setdefault(rec_id, 1)

                    PENDING_META_QUEUE[ws].append({
                        "RECORDING_ID": rec_id,
                        "FRAME_NO": frame_no,
                    })
                    CONSOLE_LOG(PREFIX, "FRAME_META_QUEUED", {"RECORDING_ID": rec_id, "FRAME_NO": frame_no})

                elif mtype == "FLUSH_CHUNK":
                    rec_id_raw = meta.get("RECORDING_ID")
                    try:
                        rec_id = int(rec_id_raw)
                    except Exception:
                        await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": "Missing/invalid RECORDING_ID in FLUSH_CHUNK"})
                        CONSOLE_LOG(PREFIX, "FLUSH_INVALID_RECORDING_ID", {"value": rec_id_raw})
                        continue

                    try:
                        FLUSH_MARKER_PATH(rec_id).write_text("flush", encoding="utf-8")
                        CONSOLE_LOG(PREFIX, "FLUSH_MARKER_WRITTEN", {"RECORDING_ID": rec_id})
                    except Exception as e:
                        CONSOLE_LOG(PREFIX, "FLUSH_MARKER_WRITE_FAILED", _bi.str(e))

                    await WEBSOCKET_SEND_JSON(ws, {"type": "FLUSH_ACK", "RECORDING_ID": rec_id})
                    CONSOLE_LOG(PREFIX, "FLUSH_ACK ->", {"RECORDING_ID": rec_id})

                elif mtype == "STOP":
                    rec_id_raw = meta.get("RECORDING_ID")
                    try:
                        rec_id = int(rec_id_raw)
                    except Exception:
                        await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": "Missing/invalid RECORDING_ID in STOP"})
                        CONSOLE_LOG(PREFIX, "STOP_INVALID_RECORDING_ID", {"value": rec_id_raw})
                        continue

                    CONSOLE_LOG(PREFIX, "STOP_RECEIVED <-", {"RECORDING_ID": rec_id})

                    try:
                        STOP_MARKER_PATH(rec_id).write_text("stop", encoding="utf-8")
                    except Exception as e:
                        CONSOLE_LOG(PREFIX, "STOP_MARKER_WRITE_FAILED", _bi.str(e))

                    await WEBSOCKET_SEND_JSON(ws, {"type": "STOP_ACK", "RECORDING_ID": rec_id})
                    CONSOLE_LOG(PREFIX, "STOP_ACK ->", {"RECORDING_ID": rec_id})

                else:
                    await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": f"Unknown message type: {mtype}"})
                    CONSOLE_LOG(PREFIX, "UNKNOWN_WS_TEXT_TYPE", meta)

            # ── BINARY FRAMES ──────────────────────────────────
            elif "bytes" in msg and msg["bytes"] is not None:
                data: bytes = msg["bytes"]
                q = PENDING_META_QUEUE.get(ws, [])
                if not q:
                    await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": "Binary payload without preceding FRAME meta"})
                    CONSOLE_LOG(PREFIX, "BINARY_WITHOUT_META", {"len": len(data)})
                    continue

                meta = q.pop(0)
                rec_id = int(meta["RECORDING_ID"])
                frame_no = int(meta["FRAME_NO"])

                if frame_no <= 0:
                    CONSOLE_LOG(PREFIX, "IGNORED_NON_POSITIVE_FRAME", {"RECORDING_ID": rec_id, "FRAME_NO": frame_no, "len": len(data)})
                    if NEXT_EXPECTED.get(rec_id, 1) < 1:
                        NEXT_EXPECTED[rec_id] = 1
                    ack = {
                        "type": "ACK",
                        "RECORDING_ID": rec_id,
                        "FRAME_NO": frame_no,
                        "NEXT_EXPECTED_FRAME_NO": NEXT_EXPECTED.get(rec_id, 1),
                        "MISSING_FRAMES": [],
                    }
                    await WEBSOCKET_SEND_JSON(ws, ack)
                    CONSOLE_LOG(PREFIX, "ACK (IGNORED) ->", ack)
                    continue

                rec_dir = Path(ACTIVE_RECORDINGS[rec_id]["TEMP_AUDIO_STREAM_DIRECTORY"])
                out_path = rec_dir / f"{frame_no:08d}.m4a"
                out_path.write_bytes(data)
                CONSOLE_LOG(PREFIX, "BINARY_SAVED", {"RECORDING_ID": rec_id, "FRAME_NO": frame_no, "path": _bi.str(out_path), "len": len(data)})

                # Update in-memory frame timings (used by Step-1)
                frames = RECORDING_AUDIO_FRAME_ARRAY.setdefault(rec_id, {})
                fr = frames.setdefault(frame_no, {"RECORDING_ID": rec_id, "FRAME_NO": frame_no})
                if not fr.get("DT_FRAME_RECEIVED"):
                    fr["DT_FRAME_RECEIVED"] = datetime.utcnow()

                # Mark first-frame and start Step-2 pipeline (if not already running)
                if not ACTIVE_RECORDINGS[rec_id]["FIRST_FRAME_RECEIVED"]:
                    ACTIVE_RECORDINGS[rec_id]["FIRST_FRAME_RECEIVED"] = True
                    CONSOLE_LOG(PREFIX, "FIRST_FRAME_RECEIVED", {"RECORDING_ID": rec_id})
                    if ACTIVE_RECORDINGS[rec_id].get("PIPELINE_TASK") is None:
                        ACTIVE_RECORDINGS[rec_id]["PIPELINE_TASK"] = asyncio.create_task(STEP_2_CREATE_AUDIO_CHUNKS(rec_id))

                next_expected = NEXT_EXPECTED.get(rec_id, 1)
                if frame_no == next_expected:
                    NEXT_EXPECTED[rec_id] = next_expected + 1

                ack = {
                    "type": "ACK",
                    "RECORDING_ID": rec_id,
                    "FRAME_NO": frame_no,
                    "NEXT_EXPECTED_FRAME_NO": NEXT_EXPECTED.get(rec_id, 1),
                    "MISSING_FRAMES": [],
                }
                await WEBSOCKET_SEND_JSON(ws, ack)
                CONSOLE_LOG(PREFIX, "ACK ->", ack)

            else:
                CONSOLE_LOG(PREFIX, "WS_KEEPALIVE_OR_EMPTY")

    except WebSocketDisconnect:
        CONSOLE_LOG(PREFIX, "WS_DISCONNECT")
        if current_recording_id is not None and current_recording_id in ACTIVE_RECORDINGS:
            try:
                sm = STOP_MARKER_PATH(current_recording_id)
                if not sm.exists():
                    sm.write_text("stop", encoding="utf-8")
                    CONSOLE_LOG(PREFIX, "STOP_MARKER_WRITTEN_ON_DISCONNECT", {"RECORDING_ID": current_recording_id})
            except Exception as e:
                CONSOLE_LOG(PREFIX, "STOP_MARKER_ON_DISCONNECT_FAILED", _bi.str(e))

    except Exception as exc:
        CONSOLE_LOG(PREFIX, "LISTENER_EXCEPTION", {"error": _bi.str(exc), "trace": traceback.format_exc()})
        try:
            await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": _bi.str(exc)})
        except Exception:
            pass
    finally:
        PENDING_META_QUEUE.pop(ws, None)
        CONSOLE_LOG(PREFIX, "WS_FINALIZED_FOR_SOCKET")

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

# Dev run:
# uvicorn SERVER_ENGINE_AUDIO_STREAM_LISTENER:APP --host 0.0.0.0 --port 7070 --reload
if __name__ == "__main__":
    import uvicorn
    CONSOLE_LOG(PREFIX, "UVICORN_RUN", {"host": "0.0.0.0", "port": 7070, "reload": True})
    uvicorn.run("SERVER_ENGINE_AUDIO_STREAM_LISTENER:APP", host="0.0.0.0", port=7070, reload=True)
