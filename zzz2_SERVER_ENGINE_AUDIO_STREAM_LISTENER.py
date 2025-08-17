# SERVER_ENGINE_AUDIO_STREAM_LISTENER.py
# ------------------------------------------------------------
# FastAPI WebSocket listener for VIOLIN_MVP audio streaming.
# Writes incoming .m4a frames to per-recording temp folders.
# On START: initializes recording plan and spawns chunk pipeline.
# On FRAME: queues TEXT meta, pairs next BINARY by arrival order, saves <FRAME_NO>.m4a.
# On STOP: drops a STOP marker; the pipeline will finish remaining chunks and finalize.
# ------------------------------------------------------------

import os
import sys
import json
import asyncio
import subprocess
from pathlib import Path
from typing import Dict, Any, List, Optional

import builtins as _bi
import traceback

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from starlette.websockets import WebSocketState

# --- Force UTF-8 console on Windows; also handle any stray unencodables gracefully
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ─────────────────────────────────────────────────────────────
# Logging (ASCII-safe)
# ─────────────────────────────────────────────────────────────
def LOG_TO_CONSOLE(msg, obj=None):
    prefix = "SERVER_ENGINE_AUDIO_STREAM_LISTENER"
    try:
        if obj is None:
            print(f"{prefix} - {msg}", flush=True)
        else:
            print(f"{prefix} - {msg} {obj}", flush=True)
    except UnicodeEncodeError:
        # Fallback: strip/replace anything not encodable
        try:
            s = f"{prefix} - {msg} {obj}".encode("utf-8", "replace").decode("ascii", "ignore")
            print(s, flush=True)
        except Exception:
            # Last resort: print without obj
            print(f"{prefix} - {msg}", flush=True)

# ─────────────────────────────────────────────────────────────
# App + CORS
# ─────────────────────────────────────────────────────────────
APP = FastAPI(title="VIOLIN_MVP Audio Stream WS Listener", version="1.1.0")
APP.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@APP.on_event("startup")
async def _log_routes_on_startup():
    print("=== ROUTES REGISTERED ===", flush=True)
    for r in APP.router.routes:
        try:
            print(f"  {r.path}  ->  {getattr(r, 'methods', ['WS'])}", flush=True)
        except Exception:
            print(f"  {r.path}", flush=True)
    print("=========================", flush=True)

# ─────────────────────────────────────────────────────────────
# Paths / runtime state
# ─────────────────────────────────────────────────────────────
BASE_TEMP_DIR = Path(os.getenv("AUDIO_TMP_DIR", "./tmp/active_recordings")).resolve()
BASE_TEMP_DIR.mkdir(parents=True, exist_ok=True)

# Per-recording state tracked by the listener
ACTIVE_RECORDINGS: Dict[str, Dict[str, Any]] = {}
# For each WebSocket, we keep a FIFO queue of pending TEXT FRAME metas
PENDING_META_QUEUE: Dict[WebSocket, List[Dict[str, Any]]] = {}
# Simple "expected next frame" counter used only to echo status back to the client
NEXT_EXPECTED: Dict[str, int] = {}

# ─────────────────────────────────────────────────────────────
# Onsets&Frames Docker management (Option A) — unchanged
# ─────────────────────────────────────────────────────────────
OAF_IMAGE = os.getenv("OAF_IMAGE", "tensorflow/magenta")
OAF_CONTAINER = os.getenv("OAF_CONTAINER", "violin_oaf_server")
# Host exposes microservice at 127.0.0.1:OAF_PORT -> container:9077
OAF_PORT = int(os.getenv("OAF_PORT", "9077"))
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", ".")).resolve()
CHECKPOINT_DIR = Path(os.getenv("CHECKPOINT_DIR", "./onsets-frames")).resolve()

def STEP_1_ENSURE_OAF_CONTAINER_RUNNING():
    """
    Ensures the O&F Docker container is up and serving DOCKER_ONSETS_AND_FRAMES_SERVER.py on 9077.
    """
    LOG_TO_CONSOLE("Start function SERVER_ENGINE_AUDIO_STREAM_LISTENER.STEP_1_ENSURE_OAF_CONTAINER_RUNNING")
    try:
        res = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", OAF_CONTAINER],
            capture_output=True, text=True
        )
        if res.returncode == 0 and "true" in (res.stdout or "").strip():
            LOG_TO_CONSOLE("OAF container already running")
            return
    except Exception as e:
        LOG_TO_CONSOLE("docker inspect failed (will try to run container)", _bi.str(e))

    LOG_TO_CONSOLE("Starting OAF container...", {"image": OAF_IMAGE, "name": OAF_CONTAINER})
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

def CREATE_TEMP_AUDIO_DIR(recording_id: str) -> Path:
    d = BASE_TEMP_DIR / recording_id
    d.mkdir(parents=True, exist_ok=True)
    return d

def STOP_MARKER_PATH(recording_id: str) -> Path:
    return (BASE_TEMP_DIR / recording_id) / "_STOP"

# ─────────────────────────────────────────────────────────────
# Startup hook (ensures OAF container is up)
# ─────────────────────────────────────────────────────────────
@APP.on_event("startup")
def STEP_0_ON_STARTUP():
    LOG_TO_CONSOLE("Start function SERVER_ENGINE_AUDIO_STREAM_LISTENER.STEP_0_ON_STARTUP")
    try:
        STEP_1_ENSURE_OAF_CONTAINER_RUNNING()
    except Exception as exc:
        LOG_TO_CONSOLE("Failed to launch OAF container (you can still run without O&F until needed).", _bi.str(exc))

# ─────────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────────
@APP.get("/health")
async def STEP_2_HEALTH():
    LOG_TO_CONSOLE("GET /health")
    return {"ok": True, "mode": "websocket", "oaf_port": OAF_PORT}

async def WEBSOCKET_SEND_JSON(ws: WebSocket, payload: Dict[str, Any]):
    if ws.client_state == WebSocketState.CONNECTED:
        await ws.send_text(json.dumps(payload))


@APP.websocket("/ws/stream")
async def WEBSOCKET_RECEIVE_INCOMING_DATA(ws: WebSocket):
    peer = getattr(ws.client, "__dict__", None)
    LOG_TO_CONSOLE("WS CONNECT -> /ws/stream", {"peer": peer})
    await ws.accept()
    LOG_TO_CONSOLE("WS ACCEPTED", {"subprotocol": getattr(ws, "subprotocol", None)})
    PENDING_META_QUEUE[ws] = []

    # Track which recording_id this socket is currently handling (one per WS expected)
    current_recording_id: Optional[str] = None

    try:
        while True:
            msg = await ws.receive()

            # ── TEXT FRAMES ─────────────────────────────────────
            if "text" in msg and msg["text"] is not None:
                raw_text = msg["text"]
                LOG_TO_CONSOLE("WS TEXT <-", raw_text)
                try:
                    meta = json.loads(raw_text)
                except Exception as exc:
                    await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": f"Bad JSON: {exc}"})
                    LOG_TO_CONSOLE("WS TEXT PARSE ERROR", _bi.str(exc))
                    continue

                mtype = meta.get("type")
                if mtype == "START":
                    rec_id = _bi.str(meta.get("RECORDING_ID") or "").strip()
                    audio_name = meta.get("AUDIO_STREAM_FILE_NAME") or None
                    if not rec_id:
                        await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": "Missing RECORDING_ID in START"})
                        LOG_TO_CONSOLE("START missing RECORDING_ID", meta)
                        continue

                    current_recording_id = rec_id
                    rec_dir = CREATE_TEMP_AUDIO_DIR(rec_id)
                    ACTIVE_RECORDINGS[rec_id] = {
                        "TEMP_AUDIO_STREAM_DIRECTORY": _bi.str(rec_dir),
                        "FIRST_FRAME_RECEIVED": False,
                        "PIPELINE_DONE": False,
                    }
                    NEXT_EXPECTED.setdefault(rec_id, 1)

                    # Spawn per-recording pipeline task
                    task = asyncio.create_task(_spawn_pipeline_task(rec_id))
                    ACTIVE_RECORDINGS[rec_id]["PIPELINE_TASK"] = task

                    LOG_TO_CONSOLE("START ACK ->", {"RECORDING_ID": rec_id, "dir": _bi.str(rec_dir), "audio_file": audio_name})
                    await WEBSOCKET_SEND_JSON(ws, {"type": "START_ACK", "RECORDING_ID": rec_id})

                elif mtype == "FRAME":
                    # Minimal FRAME meta: only RECORDING_ID and FRAME_NO are required
                    rec_id = _bi.str(meta.get("RECORDING_ID") or "").strip()
                    frame_no = meta.get("FRAME_NO")

                    if not rec_id or frame_no is None:
                        await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": "FRAME meta requires RECORDING_ID and FRAME_NO"})
                        LOG_TO_CONSOLE("FRAME meta missing required fields", meta)
                        continue

                    if current_recording_id is None:
                        current_recording_id = rec_id

                    # If this socket suddenly switches to a different recording_id, reject
                    if rec_id != current_recording_id:
                        await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": "Multiple RECORDING_IDs on one WebSocket not supported"})
                        LOG_TO_CONSOLE("FRAME with different RECORDING_ID on same WS", {"expected": current_recording_id, "got": rec_id})
                        continue

                    # Ensure state exists
                    if rec_id not in ACTIVE_RECORDINGS:
                        rec_dir = CREATE_TEMP_AUDIO_DIR(rec_id)
                        ACTIVE_RECORDINGS[rec_id] = {
                            "TEMP_AUDIO_STREAM_DIRECTORY": _bi.str(rec_dir),
                            "FIRST_FRAME_RECEIVED": False,
                            "PIPELINE_DONE": False,
                        }
                        NEXT_EXPECTED.setdefault(rec_id, 1)

                    # Queue meta FIFO; we'll pair the very next binary payload with this meta
                    PENDING_META_QUEUE[ws].append({
                        "RECORDING_ID": rec_id,
                        "FRAME_NO": int(frame_no),
                    })
                    LOG_TO_CONSOLE("FRAME META QUEUED", {"RECORDING_ID": rec_id, "FRAME_NO": int(frame_no)})

                elif mtype == "STOP":
                    rec_id = _bi.str(meta.get("RECORDING_ID") or "").strip()
                    if not rec_id:
                        await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": "Missing RECORDING_ID in STOP"})
                        LOG_TO_CONSOLE("STOP missing RECORDING_ID", meta)
                        continue

                    LOG_TO_CONSOLE("STOP received <-", {"RECORDING_ID": rec_id})

                    # Drop a STOP marker; the pipeline will finish remaining chunks and then finalize.
                    try:
                        STOP_MARKER_PATH(rec_id).write_text("stop", encoding="utf-8")
                    except Exception as e:
                        LOG_TO_CONSOLE("Failed to write STOP marker", _bi.str(e))

                    await WEBSOCKET_SEND_JSON(ws, {"type": "STOP_ACK", "RECORDING_ID": rec_id})
                    LOG_TO_CONSOLE("STOP ACK ->", {"RECORDING_ID": rec_id})

                else:
                    await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": f"Unknown message type: {mtype}"})
                    LOG_TO_CONSOLE("UNKNOWN WS TEXT TYPE", meta)

            # ── BINARY FRAMES ──────────────────────────────────
            elif "bytes" in msg and msg["bytes"] is not None:
                data: bytes = msg["bytes"]
                q = PENDING_META_QUEUE.get(ws, [])
                if not q:
                    await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": "Binary payload without preceding FRAME meta"})
                    LOG_TO_CONSOLE("BINARY WITHOUT META", {"len": len(data)})
                    continue

                # FIFO pair: pop the oldest pending meta for this socket
                meta = q.pop(0)
                rec_id = meta["RECORDING_ID"]
                frame_no = int(meta["FRAME_NO"])

                # Ignore non-positive frames completely (still ACK for hygiene)
                if frame_no <= 0:
                    LOG_TO_CONSOLE("IGNORED NON-POSITIVE FRAME", {"RECORDING_ID": rec_id, "FRAME_NO": frame_no, "len": len(data)})
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
                    LOG_TO_CONSOLE("ACK (IGNORED) ->", ack)
                    continue

                # Persist .m4a
                rec_dir = Path(ACTIVE_RECORDINGS[rec_id]["TEMP_AUDIO_STREAM_DIRECTORY"])
                out_path = rec_dir / f"{frame_no:08d}.m4a"
                out_path.write_bytes(data)
                LOG_TO_CONSOLE("BINARY SAVED", {"RECORDING_ID": rec_id, "FRAME_NO": frame_no, "path": _bi.str(out_path), "len": len(data)})

                if not ACTIVE_RECORDINGS[rec_id]["FIRST_FRAME_RECEIVED"]:
                    ACTIVE_RECORDINGS[rec_id]["FIRST_FRAME_RECEIVED"] = True
                    LOG_TO_CONSOLE("FIRST FRAME RECEIVED", {"RECORDING_ID": rec_id})

                # Update simple NEXT_EXPECTED counter for client visibility only
                next_expected = NEXT_EXPECTED.get(rec_id, 1)
                if frame_no == next_expected:
                    NEXT_EXPECTED[rec_id] = next_expected + 1

                ack = {
                    "type": "ACK",
                    "RECORDING_ID": rec_id,
                    "FRAME_NO": frame_no,
                    "NEXT_EXPECTED_FRAME_NO": NEXT_EXPECTED.get(rec_id, 1),
                    "MISSING_FRAMES": [],  # could compute gaps later if needed
                }
                await WEBSOCKET_SEND_JSON(ws, ack)
                LOG_TO_CONSOLE("ACK ->", ack)

            else:
                # pings / keepalives (no-op)
                LOG_TO_CONSOLE("WS KEEPALIVE/EMPTY")

    except WebSocketDisconnect:
        LOG_TO_CONSOLE("WS DISCONNECT")
        # If the socket drops without an explicit STOP, nudge the pipeline by writing a STOP marker
        if current_recording_id and current_recording_id in ACTIVE_RECORDINGS:
            try:
                sm = STOP_MARKER_PATH(current_recording_id)
                if not sm.exists():
                    sm.write_text("stop", encoding="utf-8")
                    LOG_TO_CONSOLE("STOP marker written on disconnect", {"RECORDING_ID": current_recording_id})
            except Exception as e:
                LOG_TO_CONSOLE("Failed to write STOP marker on disconnect", _bi.str(e))

    except Exception as exc:
        LOG_TO_CONSOLE("Listener exception", _bi.str(exc))
        try:
            await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": _bi.str(exc)})
        except Exception:
            pass
    finally:
        # Clean per-WS meta queue
        PENDING_META_QUEUE.pop(ws, None)
        LOG_TO_CONSOLE("WEBSOCKET_RECEIVE_INCOMING_DATA finalized for this socket")

# ─────────────────────────────────────────────────────────────
# Echo test endpoint (bound directly on APP)
# ─────────────────────────────────────────────────────────────
@APP.websocket("/ws/echo")
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
    LOG_TO_CONSOLE("Start function __main__ -> uvicorn.run")
    uvicorn.run("SERVER_ENGINE_AUDIO_STREAM_LISTENER:APP", host="0.0.0.0", port=7070, reload=True)