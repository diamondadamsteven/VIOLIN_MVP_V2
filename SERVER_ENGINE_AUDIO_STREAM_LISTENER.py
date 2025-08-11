# SERVER_ENGINE_AUDIO_STREAM_LISTENER.py
# ------------------------------------------------------------
# FastAPI WebSocket listener for VIOLIN_MVP audio streaming.
# Keeps an Onsets&Frames Docker container running (Option A)
# that exposes DOCKER_ONSETS_AND_FRAMES_SERVER.py on 9077 (in-container).
# Host side maps 127.0.0.1:OAF_PORT -> container:9077.
# ------------------------------------------------------------

import os
import json
import subprocess
from pathlib import Path
from typing import Dict, Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from starlette.websockets import WebSocketState

from SERVER_ENGINE_AUDIO_STREAM_PROCESSOR import (
    PROCESS_AUDIO_STREAM,
    PROCESS_STOP_RECORDING,
    REGISTER_RECORDING_CONTEXT_HINT,
)

# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────
def LOG_TO_CONSOLE(msg, obj=None):
    prefix = "SERVER_ENGINE_AUDIO_STREAM_LISTENER"
    if obj is None:
        print(f"{prefix} - {msg}", flush=True)
    else:
        print(f"{prefix} - {msg} {obj}", flush=True)

# ─────────────────────────────────────────────────────────────
# App + CORS
# ─────────────────────────────────────────────────────────────
APP = FastAPI(title="VIOLIN_MVP Audio Stream WS Listener", version="1.0.0")
APP.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────────
# Paths / runtime state
# ─────────────────────────────────────────────────────────────
BASE_TEMP_DIR = Path(os.getenv("AUDIO_TMP_DIR", "./tmp/active_recordings")).resolve()
BASE_TEMP_DIR.mkdir(parents=True, exist_ok=True)

ACTIVE_RECORDINGS: Dict[str, Dict[str, Any]] = {}
PENDING_META: Dict[WebSocket, Dict[int, Dict[str, Any]]] = {}
NEXT_EXPECTED: Dict[str, int] = {}

# ─────────────────────────────────────────────────────────────
# Onsets&Frames Docker management (Option A)
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
        # Is it already running?
        res = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", OAF_CONTAINER],
            capture_output=True, text=True
        )
        if res.returncode == 0 and "true" in (res.stdout or "").strip():
            LOG_TO_CONSOLE("OAF container already running")
            return
    except Exception as e:
        LOG_TO_CONSOLE("docker inspect failed (will try to run container)", str(e))

    # (Re)start container
    LOG_TO_CONSOLE("Starting OAF container…", {"image": OAF_IMAGE, "name": OAF_CONTAINER})
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

# ─────────────────────────────────────────────────────────────
# Startup hook (ensures OAF container is up)
# ─────────────────────────────────────────────────────────────
@APP.on_event("startup")
def STEP_0_ON_STARTUP():
    LOG_TO_CONSOLE("Start function SERVER_ENGINE_AUDIO_STREAM_LISTENER.STEP_0_ON_STARTUP")
    try:
        STEP_1_ENSURE_OAF_CONTAINER_RUNNING()
    except Exception as exc:
        LOG_TO_CONSOLE("Failed to launch OAF container (you can still run without O&F until needed).", str(exc))

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
    LOG_TO_CONSOLE("Start function SERVER_ENGINE_AUDIO_STREAM_LISTENER.WEBSOCKET_RECEIVE_INCOMING_DATA")
    await ws.accept()
    PENDING_META[ws] = {}

    try:
        while True:
            msg = await ws.receive()
            if "text" in msg and msg["text"] is not None:
                try:
                    meta = json.loads(msg["text"])
                except Exception as exc:
                    await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": f"Bad JSON: {exc}"})
                    continue

                mtype = meta.get("type")
                LOG_TO_CONSOLE("WS text received", meta)

                if mtype == "START":
                    rec_id = str(meta.get("RECORDING_ID") or "").strip()
                    audio_name = meta.get("AUDIO_STREAM_FILE_NAME") or None
                    if not rec_id:
                        await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": "Missing RECORDING_ID in START"})
                        continue

                    rec_dir = CREATE_TEMP_AUDIO_DIR(rec_id)
                    ACTIVE_RECORDINGS[rec_id] = {
                        "TEMP_AUDIO_STREAM_DIRECTORY": str(rec_dir),
                        "FIRST_FRAME_RECEIVED": False,
                    }
                    NEXT_EXPECTED.setdefault(rec_id, 0)

                    # Hint processor with filename chosen by DB at record-start
                    REGISTER_RECORDING_CONTEXT_HINT(rec_id, AUDIO_STREAM_FILE_NAME=audio_name)

                    await WEBSOCKET_SEND_JSON(ws, {"type": "START_ACK", "RECORDING_ID": rec_id})

                elif mtype == "FRAME":
                    rec_id = str(meta.get("RECORDING_ID") or "").strip()
                    frame_no = meta.get("FRAME_NO")
                    frame_dur = meta.get("FRAME_DURATION_IN_MS")
                    overlap = meta.get("COUNTDOWN_OVERLAP_MS", 0)
                    bytes_len = meta.get("BYTES_LEN")
                    if any(v is None for v in (rec_id, frame_no, frame_dur, bytes_len)):
                        await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": "FRAME meta missing required fields"})
                        continue

                    if rec_id not in ACTIVE_RECORDINGS:
                        rec_dir = CREATE_TEMP_AUDIO_DIR(rec_id)
                        ACTIVE_RECORDINGS[rec_id] = {
                            "TEMP_AUDIO_STREAM_DIRECTORY": str(rec_dir),
                            "FIRST_FRAME_RECEIVED": False,
                        }
                        NEXT_EXPECTED.setdefault(rec_id, 0)

                    PENDING_META[ws][int(frame_no)] = {
                        "RECORDING_ID": rec_id,
                        "FRAME_NO": int(frame_no),
                        "FRAME_DURATION_IN_MS": int(frame_dur),
                        "COUNTDOWN_OVERLAP_MS": int(overlap or 0),
                        "BYTES_LEN": int(bytes_len),
                    }

                elif mtype == "STOP":
                    rec_id = str(meta.get("RECORDING_ID") or "").strip()
                    if not rec_id:
                        await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": "Missing RECORDING_ID in STOP"})
                        continue

                    LOG_TO_CONSOLE("STOP received", {"RECORDING_ID": rec_id})
                    await PROCESS_STOP_RECORDING(rec_id)
                    ACTIVE_RECORDINGS.pop(rec_id, None)
                    NEXT_EXPECTED.pop(rec_id, None)
                    await WEBSOCKET_SEND_JSON(ws, {"type": "STOP_ACK", "RECORDING_ID": rec_id})

                else:
                    await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": f"Unknown message type: {mtype}"})

            elif "bytes" in msg and msg["bytes"] is not None:
                data: bytes = msg["bytes"]
                meta_map = PENDING_META.get(ws, {})
                match_key = None
                meta = None
                for k, m in list(meta_map.items()):
                    if m["BYTES_LEN"] == len(data):
                        match_key = k
                        meta = m
                        break

                if not meta:
                    await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": "Binary payload without matching FRAME meta"})
                    continue

                del meta_map[match_key]

                rec_id = meta["RECORDING_ID"]
                frame_no = meta["FRAME_NO"]
                frame_dur = meta["FRAME_DURATION_IN_MS"]
                overlap = meta["COUNTDOWN_OVERLAP_MS"]

                rec_dir = Path(ACTIVE_RECORDINGS[rec_id]["TEMP_AUDIO_STREAM_DIRECTORY"])
                out_path = rec_dir / f"{int(frame_no):08d}.m4a"
                out_path.write_bytes(data)
                LOG_TO_CONSOLE("Saved frame", {"RECORDING_ID": rec_id, "FRAME_NO": frame_no, "path": str(out_path)})

                if not ACTIVE_RECORDINGS[rec_id]["FIRST_FRAME_RECEIVED"]:
                    ACTIVE_RECORDINGS[rec_id]["FIRST_FRAME_RECEIVED"] = True

                frame_start_ms = int(frame_no) * int(frame_dur)
                frame_end_ms = frame_start_ms + int(frame_dur) - 1

                await PROCESS_AUDIO_STREAM(
                    RECORDING_ID=rec_id,
                    FRAME_NO=int(frame_no),
                    FRAME_START_MS=frame_start_ms,
                    FRAME_END_MS=frame_end_ms,
                    FRAME_DURATION_IN_MS=int(frame_dur),
                    COUNTDOWN_OVERLAP_MS=int(overlap or 0),
                    AUDIO_STREAM_FILE_PATH=str(out_path),
                )

                next_expected = NEXT_EXPECTED.get(rec_id, 0)
                if frame_no == next_expected:
                    NEXT_EXPECTED[rec_id] = next_expected + 1

                await WEBSOCKET_SEND_JSON(ws, {
                    "type": "ACK",
                    "RECORDING_ID": rec_id,
                    "FRAME_NO": frame_no,
                    "NEXT_EXPECTED_FRAME_NO": NEXT_EXPECTED.get(rec_id, 0),
                    "MISSING_FRAMES": [],
                })

            else:
                # ping/empty
                pass

    except WebSocketDisconnect:
        LOG_TO_CONSOLE("WebSocketDisconnect")
    except Exception as exc:
        LOG_TO_CONSOLE("Listener exception", str(exc))
        await WEBSOCKET_SEND_JSON(ws, {"type": "ERROR", "reason": str(exc)})
    finally:
        PENDING_META.pop(ws, None)
        LOG_TO_CONSOLE("WEBSOCKET_RECEIVE_INCOMING_DATA finalized for this socket")

# Dev run:
# uvicorn SERVER_ENGINE_AUDIO_STREAM_LISTENER:APP --host 0.0.0.0 --port 7070 --reload
if __name__ == "__main__":
    import uvicorn
    LOG_TO_CONSOLE("Start function __main__ → uvicorn.run")
    uvicorn.run("SERVER_ENGINE_AUDIO_STREAM_LISTENER:APP", host="0.0.0.0", port=7070, reload=True)
