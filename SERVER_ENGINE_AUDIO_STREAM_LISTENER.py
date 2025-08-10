# SERVER_ENGINE_AUDIO_STREAM_LISTENER.py
# ------------------------------------------------------------
# FastAPI listener for VIOLIN_MVP audio streaming (HTTP + WebSocket)
# - STEP_1_START_RECORDING (HTTP): initialize recording runtime + temp folder
# - STEP_2_RECEIVE_AUDIO_CHUNK_AND_SAVE_TO_FILE (HTTP): save a micro-chunk, queue processing
# - STEP_3_STOP_RECORDING (HTTP): finalize a take
# - WS /ws/stream/{RECORDING_ID}: receive binary frames with header+audio, ACK with gap info
#
# All variables/functions authored here use ALL_CAPS naming for clarity.
# ------------------------------------------------------------

import os
import json
import struct
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
from collections import deque

import uvicorn
from fastapi import (
    FastAPI, UploadFile, File, Form, BackgroundTasks, HTTPException, WebSocket, WebSocketDisconnect
)
from fastapi.middleware.cors import CORSMiddleware

from SERVER_ENGINE_AUDIO_STREAM_PROCESSOR import (
    PROCESS_AUDIO_STREAM,
    PROCESS_STOP_RECORDING,
)

# =========================
# CONFIG
# =========================
PORT = int(os.getenv("ENGINE_PORT", "7070"))
TMP_ROOT = Path(os.getenv("ENGINE_TMP_DIR", "./ENGINE_TMP")).resolve()
TMP_ROOT.mkdir(parents=True, exist_ok=True)

# How many frames to keep in the rolling window for gap detection (WS ACK)
GAP_WINDOW = 512
MISSING_REPORT_MAX = 64  # limit how many missing frame numbers we echo back per ACK

# =========================
# APP INIT
# =========================
APP = FastAPI(title="VIOLIN_MVP Audio Engine (Listener)")
APP.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# RUNTIME STATE (in-memory)
# =========================
# Transient info for in-progress recordings
# Key: RECORDING_ID (str)
# Value: {
#   "STREAMED_CHUNK_DURATION_IN_MS": int (duplicated in RUNTIME cache, but handy here),
#   "TEMP_AUDIO_STREAM_DIRECTORY": str,
#   "AUDIO_CHUNK_START_DT": datetime,
#   "HIGHEST_FRAME_NO_SEEN": int,        # for WS ACK gap tracking
#   "RECEIVED_FRAMES": deque[int],       # last GAP_WINDOW frame numbers seen
# }
ACTIVE_RECORDINGS: Dict[str, Dict[str, Any]] = {}

# Lightweight runtime cache for things the processor needs (avoid hard-coding)
# Key: RECORDING_ID (str)
# Value: {
#   "STREAMED_CHUNK_DURATION_IN_MS": int
# }
RUNTIME_BY_RECORDING_ID: Dict[str, Dict[str, Any]] = {}


def _GET_OR_INIT_WS_STATE(RECORDING_ID: str) -> Dict[str, Any]:
    R = ACTIVE_RECORDINGS[RECORDING_ID]
    if "HIGHEST_FRAME_NO_SEEN" not in R:
        R["HIGHEST_FRAME_NO_SEEN"] = -10**9  # very small sentinel
    if "RECEIVED_FRAMES" not in R:
        R["RECEIVED_FRAMES"] = deque(maxlen=GAP_WINDOW)
    return R


# =========================
# STEP 1: START (HTTP)
# =========================
@APP.post("/STEP_1_START_RECORDING")
async def STEP_1_START_RECORDING(
    RECORDING_ID: int = Form(...),
    STREAMED_CHUNK_DURATION_IN_MS: int = Form(...),
):
    """
    Initialize a new (or resumed) recording session's temp storage + runtime config.
    The processor will query DB for mode/BPM/etc using RECORDING_ID.
    """
    L_RECORDING_ID = str(RECORDING_ID)

    TEMP_AUDIO_STREAM_DIRECTORY = TMP_ROOT / L_RECORDING_ID
    TEMP_AUDIO_STREAM_DIRECTORY.mkdir(parents=True, exist_ok=True)

    AUDIO_CHUNK_START_DT = datetime.utcnow()

    ACTIVE_RECORDINGS[L_RECORDING_ID] = {
        "STREAMED_CHUNK_DURATION_IN_MS": int(STREAMED_CHUNK_DURATION_IN_MS),
        "TEMP_AUDIO_STREAM_DIRECTORY": str(TEMP_AUDIO_STREAM_DIRECTORY),
        "AUDIO_CHUNK_START_DT": AUDIO_CHUNK_START_DT,
        # init WS gap tracking fields lazily
    }

    # Cache runtime for processor (so it can read duration without hard-coding)
    RUNTIME_BY_RECORDING_ID[L_RECORDING_ID] = {
        "STREAMED_CHUNK_DURATION_IN_MS": int(STREAMED_CHUNK_DURATION_IN_MS),
    }

    return {
        "RECORDING_ID": L_RECORDING_ID,
        "STREAMED_CHUNK_DURATION_IN_MS": ACTIVE_RECORDINGS[L_RECORDING_ID]["STREAMED_CHUNK_DURATION_IN_MS"],
        "TEMP_AUDIO_STREAM_DIRECTORY": ACTIVE_RECORDINGS[L_RECORDING_ID]["TEMP_AUDIO_STREAM_DIRECTORY"],
    }


# =========================
# STEP 2: RECEIVE CHUNK (HTTP)
# =========================
@APP.post("/STEP_2_RECEIVE_AUDIO_CHUNK_AND_SAVE_TO_FILE/{RECORDING_ID}")
async def STEP_2_RECEIVE_AUDIO_CHUNK_AND_SAVE_TO_FILE(
    RECORDING_ID: str,
    STREAMED_CHUNK_NO: int = Form(...),                       # negative during countdown, 0 at time zero
    AUDIO_CHUNK_DATA: UploadFile = File(...),                 # .m4a bytes (AAC) for this micro-chunk
    FAST_API_BACKGROUND_TASK: BackgroundTasks = None,         # to queue processing without blocking
    # Optional boundary precision fields:
    COUNTDOWN_ZERO_IN_THIS_CHUNK: Optional[str] = Form(default=None),          # 'Y' if this chunk crosses t=0
    COUNTDOWN_ZERO_OFFSET_MS_IN_CHUNK: Optional[int] = Form(default=None),     # 0..(DURATION-1)
):
    """
    Save one chunk via HTTP and schedule processing. WS is preferred for lower overhead,
    but this remains for compatibility and debugging.
    """
    if RECORDING_ID not in ACTIVE_RECORDINGS:
        raise HTTPException(status_code=404, detail="Unknown RECORDING_ID")

    TEMP_AUDIO_STREAM_DIRECTORY = ACTIVE_RECORDINGS[RECORDING_ID]["TEMP_AUDIO_STREAM_DIRECTORY"]
    AUDIO_STREAM_FILE_PATH = Path(TEMP_AUDIO_STREAM_DIRECTORY) / f"{STREAMED_CHUNK_NO:06d}.m4a"

    # Write bytes
    AUDIO_STREAM_FILE_PATH.write_bytes(await AUDIO_CHUNK_DATA.read())

    # Queue processing
    if FAST_API_BACKGROUND_TASK is not None:
        FAST_API_BACKGROUND_TASK.add_task(
            PROCESS_AUDIO_STREAM,
            RECORDING_ID=RECORDING_ID,
            STREAMED_CHUNK_NO=STREAMED_CHUNK_NO,
            AUDIO_CHUNK_FILE_PATH=str(AUDIO_STREAM_FILE_PATH),
            COUNTDOWN_ZERO_IN_THIS_CHUNK=COUNTDOWN_ZERO_IN_THIS_CHUNK,
            COUNTDOWN_ZERO_OFFSET_MS_IN_CHUNK=COUNTDOWN_ZERO_OFFSET_MS_IN_CHUNK,
            RUNTIME_INFO=RUNTIME_BY_RECORDING_ID.get(RECORDING_ID, {}),
        )

    # Minimal HTTP response; (we don't do gap ACKs for HTTP path)
    return {
        "RECORDING_ID": RECORDING_ID,
        "STREAMED_CHUNK_NO": STREAMED_CHUNK_NO,
        "SAVED": True,
        "COUNTDOWN_ZERO_IN_THIS_CHUNK": COUNTDOWN_ZERO_IN_THIS_CHUNK,
        "COUNTDOWN_ZERO_OFFSET_MS_IN_CHUNK": COUNTDOWN_ZERO_OFFSET_MS_IN_CHUNK,
    }


# =========================
# STEP 3: STOP (HTTP)
# =========================
@APP.post("/STEP_3_STOP_RECORDING/{RECORDING_ID}")
async def STEP_3_STOP_RECORDING(RECORDING_ID: str, FAST_API_BACKGROUND_TASK: BackgroundTasks):
    """
    Client signals end of recording. We finalize on the processor side.
    Do NOT create a new RECORDING_ID here; next take will call STEP_1 again when needed.
    """
    if RECORDING_ID not in ACTIVE_RECORDINGS:
        # If it's already cleared, still attempt finalize
        FAST_API_BACKGROUND_TASK.add_task(PROCESS_STOP_RECORDING, RECORDING_ID=RECORDING_ID)
        return {"RECORDING_ID": RECORDING_ID, "STOPPED": True, "NOTE": "Unknown in ACTIVE_RECORDINGS, finalize anyway."}

    FAST_API_BACKGROUND_TASK.add_task(PROCESS_STOP_RECORDING, RECORDING_ID=RECORDING_ID)

    # Optional: clear immediately; or delay a bit to allow in-flight frames
    del ACTIVE_RECORDINGS[RECORDING_ID]
    # Keep runtime cache around for a short while if desired; for now remove:
    RUNTIME_BY_RECORDING_ID.pop(RECORDING_ID, None)

    return {"RECORDING_ID": RECORDING_ID, "STOPPED": True}


# =========================
# WEBSOCKET: STREAM FRAMES
# =========================
@APP.websocket("/ws/stream/{RECORDING_ID}")
async def WS_STREAM(RECORDING_ID: str, websocket: WebSocket):
    """
    WebSocket streaming:
    Each message = [2-byte big-endian header length][UTF-8 JSON header][binary audio bytes].
    Header fields:
      - FRAME_NO (int): negative during countdown, 0 at time zero, then 1,2,...
      - COUNTDOWN_ZERO_IN_THIS_CHUNK: 'Y' | omitted
      - COUNTDOWN_ZERO_OFFSET_MS_IN_CHUNK: int 0..(DURATION-1) | omitted
    We save as {FRAME_NO:06d}.m4a then queue PROCESS_AUDIO_STREAM().
    We reply with an ACK JSON containing NEXT_EXPECTED_FRAME_NO and MISSING_FRAMES (rolling window).
    """
    await websocket.accept()

    if RECORDING_ID not in ACTIVE_RECORDINGS:
        await websocket.close(code=4404)  # Not found
        return

    STATE = _GET_OR_INIT_WS_STATE(RECORDING_ID)
    TEMP_AUDIO_STREAM_DIRECTORY = ACTIVE_RECORDINGS[RECORDING_ID]["TEMP_AUDIO_STREAM_DIRECTORY"]

    try:
        while True:
            DATA: bytes = await websocket.receive_bytes()
            if len(DATA) < 2:
                continue

            HEADER_LEN = struct.unpack(">H", DATA[:2])[0]
            if len(DATA) < 2 + HEADER_LEN:
                continue

            HEADER_BYTES = DATA[2:2+HEADER_LEN]
            AUDIO_BYTES = DATA[2+HEADER_LEN:]

            # Parse header JSON
            try:
                HEADER = json.loads(HEADER_BYTES.decode("utf-8"))
            except Exception:
                continue

            FRAME_NO = int(HEADER.get("FRAME_NO"))
            COUNTDOWN_ZERO_IN_THIS_CHUNK = HEADER.get("COUNTDOWN_ZERO_IN_THIS_CHUNK")
            COUNTDOWN_ZERO_OFFSET_MS_IN_CHUNK = HEADER.get("COUNTDOWN_ZERO_OFFSET_MS_IN_CHUNK")

            # Save frame as file
            AUDIO_STREAM_FILE_PATH = Path(TEMP_AUDIO_STREAM_DIRECTORY) / f"{FRAME_NO:06d}.m4a"
            AUDIO_STREAM_FILE_PATH.write_bytes(AUDIO_BYTES)

            # Queue processing
            asyncio.create_task(
                PROCESS_AUDIO_STREAM(
                    RECORDING_ID=RECORDING_ID,
                    STREAMED_CHUNK_NO=FRAME_NO,
                    AUDIO_CHUNK_FILE_PATH=str(AUDIO_STREAM_FILE_PATH),
                    COUNTDOWN_ZERO_IN_THIS_CHUNK=COUNTDOWN_ZERO_IN_THIS_CHUNK,
                    COUNTDOWN_ZERO_OFFSET_MS_IN_CHUNK=COUNTDOWN_ZERO_OFFSET_MS_IN_CHUNK,
                    RUNTIME_INFO=RUNTIME_BY_RECORDING_ID.get(RECORDING_ID, {}),
                )
            )

            # ----- GAP TRACKING / ACK -----
            STATE["RECEIVED_FRAMES"].append(FRAME_NO)
            if FRAME_NO > STATE["HIGHEST_FRAME_NO_SEEN"]:
                STATE["HIGHEST_FRAME_NO_SEEN"] = FRAME_NO

            HIGH = STATE["HIGHEST_FRAME_NO_SEEN"]
            LOW = HIGH - (GAP_WINDOW - 1)
            HAVE = set(STATE["RECEIVED_FRAMES"])

            # Find up to MISSING_REPORT_MAX missing frames in the recent range
            MISSING = []
            n = HIGH
            cnt = 0
            while n >= LOW and cnt < MISSING_REPORT_MAX:
                if n not in HAVE:
                    MISSING.append(n)
                n -= 1
                cnt += 1
            MISSING.sort()

            ACK = {
                "OK": True,
                "FRAME_NO": FRAME_NO,
                "NEXT_EXPECTED_FRAME_NO": STATE["HIGHEST_FRAME_NO_SEEN"] + 1,
                "MISSING_FRAMES": MISSING,
            }
            await websocket.send_json(ACK)

    except WebSocketDisconnect:
        # Client closed WS
        pass


# =========================
# OPTIONAL: RUNTIME HELPER
# =========================
@APP.get("/RUNTIME_INFO/{RECORDING_ID}")
async def GET_RUNTIME_INFO(RECORDING_ID: str):
    """
    Optional helper for diagnostics; shows cached runtime info.
    """
    return {
        "ACTIVE": RECORDING_ID in ACTIVE_RECORDINGS,
        "ACTIVE_RECORDING": ACTIVE_RECORDINGS.get(RECORDING_ID),
        "RUNTIME_INFO": RUNTIME_BY_RECORDING_ID.get(RECORDING_ID),
    }


# =========================
# ENTRYPOINT
# =========================
if __name__ == "__main__":
    uvicorn.run(APP, host="0.0.0.0", port=PORT, log_level="info")
