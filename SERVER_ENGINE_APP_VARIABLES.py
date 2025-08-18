# SERVER_ENGINE_APP_VARIABLES.py
from pathlib import Path
from typing import Dict, Optional
try:
    from typing import TypedDict, Required, NotRequired  # py3.11+
except ImportError:  # pragma: no cover
    from typing_extensions import TypedDict, Required, NotRequired

import datetime  # for datetime.datetime
import os

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request

WEBSOCKET_LISTENER = FastAPI(title="VIOLIN_MVP Audio Stream WS Listener", version="1.4.0")

# Paths
PROJECT_ROOT_DIR = Path(__file__).resolve().parent
PROJECT_RECORDINGS_DIR = PROJECT_ROOT_DIR.parent / (PROJECT_ROOT_DIR.name + "_RECORDINGS")

TEMP_RECORDING_AUDIO_DIR = PROJECT_RECORDINGS_DIR / "RECORDING_AUDIO_TEMP"
TEMP_RECORDING_AUDIO_DIR.mkdir(parents=True, exist_ok=True)

RECORDING_AUDIO_DIR = PROJECT_RECORDINGS_DIR / "RECORDING_AUDIO"
RECORDING_AUDIO_DIR.mkdir(parents=True, exist_ok=True)

OAF_IMAGE = os.getenv("OAF_IMAGE", "violin/oaf:latest")
OAF_CONTAINER = os.getenv("OAF_CONTAINER", "violin_oaf_server")
# Host exposes microservice at 127.0.0.1:OAF_PORT -> container:9077
OAF_PORT = int(os.getenv("OAF_PORT", "9077"))


# ─────────────────────────────────────────────────────────────
# TypedDicts
# ─────────────────────────────────────────────────────────────

class RECORDING_AUDIO_FRAME_DICT(TypedDict):
    RECORDING_ID: Required[int]           # bigint
    FRAME_NO: Required[int]               # int
    DT_FRAME_RECEIVED: NotRequired[Optional[datetime.datetime]]
    AUDIO_FRAME_DATA: Optional[bytes] 
    # DT_FRAME_CONCATENATED_TO_AUDIO_CHUNK: NotRequired[Optional[datetime.datetime]]
    #AUDIO_CHUNK_NO: NotRequired[Optional[int]]

class RECORDING_AUDIO_CHUNK_DICT(TypedDict):
    # Required
    RECORDING_ID: Required[int]           # bigint
    AUDIO_CHUNK_NO: Required[int]         # int
    AUDIO_CHUNK_DURATION_IN_MS: Required[int]
    START_MS: Required[int]
    END_MS: Required[int]
    MIN_AUDIO_STREAM_FRAME_NO: Required[int]
    MAX_AUDIO_STREAM_FRAME_NO: Required[int]
    AUDIO_CHUNK_DATA_16K: Optional[bytes]       # ← if truly needed
    AUDIO_CHUNK_DATA_22050: Optional[bytes]

    # Optional flags
    YN_RUN_FFT: NotRequired[Optional[str]]
    YN_RUN_ONS: NotRequired[Optional[str]]
    YN_RUN_PYIN: NotRequired[Optional[str]]
    YN_RUN_CREPE: NotRequired[Optional[str]]

    # Optional timing/metrics
    DT_COMPLETE_FRAMES_RECEIVED: NotRequired[Optional[datetime.datetime]]
    DT_START_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK: NotRequired[Optional[datetime.datetime]]
    DT_COMPLETE_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK: NotRequired[Optional[datetime.datetime]]
    DT_AUDIO_CHUNK_CONVERTED_TO_WAV: NotRequired[Optional[datetime.datetime]]
    DT_AUDIO_CHUNK_WAV_SAVED_TO_FILE: NotRequired[Optional[datetime.datetime]]
    DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_16K: NotRequired[Optional[datetime.datetime]]
    DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_22050: NotRequired[Optional[datetime.datetime]]
    DT_AUDIO_CHUNK_PREPARATION_COMPLETE: NotRequired[Optional[datetime.datetime]]
    DT_START_AUDIO_CHUNK_PROCESS: NotRequired[Optional[datetime.datetime]]
    DT_END_AUDIO_CHUNK_PROCESS: NotRequired[Optional[datetime.datetime]]

    DT_START_FFT: NotRequired[Optional[datetime.datetime]]
    FFT_DURATION_IN_MS: NotRequired[Optional[int]]
    FFT_RECORD_CNT: NotRequired[Optional[int]]

    DT_START_ONS: NotRequired[Optional[datetime.datetime]]
    ONS_DURATION_IN_MS: NotRequired[Optional[int]]
    ONS_RECORD_CNT: NotRequired[Optional[int]]

    DT_START_PYIN: NotRequired[Optional[datetime.datetime]]
    PYIN_DURATION_IN_MS: NotRequired[Optional[int]]
    PYIN_RECORD_CNT: NotRequired[Optional[int]]

    DT_START_CREPE: NotRequired[Optional[datetime.datetime]]
    CREPE_DURATION_IN_MS: NotRequired[Optional[int]]
    CREPE_RECORD_CNT: NotRequired[Optional[int]]

    DT_START_VOLUME: NotRequired[Optional[datetime.datetime]]
    VOLUME_10_MS_DURATION_IN_MS: NotRequired[Optional[int]]
    VOLUME_1_MS_DURATION_IN_MS: NotRequired[Optional[int]]
    VOLUME_10_MS_RECORD_CNT: NotRequired[Optional[int]]
    VOLUME_1_MS_RECORD_CNT: NotRequired[Optional[int]]

    DT_START_P_ENGINE_ALL_MASTER: NotRequired[Optional[datetime.datetime]]
    P_ENGINE_ALL_MASTER_DURATION_IN_MS: NotRequired[Optional[int]]
    DT_ADDED: NotRequired[Optional[datetime.datetime]]

    # TOTAL_PROCESSING_DURATION_IN_MS: NotRequired[Optional[int]]

class RECORDING_CONFIG_DICT(TypedDict):
    # Required id; the rest can be filled in as we learn them
    RECORDING_ID: Required[int]           # bigint
    WEBSOCKET_CONNECTION_ID: NotRequired[Optional[int]]
    DT_RECORDING_START: NotRequired[Optional[datetime.datetime]]
    DT_RECORDING_STOP: NotRequired[Optional[datetime.datetime]]

    VIOLINIST_ID: NotRequired[Optional[int]]
    COMPOSE_PLAY_OR_PRACTICE: NotRequired[Optional[str]]
    AUDIO_STREAM_FILE_NAME: NotRequired[Optional[str]]
    AUDIO_STREAM_FRAME_SIZE_IN_MS: NotRequired[Optional[int]]

    AUDIO_CHUNK_DURATION_IN_MS: NotRequired[Optional[int]]
    CNT_FRAMES_PER_AUDIO_CHUNK: NotRequired[Optional[int]]
    YN_RUN_FFT: NotRequired[Optional[str]]
    COMPOSE_CURRENT_AUDIO_CHUNK_NO: NotRequired[Optional[int]]


class RECORDING_WEBSOCKET_MESSAGE_DICT(TypedDict):
    # Identity
    MESSAGE_ID: int
    RECORDING_ID: int
    MESSAGE_TYPE: str                # e.g. "START", "STOP", "FRAME", "CHUNK"
    AUDIO_FRAME_NO: NotRequired[Optional[int]]  # If this is a FRAME message
    DT_MESSAGE_RECEIVED: datetime.datetime  # when message was received
    DT_MESSAGE_PROCESS_STARTED: NotRequired[Optional[datetime.datetime]]
    WEBSOCKET_CONNECTION_ID: NotRequired[Optional[int]]
    
 
class RECORDING_WEBSOCKET_CONNECTION_DICT(TypedDict):
    # Identity
    WEBSOCKET_CONNECTION_ID: int                        # bigint, unique per connection (incrementing or timestamp-based)
    # RECORDING_ID: NotRequired[Optional[int]]  # If this WS is bound to a recording (set after START)
    CLIENT_HOST_IP_ADDRESS: NotRequired[Optional[str]]   # ws.client.host
    CLIENT_PORT: NotRequired[Optional[int]]   # ws.client.port
    CLIENT_HEADERS: NotRequired[Dict[str, str]]  # optional: origin, user-agent, etc.

    # Lifecycle
    DT_CONNECTION_REQUEST: datetime.datetime  # when client attempted connection
    DT_CONNECTION_ACCEPTED: NotRequired[Optional[datetime.datetime]]  # when ws.accept() succeeded
    DT_CONNECTION_CLOSED: NotRequired[Optional[datetime.datetime]]    # when disconnect/close occurred

# ─────────────────────────────────────────────────────────────
# Global in-memory stores
# ─────────────────────────────────────────────────────────────
# One config per Websocket Connection (bigint)
RECORDING_WEBSOCKET_MESSAGE_ARRAY: Dict[int, RECORDING_WEBSOCKET_MESSAGE_DICT] = {}

# One per Websocket Connection 
RECORDING_WEBSOCKET_CONNECTION_ARRAY: Dict[int, RECORDING_WEBSOCKET_CONNECTION_DICT] = {}

# One config per recording_id (bigint)
RECORDING_CONFIG_ARRAY: Dict[int, RECORDING_CONFIG_DICT] = {}

# Many chunks per recording_id: access via RECORDING_AUDIO_CHUNK_ARRAY[RECORDING_ID][AUDIO_CHUNK_NO]
RECORDING_AUDIO_CHUNK_ARRAY: Dict[int, Dict[int, RECORDING_AUDIO_CHUNK_DICT]] = {}

# Many frames per recording_id: access via RECORDING_AUDIO_FRAME_ARRAY[RECORDING_ID][FRAME_NO]
RECORDING_AUDIO_FRAME_ARRAY: Dict[int, Dict[int, RECORDING_AUDIO_FRAME_DICT]] = {}
