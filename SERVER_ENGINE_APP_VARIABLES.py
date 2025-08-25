# SERVER_ENGINE_APP_VARIABLES.py
from pathlib import Path
from typing import TypedDict, NotRequired, Optional, Dict, Any, List, Literal
# or just:
# from typing import Literal

try:
    from typing import TypedDict, Required, NotRequired  # py3.11+
except ImportError:  # pragma: no cover
    from typing_extensions import TypedDict, Required, NotRequired

import datetime  # for datetime.datetime
import os

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
import numpy as np
from numpy.typing import NDArray

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
# Audio Frame Alignment System
# ─────────────────────────────────────────────────────────────

# Audio frame alignment constants
AUDIO_FRAME_MS = 100  # Target frame size in milliseconds
AUDIO_SAMPLE_RATE = 44100  # Target sample rate
AUDIO_BYTES_PER_SAMPLE = 2  # PCM16 = 2 bytes per sample
AUDIO_SAMPLES_PER_FRAME = (AUDIO_FRAME_MS * AUDIO_SAMPLE_RATE) // 1000  # 4410 samples per 100ms frame
AUDIO_BYTES_PER_FRAME = AUDIO_SAMPLES_PER_FRAME * AUDIO_BYTES_PER_SAMPLE  # 8820 bytes per 100ms frame

# Audio frame alignment buffers (per recording)
# Key: RECORDING_ID, Value: AudioFrameAlignmentBuffer
AUDIO_FRAME_ALIGNMENT_BUFFERS: Dict[int, 'AudioFrameAlignmentBuffer'] = {}

# ─────────────────────────────────────────────────────────────
# TypedDicts
# ─────────────────────────────────────────────────────────────

class ENGINE_DB_LOG_RECORDING_CONFIG_DICT(TypedDict):
    RECORDING_ID: Required[int]           # bigint
    DT_RECORDING_START: NotRequired[Optional[datetime.datetime]]
    DT_RECORDING_END: NotRequired[Optional[datetime.datetime]]
    DT_RECORDING_DATA_QUEDED_FOR_PURGING: NotRequired[Optional[datetime.datetime]] 
    DT_RECORDING_DATA_PURGED: NotRequired[Optional[datetime.datetime]] 
    COMPOSE_PLAY_OR_PRACTICE: NotRequired[Optional[str]]
    AUDIO_STREAM_FILE_NAME: NotRequired[Optional[str]]
    COMPOSE_YN_RUN_FFT: NotRequired[Optional[str]]
    WEBSOCKET_CONNECTION_ID: Optional[int]
    
class ENGINE_DB_LOG_WEBSOCKET_MESSAGE_DICT(TypedDict):
    RECORDING_ID: Optional[int]
    MESSAGE_TYPE: NotRequired[Optional[str]]
    AUDIO_FRAME_NO: Optional[int]
    DT_MESSAGE_RECEIVED: NotRequired[Optional[datetime.datetime]]
    DT_MESSAGE_PROCESS_QUEDED_TO_START: NotRequired[Optional[datetime.datetime]]
    DT_MESSAGE_PROCESS_STARTED: NotRequired[Optional[datetime.datetime]]
    WEBSOCKET_CONNECTION_ID: Optional[int]
    MESSAGE_ID: Optional[int]


class ENGINE_DB_LOG_WEBSOCKET_CONNECTION_DICT(TypedDict):
    WEBSOCKET_CONNECTION_ID: Optional[int]
    CLIENT_HOST_IP_ADDRESS: NotRequired[Optional[str]]
    CLIENT_PORT: NotRequired[Optional[str]]
    CLIENT_HEADERS: NotRequired[Optional[str]]
    DT_CONNECTION_REQUEST: NotRequired[Optional[datetime.datetime]]
    DT_CONNECTION_ACCEPTED: NotRequired[Optional[datetime.datetime]]
    DT_CONNECTION_CLOSED: NotRequired[Optional[datetime.datetime]]
    DT_WEBSOCKET_DISCONNECT_EVENT: NotRequired[Optional[datetime.datetime]]


class WEBSOCKET_AUDIO_FRAME_DICT(TypedDict):
    RECORDING_ID: Optional[int]
    AUDIO_FRAME_NO: Optional[int]
    # --- memory-only payload (do NOT persist) ---
    AUDIO_FRAME_BYTES: NotRequired[bytes]
    AUDIO_ARRAY_16000: NotRequired[NDArray[np.float32]]       # mono float32 @ 16k
    AUDIO_ARRAY_22050: NotRequired[NDArray[np.float32]]       # mono float32 @ 22.05k

class ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_DICT(TypedDict):
    RECORDING_ID: Optional[int]
    AUDIO_FRAME_NO: Optional[int]
    START_MS: Optional[int]
    END_MS: Optional[int]

    # --- memory-only payload (do NOT persist) ---
    AUDIO_FRAME_SIZE_BYTES: NotRequired[int]
    AUDIO_FRAME_ENCODING: NotRequired[Literal["raw", "pcm16", "base64", "hex"]]
    AUDIO_FRAME_SHA256_HEX: NotRequired[str]
    YN_RUN_FFT: NotRequired[Optional[str]]
    YN_RUN_ONS: NotRequired[Optional[str]]
    YN_RUN_PYIN: NotRequired[Optional[str]]
    YN_RUN_CREPE: NotRequired[Optional[str]]
    DT_FRAME_RECEIVED: NotRequired[Optional[datetime.datetime]]
    DT_FRAME_PAIRED_WITH_WEBSOCKETS_METADATA: NotRequired[Optional[datetime.datetime]]
    DT_FRAME_DECODED_FROM_BASE64_TO_BYTES: NotRequired[Optional[datetime.datetime]]

    DT_FRAME_DECODED_FROM_BYTES_INTO_AUDIO_SAMPLES: NotRequired[Optional[datetime.datetime]]
    DT_FRAME_RESAMPLED_TO_44100: NotRequired[Optional[datetime.datetime]]

    DT_FRAME_CONVERTED_TO_PCM16_WITH_SAMPLE_RATE_44100: NotRequired[Optional[datetime.datetime]]
    DT_FRAME_APPENDED_TO_RAW_FILE: NotRequired[Optional[datetime.datetime]]
    DT_FRAME_RESAMPLED_TO_16000: NotRequired[Optional[datetime.datetime]]
    DT_FRAME_RESAMPLED_22050: NotRequired[Optional[datetime.datetime]]
    DT_PROCESSING_QUEDED_TO_START: NotRequired[Optional[datetime.datetime]]
    DT_PROCESSING_START: NotRequired[Optional[datetime.datetime]]
    DT_PROCESSING_END: NotRequired[Optional[datetime.datetime]]
    DT_START_FFT: NotRequired[Optional[datetime.datetime]]
    DT_END_FFT: NotRequired[Optional[datetime.datetime]]
    DT_START_ONS: NotRequired[Optional[datetime.datetime]]
    DT_END_ONS: NotRequired[Optional[datetime.datetime]]
    DT_START_PYIN: NotRequired[Optional[datetime.datetime]]
    DT_END_PYIN: NotRequired[Optional[datetime.datetime]]
    DT_START_CREPE: NotRequired[Optional[datetime.datetime]]
    DT_END_CREPE: NotRequired[Optional[datetime.datetime]]
    DT_START_VOLUME_1_MS: NotRequired[Optional[datetime.datetime]]
    DT_END_VOLUME_1_MS: NotRequired[Optional[datetime.datetime]]
    DT_START_VOLUME_10_MS: NotRequired[Optional[datetime.datetime]]
    DT_END_VOLUME_10_MS: NotRequired[Optional[datetime.datetime]]
    FFT_RECORD_CNT: Optional[int]
    ONS_RECORD_CNT: Optional[int]
    PYIN_RECORD_CNT: Optional[int]
    CREPE_RECORD_CNT: Optional[int]
    VOLUME_1_MS_RECORD_CNT: Optional[int]
    VOLUME_10_MS_RECORD_CNT: Optional[int]


class ENGINE_DB_LOG_STEPS_DICT(TypedDict):
    STEP_NAME: NotRequired[Optional[str]]
    PYTHON_FUNCTION_NAME: NotRequired[Optional[str]]
    PYTHON_FILE_NAME: NotRequired[Optional[str]]
    RECORDING_ID: Optional[int]
    AUDIO_CHUNK_NO: Optional[int]
    FRAME_NO: Optional[int]
    DT_STEP_CALLED: NotRequired[Optional[datetime.datetime]]
    STEP_ID: Optional[int]

# ─────────────────────────────────────────────────────────────
# Global in-memory stores
# ─────────────────────────────────────────────────────────────
# One config per Websocket Connection (bigint)
ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY: Dict[int, ENGINE_DB_LOG_RECORDING_CONFIG_DICT] = {}  #int = RECORDING_ID
ENGINE_DB_LOG_STEPS_ARRAY: Dict[int, ENGINE_DB_LOG_STEPS_DICT] = {}  #int = STEP_ID
ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY: Dict[int, Dict[int, ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_DICT]] = {}  #int = RECORDING_ID, AUDIO_FRAME_NO
ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY: Dict[int, ENGINE_DB_LOG_WEBSOCKET_CONNECTION_DICT] = {}  #int = WEBSOCKET_CONNECTION_ID
ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY: Dict[int, ENGINE_DB_LOG_WEBSOCKET_MESSAGE_DICT] = {}  #int = MESSAGE_ID
WEBSOCKET_AUDIO_FRAME_ARRAY: Dict[int, Dict[int, WEBSOCKET_AUDIO_FRAME_DICT]] = {}  #int = RECORDING_ID, AUDIO_FRAME_NO


# ─────────────────────────────────────────────────────────────
# Audio Frame Alignment Buffer Class
# ─────────────────────────────────────────────────────────────

class AudioFrameAlignmentBuffer:
    """
    Manages audio frame alignment for a single recording.
    Accumulates variable-length audio chunks and produces exact 100ms frames.
    """
    
    def __init__(self, recording_id: int):
        self.recording_id = recording_id
        self.audio_buffer = bytearray()  # Accumulated audio bytes
        self.frame_counter = 1  # Next frame number to produce
        self.total_bytes_received = 0
        self.total_frames_produced = 0
        self.last_frame_time = None
        
    def add_audio_chunk(self, audio_bytes: bytes, frame_no: int) -> list[tuple[int, bytes]]:
        """
        Add a new audio chunk and return any complete frames that can be produced.
        Returns: list of (frame_no, frame_bytes) tuples
        """
        self.audio_buffer.extend(audio_bytes)
        self.total_bytes_received += len(audio_bytes)
        
        complete_frames = []
        
        # Keep producing frames while we have enough data
        while len(self.audio_buffer) >= AUDIO_BYTES_PER_FRAME:
            # Extract exactly one frame
            frame_bytes = bytes(self.audio_buffer[:AUDIO_BYTES_PER_FRAME])
            self.audio_buffer = self.audio_buffer[AUDIO_BYTES_PER_FRAME:]
            
            # Create frame with proper numbering
            frame_no = self.frame_counter
            self.frame_counter += 1
            self.total_frames_produced += 1
            
            complete_frames.append((frame_no, frame_bytes))
            
        return complete_frames
    
    def get_buffer_status(self) -> dict:
        """Get current buffer status for debugging"""
        return {
            "recording_id": self.recording_id,
            "buffer_bytes": len(self.audio_buffer),
            "buffer_samples": len(self.audio_buffer) // AUDIO_BYTES_PER_SAMPLE,
            "buffer_ms": (len(self.audio_buffer) // AUDIO_BYTES_PER_SAMPLE * 1000) // AUDIO_SAMPLE_RATE,
            "buffer_frames": len(self.audio_buffer) / AUDIO_BYTES_PER_FRAME,
            "total_bytes_received": self.total_bytes_received,
            "total_frames_produced": self.total_frames_produced,
            "next_frame_no": self.frame_counter,
            "can_produce_frame": len(self.audio_buffer) >= AUDIO_BYTES_PER_FRAME
        }
    
    def flush_remaining_audio(self) -> list[tuple[int, bytes]]:
        """
        Flush any remaining audio data as the final frame(s).
        This may produce a frame shorter than 100ms.
        """
        if not self.audio_buffer:
            return []
            
        # Pad to even byte length if needed
        if len(self.audio_buffer) % 2 != 0:
            self.audio_buffer.append(0)  # Add padding byte
            
        frame_bytes = bytes(self.audio_buffer)
        frame_no = self.frame_counter
        self.frame_counter += 1
        self.total_frames_produced += 1
        
        self.audio_buffer.clear()
        return [(frame_no, frame_bytes)]

