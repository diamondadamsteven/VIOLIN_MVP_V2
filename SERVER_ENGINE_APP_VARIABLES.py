# SERVER_ENGINE_APP_VARIABLES.py
from pathlib import Path
from typing import Dict, Optional
try:
    from typing import TypedDict, Required, NotRequired  # py3.11+
except ImportError:  # pragma: no cover
    from typing_extensions import TypedDict, Required, NotRequired

import datetime  # for datetime.datetime

# Paths
PROJECT_ROOT_DIR = Path(__file__).resolve().parent

TEMP_RECORDING_AUDIO_DIR = PROJECT_ROOT_DIR / "RECORDING_AUDIO_TEMP"
TEMP_RECORDING_AUDIO_DIR.mkdir(parents=True, exist_ok=True)

RECORDING_AUDIO_DIR = PROJECT_ROOT_DIR / "RECORDING_AUDIO"
RECORDING_AUDIO_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────
# TypedDicts
# ─────────────────────────────────────────────────────────────

class RECORDING_AUDIO_FRAME_DICT(TypedDict):
    RECORDING_ID: Required[int]           # bigint
    FRAME_NO: Required[int]               # int
    DT_FRAME_RECEIVED: NotRequired[Optional[datetime.datetime]]
    DT_FRAME_CONCATENATED_TO_AUDIO_CHUNK: NotRequired[Optional[datetime.datetime]]
    AUDIO_CHUNK_NO: NotRequired[Optional[int]]

class RECORDING_AUDIO_CHUNK_DICT(TypedDict):
    # Required
    RECORDING_ID: Required[int]           # bigint
    AUDIO_CHUNK_NO: Required[int]         # int
    AUDIO_CHUNK_DURATION_IN_MS: Required[int]
    START_MS: Required[int]
    END_MS: Required[int]
    MIN_AUDIO_STREAM_FRAME_NO: Required[int]
    MAX_AUDIO_STREAM_FRAME_NO: Required[int]

    # Optional flags
    YN_RUN_FFT: NotRequired[Optional[str]]
    YN_RUN_ONS: NotRequired[Optional[str]]
    YN_RUN_PYIN: NotRequired[Optional[str]]
    YN_RUN_CREPE: NotRequired[Optional[str]]

    # Optional timing/metrics
    DT_COMPLETE_FRAMES_RECEIVED: NotRequired[Optional[datetime.datetime]]

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

    TOTAL_PROCESSING_DURATION_IN_MS: NotRequired[Optional[int]]

class RECORDING_CONFIG_DICT(TypedDict):
    # Required id; the rest can be filled in as we learn them
    RECORDING_ID: Required[int]           # bigint

    DT_RECORDING_START: NotRequired[Optional[datetime.datetime]]
    VIOLINIST_ID: NotRequired[Optional[int]]
    COMPOSE_PLAY_OR_PRACTICE: NotRequired[Optional[str]]
    AUDIO_STREAM_FILE_NAME: NotRequired[Optional[str]]
    AUDIO_STREAM_FRAME_SIZE_IN_MS: NotRequired[Optional[int]]

    AUDIO_CHUNK_DURATION_IN_MS: NotRequired[Optional[int]]
    CNT_FRAMES_PER_AUDIO_CHUNK: NotRequired[Optional[int]]
    YN_RUN_FFT: NotRequired[Optional[str]]

# ─────────────────────────────────────────────────────────────
# Global in-memory stores
# ─────────────────────────────────────────────────────────────
# One config per recording_id (bigint)
RECORDING_CONFIG_ARRAY: Dict[int, RECORDING_CONFIG_DICT] = {}

# Many chunks per recording_id: access via RECORDING_AUDIO_CHUNK_ARRAY[RECORDING_ID][AUDIO_CHUNK_NO]
RECORDING_AUDIO_CHUNK_ARRAY: Dict[int, Dict[int, RECORDING_AUDIO_CHUNK_DICT]] = {}

# Many frames per recording_id: access via RECORDING_AUDIO_FRAME_ARRAY[RECORDING_ID][FRAME_NO]
RECORDING_AUDIO_FRAME_ARRAY: Dict[int, Dict[int, RECORDING_AUDIO_FRAME_DICT]] = {}
