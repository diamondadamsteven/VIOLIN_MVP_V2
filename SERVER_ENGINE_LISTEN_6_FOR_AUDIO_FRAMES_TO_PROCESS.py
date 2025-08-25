# SERVER_ENGINE_LISTEN_6_FOR_AUDIO_FRAMES_TO_PROCESS.py
from __future__ import annotations

import asyncio
from datetime import datetime

from SERVER_ENGINE_APP_VARIABLES import (
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY,  # durable: per-frame metadata (no bytes/arrays)
    WEBSOCKET_AUDIO_FRAME_ARRAY,                # volatile: per-frame bytes/arrays
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    ENGINE_DB_LOG_FUNCTIONS_INS,
    CONSOLE_LOG,
    schedule_coro,   # loop/thread-safe scheduler
)

# Per-frame analyzers (all async)
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT import SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN import SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE import SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME_1_MS import SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME_1_MS
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME_10_MS import SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME_10_MS

PREFIX = "STAGE6_FRAMES"


# ─────────────────────────────────────────────────────────────
# Scanner: queue frames that are ready to analyze
# ─────────────────────────────────────────────────────────────
def SERVER_ENGINE_LISTEN_6_FOR_AUDIO_FRAMES_TO_PROCESS() -> None:
    """
    Find frames not yet queued (DT_PROCESSING_QUEDED_TO_START is NULL),
    stamp the queue time, and schedule PROCESS_THE_AUDIO_FRAME.

    Assumes Stage-3B has already:
      • created/updated ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY (durable metadata)
      • populated WEBSOCKET_AUDIO_FRAME_ARRAY with analyzer inputs
    """
    to_launch = [
        (int(RECORDING_ID), int(AUDIO_FRAME_NO))
        for RECORDING_ID, META_BY_FRAME_NO in ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY.items()
        for AUDIO_FRAME_NO, FRAME_META in META_BY_FRAME_NO.items()
        if FRAME_META.get("DT_PROCESSING_QUEDED_TO_START") is None
    ]

    for RECORDING_ID, AUDIO_FRAME_NO in to_launch:
        ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_PROCESSING_QUEDED_TO_START"] = datetime.now()
        CONSOLE_LOG(PREFIX, "queuing_frame_for_analysis", {
            "rid": RECORDING_ID,
            "frame": AUDIO_FRAME_NO,
            "note": "Audio arrays ready, queuing for analysis"
        })
        schedule_coro(PROCESS_THE_AUDIO_FRAME(RECORDING_ID=RECORDING_ID, AUDIO_FRAME_NO=AUDIO_FRAME_NO))


# ─────────────────────────────────────────────────────────────
# Worker: process a single frame (run analyzers in parallel)
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def PROCESS_THE_AUDIO_FRAME(RECORDING_ID: int, AUDIO_FRAME_NO: int) -> None:
    """
    PROCESS AUDIO FRAME:
      1) Mark DT_PROCESSING_START
      2) Get audio arrays from volatile store
      3) Run analyzers (FFT, PYIN, CREPE) based on gating flags
      4) Mark DT_PROCESSING_END
    """
    # 1) Mark processing started
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD = ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD["DT_PROCESSING_START"] = datetime.now()

    # 2) Get audio arrays from volatile store (SIMPLE - no complex checks)
    WEBSOCKET_AUDIO_FRAME_RECORD = WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]
    AUDIO_ARRAY_22050 = WEBSOCKET_AUDIO_FRAME_RECORD["AUDIO_ARRAY_22050"]
    AUDIO_ARRAY_16000 = WEBSOCKET_AUDIO_FRAME_RECORD["AUDIO_ARRAY_16000"]

    # Per-frame gating flags (set in Stage-3A/3B depending on mode)
    YN_RUN_FFT   = ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD.get("YN_RUN_FFT", "Y")
    YN_RUN_PYIN  = ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD.get("YN_RUN_PYIN", "Y")
    YN_RUN_CREPE = ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD.get("YN_RUN_CREPE", "Y")

    tasks: list[asyncio.Task] = []

    if YN_RUN_FFT == "Y":
        tasks.append(asyncio.create_task(
            SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT(
                int(RECORDING_ID),
                int(AUDIO_FRAME_NO),
                AUDIO_ARRAY_22050  # 22.05k
            )
        ))

    if YN_RUN_PYIN == "Y":
        tasks.append(asyncio.create_task(
            SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN(
                int(RECORDING_ID),
                int(AUDIO_FRAME_NO),
                AUDIO_ARRAY_22050  # 22.05k
            )
        ))

    if YN_RUN_CREPE == "Y":
        tasks.append(asyncio.create_task(
            SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE(
                int(RECORDING_ID),
                int(AUDIO_FRAME_NO),
                AUDIO_ARRAY_16000  # 16k
            )
        ))

    # Wait for all tasks to complete
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    # 4) Mark processing completed
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD["DT_PROCESSING_END"] = datetime.now()
