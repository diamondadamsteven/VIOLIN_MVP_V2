# SERVER_ENGINE_LISTEN_6_FOR_AUDIO_FRAMES_TO_PROCESS.py
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Dict, Any

import numpy as np

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
    Find frames that haven't been queued yet, stamp the queue time, and schedule
    PROCESS_THE_AUDIO_FRAME. We assume Stage-3B already populated both:
      • ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY (durable metadata)
      • WEBSOCKET_AUDIO_FRAME_ARRAY (volatile arrays)
    """
    PROCESS_THE_AUDIO_FRAME_ARRAY: list[tuple[int, int]] = []

    for RECORDING_ID, ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY_2 in ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY.items():
        for AUDIO_FRAME_NO, ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD in ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY_2.items():
            if ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD.get("DT_PROCESSING_QUEDED_TO_START") is None:
                PROCESS_THE_AUDIO_FRAME_ARRAY.append((int(RECORDING_ID), int(AUDIO_FRAME_NO)))

    for RECORDING_ID, AUDIO_FRAME_NO in PROCESS_THE_AUDIO_FRAME_ARRAY:
        ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_PROCESSING_QUEDED_TO_START"] = datetime.now()
        schedule_coro(PROCESS_THE_AUDIO_FRAME(RECORDING_ID=RECORDING_ID, AUDIO_FRAME_NO=AUDIO_FRAME_NO))


# ─────────────────────────────────────────────────────────────
# Worker: process a single frame (run analyzers in parallel)
# ─────────────────────────────────────────────────────────────
async def PROCESS_THE_AUDIO_FRAME(RECORDING_ID: int, AUDIO_FRAME_NO: int) -> None:
    """
    Run per-frame analyzers in parallel:
      • FFT (22.05k), PYIN (22.05k), VOLUME_1_MS (22.05k), VOLUME_10_MS (22.05k), CREPE (16k)
    Each analyzer stamps its own DT_START_*/DT_END_* and *_RECORD_CNT.
    This wrapper stamps DT_PROCESSING_START/END and frees the volatile arrays.
    """
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD = ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD["DT_PROCESSING_START"] = datetime.now()

    WEBSOCKET_AUDIO_FRAME_ARRAY_2 = WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID]
    WEBSOCKET_AUDIO_FRAME_RECORD = WEBSOCKET_AUDIO_FRAME_ARRAY_2[AUDIO_FRAME_NO]

    # Inputs (Stage-3B guarantees these exist)
    AUDIO_ARRAY_22050 = WEBSOCKET_AUDIO_FRAME_RECORD["AUDIO_ARRAY_22050"]
    AUDIO_ARRAY_16000 = WEBSOCKET_AUDIO_FRAME_RECORD["AUDIO_ARRAY_16000"]

    YN_RUN_FFT   = ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD["YN_RUN_FFT"]
    YN_RUN_PYIN  = ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD["YN_RUN_PYIN"]
    YN_RUN_CREPE = ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD["YN_RUN_CREPE"]

    tasks: list[asyncio.Task] = []

    if YN_RUN_FFT=="Y":
        tasks.append(asyncio.create_task(
            SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT(
                int(RECORDING_ID),
                int(AUDIO_FRAME_NO),
                AUDIO_ARRAY_22050
            )
        ))

    if YN_RUN_PYIN=="Y":
        tasks.append(asyncio.create_task(
            SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN(
                int(RECORDING_ID),
                int(AUDIO_FRAME_NO),
                AUDIO_ARRAY_22050,   # 22.05k
            )
        ))

    if YN_RUN_CREPE=="Y":
        tasks.append(asyncio.create_task(
            SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE(
                int(RECORDING_ID),
                int(AUDIO_FRAME_NO),
                AUDIO_ARRAY_16000,   # 16k
            )
        ))

    tasks.append(asyncio.create_task(
        SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME_1_MS(
            int(RECORDING_ID),
            int(AUDIO_FRAME_NO),
            AUDIO_ARRAY_22050,   # 22.05k
        )
    ))

    tasks.append(asyncio.create_task(
        SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME_10_MS(
            int(RECORDING_ID),
            int(AUDIO_FRAME_NO),
            AUDIO_ARRAY_22050,   # 22.05k
        )
    ))

    # Run everything in parallel; surface errors to logs without extra guards
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            CONSOLE_LOG(PREFIX, "analyzer_error", {
                "rid": int(RECORDING_ID),
                "frame": int(AUDIO_FRAME_NO),
                "err": str(r),
            })

    # Stamp end
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD["DT_PROCESSING_END"] = datetime.now()

    # Free volatile arrays; leave the per-frame dict for traceability
    WEBSOCKET_AUDIO_FRAME_RECORD.pop("AUDIO_ARRAY_16000", None)
    WEBSOCKET_AUDIO_FRAME_RECORD.pop("AUDIO_ARRAY_22050", None)

    CONSOLE_LOG(PREFIX, "frame_done", {
        "rid": int(RECORDING_ID),
        "frame": int(AUDIO_FRAME_NO),
    })
