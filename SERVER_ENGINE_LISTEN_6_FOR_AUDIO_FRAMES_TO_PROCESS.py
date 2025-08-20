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
        schedule_coro(PROCESS_THE_AUDIO_FRAME(RECORDING_ID=RECORDING_ID, AUDIO_FRAME_NO=AUDIO_FRAME_NO))


# ─────────────────────────────────────────────────────────────
# Worker: process a single frame (run analyzers in parallel)
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def PROCESS_THE_AUDIO_FRAME(RECORDING_ID: int, AUDIO_FRAME_NO: int) -> None:
    """
    Run per-frame analyzers in parallel:
      • FFT (22.05k), PYIN (22.05k), CREPE (16k),
        VOLUME_1_MS (22.05k), VOLUME_10_MS (22.05k)

    Each analyzer stamps its own DT_START_*/DT_END_* and *_RECORD_CNT.
    This wrapper stamps DT_PROCESSING_START/END and frees volatile arrays.
    """
    # Durable per-frame metadata (must already exist from Stage-3B)
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD = ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD["DT_PROCESSING_START"] = datetime.now()

    # Volatile per-frame buffers (created in Stage-3B)
    WEBSOCKET_AUDIO_FRAME_RECORD = WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]

    # Analyzer inputs prepared by Stage-3B
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

    # Always compute volume metrics if 22.05k exists
    tasks.append(asyncio.create_task(
        SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME_1_MS(
            int(RECORDING_ID),
            int(AUDIO_FRAME_NO),
            AUDIO_ARRAY_22050  # 22.05k
        )
    ))
    tasks.append(asyncio.create_task(
        SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME_10_MS(
            int(RECORDING_ID),
            int(AUDIO_FRAME_NO),
            AUDIO_ARRAY_22050  # 22.05k
        )
    ))

    # Run in parallel; analyzer decorators will centrally log Start/End/Error.
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

    # Free volatile arrays; keep the per-frame dict for traceability
    WEBSOCKET_AUDIO_FRAME_RECORD.pop("AUDIO_ARRAY_16000", None)
    WEBSOCKET_AUDIO_FRAME_RECORD.pop("AUDIO_ARRAY_22050", None)

    CONSOLE_LOG(PREFIX, "frame_done", {
        "rid": int(RECORDING_ID),
        "frame": int(AUDIO_FRAME_NO),
    })
