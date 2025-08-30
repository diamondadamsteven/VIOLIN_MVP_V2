# SERVER_ENGINE_LISTEN_6_FOR_AUDIO_FRAMES_TO_PROCESS.py
from __future__ import annotations

import asyncio
from datetime import datetime

from SERVER_ENGINE_APP_VARIABLES import (
    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY,  # durable: per-frame metadata (no bytes/arrays)
    SPLIT_100_MS_AUDIO_FRAME_ARRAY,
    ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY
                   # volatile: per-frame bytes/arrays
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    ENGINE_DB_LOG_FUNCTIONS_INS,
    CONSOLE_LOG,
    ENGINE_DB_LOG_TABLE_INS
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
async def SERVER_ENGINE_LISTEN_6_FOR_AUDIO_FRAMES_TO_PROCESS() -> None:
    # CONSOLE_LOG("SCANNER", "=== 6_FOR_AUDIO_FRAMES_TO_PROCESS scanner starting ===")
    while True:
        SPLIT_100_MS_AUDIO_FRAME_NO_ARRAY = [
            (int(RECORDING_ID), int(AUDIO_FRAME_NO))
            for RECORDING_ID, ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY_2 in ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY.items()
            for AUDIO_FRAME_NO, ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_RECORD in ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY_2.items()
            if ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_RECORD.get("DT_PROCESSING_QUEUED_TO_START") is None
        ]

        for RECORDING_ID, AUDIO_FRAME_NO in SPLIT_100_MS_AUDIO_FRAME_NO_ARRAY:
            ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_PROCESSING_QUEUED_TO_START"] = datetime.now()
            # CONSOLE_LOG(PREFIX, "queuing_frame_for_analysis", {
            #     "rid": RECORDING_ID,
            #     "frame": AUDIO_FRAME_NO,
            #     "note": "Audio arrays ready, queuing for analysis"
            # })
            # Create task but don't await it (runs concurrently)
            asyncio.create_task(PROCESS_THE_AUDIO_FRAME(RECORDING_ID=RECORDING_ID, AUDIO_FRAME_NO=AUDIO_FRAME_NO))
        
        # if SPLIT_100_MS_AUDIO_FRAME_NO_ARRAY:
        #     CONSOLE_LOG("SCANNER", f"6_FOR_AUDIO_FRAMES: found {len(SPLIT_100_MS_AUDIO_FRAME_NO_ARRAY)} frames ready to analyze")
        
        # Sleep to prevent excessive CPU usage
        await asyncio.sleep(0.1)  # 100ms delay between scans


# ─────────────────────────────────────────────────────────────
# Worker: process a single frame (run analyzers in parallel)
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def PROCESS_THE_AUDIO_FRAME(RECORDING_ID: int, AUDIO_FRAME_NO: int) -> None:
    CONSOLE_LOG("SCANNER", f"PROCESS_THE_AUDIO_FRAME: {RECORDING_ID}, {AUDIO_FRAME_NO}")
    # 1) Mark processing started
    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_RECORD = ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]
    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_RECORD["DT_PROCESSING_START"] = datetime.now()

    # 2) Get audio arrays from volatile store (SIMPLE - no complex checks)
    SPLIT_100_MS_AUDIO_FRAME_RECORD = SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]
    AUDIO_ARRAY_22050 = SPLIT_100_MS_AUDIO_FRAME_RECORD["AUDIO_ARRAY_22050"]
    CONSOLE_LOG(PREFIX, "PROCESS_THE_AUDIO_FRAME", {"rid": RECORDING_ID, "frame": AUDIO_FRAME_NO, "size1": SPLIT_100_MS_AUDIO_FRAME_RECORD["AUDIO_ARRAY_22050"]})
    CONSOLE_LOG(PREFIX, "PROCESS_THE_AUDIO_FRAME", {"rid": RECORDING_ID, "frame": AUDIO_FRAME_NO, "size2": len(AUDIO_ARRAY_22050)})

    AUDIO_ARRAY_16000 = SPLIT_100_MS_AUDIO_FRAME_RECORD["AUDIO_ARRAY_16000"]

    # Per-frame gating flags (set in Stage-3A/3B depending on mode)
    YN_RUN_FFT   = ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_RECORD["YN_RUN_FFT"]
    YN_RUN_PYIN  = ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_RECORD["YN_RUN_PYIN"]
    YN_RUN_CREPE = ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_RECORD["YN_RUN_CREPE"]

    AUDIO_PROCESSING_TASK_ARRAY: list[asyncio.Task] = []

    AUDIO_PROCESSING_TASK_ARRAY.append(asyncio.create_task(
        SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME_1_MS(
            int(RECORDING_ID),
            int(AUDIO_FRAME_NO),
            AUDIO_ARRAY_16000  # 22.05k
        )
    ))

    # AUDIO_PROCESSING_TASK_ARRAY.append(asyncio.create_task(
    #     SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME_10_MS(
    #         int(RECORDING_ID),
    #         int(AUDIO_FRAME_NO),
    #         AUDIO_ARRAY_16000  # 22.05k
    #     )
    # ))

    if YN_RUN_FFT == "Y":
        AUDIO_PROCESSING_TASK_ARRAY.append(asyncio.create_task(
            SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT(
                int(RECORDING_ID),
                int(AUDIO_FRAME_NO),
                AUDIO_ARRAY_16000  # 22.05k
            )
        ))

    if YN_RUN_PYIN == "Y":
        AUDIO_PROCESSING_TASK_ARRAY.append(asyncio.create_task(
            SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN(
                int(RECORDING_ID),
                int(AUDIO_FRAME_NO),
                AUDIO_ARRAY_22050  # 22.05k
            )
        ))

    if YN_RUN_CREPE == "Y":
        AUDIO_PROCESSING_TASK_ARRAY.append(asyncio.create_task(
            SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE(
                int(RECORDING_ID),
                int(AUDIO_FRAME_NO),
                AUDIO_ARRAY_16000  # 16k
            )
        ))

    # Wait for all tasks to complete
    if AUDIO_PROCESSING_TASK_ARRAY:
        await asyncio.gather(*AUDIO_PROCESSING_TASK_ARRAY, return_exceptions=True)

    # 4) Mark processing completed
    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_RECORD["DT_PROCESSING_END"] = datetime.now()
    ENGINE_DB_LOG_TABLE_INS("ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME", ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO])
