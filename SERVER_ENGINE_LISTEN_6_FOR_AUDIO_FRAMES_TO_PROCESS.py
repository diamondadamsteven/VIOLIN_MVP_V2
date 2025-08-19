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

# Per-frame analyzers (all are async)
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
    Find frames that:
      • have not started processing (DT_PROCESSING_START is null/missing), and
      • have resampled audio present in WEBSOCKET_AUDIO_FRAME_ARRAY
        (22.05k for FFT/PYIN/VOLUME, 16k for CREPE — we check per analyzer later)

    Queue each frame once by stamping DT_PROCESSING_START immediately
    (prevents duplicate scheduling) and scheduling PROCESS_THE_AUDIO_FRAME.
    """
    to_launch: list[tuple[int, int]] = []

    # Iterate durable metadata to know which frames exist
    for RECORDING_ID, frames_map in list(ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY.items()):
        if not isinstance(frames_map, dict):
            continue
        for AUDIO_FRAME_NO, META in list(frames_map.items()):
            if not isinstance(META, dict):
                continue
            # Skip if already started (or completed)
            if META.get("DT_PROCESSING_START") is not None:
                continue

            # Volatile per-frame entry must exist; arrays are checked later inside the worker
            rec_vol: Dict[int, Dict[str, Any]] = WEBSOCKET_AUDIO_FRAME_ARRAY.get(RECORDING_ID, {})
            if AUDIO_FRAME_NO not in rec_vol:
                continue

            to_launch.append((int(RECORDING_ID), int(AUDIO_FRAME_NO)))

    # Stamp start and schedule
    for RECORDING_ID, AUDIO_FRAME_NO in to_launch:
        try:
            ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_PROCESSING_START"] = datetime.now()
        except Exception:
            # If the row disappeared, skip
            continue
        schedule_coro(PROCESS_THE_AUDIO_FRAME(RECORDING_ID=RECORDING_ID, AUDIO_FRAME_NO=AUDIO_FRAME_NO))


# ─────────────────────────────────────────────────────────────
# Worker: process a single frame (run analyzers in parallel)
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def PROCESS_THE_AUDIO_FRAME(RECORDING_ID: int, AUDIO_FRAME_NO: int) -> None:
    """
    Run per-frame analyzers:
      • FFT (22.05k), PYIN (22.05k), VOLUME_1_MS (22.05k), VOLUME_10_MS (22.05k), CREPE (16k)
    Analyzers stamp their own DT_START_/DT_END_ and *_RECORD_CNT fields.
    This wrapper stamps DT_PROCESSING_END and frees volatile arrays afterward.
    """
    META = ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY.get(RECORDING_ID, {}).get(AUDIO_FRAME_NO)
    if META is None:
        CONSOLE_LOG(PREFIX, "frame_meta_missing", {"rid": int(RECORDING_ID), "frame": int(AUDIO_FRAME_NO)})
        return

    # Volatile store entry (bytes/arrays)
    VOL = WEBSOCKET_AUDIO_FRAME_ARRAY.get(RECORDING_ID, {}).get(AUDIO_FRAME_NO)
    if VOL is None:
        CONSOLE_LOG(PREFIX, "frame_vol_missing", {"rid": int(RECORDING_ID), "frame": int(AUDIO_FRAME_NO)})
        # If we can't find arrays, mark end and bail to avoid re-queuing forever
        try:
            META["DT_PROCESSING_END"] = datetime.now()
        except Exception:
            pass
        return

    # Inputs (these may be missing individually — analyzers handle absence)
    AUDIO_ARRAY_22050 = VOL.get("AUDIO_ARRAY_22050")
    AUDIO_ARRAY_16000 = VOL.get("AUDIO_ARRAY_16000")

    # Determine which analyzers to run (respect per-frame flags if present; default = 'Y')
    def _flag(meta_key: str) -> bool:
        v = (META.get(meta_key) or "Y")
        return str(v).upper() == "Y"

    run_fft   = _flag("YN_RUN_FFT")   and isinstance(AUDIO_ARRAY_22050, np.ndarray)
    run_pyin  = _flag("YN_RUN_PYIN")  and isinstance(AUDIO_ARRAY_22050, np.ndarray)
    run_crepe = _flag("YN_RUN_CREPE") and isinstance(AUDIO_ARRAY_16000, np.ndarray)
    run_v1ms  = True and isinstance(AUDIO_ARRAY_22050, np.ndarray)  # always if 22k present
    run_v10ms = True and isinstance(AUDIO_ARRAY_22050, np.ndarray)  # always if 22k present

    # If nothing to run, end early
    if not any([run_fft, run_pyin, run_crepe, run_v1ms, run_v10ms]):
        CONSOLE_LOG(PREFIX, "no_analyzers_to_run", {
            "rid": int(RECORDING_ID), "frame": int(AUDIO_FRAME_NO),
            "has_22k": isinstance(AUDIO_ARRAY_22050, np.ndarray),
            "has_16k": isinstance(AUDIO_ARRAY_16000, np.ndarray),
        })
        try:
            META["DT_PROCESSING_END"] = datetime.now()
        except Exception:
            pass
        return

    # Build tasks (all analyzers are async)
    tasks: list[asyncio.Task] = []

    if run_fft:
        tasks.append(asyncio.create_task(
            SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT(
                int(RECORDING_ID),
                int(AUDIO_FRAME_NO),
                AUDIO_ARRAY_22050,           # 22.05k
                22050,
            )
        ))

    if run_pyin:
        tasks.append(asyncio.create_task(
            SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN(
                int(RECORDING_ID),
                int(AUDIO_FRAME_NO),
                AUDIO_ARRAY_22050,           # 22.05k
            )
        ))

    if run_crepe:
        tasks.append(asyncio.create_task(
            SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE(
                int(RECORDING_ID),
                int(AUDIO_FRAME_NO),
                AUDIO_ARRAY_16000,           # 16k (explicitly pass to avoid re-fetch)
            )
        ))

    if run_v1ms:
        tasks.append(asyncio.create_task(
            SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME_1_MS(
                int(RECORDING_ID),
                int(AUDIO_FRAME_NO),
                AUDIO_ARRAY_22050,           # 22.05k
            )
        ))

    if run_v10ms:
        tasks.append(asyncio.create_task(
            SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME_10_MS(
                int(RECORDING_ID),
                int(AUDIO_FRAME_NO),
                AUDIO_ARRAY_22050,           # 22.05k
            )
        ))

    # Run everything in parallel; analyzers log their own details
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        # Surface any exceptions in logs (without crashing)
        for r in results:
            if isinstance(r, Exception):
                CONSOLE_LOG(PREFIX, "analyzer_error", {
                    "rid": int(RECORDING_ID),
                    "frame": int(AUDIO_FRAME_NO),
                    "err": str(r),
                })
    finally:
        # Stamp processing end
        try:
            META["DT_PROCESSING_END"] = datetime.now()
        except Exception:
            pass

        # Free volatile arrays to save memory; keep the dict shell for traceability
        try:
            vol_rec = WEBSOCKET_AUDIO_FRAME_ARRAY.get(RECORDING_ID, {}).get(AUDIO_FRAME_NO)
            if isinstance(vol_rec, dict):
                vol_rec.pop("AUDIO_ARRAY_16000", None)
                vol_rec.pop("AUDIO_ARRAY_22050", None)
        except Exception:
            # Non-fatal
            pass

        CONSOLE_LOG(PREFIX, "frame_done", {
            "rid": int(RECORDING_ID),
            "frame": int(AUDIO_FRAME_NO),
        })
