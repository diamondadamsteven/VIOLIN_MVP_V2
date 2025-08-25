# SERVER_ENGINE_LISTEN_3C_FOR_STOP.py
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from hashlib import sha256

from SERVER_ENGINE_APP_VARIABLES import (
    ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY,
    ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY,
    ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY,
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY,
    AUDIO_CHUNK_ACCUMULATORS,                    # simplified dictionary structure
    AUDIO_BYTES_PER_FRAME,                       # frame size constants
    AUDIO_SAMPLES_PER_FRAME,                     # samples per frame
    AUDIO_SAMPLE_RATE,                           # sample rate
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    ENGINE_DB_LOG_FUNCTIONS_INS,
    DB_INSERT_TABLE,
    schedule_coro,
)

# ─────────────────────────────────────────────────────────────
# Scanner: queue unprocessed STOP messages
# ─────────────────────────────────────────────────────────────
def SERVER_ENGINE_LISTEN_3C_FOR_STOP() -> None:
    """
    Find STOP messages not yet queued, stamp queue time, and schedule processing.
    """
    to_launch = []
    for MESSAGE_ID, MSG in list(ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.items()):
        if MSG.get("DT_MESSAGE_PROCESS_QUEDED_TO_START") is None and str(MSG.get("MESSAGE_TYPE", "")).upper() == "STOP":
            to_launch.append(MESSAGE_ID)

    for MESSAGE_ID in to_launch:
        MSG = ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID)
        if MSG is None:
            continue
        MSG["DT_MESSAGE_PROCESS_QUEDED_TO_START"] = datetime.now()
        schedule_coro(PROCESS_WEBSOCKET_MESSAGE_TYPE_STOP(MESSAGE_ID))


# ─────────────────────────────────────────────────────────────
# Worker: process a single STOP message
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def PROCESS_WEBSOCKET_MESSAGE_TYPE_STOP(MESSAGE_ID: int) -> None:
    """
    PROCESS STOP:
      1) Mark DT_MESSAGE_PROCESS_STARTED
      2) Persist message row
      3) Mark websocket connection closed (if we can map it) and persist
      4) Set DT_RECORDING_STOP on recording config and persist
      5) Remove STOP message from in-memory queue
    """
    MSG = ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID)
    if MSG is None:
        return

    # 1) mark started (idempotent)
    MSG["DT_MESSAGE_PROCESS_STARTED"] = datetime.now()

    # 2) persist message
    DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_MESSAGE", MSG, fire_and_forget=True)

    # Resolve recording/connection
    RECORDING_ID = int(MSG.get("RECORDING_ID") or 0)
    CFG = ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY.get(RECORDING_ID, {})
    WEBSOCKET_CONNECTION_ID = CFG.get("WEBSOCKET_CONNECTION_ID") or MSG.get("WEBSOCKET_CONNECTION_ID")

    # 3) mark connection closed if known and persist
    if WEBSOCKET_CONNECTION_ID in ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY:
        CONN_ROW = ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY[WEBSOCKET_CONNECTION_ID]
        CONN_ROW["DT_CONNECTION_CLOSED"] = datetime.now()
        DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_CONNECTION", CONN_ROW, fire_and_forget=True)

    # 4) stamp recording stop and persist
    if RECORDING_ID in ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY:
        ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[RECORDING_ID]["DT_RECORDING_END"] = datetime.now()
        DB_INSERT_TABLE("ENGINE_DB_LOG_RECORDING_CONFIG", ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[RECORDING_ID], fire_and_forget=True)

    # 4.5) NEW: Flush any remaining audio data from the audio chunk accumulator using direct dictionary access
    remaining_frames = []
    if RECORDING_ID in AUDIO_CHUNK_ACCUMULATORS:
        accumulator = AUDIO_CHUNK_ACCUMULATORS[RECORDING_ID]
        
        # Flush any remaining audio data as the final frame
        if accumulator['audio_buffer']:
            # Pad to even byte length if needed
            if len(accumulator['audio_buffer']) % 2 != 0:
                accumulator['audio_buffer'].append(0)  # Add padding byte
            
            frame_bytes = bytes(accumulator['audio_buffer'])
            
            # Calculate time-based frame number for the remaining audio
            total_samples_processed = accumulator['total_bytes_received'] // 2  # PCM16 = 2 bytes per sample
            frame_no = (total_samples_processed // AUDIO_SAMPLES_PER_FRAME)
            
            accumulator['total_frames_produced'] += 1
            remaining_frames.append((frame_no, frame_bytes))
            
            # Clean up the accumulator
            del AUDIO_CHUNK_ACCUMULATORS[RECORDING_ID]
    
    if remaining_frames:
        # Process any remaining frames that were flushed
        for frame_no, frame_bytes in remaining_frames:
            # Create a minimal frame record for the flushed frame
            # frame_no is now time-based: Frame 0 = 0-99ms, Frame 1 = 100-199ms, etc.
            start_ms = frame_no * 100
            end_ms = (frame_no * 100) + 99
            
            flushed_frame_record = {
                "RECORDING_ID": RECORDING_ID,
                "AUDIO_FRAME_NO": frame_no,
                "START_MS": start_ms,  # 100ms per frame
                "END_MS": end_ms,
                "DT_FRAME_RECEIVED": MSG.get("DT_MESSAGE_RECEIVED", datetime.now()),
                "DT_FRAME_PAIRED_WITH_WEBSOCKETS_METADATA": datetime.now(),
                "AUDIO_FRAME_SIZE_BYTES": len(frame_bytes),
                "AUDIO_FRAME_SHA256_HEX": "flushed_remaining_audio",  # Special marker
                "NOTE": f"Final frame from audio alignment buffer flush: {start_ms}-{end_ms}ms"
            }
            
            # Store the flushed frame record
            if RECORDING_ID not in ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY:
                ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID] = {}
            ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][frame_no] = flushed_frame_record
            
            # Persist the flushed frame record
            DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME", flushed_frame_record, fire_and_forget=True)

    # 5) remove the STOP message (optional; keeps memory tidy)
    ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.pop(MESSAGE_ID, None)
