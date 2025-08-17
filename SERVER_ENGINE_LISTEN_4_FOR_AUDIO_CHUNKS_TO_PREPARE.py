from __future__ import annotations
from datetime import datetime

from SERVER_ENGINE_APP_VARIABLES import (
    RECORDING_CONFIG_ARRAY,
    RECORDING_AUDIO_CHUNK_ARRAY,
    RECORDING_AUDIO_FRAME_ARRAY,
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    DB_LOG_FUNCTIONS,
    DB_LOG_ENGINE_DB_RECORDING_AUDIO_CHUNK,
)

def SERVER_ENGINE_LISTEN_4_FOR_AUDIO_CHUNKS_TO_PREPARE() -> None:
    """
    Step 1) If COMPOSE and there are no chunks with DT_COMPLETE_FRAMES_RECEIVED is null,
            insert next chunk and increment COMPOSE_CURRENT_AUDIO_CHUNK_NO
    Step 2) For any chunk whose frame range is fully present, set DT_COMPLETE_FRAMES_RECEIVED
    """
    now = datetime.now()

    # Step 1: compose seeding
    for rid, cfg in list(RECORDING_CONFIG_ARRAY.items()):
        mode = str(cfg.get("COMPOSE_PLAY_OR_PRACTICE") or "").upper()
        if mode != "COMPOSE":
            continue

        RECORDING_AUDIO_CHUNK_ARRAY.setdefault(rid, {})

        # Is there any chunk awaiting frames (DT_COMPLETE_FRAMES_RECEIVED is null)?
        has_open = False
        for ch in RECORDING_AUDIO_CHUNK_ARRAY[rid].values():
            if ch.get("DT_COMPLETE_FRAMES_RECEIVED") is None:
                has_open = True
                break

        if not has_open:
            current_no = int(cfg.get("COMPOSE_CURRENT_AUDIO_CHUNK_NO") or 1)
            dur_ms = int(cfg.get("AUDIO_CHUNK_DURATION_IN_MS") or 0)
            cnt_frames = int(cfg.get("CNT_FRAMES_PER_AUDIO_CHUNK") or 0)
            min_frame = 1 + (current_no - 1) * cnt_frames
            max_frame = current_no * cnt_frames
            start_ms  = (current_no - 1) * dur_ms
            end_ms    = current_no * dur_ms - 1

            # Seed chunk with defaults for all YN flags so DB logging won’t KeyError
            RECORDING_AUDIO_CHUNK_ARRAY[rid][current_no] = {
                "RECORDING_ID": rid,
                "AUDIO_CHUNK_NO": current_no,
                "AUDIO_CHUNK_DURATION_IN_MS": dur_ms,
                "START_MS": start_ms,
                "END_MS": end_ms,
                "MIN_AUDIO_STREAM_FRAME_NO": min_frame,
                "MAX_AUDIO_STREAM_FRAME_NO": max_frame,
                "YN_RUN_FFT":   cfg.get("YN_RUN_FFT", "N"),
                "YN_RUN_ONS":   cfg.get("YN_RUN_ONS", "N"),
                "YN_RUN_PYIN":  cfg.get("YN_RUN_PYIN", "N"),
                "YN_RUN_CREPE": cfg.get("YN_RUN_CREPE", "N"),
            }
            cfg["COMPOSE_CURRENT_AUDIO_CHUNK_NO"] = current_no + 1
            RECORDING_CONFIG_ARRAY[rid] = cfg

            # OK to log immediately; the logger now uses .get() defaults for optional fields
            DB_LOG_ENGINE_DB_RECORDING_AUDIO_CHUNK(rid, current_no)

    # Step 2: mark chunks complete when all frames arrive
    for rid, chunks in list(RECORDING_AUDIO_CHUNK_ARRAY.items()):
        frames = RECORDING_AUDIO_FRAME_ARRAY.get(rid, {})
        for chno, ch in list(chunks.items()):
            if ch.get("DT_COMPLETE_FRAMES_RECEIVED") is not None:
                continue
            lo = int(ch["MIN_AUDIO_STREAM_FRAME_NO"])
            hi = int(ch["MAX_AUDIO_STREAM_FRAME_NO"])
            expected = hi - lo + 1
            count = sum(1 for f in frames.keys() if lo <= int(f) <= hi)
            if count == expected:
                ch["DT_COMPLETE_FRAMES_RECEIVED"] = now
