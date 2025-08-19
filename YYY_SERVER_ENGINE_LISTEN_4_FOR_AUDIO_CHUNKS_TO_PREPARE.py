# SERVER_ENGINE_LISTEN_4_FOR_AUDIO_CHUNKS_TO_PREPARE.py
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

def _frames_ready(frames_map: dict, lo: int, hi: int) -> bool:
    """Return True iff every frame in [lo..hi] is present (int or str keys)."""
    if not frames_map:
        return False
    present = 0
    need = hi - lo + 1
    for f in range(lo, hi + 1):
        if f in frames_map or str(f) in frames_map:
            present += 1
        else:
            return False
    return present == need

def SERVER_ENGINE_LISTEN_4_FOR_AUDIO_CHUNKS_TO_PREPARE() -> None:
    """
    Step 1) If COMPOSE and there are no chunks with DT_COMPLETE_FRAMES_RECEIVED is null,
            consider the *next* chunk number but only insert the chunk if all frames
            for that chunk range already exist in RECORDING_AUDIO_FRAME_ARRAY.
    Step 2) For any existing chunk whose frame range is fully present, set DT_COMPLETE_FRAMES_RECEIVED.
    """
    now = datetime.now()

    # Step 1: compose seeding (gate on frames already received)
    for rid, cfg in list(RECORDING_CONFIG_ARRAY.items()):
        mode = str(cfg.get("COMPOSE_PLAY_OR_PRACTICE") or "").upper()
        if mode != "COMPOSE":
            continue

        RECORDING_AUDIO_CHUNK_ARRAY.setdefault(rid, {})

        # Any chunk currently waiting for frames?
        has_open = False
        for ch in RECORDING_AUDIO_CHUNK_ARRAY[rid].values():
            if ch.get("DT_COMPLETE_FRAMES_RECEIVED") is None:
                has_open = True
                break

        if has_open:
            # Don't propose a new chunk until the open one is complete.
            continue

        # Propose the next chunk, but only insert if all frames in its range are present.
        current_no = int(cfg.get("COMPOSE_CURRENT_AUDIO_CHUNK_NO") or 1)
        dur_ms     = int(cfg.get("AUDIO_CHUNK_DURATION_IN_MS") or 0)
        cnt_frames = int(cfg.get("CNT_FRAMES_PER_AUDIO_CHUNK") or 0)

        if cnt_frames <= 0 or dur_ms <= 0:
            # Misconfiguration: nothing to do.
            continue

        min_frame = 1 + (current_no - 1) * cnt_frames
        max_frame = current_no * cnt_frames
        start_ms  = (current_no - 1) * dur_ms
        end_ms    = current_no * dur_ms - 1

        frames_map = RECORDING_AUDIO_FRAME_ARRAY.get(rid, {}) or {}

        # Only seed the chunk if *all* frames for this chunk already exist.
        if _frames_ready(frames_map, min_frame, max_frame):
            RECORDING_AUDIO_CHUNK_ARRAY[rid][current_no] = {
                "RECORDING_ID": rid,
                "AUDIO_CHUNK_NO": current_no,
                "AUDIO_CHUNK_DURATION_IN_MS": dur_ms,
                "START_MS": start_ms,
                "END_MS": end_ms,
                "MIN_AUDIO_STREAM_FRAME_NO": min_frame,
                "MAX_AUDIO_STREAM_FRAME_NO": max_frame,
                # Seed YN flags so DB logging won’t KeyError
                "YN_RUN_FFT":   cfg.get("YN_RUN_FFT", "N"),
                "YN_RUN_ONS":   cfg.get("YN_RUN_ONS", "N"),
                "YN_RUN_PYIN":  cfg.get("YN_RUN_PYIN", "N"),
                "YN_RUN_CREPE": cfg.get("YN_RUN_CREPE", "N"),
            }
            # Bump compose pointer *after* successful seed
            cfg["COMPOSE_CURRENT_AUDIO_CHUNK_NO"] = current_no + 1
            RECORDING_CONFIG_ARRAY[rid] = cfg

            # Log immediately (non-blocking logger)
            DB_LOG_ENGINE_DB_RECORDING_AUDIO_CHUNK(rid, current_no)
        # else: frames not ready yet — do nothing this tick

    # Step 2: mark chunks complete when all frames arrive
    for rid, chunks in list(RECORDING_AUDIO_CHUNK_ARRAY.items()):
        frames = RECORDING_AUDIO_FRAME_ARRAY.get(rid, {}) or {}
        for chno, ch in list(chunks.items()):
            if ch.get("DT_COMPLETE_FRAMES_RECEIVED") is not None:
                continue
            lo = int(ch["MIN_AUDIO_STREAM_FRAME_NO"])
            hi = int(ch["MAX_AUDIO_STREAM_FRAME_NO"])
            if _frames_ready(frames, lo, hi):
                ch["DT_COMPLETE_FRAMES_RECEIVED"] = now
