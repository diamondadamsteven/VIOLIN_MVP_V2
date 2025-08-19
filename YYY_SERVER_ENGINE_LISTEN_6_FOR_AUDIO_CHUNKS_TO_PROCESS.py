from __future__ import annotations
from datetime import datetime
import asyncio
from typing import Optional

import numpy as np  # for PCM→float decoding

from SERVER_ENGINE_APP_VARIABLES import (
    RECORDING_CONFIG_ARRAY,
    RECORDING_AUDIO_CHUNK_ARRAY,
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    DB_LOG_FUNCTIONS,
    DB_CONNECT_CTX,
    DB_EXEC_SP_NO_RESULT,
    DB_EXEC_SP_SINGLE_ROW,
    DB_LOG_ENGINE_DB_RECORDING_AUDIO_CHUNK,
    CONSOLE_LOG,
    schedule_coro,
)

# Analyzer entries
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE import SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE  # async wrapper
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT   import SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS   import SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN  import SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME import SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def _get_chunk(RECORDING_ID: int, AUDIO_CHUNK_NO: int):
    """
    Return the chunk dict regardless of int/str key, or None if not present.
    """
    chunks = RECORDING_AUDIO_CHUNK_ARRAY.get(RECORDING_ID) \
             or RECORDING_AUDIO_CHUNK_ARRAY.get(str(RECORDING_ID))
    if not chunks:
        return None
    return chunks.get(AUDIO_CHUNK_NO) or chunks.get(str(AUDIO_CHUNK_NO))


def _pcm16_to_float32_array(pcm: Optional[bytes]) -> Optional[np.ndarray]:
    """
    Decode mono PCM16 (little-endian) bytes → float32 in [-1, 1].
    Returns None if pcm is falsy.
    """
    if not pcm:
        return None
    try:
        arr = np.frombuffer(pcm, dtype=np.int16).astype(np.float32) / 32768.0
        return arr
    except Exception:
        return None


# ─────────────────────────────────────────────
# Scanner entry
# ─────────────────────────────────────────────
def SERVER_ENGINE_LISTEN_6_FOR_AUDIO_CHUNKS_TO_PROCESS() -> None:
    """
    Step 1) For chunks with DT_AUDIO_CHUNK_PREPARATION_COMPLETE not null and
            DT_START_AUDIO_CHUNK_PROCESS null, launch PROCESS_THE_AUDIO_CHUNK.
    """
    to_launch = []
    for rid, chunks in list(RECORDING_AUDIO_CHUNK_ARRAY.items()):
        for chno, ch in list(chunks.items()):
            if ch.get("DT_AUDIO_CHUNK_PREPARATION_COMPLETE") and ch.get("DT_START_AUDIO_CHUNK_PROCESS") is None:
                to_launch.append((rid, chno))
    for rid, chno in to_launch:
        schedule_coro(PROCESS_THE_AUDIO_CHUNK(RECORDING_ID=rid, AUDIO_CHUNK_NO=chno))


# ─────────────────────────────────────────────
# Orchestrator for a single chunk
# ─────────────────────────────────────────────
@DB_LOG_FUNCTIONS()
async def PROCESS_THE_AUDIO_CHUNK(RECORDING_ID: int, AUDIO_CHUNK_NO: int) -> None:
    """
    Implements Steps 1–13 and explicitly waits for
    FFT, ONS, PYIN, CREPE, VOLUME to finish before calling P_ENGINE_ALL_MASTER.
    """
    ch = _get_chunk(RECORDING_ID, AUDIO_CHUNK_NO)
    if ch is None:
        CONSOLE_LOG("STAGE6", "chunk_not_ready_at_process_entry", {
            "rid": int(RECORDING_ID), "chunk": int(AUDIO_CHUNK_NO)
        })
        return

    cfg  = RECORDING_CONFIG_ARRAY.get(RECORDING_ID, {}) or {}
    mode = str(cfg.get("COMPOSE_PLAY_OR_PRACTICE") or "").upper()

    # Prepared inputs from Stage-5 (CONCATENATE)
    start_ms = int(ch.get("START_MS") or 0)

    # Prefer arrays if present; otherwise decode PCM16 bytes on-the-fly.
    audio_22k = ch.get("AUDIO_ARRAY_22050")
    sr_22k    = int(ch.get("SAMPLE_RATE_22050") or 22050)
    if audio_22k is None:
        audio_22k = _pcm16_to_float32_array(ch.get("AUDIO_CHUNK_DATA_22050"))
        if audio_22k is not None:
            ch["AUDIO_ARRAY_22050"] = audio_22k
            ch["SAMPLE_RATE_22050"] = 22050

    audio_16k = ch.get("AUDIO_ARRAY_16000")
    sr_16k    = int(ch.get("SAMPLE_RATE_16000") or 16000)
    if audio_16k is None:
        audio_16k = _pcm16_to_float32_array(ch.get("AUDIO_CHUNK_DATA_16K"))
        if audio_16k is not None:
            ch["AUDIO_ARRAY_16000"] = audio_16k
            ch["SAMPLE_RATE_16000"] = 16000

    # Step 1) mark process start
    ch["DT_START_AUDIO_CHUNK_PROCESS"] = datetime.now()

    # Step 2–3) FFT scheduling (mode-aware). We'll run the DB FFT aggregator AFTER gather().
    tasks: list[asyncio.Future] = []
    compose_run_fft = False

    if mode == "COMPOSE":
        if str(cfg.get("YN_RUN_FFT") or "").upper() == "Y" and isinstance(audio_22k, np.ndarray):
            ch["DT_START_FFT"] = datetime.now()
            tasks.append(asyncio.to_thread(
                SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT,
                int(RECORDING_ID), int(AUDIO_CHUNK_NO),
                int(start_ms),
                audio_22k, int(sr_22k),
            ))
            compose_run_fft = True
        # Per-chunk flags for ONS/PYIN/CREPE
        with DB_CONNECT_CTX() as conn:
            row = DB_EXEC_SP_SINGLE_ROW(
                conn,
                "P_ENGINE_SONG_AUDIO_CHUNK_NO_FOR_COMPOSE_GET",
                RECORDING_ID=int(RECORDING_ID),
                AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
            ) or {}
        ch["YN_RUN_ONS"]   = row.get("YN_RUN_ONS")
        ch["YN_RUN_PYIN"]  = row.get("YN_RUN_PYIN")
        ch["YN_RUN_CREPE"] = row.get("YN_RUN_CREPE")

    else:  # PLAY or PRACTICE
        if str(ch.get("YN_RUN_FFT") or "").upper() == "Y" and isinstance(audio_22k, np.ndarray):
            ch["DT_START_FFT"] = datetime.now()
            tasks.append(asyncio.to_thread(
                SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT,
                int(RECORDING_ID), int(AUDIO_CHUNK_NO),
                int(start_ms),
                audio_22k, int(sr_22k),
            ))

    # Steps 4–7) Launch analyzers; collect awaitables

    # ONS (16 kHz)
    if str(ch.get("YN_RUN_ONS") or cfg.get("YN_RUN_ONS") or "").upper() == "Y" and isinstance(audio_16k, np.ndarray):
        ch["DT_START_ONS"] = datetime.now()
        tasks.append(asyncio.to_thread(
            SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS,
            int(RECORDING_ID), int(AUDIO_CHUNK_NO),
            int(start_ms),
            audio_16k, int(sr_16k),
        ))

    # pYIN (22.05 kHz)
    if str(ch.get("YN_RUN_PYIN") or cfg.get("YN_RUN_PYIN") or "").upper() == "Y" and isinstance(audio_22k, np.ndarray):
        ch["DT_START_PYIN"] = datetime.now()
        tasks.append(asyncio.to_thread(
            SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN,
            int(RECORDING_ID), int(AUDIO_CHUNK_NO),
            int(start_ms),
            audio_22k, int(sr_22k),
        ))

    # CREPE (16 kHz) — async wrapper reads audio from the chunk
    if str(ch.get("YN_RUN_CREPE") or cfg.get("YN_RUN_CREPE") or "").upper() == "Y" and isinstance(audio_16k, np.ndarray):
        ch["DT_START_CREPE"] = datetime.now()
        tasks.append(asyncio.create_task(
            SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE(int(RECORDING_ID), int(AUDIO_CHUNK_NO))
        ))

    # VOLUME (always, if 22.05 kHz is available)
    if isinstance(audio_22k, np.ndarray):
        ch["DT_START_VOLUME"] = datetime.now()
        tasks.append(asyncio.to_thread(
            SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME,
            int(RECORDING_ID), int(AUDIO_CHUNK_NO),
            int(start_ms),
            audio_22k, int(sr_22k),
        ))
    else:
        CONSOLE_LOG("STAGE6", "volume_skipped_no_22k_audio", {
            "rid": int(RECORDING_ID), "chunk": int(AUDIO_CHUNK_NO)
        })

    # Step 8) Wait for ALL analyzers to complete
    if tasks:
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            CONSOLE_LOG("STAGE6", "analyzer_gather_error", {
                "rid": int(RECORDING_ID),
                "chunk": int(AUDIO_CHUNK_NO),
                "err": str(e),
            })

    # (COMPOSE only) Now that FFT rows exist, call the appropriate FFT aggregator SP.
    if mode == "COMPOSE":
        with DB_CONNECT_CTX() as conn:
            if compose_run_fft:
                DB_EXEC_SP_NO_RESULT(
                    conn, "P_ENGINE_ALL_METHOD_FFT",
                    RECORDING_ID=int(RECORDING_ID), AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO)
                )
            else:
                DB_EXEC_SP_NO_RESULT(
                    conn, "P_ENGINE_ALL_METHOD_COMPOSE_DONT_RUN_FFT",
                    RECORDING_ID=int(RECORDING_ID), AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO)
                )

    # Step 9) P_ENGINE_ALL_MASTER (runs **after** analyzers are done)
    ch["DT_START_P_ENGINE_ALL_MASTER"] = datetime.now()
    violinist_id = (cfg or {}).get("VIOLINIST_ID")
    with DB_CONNECT_CTX() as conn:
        DB_EXEC_SP_NO_RESULT(
            conn,
            "P_ENGINE_ALL_MASTER",
            VIOLINIST_ID=violinist_id,
            RECORDING_ID=int(RECORDING_ID),
            COMPOSE_PLAY_OR_PRACTICE=mode,
            AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
        )

    # Step 11) End
    ch["DT_END_AUDIO_CHUNK_PROCESS"] = datetime.now()

    # Step 12) DB log the chunk snapshot
    try:
        DB_LOG_ENGINE_DB_RECORDING_AUDIO_CHUNK(int(RECORDING_ID), int(AUDIO_CHUNK_NO))
    except Exception:
        pass

    # Step 13) Remove chunk from memory to free resources
    try:
        chunks = RECORDING_AUDIO_CHUNK_ARRAY.get(RECORDING_ID) \
                 or RECORDING_AUDIO_CHUNK_ARRAY.get(str(RECORDING_ID))
        if chunks is not None:
            if AUDIO_CHUNK_NO in chunks:
                del chunks[AUDIO_CHUNK_NO]
            elif str(AUDIO_CHUNK_NO) in chunks:
                del chunks[str(AUDIO_CHUNK_NO)]
            if not chunks:
                if RECORDING_ID in RECORDING_AUDIO_CHUNK_ARRAY:
                    del RECORDING_AUDIO_CHUNK_ARRAY[RECORDING_ID]
                elif str(RECORDING_ID) in RECORDING_AUDIO_CHUNK_ARRAY:
                    del RECORDING_AUDIO_CHUNK_ARRAY[str(RECORDING_ID)]
    except Exception:
        pass
