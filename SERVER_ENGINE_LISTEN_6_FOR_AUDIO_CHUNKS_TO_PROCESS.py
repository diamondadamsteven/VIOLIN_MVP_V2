# SERVER_ENGINE_LISTEN_6_FOR_AUDIO_CHUNKS_TO_PROCESS.py
from __future__ import annotations
from datetime import datetime
import asyncio
from typing import Optional

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
    schedule_coro
)

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def _get_chunk(RECORDING_ID: int, AUDIO_CHUNK_NO: int):
    """Return chunk dict regardless of int/str key, or None if not present yet."""
    chunks = RECORDING_AUDIO_CHUNK_ARRAY.get(RECORDING_ID)
    if chunks is None:
        chunks = RECORDING_AUDIO_CHUNK_ARRAY.get(str(RECORDING_ID))
        if chunks is None:
            return None

    if AUDIO_CHUNK_NO in chunks:
        return chunks[AUDIO_CHUNK_NO]
    if str(AUDIO_CHUNK_NO) in chunks:
        return chunks[str(AUDIO_CHUNK_NO)]
    return None

# ─────────────────────────────────────────────
# Lightweight placeholder analyzers
# (Replace these with your real implementations)
# ─────────────────────────────────────────────
def _stamp_start(ch, key):
    ch[key] = datetime.now()

def _finish_duration(ch, key_duration_ms: str, rec_count_key: Optional[str] = None):
    ch[key_duration_ms] = 1  # placeholder 1ms
    if rec_count_key:
        ch[rec_count_key] = 0

async def SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT(RECORDING_ID: int, AUDIO_CHUNK_NO: int):
    ch = _get_chunk(RECORDING_ID, AUDIO_CHUNK_NO)
    if ch is None:
        CONSOLE_LOG("STAGE6", "chunk_not_ready", {"rid": int(RECORDING_ID), "chunk": AUDIO_CHUNK_NO, "stage": "FFT"})
        return
    _stamp_start(ch, "DT_START_FFT")
    _finish_duration(ch, "FFT_DURATION_IN_MS", "FFT_RECORD_CNT")

async def SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS(RECORDING_ID: int, AUDIO_CHUNK_NO: int):
    ch = _get_chunk(RECORDING_ID, AUDIO_CHUNK_NO)
    if ch is None:
        CONSOLE_LOG("STAGE6", "chunk_not_ready", {"rid": int(RECORDING_ID), "chunk": AUDIO_CHUNK_NO, "stage": "ONS"})
        return
    _stamp_start(ch, "DT_START_ONS")
    _finish_duration(ch, "ONS_DURATION_IN_MS", "ONS_RECORD_CNT")

async def SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN(RECORDING_ID: int, AUDIO_CHUNK_NO: int):
    ch = _get_chunk(RECORDING_ID, AUDIO_CHUNK_NO)
    if ch is None:
        CONSOLE_LOG("STAGE6", "chunk_not_ready", {"rid": int(RECORDING_ID), "chunk": AUDIO_CHUNK_NO, "stage": "PYIN"})
        return
    _stamp_start(ch, "DT_START_PYIN")
    _finish_duration(ch, "PYIN_DURATION_IN_MS", "PYIN_RECORD_CNT")

async def SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE(RECORDING_ID: int, AUDIO_CHUNK_NO: int):
    ch = _get_chunk(RECORDING_ID, AUDIO_CHUNK_NO)
    if ch is None:
        CONSOLE_LOG("STAGE6", "chunk_not_ready", {"rid": int(RECORDING_ID), "chunk": AUDIO_CHUNK_NO, "stage": "CREPE"})
        return
    _stamp_start(ch, "DT_START_CREPE")
    _finish_duration(ch, "CREPE_DURATION_IN_MS", "CREPE_RECORD_CNT")

async def SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME(RECORDING_ID: int, AUDIO_CHUNK_NO: int):
    ch = _get_chunk(RECORDING_ID, AUDIO_CHUNK_NO)
    if ch is None:
        CONSOLE_LOG("STAGE6", "chunk_not_ready", {"rid": int(RECORDING_ID), "chunk": AUDIO_CHUNK_NO, "stage": "VOLUME"})
        return
    _stamp_start(ch, "DT_START_VOLUME")
    _finish_duration(ch, "VOLUME_10_MS_DURATION_IN_MS", "VOLUME_10_MS_RECORD_CNT")
    _finish_duration(ch, "VOLUME_1_MS_DURATION_IN_MS", "VOLUME_1_MS_RECORD_CNT")

# ─────────────────────────────────────────────

def SERVER_ENGINE_LISTEN_6_FOR_AUDIO_CHUNKS_TO_PROCESS() -> None:
    """
    Step 1) For chunks with DT_AUDIO_CHUNK_PREPARATION_COMPLETE not null and DT_START_AUDIO_CHUNK_PROCESS null,
            launch PROCESS_THE_AUDIO_CHUNK
    """
    to_launch = []
    for rid, chunks in list(RECORDING_AUDIO_CHUNK_ARRAY.items()):
        for chno, ch in list(chunks.items()):
            if ch.get("DT_AUDIO_CHUNK_PREPARATION_COMPLETE") and ch.get("DT_START_AUDIO_CHUNK_PROCESS") is None:
                to_launch.append((rid, chno))
    for rid, chno in to_launch:
        schedule_coro(PROCESS_THE_AUDIO_CHUNK(RECORDING_ID=rid, AUDIO_CHUNK_NO=chno))


@DB_LOG_FUNCTIONS()
async def PROCESS_THE_AUDIO_CHUNK(RECORDING_ID: int, AUDIO_CHUNK_NO: int) -> None:
    """
    Implements Steps 1–13 from your spec.
    """
    ch = _get_chunk(RECORDING_ID, AUDIO_CHUNK_NO)
    if ch is None:
        CONSOLE_LOG("STAGE6", "chunk_not_ready_at_process_entry", {"rid": int(RECORDING_ID), "chunk": AUDIO_CHUNK_NO})
        return

    cfg = RECORDING_CONFIG_ARRAY.get(RECORDING_ID, {})
    mode = str(cfg.get("COMPOSE_PLAY_OR_PRACTICE") or "").upper()
    ch["DT_START_AUDIO_CHUNK_PROCESS"] = datetime.now()

    # Step 2 & 3 & 4–7 per mode/flags
    if mode == "COMPOSE":
        if str(cfg.get("YN_RUN_FFT") or "").upper() == "Y":
            await SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT(RECORDING_ID, AUDIO_CHUNK_NO)
            with DB_CONNECT_CTX() as conn:
                DB_EXEC_SP_NO_RESULT(conn, "P_ENGINE_ALL_METHOD_FFT",
                                     RECORDING_ID=RECORDING_ID, AUDIO_CHUNK_NO=AUDIO_CHUNK_NO)
        else:
            with DB_CONNECT_CTX() as conn:
                DB_EXEC_SP_NO_RESULT(conn, "P_ENGINE_ALL_METHOD_COMPOSE_DONT_RUN_FFT",
                                     RECORDING_ID=RECORDING_ID, AUDIO_CHUNK_NO=AUDIO_CHUNK_NO)

        # Determine next compose chunk number if needed
        with DB_CONNECT_CTX() as conn:
            row = DB_EXEC_SP_SINGLE_ROW(
                conn,
                "P_ENGINE_SONG_AUDIO_CHUNK_NO_FOR_COMPOSE_GET",
                RECORDING_ID=int(RECORDING_ID),
                AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
            ) or {}

        # Update the chunk flags from the SP row
        ch["YN_RUN_ONS"]   = row.get("YN_RUN_ONS")
        ch["YN_RUN_PYIN"]  = row.get("YN_RUN_PYIN")
        ch["YN_RUN_CREPE"] = row.get("YN_RUN_CREPE")

    else:  # PLAY or PRACTICE
        if str(ch.get("YN_RUN_FFT") or "").upper() == "Y":
            await SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT(RECORDING_ID, AUDIO_CHUNK_NO)

    # Steps 4–7: schedule analyzers BUT await them to avoid chunk-deletion races
    tasks = []
    if str(ch.get("YN_RUN_ONS") or cfg.get("YN_RUN_ONS") or "").upper() == "Y":
        tasks.append(asyncio.create_task(SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS(RECORDING_ID, AUDIO_CHUNK_NO)))
    if str(ch.get("YN_RUN_PYIN") or cfg.get("YN_RUN_PYIN") or "").upper() == "Y":
        tasks.append(asyncio.create_task(SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN(RECORDING_ID, AUDIO_CHUNK_NO)))
    if str(ch.get("YN_RUN_CREPE") or cfg.get("YN_RUN_CREPE") or "").upper() == "Y":
        tasks.append(asyncio.create_task(SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE(RECORDING_ID, AUDIO_CHUNK_NO)))
    tasks.append(asyncio.create_task(SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME(RECORDING_ID, AUDIO_CHUNK_NO)))

    if tasks:
        await asyncio.gather(*tasks)

    # Step 8) wait-until-all-finished (done by gather)
    # Step 9)
    ch["DT_START_P_ENGINE_ALL_MASTER"] = datetime.now()

    cfg = RECORDING_CONFIG_ARRAY.get(int(RECORDING_ID), {})
    violinist_id = cfg.get("VIOLINIST_ID")
    mode         = (cfg.get("COMPOSE_PLAY_OR_PRACTICE") or "").upper()

    with DB_CONNECT_CTX() as conn:
        DB_EXEC_SP_NO_RESULT(
            conn,
            "P_ENGINE_ALL_MASTER",
            VIOLINIST_ID=violinist_id,
            RECORDING_ID=int(RECORDING_ID),
            COMPOSE_PLAY_OR_PRACTICE=mode,
            AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
        )

    # Step 11) end
    ch["DT_END_AUDIO_CHUNK_PROCESS"] = datetime.now()

    # Step 12) DB log the chunk snapshot
    try:
        DB_LOG_ENGINE_DB_RECORDING_AUDIO_CHUNK(RECORDING_ID, AUDIO_CHUNK_NO)
    except Exception:
        pass

    # Step 13) remove from array to free memory
    try:
        # Resolve the exact key we used (int or str) before deleting
        chunks = RECORDING_AUDIO_CHUNK_ARRAY.get(RECORDING_ID) or RECORDING_AUDIO_CHUNK_ARRAY.get(str(RECORDING_ID))
        if chunks is not None:
            if AUDIO_CHUNK_NO in chunks:
                del chunks[AUDIO_CHUNK_NO]
            elif str(AUDIO_CHUNK_NO) in chunks:
                del chunks[str(AUDIO_CHUNK_NO)]
            if not chunks:
                # delete the outer map if empty
                if RECORDING_ID in RECORDING_AUDIO_CHUNK_ARRAY:
                    del RECORDING_AUDIO_CHUNK_ARRAY[RECORDING_ID]
                elif str(RECORDING_ID) in RECORDING_AUDIO_CHUNK_ARRAY:
                    del RECORDING_AUDIO_CHUNK_ARRAY[str(RECORDING_ID)]
    except Exception:
        pass
