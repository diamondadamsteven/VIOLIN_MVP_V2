# SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS.py
# ----------------------------------------------------------------------
# Onsets & Frames processing for a single audio chunk.
#   • Stage-6 calls the async wrapper with (RECORDING_ID, AUDIO_CHUNK_NO)
#   • Wrapper pulls audio_16k + start_ms from RECORDING_AUDIO_CHUNK_ARRAY
#   • Runs the real worker off the event loop (asyncio.to_thread)
#   • Microservice returns MIDI → parse → chunk-relative rows (ms)
#   • Convert to ABS times and bulk-insert into ENGINE_LOAD_NOTE:
#       (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS,
#        NOTE_MIDI_PITCH_NO, VOLUME_MIDI_VELOCITY_NO, SOURCE_METHOD='ONS')
#   • Stamps ONS_RECORD_CNT and ONS_DURATION_IN_MS back into the chunk dict
# ----------------------------------------------------------------------

from __future__ import annotations

import asyncio
import os
import traceback
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Tuple, Optional

import builtins as _bi
import requests
import numpy as np
import soundfile as sf  # for temp WAV write

# MIDI parsing
try:
    import pretty_midi  # type: ignore
except Exception:  # pragma: no cover
    pretty_midi = None

from SERVER_ENGINE_APP_VARIABLES import TEMP_RECORDING_AUDIO_DIR, RECORDING_AUDIO_CHUNK_ARRAY
from SERVER_ENGINE_APP_FUNCTIONS import (
    CONSOLE_LOG,
    DB_CONNECT_CTX,
    DB_BULK_INSERT,
    DB_LOG_FUNCTIONS,
)

PREFIX = "ONS"

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────
def _get_chunk(RECORDING_ID: int, AUDIO_CHUNK_NO: int) -> Optional[dict]:
    chunks = RECORDING_AUDIO_CHUNK_ARRAY.get(RECORDING_ID) or RECORDING_AUDIO_CHUNK_ARRAY.get(str(RECORDING_ID))
    if not chunks:
        return None
    if AUDIO_CHUNK_NO in chunks:
        return chunks[AUDIO_CHUNK_NO]
    if str(AUDIO_CHUNK_NO) in chunks:
        return chunks[str(AUDIO_CHUNK_NO)]
    return None

# ─────────────────────────────────────────────────────────────
# DB bulk insert
# ─────────────────────────────────────────────────────────────
@DB_LOG_FUNCTIONS()
def _db_load_note_rows(
    conn,
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    rows: Iterable[Tuple[int, int, int, int, str]],
) -> None:
    """
    ENGINE_LOAD_NOTE columns:
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS,
       NOTE_MIDI_PITCH_NO, VOLUME_MIDI_VELOCITY_NO, SOURCE_METHOD)
    """
    sql = """
      INSERT INTO ENGINE_LOAD_NOTE
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS,
       NOTE_MIDI_PITCH_NO, VOLUME_MIDI_VELOCITY_NO, SOURCE_METHOD)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    DB_BULK_INSERT(
        conn,
        sql,
        (
            (RECORDING_ID, AUDIO_CHUNK_NO, s, e, midi, vel, src)
            for (s, e, midi, vel, src) in rows
        ),
    )

# ─────────────────────────────────────────────────────────────
# O&F microservice URL
# ─────────────────────────────────────────────────────────────
@DB_LOG_FUNCTIONS()
def _ons_service_url() -> str:
    host = os.getenv("OAF_HOST", "127.0.0.1")
    port = int(os.getenv("OAF_PORT", "9077"))
    return f"http://{host}:{port}"

# ─────────────────────────────────────────────────────────────
# Microservice call
# ─────────────────────────────────────────────────────────────
@DB_LOG_FUNCTIONS()
def _ons_run_and_get_midi_path(wav_path: Path) -> Optional[Path]:
    url = _ons_service_url().rstrip("/") + "/transcribe"
    payload = {"audio_path": _bi.str(wav_path)}
    CONSOLE_LOG(PREFIX, "HTTP_POST", {"url": url, "payload": payload})

    try:
        resp = requests.post(url, json=payload, timeout=120)
    except Exception as exc:
        CONSOLE_LOG(PREFIX, "HTTP_REQUEST_FAILED", {"err": _bi.str(exc)})
        return None

    if not resp.ok:
        CONSOLE_LOG(PREFIX, "HTTP_ERROR", {"status": resp.status_code, "text": (resp.text or "")[:300]})
        return None

    try:
        data = resp.json()
    except Exception:
        CONSOLE_LOG(PREFIX, "BAD_JSON", {"text": (resp.text or "")[:300]})
        return None

    if not data.get("ok"):
        CONSOLE_LOG(PREFIX, "SERVICE_NOT_OK", data)
        return None

    midi = Path(_bi.str(data.get("midi_path", ""))).resolve()
    if not midi.exists():
        CONSOLE_LOG(PREFIX, "MIDI_NOT_FOUND", {"midi_path": _bi.str(midi)})
        return None
    return midi

# ─────────────────────────────────────────────────────────────
# MIDI → note rows (relative)
# ─────────────────────────────────────────────────────────────
@DB_LOG_FUNCTIONS()
def _midi_to_relative_note_rows(midi_path: Path) -> List[Tuple[int, int, int, int]]:
    """
    Returns chunk-relative rows:
      [(START_MS_REL, END_MS_REL, MIDI_PITCH, VELOCITY), ...]
    """
    if pretty_midi is None:
        CONSOLE_LOG(PREFIX, "PRETTY_MIDI_MISSING")
        return []
    try:
        pm = pretty_midi.PrettyMIDI(_bi.str(midi_path))
    except Exception as exc:
        CONSOLE_LOG(PREFIX, "PRETTY_MIDI_LOAD_FAILED", {"err": _bi.str(exc)})
        return []

    rows: List[Tuple[int, int, int, int]] = []
    for inst in pm.instruments:
        for n in inst.notes:
            s = int(round(float(n.start) * 1000.0))
            e = int(round(float(n.end)   * 1000.0))
            rows.append((s, e, int(n.pitch), int(n.velocity)))
    return rows

# ─────────────────────────────────────────────────────────────
# Real worker (sync): write WAV, call service, parse MIDI, DB insert
# Returns number of rows inserted
# ─────────────────────────────────────────────────────────────
@DB_LOG_FUNCTIONS()
def RUN_ONS_REAL(
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    AUDIO_CHUNK_START_MS: int,
    AUDIO_ARRAY_16000: np.ndarray,
    SAMPLE_RATE_16000: int,
) -> int:
    # Ensure per-chunk dict exists for stamping counts even on early exit
    chunks = RECORDING_AUDIO_CHUNK_ARRAY.setdefault(int(RECORDING_ID), {})
    ch = chunks.setdefault(int(AUDIO_CHUNK_NO), {
        "RECORDING_ID": int(RECORDING_ID),
        "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
    })

    if not isinstance(AUDIO_ARRAY_16000, np.ndarray) or AUDIO_ARRAY_16000.size == 0:
        CONSOLE_LOG(PREFIX, "EMPTY_AUDIO", {"SR": int(SAMPLE_RATE_16000)})
        ch["ONS_RECORD_CNT"] = 0
        return 0
    if int(SAMPLE_RATE_16000) != 16000:
        CONSOLE_LOG(PREFIX, "BAD_SR", {"SR": int(SAMPLE_RATE_16000), "expected": 16000})
        ch["ONS_RECORD_CNT"] = 0
        return 0

    # Ensure per-recording temp dir and make a stable temp WAV name
    rec_dir = (TEMP_RECORDING_AUDIO_DIR / str(RECORDING_ID))
    rec_dir.mkdir(parents=True, exist_ok=True)
    wav_path = rec_dir / f"ons_chunk_{int(AUDIO_CHUNK_NO):06d}_16k.wav"

    # Write temp WAV (mono float32 → PCM_16 is fine; O&F just needs audio content)
    sf.write(_bi.str(wav_path), AUDIO_ARRAY_16000.astype("float32"), int(SAMPLE_RATE_16000), subtype="PCM_16")

    CONSOLE_LOG(PREFIX, "BEGIN", {
        "RECORDING_ID": int(RECORDING_ID),
        "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
        "AUDIO_CHUNK_START_MS": int(AUDIO_CHUNK_START_MS),
        "SR_16K": int(SAMPLE_RATE_16000),
        "WAV_PATH": _bi.str(wav_path),
    })

    midi_path = _ons_run_and_get_midi_path(wav_path)
    if not midi_path:
        CONSOLE_LOG(PREFIX, "NO_MIDI_RETURNED")
        ch["ONS_RECORD_CNT"] = 0
        return 0

    rel_rows = _midi_to_relative_note_rows(midi_path)
    CONSOLE_LOG(PREFIX, "ROWS_RELATIVE", {"count": len(rel_rows)})
    if not rel_rows:
        ch["ONS_RECORD_CNT"] = 0
        return 0

    # Convert to absolute times and tag with source
    base = int(AUDIO_CHUNK_START_MS)
    rows_abs_with_src: List[Tuple[int, int, int, int, str]] = []
    for (s_rel, e_rel, midi, vel) in rel_rows:
        rows_abs_with_src.append((base + int(s_rel), base + int(e_rel), int(midi), int(vel), "ONS"))

    # Stamp ONS record count (for Stage-6 DB_LOG_RECORDING_AUDIO_CHUNK)
    ch["ONS_RECORD_CNT"] = int(len(rows_abs_with_src))

    with DB_CONNECT_CTX() as conn:
        _db_load_note_rows(
            conn=conn,
            RECORDING_ID=int(RECORDING_ID),
            AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
            rows=rows_abs_with_src,
        )

    CONSOLE_LOG(PREFIX, "DB_INSERT_OK", {
        "RECORDING_ID": int(RECORDING_ID),
        "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
        "ROW_COUNT": len(rows_abs_with_src),
    })
    return int(len(rows_abs_with_src))

# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY (Stage-6 calls this): wrapper that reads chunk data,
# runs the real worker off-thread, and stamps duration.
# ─────────────────────────────────────────────────────────────
@DB_LOG_FUNCTIONS()
async def SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS(RECORDING_ID: int, AUDIO_CHUNK_NO: int) -> None:
    """
    Wrapper used by PROCESS_THE_AUDIO_CHUNK in Stage-6.
    Pulls AUDIO_ARRAY_16000 / SAMPLE_RATE_16000 / START_MS from the chunk dict,
    then runs RUN_ONS_REAL() in a worker thread. Stamps start/duration fields.
    """
    ch = _get_chunk(RECORDING_ID, AUDIO_CHUNK_NO)
    if ch is None:
        CONSOLE_LOG(PREFIX, "chunk_not_ready", {"rid": int(RECORDING_ID), "chunk": int(AUDIO_CHUNK_NO)})
        return

    audio_16k = ch.get("AUDIO_ARRAY_16000")
    sr_16k    = int(ch.get("SAMPLE_RATE_16000") or 0)
    start_ms  = int(ch.get("START_MS") or 0)

    # Stamp start time on the chunk for DB logging in Stage-6
    ch["DT_START_ONS"] = datetime.now()

    if audio_16k is None or sr_16k != 16000:
        # Nothing to do (log but don't crash)
        CONSOLE_LOG(PREFIX, "ons_missing_inputs", {
            "rid": int(RECORDING_ID), "chunk": int(AUDIO_CHUNK_NO),
            "has_audio": audio_16k is not None, "sr": sr_16k
        })
        ch["ONS_RECORD_CNT"] = 0
        ch["ONS_DURATION_IN_MS"] = 0
        return

    t0 = datetime.now()
    try:
        count = await asyncio.to_thread(
            RUN_ONS_REAL,
            int(RECORDING_ID),
            int(AUDIO_CHUNK_NO),
            int(start_ms),
            audio_16k,
            int(sr_16k),
        )
    except Exception as exc:
        CONSOLE_LOG(PREFIX, "FATAL_ERROR", {
            "ERROR": _bi.str(exc),
            "TRACE": traceback.format_exc(),
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
        })
        count = 0

    elapsed_ms = max(1, int((datetime.now() - t0).total_seconds() * 1000))
    ch["ONS_DURATION_IN_MS"] = elapsed_ms
    ch["ONS_RECORD_CNT"] = int(count or 0)
