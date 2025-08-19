# SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS.py
# ----------------------------------------------------------------------
# Onsets & Frames for a single 100 ms websocket audio frame.
#   • Input: mono float32 audio at 16 kHz (AUDIO_ARRAY_16000)
#   • START_MS = 100 * (AUDIO_FRAME_NO - 1)
#   • Calls O&F microservice -> MIDI; parses to (START_MS, END_MS, PITCH, VELOCITY)
#   • Bulk insert into ENGINE_LOAD_NOTE with SOURCE_METHOD='ONS'
#   • Stamps ONS_RECORD_CNT / DT_START_ONS / DT_END_ONS into per-frame metadata
# ----------------------------------------------------------------------

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Tuple, Optional
import asyncio
import builtins as _bi
import requests
import numpy as np
import soundfile as sf  # write temp wav

# MIDI parsing
try:
    import pretty_midi  # type: ignore
except Exception:  # pragma: no cover
    pretty_midi = None

from SERVER_ENGINE_APP_VARIABLES import (
    TEMP_RECORDING_AUDIO_DIR,
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY,  # per-frame metadata (assumed to exist)
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    CONSOLE_LOG,
    DB_CONNECT_CTX,
    DB_BULK_INSERT,
    ENGINE_DB_LOG_FUNCTIONS_INS,  # logging decorator
)

PREFIX = "ONS"

# Row shape for ENGINE_LOAD_NOTE inserts:
# (START_MS, END_MS, NOTE_MIDI_PITCH_NO, VOLUME_MIDI_VELOCITY_NO, SOURCE_METHOD)
NoteRow = Tuple[int, int, int, int, str]

# ─────────────────────────────────────────────────────────────
# DB bulk insert (frame-keyed)
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
def ENGINE_LOAD_NOTE_INS(
    conn,
    RECORDING_ID: int,
    AUDIO_FRAME_NO: int,
    SAMPLE_RATE: int,                 # 16000 for ONS here
    rows_abs_with_src: Iterable[NoteRow],
) -> None:
    """
    ENGINE_LOAD_NOTE columns:
      (RECORDING_ID, START_MS, END_MS,
       NOTE_MIDI_PITCH_NO, VOLUME_MIDI_VELOCITY_NO, SOURCE_METHOD,
       AUDIO_FRAME_NO, SAMPLE_RATE)
    """
    sql = """
      INSERT INTO ENGINE_LOAD_NOTE
      (RECORDING_ID, START_MS, END_MS,
       NOTE_MIDI_PITCH_NO, VOLUME_MIDI_VELOCITY_NO, SOURCE_METHOD,
       AUDIO_FRAME_NO, SAMPLE_RATE)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    DB_BULK_INSERT(
        conn,
        sql,
        (
            (RECORDING_ID, start_ms, end_ms, pitch_no, velocity_no, source_method, AUDIO_FRAME_NO, SAMPLE_RATE)
            for (start_ms, end_ms, pitch_no, velocity_no, source_method) in rows_abs_with_src
        ),
    )

# ─────────────────────────────────────────────────────────────
# O&F microservice URL
# ─────────────────────────────────────────────────────────────
def _ons_service_url() -> str:
    host = os.getenv("OAF_HOST", "127.0.0.1")
    port = int(os.getenv("OAF_PORT", "9077"))
    return f"http://{host}:{port}"

# ─────────────────────────────────────────────────────────────
# Microservice call
# ─────────────────────────────────────────────────────────────
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
# MIDI → relative note rows
# ─────────────────────────────────────────────────────────────
def _midi_to_relative_note_rows(midi_path: Path) -> List[Tuple[int, int, int, int]]:
    """
    Returns frame-relative rows:
      [(start_ms_rel, end_ms_rel, pitch_no, velocity_no), ...]
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
            start_ms_rel = int(round(float(n.start) * 1000.0))
            end_ms_rel   = int(round(float(n.end)   * 1000.0))
            rows.append((start_ms_rel, end_ms_rel, int(n.pitch), int(n.velocity)))
    return rows

# ─────────────────────────────────────────────────────────────
# Real worker (sync): write WAV, call service, parse MIDI, DB insert
# Returns number of rows inserted
# ─────────────────────────────────────────────────────────────
def RUN_ONS_REAL(
    RECORDING_ID: int,
    AUDIO_FRAME_NO: int,
    START_MS: int,
    AUDIO_ARRAY_16000: np.ndarray,
    SAMPLE_RATE: int,
) -> int:
    # Validate audio
    if not isinstance(AUDIO_ARRAY_16000, np.ndarray) or AUDIO_ARRAY_16000.size == 0:
        CONSOLE_LOG(PREFIX, "EMPTY_AUDIO", {"sr": int(SAMPLE_RATE)})
        return 0
    if int(SAMPLE_RATE) != 16000:
        CONSOLE_LOG(PREFIX, "BAD_SR", {"sr": int(SAMPLE_RATE), "expected": 16000})
        return 0

    # Ensure per-recording temp dir and make a stable temp WAV name
    rec_dir = (TEMP_RECORDING_AUDIO_DIR / str(RECORDING_ID))
    rec_dir.mkdir(parents=True, exist_ok=True)
    wav_path = rec_dir / f"ons_frame_{int(AUDIO_FRAME_NO):06d}_16k.wav"

    # Write temp WAV (mono float32 → PCM_16)
    sf.write(_bi.str(wav_path), AUDIO_ARRAY_16000.astype("float32"), int(SAMPLE_RATE), subtype="PCM_16")

    CONSOLE_LOG(PREFIX, "BEGIN", {
        "rid": int(RECORDING_ID),
        "frame": int(AUDIO_FRAME_NO),
        "start_ms": int(START_MS),
        "sr_16k": int(SAMPLE_RATE),
        "wav": _bi.str(wav_path),
    })

    midi_path = _ons_run_and_get_midi_path(wav_path)
    if not midi_path:
        CONSOLE_LOG(PREFIX, "NO_MIDI_RETURNED")
        return 0

    rel_rows = _midi_to_relative_note_rows(midi_path)
    CONSOLE_LOG(PREFIX, "ROWS_RELATIVE", {"count": len(rel_rows)})
    if not rel_rows:
        return 0

    # Convert to absolute ms and tag with source
    rows_abs_with_src: List[NoteRow] = [
        (START_MS + s_rel, START_MS + e_rel, pitch_no, velocity_no, "ONS")
        for (s_rel, e_rel, pitch_no, velocity_no) in rel_rows
    ]

    # Bulk insert
    with DB_CONNECT_CTX() as conn:
        ENGINE_LOAD_NOTE_INS(
            conn=conn,
            RECORDING_ID=int(RECORDING_ID),
            AUDIO_FRAME_NO=int(AUDIO_FRAME_NO),
            SAMPLE_RATE=int(SAMPLE_RATE),
            rows_abs_with_src=rows_abs_with_src,
        )

    CONSOLE_LOG(PREFIX, "DB_INSERT_OK", {
        "rid": int(RECORDING_ID),
        "frame": int(AUDIO_FRAME_NO),
        "row_count": len(rows_abs_with_src),
    })
    return int(len(rows_abs_with_src))

# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY: per-frame ONS (async wrapper)
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS(
    RECORDING_ID: int,
    AUDIO_FRAME_NO: int,
    AUDIO_ARRAY_16000: np.ndarray,
) -> int:
    """
    Inputs:
      • RECORDING_ID, AUDIO_FRAME_NO
      • AUDIO_ARRAY_16000: mono float32 at 16 kHz
      • SAMPLE_RATE: must be 16000
    Returns number of rows inserted.
    """

    SAMPLE_RATE = 16000

    # 100 ms per websocket frame
    START_MS = 100 * (AUDIO_FRAME_NO - 1)

    # Stamp start
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_START_ONS"] = datetime.now()

    # Offload the blocking work (WAV write + HTTP + pretty_midi) to a thread
    t0 = datetime.now()
    try:
        count = await asyncio.to_thread(
            RUN_ONS_REAL,
            int(RECORDING_ID),
            int(AUDIO_FRAME_NO),
            int(START_MS),
            AUDIO_ARRAY_16000,
            int(SAMPLE_RATE),
        )
    except Exception as exc:
        CONSOLE_LOG(PREFIX, "FATAL_ERROR", {
            "err": _bi.str(exc),
            "rid": int(RECORDING_ID),
            "frame": int(AUDIO_FRAME_NO),
        })
        count = 0

    # Stamp count / end
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["ONS_RECORD_CNT"] = int(count or 0)
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_ONS"] = datetime.now()
    return int(count or 0)
