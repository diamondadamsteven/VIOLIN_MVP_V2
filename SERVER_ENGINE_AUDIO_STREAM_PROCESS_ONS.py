# SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS.py
# ----------------------------------------------------------------------
# Onsets & Frames processing for a single audio chunk.
#   • Accept 16 kHz mono float32 audio (array)
#   • Temp-write WAV for the O&F microservice (expects a file path)
#   • Call microservice → MIDI path
#   • Parse MIDI → (START_MS, END_MS, MIDI, VELOCITY) relative
#   • Convert to ABSOLUTE times with AUDIO_CHUNK_START_MS
#   • Bulk insert into ENGINE_LOAD_NOTE (SOURCE_METHOD='ONS')
# ----------------------------------------------------------------------

from __future__ import annotations

import os
import traceback
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

from SERVER_ENGINE_APP_VARIABLES import TEMP_RECORDING_AUDIO_DIR, RECORDING_AUDIO_CHUNK_ARRAY  # <-- added
from SERVER_ENGINE_APP_FUNCTIONS import (
    CONSOLE_LOG,
    DB_CONNECT,
    DB_BULK_INSERT,
)

PREFIX = "ONS"

# ─────────────────────────────────────────────────────────────
# DB bulk insert
# ─────────────────────────────────────────────────────────────
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
# MIDI → note rows (relative)
# ─────────────────────────────────────────────────────────────
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
# PUBLIC ENTRY (called by Step-2)
# ─────────────────────────────────────────────────────────────
def SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS(
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    AUDIO_CHUNK_START_MS: int,
    AUDIO_ARRAY_16000: np.ndarray,
    SAMPLE_RATE_16000: int,
) -> None:
    """
    Inputs:
      • RECORDING_ID, AUDIO_CHUNK_NO
      • AUDIO_CHUNK_START_MS: absolute ms offset for this chunk
      • AUDIO_ARRAY_16000: mono float32 at 16,000 Hz
      • SAMPLE_RATE_16000: expected 16000

    Behavior:
      • Temp-write WAV for microservice
      • Microservice → MIDI path
      • Parse MIDI → relative rows
      • Offset to ABS ms and bulk-insert to ENGINE_LOAD_NOTE (SOURCE_METHOD='ONS')
    """
    try:
        if not isinstance(AUDIO_ARRAY_16000, np.ndarray) or AUDIO_ARRAY_16000.size == 0:
            CONSOLE_LOG(PREFIX, "EMPTY_AUDIO", {"SR": SAMPLE_RATE_16000})
            # stamp zero so Step-2 logging has an explicit count
            chunks = RECORDING_AUDIO_CHUNK_ARRAY.setdefault(int(RECORDING_ID), {})
            chunks.setdefault(int(AUDIO_CHUNK_NO), {"RECORDING_ID": int(RECORDING_ID), "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO)})["ONS_RECORD_CNT"] = 0
            return
        if int(SAMPLE_RATE_16000) <= 0:
            CONSOLE_LOG(PREFIX, "BAD_SR", {"SR": SAMPLE_RATE_16000})
            chunks = RECORDING_AUDIO_CHUNK_ARRAY.setdefault(int(RECORDING_ID), {})
            chunks.setdefault(int(AUDIO_CHUNK_NO), {"RECORDING_ID": int(RECORDING_ID), "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO)})["ONS_RECORD_CNT"] = 0
            return

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
            chunks = RECORDING_AUDIO_CHUNK_ARRAY.setdefault(int(RECORDING_ID), {})
            chunks.setdefault(int(AUDIO_CHUNK_NO), {"RECORDING_ID": int(RECORDING_ID), "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO)})["ONS_RECORD_CNT"] = 0
            return

        rel_rows = _midi_to_relative_note_rows(midi_path)
        CONSOLE_LOG(PREFIX, "ROWS_RELATIVE", {"count": len(rel_rows)})
        if not rel_rows:
            chunks = RECORDING_AUDIO_CHUNK_ARRAY.setdefault(int(RECORDING_ID), {})
            chunks.setdefault(int(AUDIO_CHUNK_NO), {"RECORDING_ID": int(RECORDING_ID), "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO)})["ONS_RECORD_CNT"] = 0
            return

        # Convert to absolute times and tag with source
        rows_abs_with_src: List[Tuple[int, int, int, int, str]] = []
        base = int(AUDIO_CHUNK_START_MS)
        for (s_rel, e_rel, midi, vel) in rel_rows:
            rows_abs_with_src.append((base + int(s_rel), base + int(e_rel), int(midi), int(vel), "ONS"))

        # NEW: stamp ONS record count in memory for Step-2's DB_LOG_RECORDING_AUDIO_CHUNK
        chunks = RECORDING_AUDIO_CHUNK_ARRAY.setdefault(int(RECORDING_ID), {})
        ch = chunks.setdefault(int(AUDIO_CHUNK_NO), {"RECORDING_ID": int(RECORDING_ID), "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO)})
        ch["ONS_RECORD_CNT"] = int(len(rows_abs_with_src))

        with DB_CONNECT() as conn:
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

    except Exception as exc:
        CONSOLE_LOG(PREFIX, "FATAL_ERROR", {
            "ERROR": _bi.str(exc),
            "TRACE": traceback.format_exc(),
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
        })
