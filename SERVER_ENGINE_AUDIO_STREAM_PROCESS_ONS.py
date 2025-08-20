# SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS.py
# ----------------------------------------------------------------------
# Onsets & Frames (real-time-ish) for 100 ms websocket audio frames.
#   • Input: mono float32 audio at 16 kHz (AUDIO_ARRAY_16000)
#   • START_MS = 100 * (AUDIO_FRAME_NO - 1)
#   • Maintains a persistent O&F session per RECORDING_ID
#   • Streams each frame as raw PCM16 bytes with absolute offset
#   • Service returns { commit_ms, notes[] }, we insert only stable notes:
#       notes where note.end_ms <= commit_ms and > last_committed_ms
#   • Bulk insert into ENGINE_LOAD_NOTE with SOURCE_METHOD='ONS'
#   • Stamps ONS_RECORD_CNT / DT_START_ONS / DT_END_ONS into per-frame metadata
#   • Call SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS_FINALIZE(RECORDING_ID)
#     after STOP to flush remaining notes and close the session.
# ----------------------------------------------------------------------

from __future__ import annotations

import os
import io
import base64
from datetime import datetime
from typing import Iterable, List, Tuple, Optional, Dict, Any

import asyncio
import builtins as _bi
import requests
import numpy as np

# pretty_midi no longer required for streaming JSON notes; keep optional
try:
    import pretty_midi  # type: ignore
except Exception:  # pragma: no cover
    pretty_midi = None  # not used in streaming mode

from SERVER_ENGINE_APP_VARIABLES import (
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY,  # per-frame metadata (assumed to exist)
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    CONSOLE_LOG,
    DB_CONNECT_CTX,
    DB_BULK_INSERT,
    ENGINE_DB_LOG_FUNCTIONS_INS,  # logging decorator
)

PREFIX = "ONS"

# Constants
SAMPLE_RATE = 16000               # 16 kHz for O&F
FRAME_MS = 100                    # 100 ms per websocket audio frame
SOURCE_METHOD = "ONS"

# ----------------------------------------------------------------------
# Row shape for ENGINE_LOAD_NOTE inserts:
# (START_MS, END_MS, NOTE_MIDI_PITCH_NO, VOLUME_MIDI_VELOCITY_NO, SOURCE_METHOD)
# ----------------------------------------------------------------------
NoteRow = Tuple[int, int, int, int, str]

# ----------------------------------------------------------------------
# In-memory session state (per RECORDING_ID)
#   session_id:      microservice session token
#   last_committed:  last commit watermark in ms (notes <= this are stable)
#   open:            whether the session is open
# ----------------------------------------------------------------------
_ONS_STREAM_STATE: Dict[int, Dict[str, Any]] = {}


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
# Helpers
# ─────────────────────────────────────────────────────────────
def _float32_to_pcm16le_bytes(x: np.ndarray) -> bytes:
    """Clamp float32 [-1,1] → PCM16 (little-endian) bytes."""
    x = np.clip(x, -1.0, 1.0).astype(np.float32, copy=False)
    return (x * 32767.0).astype("<i2", copy=False).tobytes()


def _ons_service_url() -> str:
    host = os.getenv("OAF_HOST", "127.0.0.1")
    port = int(os.getenv("OAF_PORT", "9077"))
    return f"http://{host}:{port}".rstrip("/")


def _get_or_open_session(RECORDING_ID: int) -> Optional[str]:
    """
    Ensure a streaming session exists for this RECORDING_ID.
    POST /session/start {sample_rate} → {ok, session_id}
    """
    state = _ONS_STREAM_STATE.get(RECORDING_ID)
    if state and state.get("open") and isinstance(state.get("session_id"), str):
        return _bi.str(state["session_id"])

    url = _ons_service_url() + "/session/start"
    resp = requests.post(url, json={"sample_rate": SAMPLE_RATE}, timeout=10)
    if not resp.ok:
        CONSOLE_LOG(PREFIX, "SESSION_START_HTTP_ERROR", {"rid": int(RECORDING_ID), "status": resp.status_code})
        return None

    data = resp.json()
    if not data.get("ok"):
        CONSOLE_LOG(PREFIX, "SESSION_START_NOT_OK", {"rid": int(RECORDING_ID), "data": data})
        return None

    session_id = _bi.str(data.get("session_id", ""))
    if not session_id:
        CONSOLE_LOG(PREFIX, "SESSION_ID_MISSING", {"rid": int(RECORDING_ID)})
        return None

    _ONS_STREAM_STATE[RECORDING_ID] = {
        "session_id": session_id,
        "last_committed": -1,   # nothing committed yet
        "open": True,
    }
    return session_id


def _session_ingest_and_get_notes(
    session_id: str,
    offset_ms: int,
    pcm16_bytes: bytes,
    finalize: bool = False,
    timeout_s: int = 15,
) -> Optional[Dict[str, Any]]:
    """
    POST raw PCM bytes to /session/ingest with headers:
      X-Session-Id: <session_id>
      X-Offset-Ms:  <absolute start offset in ms>
      X-Finalize:   0/1

    Expected JSON response:
      {
        "ok": true,
        "commit_ms": <int>,                 # watermark: notes with end_ms <= commit_ms are stable
        "notes": [                          # (absolute times in ms)
          {"start_ms": int, "end_ms": int, "pitch": int, "velocity": int},
          ...
        ]
      }
    """
    url = _ons_service_url() + "/session/ingest"
    headers = {
        "Content-Type": "application/octet-stream",
        "X-Session-Id": session_id,
        "X-Offset-Ms": _bi.str(int(offset_ms)),
        "X-Finalize": "1" if finalize else "0",
        "X-Sample-Rate": _bi.str(int(SAMPLE_RATE)),
    }

    resp = requests.post(url, data=pcm16_bytes, headers=headers, timeout=timeout_s)

    if not resp.ok:
        # Some servers use 204 on finalize with no body; tolerate that by returning empty ok
        if finalize and resp.status_code == 204:
            return {"ok": True, "commit_ms": None, "notes": []}
        CONSOLE_LOG(PREFIX, "SESSION_INGEST_HTTP_ERROR", {"sid": session_id, "status": resp.status_code})
        return None

    data = resp.json()
    if not data.get("ok", False):
        CONSOLE_LOG(PREFIX, "SESSION_INGEST_NOT_OK", {"sid": session_id, "data": {k: data.get(k) for k in ("ok","error","commit_ms")}})
        return None

    # Normalize shapes
    notes = data.get("notes") or []
    commit_ms = data.get("commit_ms", None)
    if commit_ms is not None:
        try:
            commit_ms = int(commit_ms)
        except Exception:
            commit_ms = None

    # Ensure required keys exist in notes; drop malformed ones
    out_notes = []
    for n in notes:
        try:
            s = int(n.get("start_ms"))
            e = int(n.get("end_ms"))
            p = int(n.get("pitch"))
            v = int(n.get("velocity"))
            out_notes.append({"start_ms": s, "end_ms": e, "pitch": p, "velocity": v})
        except Exception:
            continue

    return {"ok": True, "commit_ms": commit_ms, "notes": out_notes}


def _insert_committed_notes(
    RECORDING_ID: int,
    AUDIO_FRAME_NO: int,
    notes: List[Dict[str, int]],
    last_committed_before: int,
    commit_ms: Optional[int],
) -> int:
    """
    Insert only the stable subset:
      end_ms <= commit_ms AND end_ms > last_committed_before
    Returns number of rows inserted.
    """
    if commit_ms is None or not notes:
        return 0

    rows: List[NoteRow] = []
    for n in notes:
        if n["end_ms"] <= commit_ms and n["end_ms"] > last_committed_before:
            rows.append((n["start_ms"], n["end_ms"], n["pitch"], n["velocity"], SOURCE_METHOD))

    if not rows:
        return 0

    with DB_CONNECT_CTX() as conn:
        ENGINE_LOAD_NOTE_INS(
            conn=conn,
            RECORDING_ID=int(RECORDING_ID),
            AUDIO_FRAME_NO=int(AUDIO_FRAME_NO),
            SAMPLE_RATE=SAMPLE_RATE,
            rows_abs_with_src=rows,
        )

    return len(rows)


# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY: per-frame ONS (streaming / real-time-ish ingest)
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
    Behavior:
      • Opens/uses a persistent O&F session per recording
      • Streams this frame’s PCM16 bytes with absolute offset
      • Inserts only stable (committed) notes returned by the service
    Returns number of NOTE rows inserted for this call.
    """
    # Stamp start
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_START_ONS"] = datetime.now()

    # Validate audio
    if not isinstance(AUDIO_ARRAY_16000, np.ndarray) or AUDIO_ARRAY_16000.size == 0:
        CONSOLE_LOG(PREFIX, "EMPTY_AUDIO", {"rid": int(RECORDING_ID), "frame": int(AUDIO_FRAME_NO)})
        ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["ONS_RECORD_CNT"] = 0
        ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_ONS"] = datetime.now()
        return 0

    # Session
    session_id = _get_or_open_session(int(RECORDING_ID))
    if not session_id:
        ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["ONS_RECORD_CNT"] = 0
        ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_ONS"] = datetime.now()
        return 0

    # Convert to PCM16 bytes and compute absolute offset
    pcm16_bytes = _float32_to_pcm16le_bytes(AUDIO_ARRAY_16000.astype(np.float32, copy=False))
    START_MS = FRAME_MS * max(int(AUDIO_FRAME_NO) - 1, 0)

    # Blocking HTTP → run in a worker thread
    resp = await asyncio.to_thread(
        _session_ingest_and_get_notes,
        session_id,
        START_MS,
        pcm16_bytes,
        False,                   # finalize=False for regular frames
    )

    if not resp or not resp.get("ok", False):
        # No rows; stamp end and exit
        ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["ONS_RECORD_CNT"] = 0
        ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_ONS"] = datetime.now()
        return 0

    # Commit logic
    state = _ONS_STREAM_STATE.get(int(RECORDING_ID), {})
    last_committed_before = int(state.get("last_committed", -1))
    commit_ms = resp.get("commit_ms")
    notes = resp.get("notes", [])

    inserted = _insert_committed_notes(
        RECORDING_ID=int(RECORDING_ID),
        AUDIO_FRAME_NO=int(AUDIO_FRAME_NO),
        notes=notes,
        last_committed_before=last_committed_before,
        commit_ms=commit_ms,
    )

    # Advance watermark if provided
    if commit_ms is not None:
        state["last_committed"] = max(last_committed_before, int(commit_ms))
        _ONS_STREAM_STATE[int(RECORDING_ID)] = state

    # Stamp count/end
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["ONS_RECORD_CNT"] = int(inserted)
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_ONS"] = datetime.now()

    CONSOLE_LOG(PREFIX, "INGEST_OK", {
        "rid": int(RECORDING_ID),
        "frame": int(AUDIO_FRAME_NO),
        "commit_ms": commit_ms,
        "notes_in": len(notes),
        "rows_inserted": int(inserted),
        "last_committed": int(_ONS_STREAM_STATE.get(int(RECORDING_ID), {}).get("last_committed", -1)),
    })

    return int(inserted)


# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY: finalize a recording’s O&F session
#   Call this after STOP (or when you know the last frame arrived)
#   to flush tail notes and close session server-side.
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS_FINALIZE(RECORDING_ID: int) -> int:
    """
    Flush any remaining notes and close the streaming session.
    Returns number of rows inserted during finalize (if any).
    """
    state = _ONS_STREAM_STATE.get(int(RECORDING_ID))
    if not state or not state.get("open"):
        return 0

    session_id = _bi.str(state.get("session_id", ""))
    if not session_id:
        return 0

    # Send an empty ingest with finalize=1 (service should flush + close)
    resp = await asyncio.to_thread(
        _session_ingest_and_get_notes,
        session_id,
        offset_ms=int(state.get("last_committed", 0)),  # offset irrelevant for finalize
        pcm16_bytes=b"",
        finalize=True,
    )

    inserted_total = 0
    if resp and resp.get("ok", False):
        commit_ms = resp.get("commit_ms")
        notes = resp.get("notes", [])
        last_committed_before = int(state.get("last_committed", -1))

        # Use AUDIO_FRAME_NO=0 for finalize inserts (or any sentinel) since they span frames
        inserted_total = _insert_committed_notes(
            RECORDING_ID=int(RECORDING_ID),
            AUDIO_FRAME_NO=0,
            notes=notes,
            last_committed_before=last_committed_before,
            commit_ms=commit_ms if commit_ms is not None else 10**12,  # treat as "commit all"
        )

    # Mark session closed
    state["open"] = False
    _ONS_STREAM_STATE[int(RECORDING_ID)] = state

    CONSOLE_LOG(PREFIX, "FINALIZE_OK", {
        "rid": int(RECORDING_ID),
        "rows_inserted": int(inserted_total),
        "last_committed": int(state.get("last_committed", -1)),
    })

    return int(inserted_total)
