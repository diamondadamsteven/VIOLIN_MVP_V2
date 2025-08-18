# SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN.py
# ----------------------------------------------------------------------
# pYIN for a single audio chunk (array-first, 22.05 kHz mono float32).
#   • Run librosa.pyin on 22.05 kHz audio
#   • Produce (START_MS, END_MS, HZ, CONFIDENCE) relative @ ~10 ms
#   • Offset by AUDIO_CHUNK_START_MS to ABS times
#   • Bulk insert into ENGINE_LOAD_HZ with SOURCE_METHOD='PYIN'
#   • Non-blocking compute/insert using schedule_coro(asyncio.to_thread(...))
# ----------------------------------------------------------------------

from __future__ import annotations

from datetime import datetime
import traceback
import inspect
import os
import asyncio
from typing import Iterable, List, Tuple

import builtins as _bi
import numpy as np

try:
    import librosa  # type: ignore
except Exception:  # pragma: no cover
    librosa = None  # type: ignore

from SERVER_ENGINE_APP_VARIABLES import RECORDING_AUDIO_CHUNK_ARRAY
from SERVER_ENGINE_APP_FUNCTIONS import (
    CONSOLE_LOG,
    DB_CONNECT_CTX,
    DB_BULK_INSERT,
    DB_LOG_FUNCTIONS,
    DB_LOG_ENGINE_DB_LOG_STEPS,   # enqueues & stamps DT_ADDED
    schedule_coro,                # loop/thread safe
)

PREFIX = "PYIN"

# ─────────────────────────────────────────────────────────────
# small helper to log a processing step (with DT_ADDED)
# ─────────────────────────────────────────────────────────────
def log_step(step: str, RECORDING_ID: int, AUDIO_CHUNK_NO: int) -> None:
    DB_LOG_ENGINE_DB_LOG_STEPS(
        STEP_NAME=step,
        PYTHON_FUNCTION_NAME=inspect.currentframe().f_back.f_code.co_name,
        PYTHON_FILE_NAME=os.path.basename(__file__),
        RECORDING_ID=int(RECORDING_ID),
        AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
        FRAME_NO=None,
        DT_ADDED=datetime.now(),
    )

# ─────────────────────────────────────────────────────────────
# DB bulk insert
# ─────────────────────────────────────────────────────────────
@DB_LOG_FUNCTIONS()
def _db_load_hz_series(
    conn,
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    SOURCE_METHOD: str,
    rows: Iterable[Tuple[int, int, float, float]],
) -> None:
    """
    ENGINE_LOAD_HZ:
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS, SOURCE_METHOD, HZ, CONFIDENCE)
    """
    sql = """
      INSERT INTO ENGINE_LOAD_HZ
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS, SOURCE_METHOD, HZ, CONFIDENCE)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    DB_BULK_INSERT(
        conn,
        sql,
        (
            (RECORDING_ID, AUDIO_CHUNK_NO, s, e, SOURCE_METHOD, float(hz), float(conf))
            for (s, e, hz, conf) in rows
        ),
    )

# ─────────────────────────────────────────────────────────────
# pYIN core (relative to the chunk)
# ─────────────────────────────────────────────────────────────
@DB_LOG_FUNCTIONS()
def _pyin_relative_series(audio_22k: np.ndarray, sr: int = 22050) -> List[Tuple[int, int, float, float]]:
    """
    Returns per-frame rows relative to the chunk:
      [(START_MS_REL, END_MS_REL, HZ, CONFIDENCE), ...] at ~10 ms hop.
    """
    if librosa is None:
        CONSOLE_LOG(PREFIX, "LIBROSA_NOT_AVAILABLE")
        return []

    if sr != 22050 or audio_22k.size == 0:
        CONSOLE_LOG(PREFIX, "BAD_INPUT", {"sr": int(sr), "size": int(audio_22k.size)})
        return []

    # ~10 ms hop @ 22.05 kHz
    hop = max(1, int(round(sr * 0.010)))  # typically 221
    frame_len = max(hop * 4, 2048)

    def _run(with_bounds: bool):
        if not with_bounds:
            return librosa.pyin(
                y=audio_22k, sr=sr,
                frame_length=frame_len, hop_length=hop, center=True
            )
        # Violin-ish / musical bounds; safe for voice too
        try:
            fmin = float(librosa.note_to_hz("G3"))
            fmax = float(librosa.note_to_hz("C8"))
        except Exception:
            fmin, fmax = 196.0, 4186.0
        return librosa.pyin(
            y=audio_22k, sr=sr,
            fmin=fmin, fmax=fmax,
            frame_length=frame_len, hop_length=hop, center=True
        )

    try:
        f0, vflag, vprob = _run(with_bounds=False)
    except TypeError:
        try:
            f0, vflag, vprob = _run(with_bounds=True)
        except Exception as exc:
            CONSOLE_LOG(PREFIX, "PYIN_FAILED", {"err": _bi.str(exc)})
            return []
    except Exception as exc:
        CONSOLE_LOG(PREFIX, "PYIN_FAILED", {"err": _bi.str(exc)})
        return []

    rows: List[Tuple[int, int, float, float]] = []
    for i, (hz, voiced_ok, conf) in enumerate(zip(f0, vflag, vprob)):
        if not voiced_ok or hz is None:
            continue
        if not np.isfinite(hz) or hz <= 0.0:
            continue
        s_rel = int(round((i * hop) * 1000.0 / sr))
        e_rel = s_rel + 9  # nominal 10 ms span
        rows.append((s_rel, e_rel, float(hz), float(conf)))

    if rows:
        starts = [s for (s, _, _, _) in rows]
        mods = sorted(set([s % 10 for s in starts]))
        steps = sorted(set(np.diff(starts))) if len(starts) > 1 else []
        CONSOLE_LOG(PREFIX, "TIMING_SUMMARY", {
            "count": len(rows),
            "first_ms": starts[0],
            "last_ms": starts[-1],
            "mods_of_10": mods[:6],
            "unique_step_sizes": steps[:6],
        })

    return rows

# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY (called by Step-2)
# ─────────────────────────────────────────────────────────────
@DB_LOG_FUNCTIONS()
def SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN(
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    AUDIO_CHUNK_START_MS: int,
    AUDIO_ARRAY_22050: np.ndarray,
    SAMPLE_RATE_22050: int,
) -> None:
    """
    Inputs (from Step-2):
      • RECORDING_ID, AUDIO_CHUNK_NO
      • AUDIO_CHUNK_START_MS: absolute start (ms) for this chunk
      • AUDIO_ARRAY_22050: mono float32 audio at 22,050 Hz (required)
      • SAMPLE_RATE_22050: must be 22050

    Behavior:
      • pYIN -> relative rows
      • Offset by AUDIO_CHUNK_START_MS to ABS times
      • Bulk insert into ENGINE_LOAD_HZ with SOURCE_METHOD='PYIN'
    """
    try:
        log_step("BEGIN", RECORDING_ID, AUDIO_CHUNK_NO)

        # Ensure a chunk map exists for stamping counts
        chunks = RECORDING_AUDIO_CHUNK_ARRAY.setdefault(int(RECORDING_ID), {})
        ch = chunks.setdefault(int(AUDIO_CHUNK_NO), {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
        })

        if not isinstance(AUDIO_ARRAY_22050, np.ndarray) or AUDIO_ARRAY_22050.size == 0:
            CONSOLE_LOG(PREFIX, "EMPTY_AUDIO")
            ch["PYIN_RECORD_CNT"] = 0
            return
        if int(SAMPLE_RATE_22050) != 22050:
            CONSOLE_LOG(PREFIX, "UNEXPECTED_SR", {"got": int(SAMPLE_RATE_22050), "expected": 22050})
            ch["PYIN_RECORD_CNT"] = 0
            return

        CONSOLE_LOG(PREFIX, "BEGIN", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
            "AUDIO_CHUNK_START_MS": int(AUDIO_CHUNK_START_MS),
            "SR": int(SAMPLE_RATE_22050),
            "SAMPLES": int(AUDIO_ARRAY_22050.shape[0]),
        })

        def _job():
            rows_rel = _pyin_relative_series(
                AUDIO_ARRAY_22050.astype(np.float32, copy=False),
                sr=22050
            )
            if not rows_rel:
                CONSOLE_LOG(PREFIX, "NO_ROWS")
                ch["PYIN_RECORD_CNT"] = 0
                return

            base = int(AUDIO_CHUNK_START_MS)
            rows_abs: List[Tuple[int, int, float, float]] = [
                (base + s_rel, base + e_rel, hz, conf) for (s_rel, e_rel, hz, conf) in rows_rel
            ]

            # stamp count for Step-2 DB logging
            ch["PYIN_RECORD_CNT"] = int(len(rows_abs))

            with DB_CONNECT_CTX() as conn:
                _db_load_hz_series(
                    conn=conn,
                    RECORDING_ID=int(RECORDING_ID),
                    AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
                    SOURCE_METHOD="PYIN",
                    rows=rows_abs,
                )

            CONSOLE_LOG(PREFIX, "DB_INSERT_OK", {
                "RECORDING_ID": int(RECORDING_ID),
                "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
                "ROW_COUNT": len(rows_abs),
            })
            log_step("DONE", RECORDING_ID, AUDIO_CHUNK_NO)

        # Offload compute + DB to a background worker safely
        schedule_coro(asyncio.to_thread(_job))

    except Exception as exc:
        CONSOLE_LOG(PREFIX, "FATAL_ERROR", {
            "ERROR": _bi.str(exc),
            "TRACE": traceback.format_exc(),
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
        })
