# SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME.py
# ----------------------------------------------------------------------
# Volume processing for a single audio chunk (array-first).
#   • Input: 22,050 Hz mono float32 array
#   • Compute:
#       - Chunk aggregate volume using 1 ms RMS windows (avg_rms, avg_db)
#       - 10 ms RMS series [(start_ms, end_ms, rms, db), ...]
#   • Insert:
#       - ENGINE_LOAD_VOLUME (one row per chunk)
#       - ENGINE_LOAD_VOLUME_10_MS (series rows)
# Notes:
#   • Pure NumPy implementation (no librosa dependency here)
#   • All times are ABSOLUTE (offset by AUDIO_CHUNK_START_MS)
# ----------------------------------------------------------------------

from __future__ import annotations

import math
import time  # <-- NEW
import traceback
from typing import Iterable, List, Tuple

import builtins as _bi
import numpy as np

from SERVER_ENGINE_APP_VARIABLES import RECORDING_AUDIO_CHUNK_ARRAY  # <-- NEW
from SERVER_ENGINE_APP_FUNCTIONS import (
    CONSOLE_LOG,
    DB_CONNECT,
    DB_BULK_INSERT,
)

PREFIX = "VOLUME"

# ─────────────────────────────────────────────────────────────
# DB loaders
# ─────────────────────────────────────────────────────────────
def _db_load_volume_aggregate_row(
    conn,
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    AUDIO_CHUNK_START_MS: int,
    avg_rms: float,
    avg_db: float,
) -> None:
    """
    ENGINE_LOAD_VOLUME:
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, VOLUME, VOLUME_IN_DB)
    """
    sql = """
      INSERT INTO ENGINE_LOAD_VOLUME
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, VOLUME, VOLUME_IN_DB)
      VALUES (?, ?, ?, ?, ?)
    """
    DB_BULK_INSERT(
        conn,
        sql,
        [(RECORDING_ID, AUDIO_CHUNK_NO, AUDIO_CHUNK_START_MS, float(avg_rms), float(avg_db))],
    )


def _db_load_volume_10ms_series(
    conn,
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    rows_10ms: Iterable[Tuple[int, int, float, float]],
) -> None:
    """
    ENGINE_LOAD_VOLUME_10_MS:
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS, VOLUME, VOLUME_IN_DB)
    """
    rows_10ms = list(rows_10ms)
    if not rows_10ms:
        return
    sql = """
      INSERT INTO ENGINE_LOAD_VOLUME_10_MS
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS, VOLUME, VOLUME_IN_DB)
      VALUES (?, ?, ?, ?, ?, ?)
    """
    DB_BULK_INSERT(
        conn,
        sql,
        ((RECORDING_ID, AUDIO_CHUNK_NO, s, e, float(v), float(db)) for (s, e, v, db) in rows_10ms),
    )

# ─────────────────────────────────────────────────────────────
# Volume math (NumPy-only, center=False)
# ─────────────────────────────────────────────────────────────
def _rms_series(
    audio: np.ndarray,
    sr: int,
    hop_ms: int,
    win_ms: int,
    base_ms: int,
    end_span_ms: int,
) -> List[Tuple[int, int, float, float]]:
    """
    Generic RMS time series:
      • hop_ms: step between frames (ms)
      • win_ms: window length (ms)
      • base_ms: absolute offset to add to frame start
      • end_span_ms: END_MS = START_MS + end_span_ms
      • center=False (no padding)
    Returns: [(START_MS_ABS, END_MS_ABS, RMS, DB), ...]
    """
    if sr <= 0 or audio.size == 0:
        return []

    hop = max(1, int(round(sr * (hop_ms / 1000.0))))
    win = max(1, int(round(sr * (win_ms / 1000.0))))
    N = int(audio.size)
    if win > N:
        return []

    hann = np.hanning(win) if win > 1 else None
    out: List[Tuple[int, int, float, float]] = []

    i = 0
    frame_idx = 0
    while i + win <= N:
        seg = audio[i:i + win]
        if hann is not None:
            seg = seg * hann
        v = float(np.sqrt(np.mean(seg * seg))) if seg.size else 0.0
        db = float(20.0 * math.log10(v + 1e-6))
        s_ms = base_ms + frame_idx * hop_ms
        e_ms = s_ms + end_span_ms
        out.append((s_ms, e_ms, v, db))
        i += hop
        frame_idx += 1

    return out


def _aggregate_from_series(series: List[Tuple[int, int, float, float]]) -> Tuple[float, float]:
    """
    Average RMS over frames; convert to dB with epsilon.
    """
    if not series:
        return 0.0, -120.0
    rms_vals = np.array([v for (_, _, v, _) in series], dtype=np.float64)
    avg_rms = float(rms_vals.mean()) if rms_vals.size else 0.0
    avg_db = float(20.0 * math.log10(avg_rms + 1e-6))
    return avg_rms, avg_db

# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY (called by Step-2)
# ─────────────────────────────────────────────────────────────
def SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME(
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

    Inserts:
      • ENGINE_LOAD_VOLUME (aggregate from 1 ms RMS)
      • ENGINE_LOAD_VOLUME_10_MS (10 ms RMS series)
    """
    try:
        # Ensure per-chunk dict exists for metric stamping
        chunks = RECORDING_AUDIO_CHUNK_ARRAY.setdefault(int(RECORDING_ID), {})  # <-- NEW
        ch = chunks.setdefault(int(AUDIO_CHUNK_NO), {                           # <-- NEW
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
        })

        def _early_zero_and_return(msg: str):
            CONSOLE_LOG(PREFIX, msg)
            ch["VOLUME_1_MS_RECORD_CNT"] = 0
            ch["VOLUME_10_MS_RECORD_CNT"] = 0
            ch["VOLUME_1_MS_DURATION_IN_MS"] = 0
            ch["VOLUME_10_MS_DURATION_IN_MS"] = 0
            return

        if not isinstance(AUDIO_ARRAY_22050, np.ndarray) or AUDIO_ARRAY_22050.size == 0:
            return _early_zero_and_return("EMPTY_AUDIO")
        if int(SAMPLE_RATE_22050) != 22050:
            return _early_zero_and_return("UNEXPECTED_SR")

        audio = AUDIO_ARRAY_22050.astype(np.float32, copy=False)
        base_ms = int(AUDIO_CHUNK_START_MS)

        CONSOLE_LOG(PREFIX, "BEGIN", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
            "AUDIO_CHUNK_START_MS": base_ms,
            "SR": int(SAMPLE_RATE_22050),
            "SAMPLES": int(audio.shape[0]),
        })

        # 1 ms RMS (for aggregate); END_MS == START_MS
        series_1ms = _rms_series(
            audio=audio, sr=22050,
            hop_ms=1, win_ms=2,
            base_ms=base_ms, end_span_ms=0,
        )
        avg_rms, avg_db = _aggregate_from_series(series_1ms)

        # 10 ms RMS series; END_MS = START_MS + 9
        series_10ms = _rms_series(
            audio=audio, sr=22050,
            hop_ms=10, win_ms=20,
            base_ms=base_ms, end_span_ms=9,
        )

        # Stamp record counts in memory (available for Step-2 logging)
        ch["VOLUME_1_MS_RECORD_CNT"] = int(len(series_1ms))        # <-- NEW
        ch["VOLUME_10_MS_RECORD_CNT"] = int(len(series_10ms))      # <-- NEW

        with DB_CONNECT() as conn:
            # Time the 1-ms aggregate insert
            t1 = time.perf_counter()                                # <-- NEW
            _db_load_volume_aggregate_row(
                conn=conn,
                RECORDING_ID=int(RECORDING_ID),
                AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
                AUDIO_CHUNK_START_MS=base_ms,
                avg_rms=avg_rms,
                avg_db=avg_db,
            )
            ch["VOLUME_1_MS_DURATION_IN_MS"] = int(                 # <-- NEW
                round((time.perf_counter() - t1) * 1000)
            )

            # Time the 10-ms series bulk insert
            t10 = time.perf_counter()                               # <-- NEW
            _db_load_volume_10ms_series(
                conn=conn,
                RECORDING_ID=int(RECORDING_ID),
                AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
                rows_10ms=series_10ms,
            )
            ch["VOLUME_10_MS_DURATION_IN_MS"] = int(                # <-- NEW
                round((time.perf_counter() - t10) * 1000)
            )

        CONSOLE_LOG(PREFIX, "DB_INSERT_OK", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
            "ROWS_10MS": len(series_10ms),
            "AVG_RMS": round(avg_rms, 6),
            "AVG_DB": round(avg_db, 3),
        })

    except Exception as exc:
        CONSOLE_LOG(PREFIX, "FATAL_ERROR", {
            "ERROR": _bi.str(exc),
            "TRACE": traceback.format_exc(),
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
        })
