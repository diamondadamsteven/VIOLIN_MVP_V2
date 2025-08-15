# SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT.py
# ----------------------------------------------------------------------
# FFT processing for a single audio chunk.
#   • Input: mono float32 audio at 22.05 kHz + ABS chunk start (ms)
#   • 100 ms Hann window / 100 ms hop
#   • Per-frame max-normalized magnitudes
#   • Absolute ms (start/end) = CHUNK_START_MS + frame offsets
#   • Bulk insert -> ENGINE_LOAD_FFT
# NOTE: Do NOT call P_ENGINE_ALL_METHOD_FFT here — Step-2 does that.
# ----------------------------------------------------------------------

from __future__ import annotations

import traceback
from typing import Iterable, List, Tuple

import builtins as _bi
import numpy as np

from SERVER_ENGINE_APP_VARIABLES import (
    RECORDING_AUDIO_CHUNK_ARRAY,  # <-- added
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    CONSOLE_LOG,
    DB_CONNECT,
    DB_BULK_INSERT,
)

PREFIX = "FFT"

# ─────────────────────────────────────────────────────────────
# DB bulk load
# ─────────────────────────────────────────────────────────────
def _db_load_fft_rows(
    conn,
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    rows: Iterable[Tuple[int, int, int, float, float, float, float]],
) -> None:
    """
    ENGINE_LOAD_FFT columns:
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS,
       FFT_BUCKET_NO, HZ_START, HZ_END, FFT_BUCKET_SIZE_IN_HZ, FFT_VALUE)
    """
    sql = """
      INSERT INTO ENGINE_LOAD_FFT
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS,
       FFT_BUCKET_NO, HZ_START, HZ_END, FFT_BUCKET_SIZE_IN_HZ, FFT_VALUE)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    DB_BULK_INSERT(
        conn,
        sql,
        (
            (RECORDING_ID, AUDIO_CHUNK_NO, s, e, bno, hz0, hz1, bsz, val)
            for (s, e, bno, hz0, hz1, bsz, val) in rows
        ),
    )

# ─────────────────────────────────────────────────────────────
# Core FFT
# ─────────────────────────────────────────────────────────────
def _compute_fft_rows(
    AUDIO_ARRAY_22050: np.ndarray,
    AUDIO_CHUNK_START_MS: int,
    SAMPLE_RATE_22050: int,
) -> List[Tuple[int, int, int, float, float, float, float]]:
    """
    100 ms Hann window / 100 ms hop, per-frame max-normalized.
    Returns rows:
      (FRAME_START_MS_ABS, FRAME_END_MS_ABS,
       FFT_BUCKET_NO, HZ_START, HZ_END, FFT_BUCKET_SIZE_IN_HZ, FFT_VALUE)
    """
    if not isinstance(AUDIO_ARRAY_22050, np.ndarray) or AUDIO_ARRAY_22050.size == 0:
        return []
    if SAMPLE_RATE_22050 <= 0:
        return []

    sr = int(SAMPLE_RATE_22050)
    win = int(round(sr * 0.100))  # 100 ms
    hop = int(round(sr * 0.100))  # 100 ms
    if win <= 0 or hop <= 0 or AUDIO_ARRAY_22050.size < win:
        return []

    bucket_hz = sr / float(win)
    hann = np.hanning(win)

    rows: List[Tuple[int, int, int, float, float, float, float]] = []
    n_frames = 1 + (AUDIO_ARRAY_22050.size - win) // hop

    for i in range(n_frames):
        start = i * hop
        end = start + win
        seg = AUDIO_ARRAY_22050[start:end]
        if seg.shape[0] != win:
            continue

        seg = seg * hann
        spec = np.fft.rfft(seg)
        mag = np.abs(spec)

        m = float(mag.max()) if mag.size else 0.0
        if m > 0.0:
            mag = mag / m

        frame_start_abs = int(round(AUDIO_CHUNK_START_MS + (start * 1000.0 / sr)))
        frame_end_abs   = int(round(AUDIO_CHUNK_START_MS + (end   * 1000.0 / sr)))

        bins = mag.shape[0]  # N/2+1
        for bno in range(bins):
            hz0 = bno * bucket_hz
            hz1 = (bno + 1) * bucket_hz
            rows.append((
                frame_start_abs,
                frame_end_abs,
                bno,
                float(hz0),
                float(hz1),
                float(bucket_hz),
                float(mag[bno]),
            ))

    return rows

# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY (called by Step-2)
# ─────────────────────────────────────────────────────────────
def SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT(
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    AUDIO_CHUNK_START_MS: int,
    AUDIO_ARRAY_22050: np.ndarray,
    SAMPLE_RATE_22050: int,
) -> None:
    """
    Inputs:
      • RECORDING_ID, AUDIO_CHUNK_NO
      • AUDIO_CHUNK_START_MS: ABS start (ms) for this chunk
      • AUDIO_ARRAY_22050: mono float32 at 22,050 Hz
      • SAMPLE_RATE_22050: sample rate for AUDIO_ARRAY_22050 (expected 22050)
    """
    try:
        CONSOLE_LOG(PREFIX, "BEGIN", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
            "AUDIO_CHUNK_START_MS": int(AUDIO_CHUNK_START_MS),
            "SR_22K": int(SAMPLE_RATE_22050),
            "SAMPLES_22K": int(getattr(AUDIO_ARRAY_22050, "shape", [0])[0] or 0),
        })

        rows = _compute_fft_rows(
            AUDIO_ARRAY_22050=AUDIO_ARRAY_22050,
            AUDIO_CHUNK_START_MS=int(AUDIO_CHUNK_START_MS),
            SAMPLE_RATE_22050=int(SAMPLE_RATE_22050),
        )

        # NEW: stamp record count in memory for Step-2's DB_LOG_RECORDING_AUDIO_CHUNK
        chunks = RECORDING_AUDIO_CHUNK_ARRAY.setdefault(int(RECORDING_ID), {})
        ch = chunks.setdefault(int(AUDIO_CHUNK_NO), {"RECORDING_ID": int(RECORDING_ID), "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO)})
        ch["FFT_RECORD_CNT"] = int(len(rows))

        CONSOLE_LOG(PREFIX, "ROWS_COMPUTED", {"COUNT": len(rows)})
        if not rows:
            CONSOLE_LOG(PREFIX, "NO_ROWS_TO_INSERT", {
                "RECORDING_ID": int(RECORDING_ID),
                "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
            })
            return

        with DB_CONNECT() as conn:
            _db_load_fft_rows(
                conn=conn,
                RECORDING_ID=int(RECORDING_ID),
                AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
                rows=rows,
            )

        CONSOLE_LOG(PREFIX, "DB_INSERT_OK", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
            "ROW_COUNT": len(rows),
        })

    except Exception as exc:
        CONSOLE_LOG(PREFIX, "FATAL_ERROR", {
            "ERROR": _bi.str(exc),
            "TRACE": traceback.format_exc(),
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
        })
