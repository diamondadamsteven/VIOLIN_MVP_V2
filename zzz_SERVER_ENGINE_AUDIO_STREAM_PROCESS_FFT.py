# SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT.py

from __future__ import annotations

from typing import Iterable, List, Tuple
from datetime import datetime
import numpy as np

from SERVER_ENGINE_APP_VARIABLES import (
    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY,  # per-frame metadata (assumed to exist)
    AUDIO_FRAME_MS
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    CONSOLE_LOG,
    DB_BULK_INSERT,
    ENGINE_DB_LOG_FUNCTIONS_INS,  # centralized Start/End/Error logging
    DB_CONNECT_CTX,
)

PREFIX = "FFT"

# Row shape matches ENGINE_LOAD_FFT (excluding RECORDING_ID/AUDIO_FRAME_NO, added at insert)
# (START_MS, END_MS, FFT_BUCKET_NO, HZ_START, HZ_END, FFT_BUCKET_SIZE_IN_HZ, FFT_VALUE)
FFTRow = Tuple[int, int, int, float, float, float, float]


# ─────────────────────────────────────────────────────────────
# DB bulk load (frame-keyed)
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
def ENGINE_LOAD_FFT_INS(
    conn,
    RECORDING_ID: int,
    AUDIO_FRAME_NO: int,
    SAMPLE_RATE: int,
    rows: Iterable[FFTRow],
) -> None:
    """
    ENGINE_LOAD_FFT columns:
      (RECORDING_ID, AUDIO_FRAME_NO, START_MS, END_MS,
       FFT_BUCKET_NO, HZ_START, HZ_END, FFT_BUCKET_SIZE_IN_HZ, FFT_VALUE, SAMPLE_RATE)
    """
    sql = """
      INSERT INTO ENGINE_LOAD_FFT
      (RECORDING_ID, AUDIO_FRAME_NO, START_MS, END_MS,
       FFT_BUCKET_NO, HZ_START, HZ_END, FFT_BUCKET_SIZE_IN_HZ, FFT_VALUE, SAMPLE_RATE)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    DB_BULK_INSERT(
        conn,
        sql,
        (
            (
                RECORDING_ID,
                AUDIO_FRAME_NO,
                start_ms,
                end_ms,
                fft_bucket_no,
                hz_start,
                hz_end,
                fft_bucket_size_in_hz,
                fft_value,
                SAMPLE_RATE,
            )
            for (start_ms, end_ms, fft_bucket_no, hz_start, hz_end, fft_bucket_size_in_hz, fft_value) in rows
        ),
    )


# ─────────────────────────────────────────────────────────────
# Core FFT (100 ms window/hop, max-normalized)
# ─────────────────────────────────────────────────────────────
def _compute_fft_rows_22050(
    AUDIO_ARRAY_22050: np.ndarray,
    START_MS: int,
    SAMPLE_RATE: int,
) -> List[FFTRow]:
    """
    Compute per-window FFT magnitudes (max-normalized) with:
      • window = 100 ms Hann
      • hop    = 100 ms
    Returns a list of FFTRow:
      (START_MS, END_MS, FFT_BUCKET_NO, HZ_START, HZ_END, FFT_BUCKET_SIZE_IN_HZ, FFT_VALUE)
    """
    if not isinstance(AUDIO_ARRAY_22050, np.ndarray) or AUDIO_ARRAY_22050.size == 0:
        return []

    # Ensure mono float32 without changing semantics
    if AUDIO_ARRAY_22050.ndim > 1:
        AUDIO_ARRAY_22050 = np.mean(AUDIO_ARRAY_22050, axis=1).astype("float32")
    else:
        AUDIO_ARRAY_22050 = AUDIO_ARRAY_22050.astype("float32", copy=False)

    sample_rate = int(SAMPLE_RATE)
    window_size_samples = int(round(sample_rate * 0.100))  # 100 ms
    hop_size_samples    = int(round(sample_rate * 0.100))  # 100 ms
    if window_size_samples <= 0 or hop_size_samples <= 0 or AUDIO_ARRAY_22050.size < window_size_samples:
        return []

    fft_bucket_size_in_hz = sample_rate / float(window_size_samples)
    hann_window = np.hanning(window_size_samples)

    rows: List[FFTRow] = []
    total_samples = AUDIO_ARRAY_22050.size
    n_windows = 1 + (total_samples - window_size_samples) // hop_size_samples

    for window_index in range(max(0, n_windows)):
        start_sample = window_index * hop_size_samples
        end_sample   = start_sample + window_size_samples
        segment = AUDIO_ARRAY_22050[start_sample:end_sample]
        if segment.shape[0] != window_size_samples:
            continue

        # Window → FFT → magnitude
        windowed = segment * hann_window
        spectrum = np.fft.rfft(windowed)
        magnitude = np.abs(spectrum)

        # Max-normalize per window
        max_val = float(magnitude.max()) if magnitude.size else 0.0
        if max_val > 0.0:
            magnitude = magnitude / max_val

        # Absolute time range for this window (ms)
        frame_start_ms = int(round(START_MS + (start_sample * 1000.0 / sample_rate)))
        frame_end_ms   = int(round(START_MS + (end_sample   * 1000.0 / sample_rate)))

        frequency_bin_count = magnitude.shape[0]  # N/2 + 1 bins
        for fft_bucket_no in range(frequency_bin_count):
            hz_start = fft_bucket_no * fft_bucket_size_in_hz
            hz_end   = (fft_bucket_no + 1) * fft_bucket_size_in_hz
            rows.append((
                frame_start_ms,
                frame_end_ms,
                fft_bucket_no,
                float(hz_start),
                float(hz_end),
                float(fft_bucket_size_in_hz),
                float(magnitude[fft_bucket_no]),
            ))

    return rows


# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY: per-frame FFT
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT(
    RECORDING_ID: int,
    AUDIO_FRAME_NO: int,
    AUDIO_ARRAY_22050: np.ndarray,
) -> int:
    """
    Inputs:
      • RECORDING_ID, AUDIO_FRAME_NO
      • AUDIO_ARRAY_22050: mono float32 at ~22,050 Hz
    Returns the number of FFT rows inserted.
    """
    SAMPLE_RATE = 22050
    START_MS = AUDIO_FRAME_MS * (AUDIO_FRAME_NO - 1)  # 100 ms per websocket frame

    # Stamp start
    META = ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]
    META["DT_START_FFT"] = datetime.now()

    # Validate input (let decorator capture exceptions)
    if not isinstance(AUDIO_ARRAY_22050, np.ndarray) or AUDIO_ARRAY_22050.size == 0:
        raise ValueError("AUDIO_ARRAY_22050 is missing or empty for FFT")

    rows = _compute_fft_rows_22050(
        AUDIO_ARRAY_22050=AUDIO_ARRAY_22050,
        START_MS=START_MS,
        SAMPLE_RATE=int(SAMPLE_RATE),
    )

    META["FFT_RECORD_CNT"] = len(rows)

    if not rows:
        CONSOLE_LOG(PREFIX, "NO_ROWS", {
            "rid": int(RECORDING_ID),
            "frame": int(AUDIO_FRAME_NO),
            "sr": int(SAMPLE_RATE),
            "samples": int(getattr(AUDIO_ARRAY_22050, "shape", [0])[0] or 0),
        })
        META["DT_END_FFT"] = datetime.now()
        return 0

    # Bulk insert
    with DB_CONNECT_CTX() as conn:
        ENGINE_LOAD_FFT_INS(
            conn=conn,
            RECORDING_ID=int(RECORDING_ID),
            AUDIO_FRAME_NO=int(AUDIO_FRAME_NO),
            SAMPLE_RATE=int(SAMPLE_RATE),
            rows=rows,
        )

    CONSOLE_LOG(PREFIX, "DB_INSERT_OK", {
        "rid": int(RECORDING_ID),
        "frame": int(AUDIO_FRAME_NO),
        "row_count": len(rows),
        "sr": int(SAMPLE_RATE),
        "samples": int(getattr(AUDIO_ARRAY_22050, "shape", [0])[0] or 0),
    })

    META["DT_END_FFT"] = datetime.now()
    return len(rows)
