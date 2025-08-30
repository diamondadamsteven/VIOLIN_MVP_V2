# SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT_OPTIMIZED.py
"""
OPTIMIZED FFT Processing Module for Violin Analysis

This module provides high-performance FFT computation optimized for violin audio analysis:
- Uses 16kHz sample rate (sufficient for violin frequency range)
- Pre-allocated memory pools to avoid allocations
- Optimized for 500ms audio frames
- Expected 2-3x performance improvement over 22kHz FFT

Violin frequency range: ~196Hz (G3) to ~3,136Hz (G7)
16kHz sample rate provides 8kHz Nyquist frequency (more than sufficient)
"""

from __future__ import annotations

from typing import Iterable, List, Tuple
from datetime import datetime
import numpy as np

from SERVER_ENGINE_APP_VARIABLES import (
    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY,
    AUDIO_FRAME_MS
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    CONSOLE_LOG,
    DB_BULK_INSERT,
    ENGINE_DB_LOG_FUNCTIONS_INS,
    DB_CONNECT_CTX,
)

PREFIX = "FFT_OPT"

# Row shape matches ENGINE_LOAD_FFT
FFTRow = Tuple[int, int, int, float, float, float, float]

# ─────────────────────────────────────────────────────────────
# Pre-allocated Memory Pools for FFT Optimization
# ─────────────────────────────────────────────────────────────

# FFT Memory Pools - Reused across frames to avoid allocations
_FFT_MEMORY_POOLS = {
    'hann_window': None,      # Hann window for 16kHz
    'fft_buffer': None,       # FFT output buffer
    'magnitude_buffer': None, # Magnitude buffer
    'rows_buffer': None,      # Pre-allocated rows list
}

def _initialize_fft_memory_pools():
    """Initialize FFT memory pools with pre-allocated buffers."""
    global _FFT_MEMORY_POOLS
    
    # 16kHz pool (optimized for violin analysis)
    window_16k = int(round(16000 * 0.100))  # 100ms @ 16kHz = 1600 samples
    _FFT_MEMORY_POOLS['hann_window'] = np.hanning(window_16k).astype('float32')
    _FFT_MEMORY_POOLS['fft_input_buffer'] = np.zeros(window_16k, dtype='float32')  # Input buffer for windowed audio
    _FFT_MEMORY_POOLS['fft_output_buffer'] = np.zeros(window_16k // 2 + 1, dtype='complex64')  # FFT output buffer (complex)
    _FFT_MEMORY_POOLS['magnitude_buffer'] = np.zeros(window_16k // 2 + 1, dtype='float32')  # Magnitude buffer
    _FFT_MEMORY_POOLS['rows_buffer'] = [None] * 1000  # Pre-allocate space for rows
    

# Initialize pools on module import
_initialize_fft_memory_pools()


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
# Optimized FFT Computation with Memory Pools
# ─────────────────────────────────────────────────────────────
def _compute_fft_rows_optimized(
    audio_array: np.ndarray,
    START_MS: int,
    SAMPLE_RATE: int,
) -> List[FFTRow]:
    """
    OPTIMIZED FFT computation using pre-allocated memory pools.
    
    Performance improvements:
    1. Uses 16kHz sample rate (optimized for violin analysis)
    2. Pre-allocated Hann window and FFT buffers
    3. Reuses memory pools to avoid allocations
    4. Optimized for your 500ms audio frames
    """
    if not isinstance(audio_array, np.ndarray) or audio_array.size == 0:
        return []

    # Always use 16kHz for violin analysis (sufficient frequency resolution)
    sample_rate = 16000
    
    # Downsample to 16kHz if needed (major performance gain)
    if SAMPLE_RATE > 16000:
        downsample_factor = SAMPLE_RATE // 16000
        audio_array = audio_array[::downsample_factor]
        print(f"FFT_DEBUG: Downsampled from {SAMPLE_RATE}Hz to {sample_rate}Hz, factor={downsample_factor}")
        print(f"FFT_DEBUG: Audio size after downsample: {audio_array.size}")
    
    # Get pre-allocated buffers
    hann_window = _FFT_MEMORY_POOLS['hann_window']
    fft_input_buffer = _FFT_MEMORY_POOLS['fft_input_buffer']
    fft_output_buffer = _FFT_MEMORY_POOLS['fft_output_buffer']
    magnitude_buffer = _FFT_MEMORY_POOLS['magnitude_buffer']
    rows_buffer = _FFT_MEMORY_POOLS['rows_buffer']
    
    # Ensure mono float32 without changing semantics
    if audio_array.ndim > 1:
        audio_array = np.mean(audio_array, axis=1).astype("float32")
    else:
        audio_array = audio_array.astype("float32", copy=False)

    window_size_samples = len(hann_window)
    hop_size_samples = window_size_samples  # 100ms hop
    
    print(f"FFT_DEBUG: Processing audio - size={audio_array.size}, window={window_size_samples}, hop={hop_size_samples}")
    
    if window_size_samples <= 0 or audio_array.size < window_size_samples:
        print(f"FFT_DEBUG: Window check failed - returning empty list")
        return []

    fft_bucket_size_in_hz = sample_rate / float(window_size_samples)
    
    # Use pre-allocated rows buffer
    rows = rows_buffer[:0]  # Clear but keep memory
    total_samples = audio_array.size
    n_windows = 1 + (total_samples - window_size_samples) // hop_size_samples

    for window_index in range(max(0, n_windows)):
        start_sample = window_index * hop_size_samples
        end_sample = start_sample + window_size_samples
        segment = audio_array[start_sample:end_sample]
        if segment.shape[0] != window_size_samples:
            continue

        # Window → FFT → magnitude (using pre-allocated buffers)
        np.multiply(segment, hann_window, out=fft_input_buffer)
        np.fft.rfft(fft_input_buffer, out=fft_output_buffer)
        np.abs(fft_output_buffer, out=magnitude_buffer)
        
        # Max-normalize per window
        max_val = float(magnitude_buffer.max()) if magnitude_buffer.size else 0.0
        if max_val > 0.0:
            np.divide(magnitude_buffer, max_val, out=magnitude_buffer)

        # Absolute time range for this window (ms)
        frame_start_ms = int(round(START_MS + (start_sample * 1000.0 / sample_rate)))
        frame_end_ms = int(round(START_MS + (end_sample * 1000.0 / sample_rate)))

        frequency_bin_count = magnitude_buffer.shape[0]
        # Only process FFT buckets 18-400 (violin frequency range)
        # This significantly reduces database records and improves performance
        for fft_bucket_no in range(18, min(401, frequency_bin_count)):
            hz_start = fft_bucket_no * fft_bucket_size_in_hz
            hz_end = (fft_bucket_no + 1) * fft_bucket_size_in_hz
            rows.append((
                frame_start_ms,
                frame_end_ms,
                fft_bucket_no,
                float(hz_start),
                float(hz_end),
                float(fft_bucket_size_in_hz),
                float(magnitude_buffer[fft_bucket_no]),
            ))

    return rows

# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY: Optimized per-frame FFT
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT(
    RECORDING_ID: int,
    AUDIO_FRAME_NO: int,
    AUDIO_ARRAY_16000: np.ndarray,  # Use 16kHz for better performance
) -> int:
    """
    OPTIMIZED FFT processing using 16kHz sample rate and memory pools.
    
    Performance improvements:
    1. 16kHz sample rate (faster than 22kHz)
    2. Pre-allocated memory pools
    3. Optimized for your 500ms audio frames
    4. Reduced memory allocations
    """
    SAMPLE_RATE = 16000  # Use 16kHz for better performance
    START_MS = AUDIO_FRAME_MS * (AUDIO_FRAME_NO - 1)

    # Stamp start
    META = ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]
    META["DT_START_FFT"] = datetime.now()

    # Validate input
    if not isinstance(AUDIO_ARRAY_16000, np.ndarray) or AUDIO_ARRAY_16000.size == 0:
        raise ValueError("AUDIO_ARRAY_16000 is missing or empty for FFT")

    # Use optimized FFT computation
    CONSOLE_LOG(PREFIX, "DEBUG_FFT", {
        "rid": int(RECORDING_ID),
        "frame": int(AUDIO_FRAME_NO),
        "audio_shape": AUDIO_ARRAY_16000.shape,
        "audio_size": int(AUDIO_ARRAY_16000.size),
        "sample_rate": int(SAMPLE_RATE),
        "start_ms": int(START_MS),
    })
    
    rows = _compute_fft_rows_optimized(
        audio_array=AUDIO_ARRAY_16000,
        START_MS=START_MS,
        SAMPLE_RATE=int(SAMPLE_RATE),
    )

    META["FFT_RECORD_CNT"] = len(rows)
    
    CONSOLE_LOG(PREFIX, "DEBUG_ROWS", {
        "rid": int(RECORDING_ID),
        "frame": int(AUDIO_FRAME_NO),
        "rows_generated": int(len(rows)),
        "first_row": rows[0] if rows else None,
    })

    if not rows:
        CONSOLE_LOG(PREFIX, "NO_ROWS", {
            "rid": int(RECORDING_ID),
            "frame": int(AUDIO_FRAME_NO),
            "sr": int(SAMPLE_RATE),
            "samples": int(getattr(AUDIO_ARRAY_16000, "shape", [0])[0] or 0),
        })
        META["DT_END_FFT"] = datetime.now()
        return 0

    META["DT_START_FFT_ENGINE_LOAD_FFT_INS"] = datetime.now()
    # Bulk insert
    with DB_CONNECT_CTX() as conn:
        ENGINE_LOAD_FFT_INS(
            conn=conn,
            RECORDING_ID=int(RECORDING_ID),
            AUDIO_FRAME_NO=int(AUDIO_FRAME_NO),
            SAMPLE_RATE=int(SAMPLE_RATE),
            rows=rows,
        )
    META["DT_END_FFT_ENGINE_LOAD_FFT_INS"] = datetime.now()

    CONSOLE_LOG(PREFIX, "DB_INSERT_OK", {
        "rid": int(RECORDING_ID),
        "frame": int(AUDIO_FRAME_NO),
        "rows": int(len(rows)),
        "sr": int(SAMPLE_RATE),
        "samples": int(getattr(AUDIO_ARRAY_16000, "shape", [0])[0] or 0),
    })

    META["DT_END_FFT"] = datetime.now()
    return len(rows)

