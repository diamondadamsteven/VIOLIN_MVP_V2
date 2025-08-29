# SERVER_ENGINE_AUDIO_PROCESSING_POOL.py
"""
Centralized audio processing pool for parallel execution of audio analysis tasks.
Eliminates bottlenecks in PYIN, resampling, CREPE, FFT, and other audio operations.
"""

from __future__ import annotations

import numpy as np
from typing import List, Tuple, Optional, Any
from concurrent.futures import ProcessPoolExecutor, Future
import multiprocessing as mp

from SERVER_ENGINE_APP_VARIABLES import AUDIO_PROCESSING_POOL, AUDIO_PROCESSING_POOL_AVAILABLE
from SERVER_ENGINE_APP_FUNCTIONS import CONSOLE_LOG

PREFIX = "AUDIO_POOL"

# Row shape for ENGINE_LOAD_HZ inserts (per reading):
# (START_MS, END_MS, HZ, CONFIDENCE)
HZRow = Tuple[int, int, float, float]

# ─────────────────────────────────────────────────────────────
# Process-safe audio processing functions
# ─────────────────────────────────────────────────────────────

def _pyin_relative_rows_worker(audio_22050: np.ndarray, sample_rate: int = 22050) -> List[HZRow]:
    """
    Worker function for PYIN processing - runs in separate process.
    Returns per-frame rows relative to the provided audio buffer.
    """
    try:
        import librosa
        
        if sample_rate != 22050 or not isinstance(audio_22050, np.ndarray) or audio_22050.size == 0:
            return []

        # ~10 ms hop @ 22.05 kHz
        hop_length = max(1, int(round(sample_rate * 0.010)))  # typically 221
        frame_length = max(hop_length * 4, 2048)

        # Run PYIN analysis
        f0, voiced_flag, voiced_prob = librosa.pyin(
            y=audio_22050, sr=sample_rate,
            fmin=180, fmax=4000,
            frame_length=frame_length, hop_length=hop_length, center=True
        )

        rows_rel: List[HZRow] = []
        for i, (hz, voiced_ok, confidence) in enumerate(zip(f0, voiced_flag, voiced_prob)):
            if not voiced_ok or hz is None:
                continue
            if not np.isfinite(hz) or hz <= 0.0:
                continue
            if hz < 20.0 or hz > 20000.0:  # Reasonable human hearing range
                continue
            if confidence < 0.1:  # Filter out very low confidence
                continue
                
            start_ms_rel = int(round((i * hop_length) * 1000.0 / sample_rate))
            end_ms_rel = start_ms_rel + 9  # nominal 10 ms span
            rows_rel.append((start_ms_rel, end_ms_rel, float(hz), float(confidence)))

        return rows_rel
        
    except Exception as e:
        CONSOLE_LOG(PREFIX, "PYIN_WORKER_ERROR", {"error": str(e)})
        return []

def _resample_worker(audio_data: np.ndarray, src_sr: int, dst_sr: int) -> np.ndarray:
    """
    Worker function for audio resampling - runs in separate process.
    """
    try:
        if src_sr == dst_sr:
            return audio_data.astype(np.float32, copy=False)

        # Try polyphase first
        try:
            from scipy.signal import resample_poly
            from math import gcd
            g = gcd(dst_sr, src_sr)
            up, down = dst_sr // g, src_sr // g
            return resample_poly(audio_data.astype(np.float32, copy=False), up, down).astype(np.float32, copy=False)
        except ImportError:
            pass

        # Fall back to librosa
        try:
            import librosa
            return librosa.resample(
                audio_data.astype(np.float32, copy=False),
                orig_sr=src_sr, target_sr=dst_sr, res_type="kaiser_best"
            ).astype(np.float32, copy=False)
        except ImportError:
            pass

        # Linear fallback
        n_out = int(round(len(audio_data) * (dst_sr / float(src_sr))))
        if n_out <= 1 or len(audio_data) == 0:
            return np.array([], dtype=np.float32)
        indices = np.linspace(0, len(audio_data) - 1, n_out)
        return np.interp(indices, np.arange(len(audio_data)), audio_data).astype(np.float32, copy=False)
        
    except Exception as e:
        CONSOLE_LOG(PREFIX, "RESAMPLE_WORKER_ERROR", {"error": str(e)})
        return audio_data.astype(np.float32, copy=False)

# ─────────────────────────────────────────────────────────────
# Public interface for parallel processing
# ─────────────────────────────────────────────────────────────

def pyin_relative_rows_parallel(audio_22050: np.ndarray, sample_rate: int = 22050) -> Future[List[HZRow]]:
    """
    Submit PYIN processing to the process pool.
    Returns a Future object that can be awaited or checked for completion.
    """
    if not AUDIO_PROCESSING_POOL_AVAILABLE or AUDIO_PROCESSING_POOL is None:
        # Fallback to synchronous processing
        return _pyin_relative_rows_worker(audio_22050, sample_rate)
    
    # Submit to process pool
    future = AUDIO_PROCESSING_POOL.submit(_pyin_relative_rows_worker, audio_22050, sample_rate)
    return future

def resample_parallel(audio_data: np.ndarray, src_sr: int, dst_sr: int) -> Future[np.ndarray]:
    """
    Submit audio resampling to the process pool.
    Returns a Future object that can be awaited or checked for completion.
    """
    if not AUDIO_PROCESSING_POOL_AVAILABLE or AUDIO_PROCESSING_POOL is None:
        # Fallback to synchronous processing
        return _resample_worker(audio_data, src_sr, dst_sr)
    
    # Submit to process pool
    future = AUDIO_PROCESSING_POOL.submit(_resample_worker, audio_data, src_sr, dst_sr)
    return future

def wait_for_futures(*futures: Future) -> List[Any]:
    """
    Wait for multiple futures to complete and return their results.
    Handles both Future objects and direct results for fallback compatibility.
    """
    results = []
    for future in futures:
        if isinstance(future, Future):
            try:
                result = future.result(timeout=30)  # 30 second timeout
                results.append(result)
            except Exception as e:
                CONSOLE_LOG(PREFIX, "FUTURE_ERROR", {"error": str(e)})
                results.append(None)
        else:
            # Direct result (fallback mode)
            results.append(future)
    return results

# ─────────────────────────────────────────────────────────────
# Pool management
# ─────────────────────────────────────────────────────────────

def get_pool_status() -> dict:
    """Get current status of the audio processing pool."""
    if not AUDIO_PROCESSING_POOL_AVAILABLE:
        return {"status": "unavailable", "workers": 0, "active": 0}
    
    pool = AUDIO_PROCESSING_POOL
    return {
        "status": "available",
        "workers": pool._max_workers,
        "active": len([f for f in pool._threads if f.is_alive()])
    }

def shutdown_pool():
    """Shutdown the audio processing pool."""
    if AUDIO_PROCESSING_POOL_AVAILABLE and AUDIO_PROCESSING_POOL:
        AUDIO_PROCESSING_POOL.shutdown(wait=True)
