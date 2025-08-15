# SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME.py
# ----------------------------------------------------------------------
# Volume processing for a single audio chunk.
# Responsibilities:
#   • Decode WAV (expected 22,050 Hz mono) to float32
#   • Compute:
#       - Chunk aggregate volume using 1 ms RMS windows (avg_rms, avg_db)
#       - 10 ms RMS time series (start_ms, end_ms, rms, db)
#   • Insert:
#       - ENGINE_LOAD_VOLUME (one row per chunk)
#       - ENGINE_LOAD_VOLUME_10_MS (series rows)
# Notes:
#   • Matches earlier POC semantics (center=False, epsilon=1e-6)
#   • All times stored are ABSOLUTE (AUDIO_CHUNK_START_MS offset applied)
# Env:
#   • VIOLIN_ODBC: pyodbc connection string
# ----------------------------------------------------------------------

import os
import math
import subprocess
import traceback
from pathlib import Path
from typing import Any, Iterable, List, Optional, Tuple

import builtins as _bi
import numpy as np

# Optional dependency (used if available; we fall back to numpy)
try:
    import librosa  # type: ignore
except Exception:  # pragma: no cover
    librosa = None


# ─────────────────────────────────────────────────────────────
# Console logging (ASCII-safe)
# ─────────────────────────────────────────────────────────────
def CONSOLE_LOG(L_MSG: str, L_OBJ: Any = None):
    L_PREFIX = "PROCESS_VOLUME"
    try:
        if L_OBJ is None:
            print(f"{L_PREFIX} - {L_MSG}", flush=True)
        else:
            print(f"{L_PREFIX} - {L_MSG} {L_OBJ}", flush=True)
    except Exception:
        try:
            L_S = f"{L_PREFIX} - {L_MSG} {L_OBJ}".encode("utf-8", "replace").decode("ascii", "ignore")
            print(L_S, flush=True)
        except Exception:
            print(f"{L_PREFIX} - {L_MSG}", flush=True)


# ─────────────────────────────────────────────────────────────
# DB helpers (ODBC)
# ─────────────────────────────────────────────────────────────
def DB_GET_CONN():
    import pyodbc  # type: ignore
    L_CONN_STR = os.getenv("VIOLIN_ODBC", "")
    if not L_CONN_STR:
        raise RuntimeError("VIOLIN_ODBC not set (ODBC connection string).")
    return pyodbc.connect(L_CONN_STR, autocommit=True)

def DB_BULK_INSERT(L_CONN, L_SQL: str, L_ROWS: Iterable[tuple]) -> None:
    L_ROWS_LIST = list(L_ROWS)
    if not L_ROWS_LIST:
        return
    L_CUR = L_CONN.cursor()
    L_CUR.fast_executemany = True
    L_CUR.executemany(L_SQL, L_ROWS_LIST)

def DB_LOAD_VOLUME_AGGREGATE_ROW(
    L_CONN,
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    AUDIO_CHUNK_START_MS: int,
    VOLUME_AGGREGATE_TUPLE: Optional[Tuple[float, float]],
) -> None:
    """
    Insert one row into ENGINE_LOAD_VOLUME:
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, VOLUME, VOLUME_IN_DB)
    START_MS is the absolute start of the chunk.
    """
    if not VOLUME_AGGREGATE_TUPLE:
        return
    L_SQL = """
      INSERT INTO ENGINE_LOAD_VOLUME
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, VOLUME, VOLUME_IN_DB)
      VALUES (?, ?, ?, ?, ?)
    """
    (L_AVG_RMS, L_AVG_DB) = VOLUME_AGGREGATE_TUPLE
    DB_BULK_INSERT(
        L_CONN,
        L_SQL,
        [(RECORDING_ID, AUDIO_CHUNK_NO, AUDIO_CHUNK_START_MS, float(L_AVG_RMS), float(L_AVG_DB))],
    )

def DB_LOAD_VOLUME_10MS_SERIES(
    L_CONN,
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    VOLUME_10MS_SERIES_ARRAY: Iterable[Tuple[int, int, float, float]],
) -> None:
    """
    Insert rows into ENGINE_LOAD_VOLUME_10_MS:
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS, VOLUME, VOLUME_IN_DB)
    """
    L_ROWS_LIST = list(VOLUME_10MS_SERIES_ARRAY)
    if not L_ROWS_LIST:
        return
    L_SQL = """
      INSERT INTO ENGINE_LOAD_VOLUME_10_MS
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS, VOLUME, VOLUME_IN_DB)
      VALUES (?, ?, ?, ?, ?, ?)
    """
    DB_BULK_INSERT(
        L_CONN,
        L_SQL,
        (
            (RECORDING_ID, AUDIO_CHUNK_NO, L_S, L_E, float(L_V), float(L_DB))
            for (L_S, L_E, L_V, L_DB) in L_ROWS_LIST
        ),
    )


# ─────────────────────────────────────────────────────────────
# Audio decode (expects 22,050 Hz mono output)
# ─────────────────────────────────────────────────────────────
def AUDIO_DECODE_WAV_TO_FLOAT_MONO_22050(WAV_PATH: Path) -> np.ndarray:
    """
    ffmpeg decode → float32 mono @ 22,050 Hz (no normalization).
    Returns writable numpy array (C-contiguous).
    """
    L_CMD = [
        "ffmpeg", "-nostdin", "-v", "error",
        "-i", _bi.str(WAV_PATH),
        "-ac", "1",
        "-ar", "22050",
        "-f", "f32le",
        "pipe:1",
    ]
    try:
        L_RAW = subprocess.check_output(L_CMD)
        L_AUDIO = np.frombuffer(L_RAW, dtype=np.float32)
        return L_AUDIO.copy()
    except subprocess.CalledProcessError as L_EXC:
        CONSOLE_LOG("FFMPEG_DECODE_FAILED", {"path": _bi.str(WAV_PATH), "err": _bi.str(L_EXC)})
        return np.zeros(0, dtype=np.float32)


# ─────────────────────────────────────────────────────────────
# Volume math (POC-matching)
# ─────────────────────────────────────────────────────────────
def VOLUME_COMPUTE_AGGREGATE_1_MS(
    AUDIO_22K: np.ndarray,
    SAMPLE_RATE: int,
    AUDIO_CHUNK_START_MS: int,
) -> Tuple[Optional[Tuple[float, float]], List[Tuple[int, int, float, float]]]:
    """
    POC-matching aggregate using 1 ms hop and ~2 ms window.
    center=False so the first frame aligns to AUDIO_CHUNK_START_MS.
    Returns:
      ( (avg_rms, avg_db), series_1ms )
    Series is [(abs_start_ms, abs_end_ms, rms, db), ...] but we don't insert 1ms series into DB;
    we only use it to compute the aggregate tuple.
    """
    if SAMPLE_RATE <= 0 or AUDIO_22K.size == 0:
        return None, []

    L_HOP_MS = 1
    # round to nearest sample count for 1 ms hop at 22.05 kHz (~22 samples)
    L_HOP = max(1, int(round(SAMPLE_RATE * 0.001)))
    L_FRAME = max(L_HOP * 2, L_HOP)  # ~2 ms window

    L_SERIES_1MS: List[Tuple[int, int, float, float]] = []

    if librosa is not None:
        try:
            L_RMS = librosa.feature.rms(
                y=AUDIO_22K,
                frame_length=L_FRAME,
                hop_length=L_HOP,
                center=False,  # align with chunk start
            )[0]
            for i, r in enumerate(L_RMS):
                L_S_MS = AUDIO_CHUNK_START_MS + i * L_HOP_MS
                L_E_MS = L_S_MS  # 1 ms granularity
                L_V = float(r)
                L_DB = float(20.0 * math.log10(L_V + 1e-6))
                L_SERIES_1MS.append((L_S_MS, L_E_MS, L_V, L_DB))
        except Exception as L_EXC:
            CONSOLE_LOG("LIBROSA_RMS_1MS_FAILED_FALLBACK", _bi.str(L_EXC))

    if not L_SERIES_1MS:
        # Numpy fallback with Hann window, no centering
        L_WIN = L_FRAME
        L_N = AUDIO_22K.size
        L_I = 0
        L_HANN = np.hanning(L_WIN) if L_WIN > 1 else None
        L_FRAME_IDX = 0
        while L_I + L_WIN <= L_N:
            L_SEG = AUDIO_22K[L_I:L_I + L_WIN]
            if L_HANN is not None:
                L_SEG = L_SEG * L_HANN
            L_V = float(np.sqrt(np.mean(L_SEG * L_SEG))) if L_SEG.size else 0.0
            L_DB = float(20.0 * math.log10(L_V + 1e-6))
            L_S_MS = AUDIO_CHUNK_START_MS + L_FRAME_IDX * L_HOP_MS
            L_E_MS = L_S_MS
            L_SERIES_1MS.append((L_S_MS, L_E_MS, L_V, L_DB))
            L_I += L_HOP
            L_FRAME_IDX += 1

    if not L_SERIES_1MS:
        return None, []

    L_AVG_RMS = float(np.mean([v for (_, _, v, _) in L_SERIES_1MS]))
    L_AVG_DB = float(20.0 * math.log10(L_AVG_RMS + 1e-6))
    return (L_AVG_RMS, L_AVG_DB), L_SERIES_1MS


def VOLUME_COMPUTE_SERIES_10_MS(
    AUDIO_22K: np.ndarray,
    SAMPLE_RATE: int,
    AUDIO_CHUNK_START_MS: int,
) -> List[Tuple[int, int, float, float]]:
    """
    POC-matching 10 ms RMS series.
    • hop_ms = 10
    • frame_length = 2 * hop_length
    • center=False (aligns first frame to chunk start)
    Returns [(abs_start_ms, abs_end_ms, rms, db), ...]
    """
    if SAMPLE_RATE <= 0 or AUDIO_22K.size == 0:
        return []

    L_HOP_MS = 10
    # POC used truncation for hop_length at 22.05 kHz
    L_HOP = max(1, int((L_HOP_MS / 1000.0) * SAMPLE_RATE))
    L_FRAME = max(L_HOP * 2, L_HOP)

    L_SERIES_10MS: List[Tuple[int, int, float, float]] = []

    if librosa is not None:
        try:
            L_RMS = librosa.feature.rms(
                y=AUDIO_22K,
                frame_length=L_FRAME,
                hop_length=L_HOP,
                center=False,
            )[0]
            for i, r in enumerate(L_RMS):
                L_S_MS = AUDIO_CHUNK_START_MS + i * L_HOP_MS
                L_E_MS = L_S_MS + (L_HOP_MS - 1)
                L_V = float(r)
                L_DB = float(20.0 * math.log10(L_V + 1e-6))
                L_SERIES_10MS.append((L_S_MS, L_E_MS, L_V, L_DB))
        except Exception as L_EXC:
            CONSOLE_LOG("LIBROSA_RMS_10MS_FAILED_FALLBACK", _bi.str(L_EXC))

    if not L_SERIES_10MS:
        # Numpy fallback with Hann window, no centering
        L_WIN = L_FRAME
        L_N = AUDIO_22K.size
        L_I = 0
        L_HANN = np.hanning(L_WIN) if L_WIN > 1 else None
        L_FRAME_IDX = 0
        while L_I + L_WIN <= L_N:
            L_SEG = AUDIO_22K[L_I:L_I + L_WIN]
            if L_HANN is not None:
                L_SEG = L_SEG * L_HANN
            L_V = float(np.sqrt(np.mean(L_SEG * L_SEG))) if L_SEG.size else 0.0
            L_DB = float(20.0 * math.log10(L_V + 1e-6))
            L_S_MS = AUDIO_CHUNK_START_MS + L_FRAME_IDX * L_HOP_MS
            L_E_MS = L_S_MS + (L_HOP_MS - 1)
            L_SERIES_10MS.append((L_S_MS, L_E_MS, L_V, L_DB))
            L_I += L_HOP
            L_FRAME_IDX += 1

    return L_SERIES_10MS


# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY
# ─────────────────────────────────────────────────────────────
def SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME(
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    WAV22050_PATH: str,
    AUDIO_CHUNK_START_MS: int,
) -> None:
    """
    Called by Step-2 for every chunk (regardless of other flags).

    Inputs:
      • RECORDING_ID, AUDIO_CHUNK_NO
      • WAV22050_PATH: absolute path to chunk WAV @ 22,050 Hz, mono
      • AUDIO_CHUNK_START_MS: absolute ms offset for this chunk

    Behavior:
      • Decode WAV → float mono 22.05 kHz
      • Compute 1 ms aggregate (avg_rms, avg_db) for ENGINE_LOAD_VOLUME
      • Compute 10 ms series for ENGINE_LOAD_VOLUME_10_MS
      • Bulk insert both
    """
    try:
        L_WAV = Path(_bi.str(WAV22050_PATH)).resolve()
        if not L_WAV.exists():
            CONSOLE_LOG("WAV_NOT_FOUND", {"path": _bi.str(L_WAV)})
            return

        CONSOLE_LOG("VOLUME_PROCESS_BEGIN", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
            "WAV22050_PATH": _bi.str(L_WAV),
            "AUDIO_CHUNK_START_MS": int(AUDIO_CHUNK_START_MS),
        })

        L_AUDIO_22K = AUDIO_DECODE_WAV_TO_FLOAT_MONO_22050(L_WAV)
        if L_AUDIO_22K.size == 0:
            CONSOLE_LOG("DECODE_EMPTY_AUDIO")
            return

        L_AGG_1MS, _ = VOLUME_COMPUTE_AGGREGATE_1_MS(
            AUDIO_22K=L_AUDIO_22K,
            SAMPLE_RATE=22050,
            AUDIO_CHUNK_START_MS=int(AUDIO_CHUNK_START_MS),
        )
        L_SERIES_10MS = VOLUME_COMPUTE_SERIES_10_MS(
            AUDIO_22K=L_AUDIO_22K,
            SAMPLE_RATE=22050,
            AUDIO_CHUNK_START_MS=int(AUDIO_CHUNK_START_MS),
        )

        with DB_GET_CONN() as L_CONN:
            DB_LOAD_VOLUME_AGGREGATE_ROW(
                L_CONN=L_CONN,
                RECORDING_ID=int(RECORDING_ID),
                AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
                AUDIO_CHUNK_START_MS=int(AUDIO_CHUNK_START_MS),
                VOLUME_AGGREGATE_TUPLE=L_AGG_1MS,
            )
            DB_LOAD_VOLUME_10MS_SERIES(
                L_CONN=L_CONN,
                RECORDING_ID=int(RECORDING_ID),
                AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
                VOLUME_10MS_SERIES_ARRAY=L_SERIES_10MS,
            )

        CONSOLE_LOG("VOLUME_DB_INSERT_OK", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
            "ROWS_10MS": len(L_SERIES_10MS),
            "HAS_AGG": bool(L_AGG_1MS),
        })

    except Exception as L_EXC:
        CONSOLE_LOG("VOLUME_FATAL_ERROR", {
            "ERROR": _bi.str(L_EXC),
            "TRACE": traceback.format_exc(),
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
        })
