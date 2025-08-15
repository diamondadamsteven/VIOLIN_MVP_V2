# SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT.py
# ----------------------------------------------------------------------
# FFT processing for a single audio chunk.
# Responsibilities:
#   • Compute per-100ms FFT magnitudes from 22.05 kHz mono float32 audio
#   • Per-frame max-normalize magnitudes
#   • Emit rows with absolute ms (chunk start offset applied)
#   • Bulk insert into ENGINE_LOAD_FFT
#
# NOTE: Do NOT call P_ENGINE_ALL_METHOD_FFT here — Step-2 does that.
# ----------------------------------------------------------------------

import os
import math
import traceback
from typing import Any, Dict, Iterable, List, Tuple

import builtins as _bi
import numpy as np

# ─────────────────────────────────────────────────────────────
# Console logging (ASCII-safe)
# ─────────────────────────────────────────────────────────────
def CONSOLE_LOG(L_MSG: str, L_OBJ: Any = None):
    L_PREFIX = "PROCESS_FFT"
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

def DB_LOAD_FFT_ROWS(
    L_CONN,
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    FFT_ROWS_ARRAY: Iterable[Tuple[int, int, int, float, float, float, float]],
) -> None:
    """
    Insert rows into ENGINE_LOAD_FFT:
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS,
       FFT_BUCKET_NO, HZ_START, HZ_END, FFT_BUCKET_SIZE_IN_HZ, FFT_VALUE)
    """
    L_SQL = """
      INSERT INTO ENGINE_LOAD_FFT
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS,
       FFT_BUCKET_NO, HZ_START, HZ_END, FFT_BUCKET_SIZE_IN_HZ, FFT_VALUE)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    DB_BULK_INSERT(
        L_CONN,
        L_SQL,
        (
            (RECORDING_ID, AUDIO_CHUNK_NO, L_S, L_E, L_BNO, L_HZ0, L_HZ1, L_BSZ, L_VAL)
            for (L_S, L_E, L_BNO, L_HZ0, L_HZ1, L_BSZ, L_VAL) in FFT_ROWS_ARRAY
        ),
    )

# ─────────────────────────────────────────────────────────────
# Core FFT
# ─────────────────────────────────────────────────────────────
def PROCESSING_FFT_COMPUTE_ROWS(
    AUDIO_ARRAY_22050: np.ndarray,
    AUDIO_CHUNK_START_MS: int,
    SAMPLE_RATE_22050: int = 22050,
) -> List[Tuple[int, int, int, float, float, float, float]]:
    """
    Compute per-100ms FFT (Hann window, hop=100ms) with per-frame max-normalized magnitude.

    Returns list of rows:
      (FRAME_START_MS_ABS, FRAME_END_MS_ABS,
       FFT_BUCKET_NO, HZ_START, HZ_END, FFT_BUCKET_SIZE_IN_HZ, FFT_VALUE)
    """
    # ── Guards
    if not isinstance(AUDIO_ARRAY_22050, np.ndarray) or AUDIO_ARRAY_22050.size == 0:
        return []
    if SAMPLE_RATE_22050 <= 0:
        return []

    L_SR = int(SAMPLE_RATE_22050)
    # 100 ms window/hop
    L_WIN = int(round(L_SR * 0.100))
    L_HOP = int(round(L_SR * 0.100))
    if L_WIN <= 0 or L_HOP <= 0 or AUDIO_ARRAY_22050.size < L_WIN:
        return []

    # Bucket size (Hz per bin)
    L_BUCKET_SIZE_IN_HZ = L_SR / float(L_WIN)

    L_ROWS: List[Tuple[int, int, int, float, float, float, float]] = []
    L_N_FRAMES = 1 + (AUDIO_ARRAY_22050.size - L_WIN) // L_HOP

    # Precompute Hann window
    L_HANN = np.hanning(L_WIN)

    for L_I in range(L_N_FRAMES):
        L_START = L_I * L_HOP
        L_END = L_START + L_WIN
        L_SEG = AUDIO_ARRAY_22050[L_START:L_END]

        # Apply window
        if L_SEG.shape[0] != L_WIN:
            # Safety: skip incomplete tail
            continue
        L_SEG = L_SEG * L_HANN

        # rFFT and magnitude
        L_SPEC = np.fft.rfft(L_SEG)
        L_MAG = np.abs(L_SPEC)

        # Per-frame max-normalize
        L_MAX = float(L_MAG.max()) if L_MAG.size else 0.0
        if L_MAX > 0.0:
            L_MAG = L_MAG / L_MAX

        # Absolute times (ms)
        L_FRAME_START_MS_ABS = int(round(AUDIO_CHUNK_START_MS + (L_START * 1000.0 / L_SR)))
        L_FRAME_END_MS_ABS   = int(round(AUDIO_CHUNK_START_MS + (L_END   * 1000.0 / L_SR)))

        # Emit bins
        L_BINS = L_MAG.shape[0]  # N/2+1 bins
        for L_BNO in range(L_BINS):
            L_HZ0 = L_BNO * L_BUCKET_SIZE_IN_HZ
            L_HZ1 = (L_BNO + 1) * L_BUCKET_SIZE_IN_HZ
            L_VAL = float(L_MAG[L_BNO])
            L_ROWS.append((
                L_FRAME_START_MS_ABS,
                L_FRAME_END_MS_ABS,
                L_BNO,
                float(L_HZ0),
                float(L_HZ1),
                float(L_BUCKET_SIZE_IN_HZ),
                L_VAL,
            ))

    return L_ROWS

# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY
# ─────────────────────────────────────────────────────────────
def SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT(
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    AUDIO_CHUNK_START_MS: int,
    AUDIO_ARRAY_22050: np.ndarray,
    SAMPLE_RATE_22050: int = 22050,
) -> None:
    """
    Public entry called by Step-2.

    Inputs:
      • RECORDING_ID (int)
      • AUDIO_CHUNK_NO (int)
      • AUDIO_CHUNK_START_MS (absolute ms for this chunk’s start)
      • AUDIO_ARRAY_22050: mono float32 array at 22,050 Hz
      • SAMPLE_RATE_22050: should be 22050 (guarded if not)

    Behavior:
      • Computes FFT rows (per-100ms)
      • Bulk-inserts into ENGINE_LOAD_FFT
      • Does NOT call P_ENGINE_ALL_METHOD_FFT (Step-2 will call it)
    """
    try:
        # ── Compute
        CONSOLE_LOG("FFT_BEGIN", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
            "AUDIO_CHUNK_START_MS": int(AUDIO_CHUNK_START_MS),
            "SR": int(SAMPLE_RATE_22050),
            "SAMPLES": int(getattr(AUDIO_ARRAY_22050, "shape", [0])[0] or 0),
        })

        L_ROWS_ARRAY = PROCESSING_FFT_COMPUTE_ROWS(
            AUDIO_ARRAY_22050=AUDIO_ARRAY_22050,
            AUDIO_CHUNK_START_MS=AUDIO_CHUNK_START_MS,
            SAMPLE_RATE_22050=SAMPLE_RATE_22050,
        )
        CONSOLE_LOG("FFT_ROWS_COMPUTED", {"COUNT": len(L_ROWS_ARRAY)})

        if not L_ROWS_ARRAY:
            CONSOLE_LOG("FFT_NO_ROWS_TO_INSERT", {
                "RECORDING_ID": int(RECORDING_ID),
                "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
            })
            return

        # ── Load to DB
        with DB_GET_CONN() as L_CONN:
            DB_LOAD_FFT_ROWS(
                L_CONN=L_CONN,
                RECORDING_ID=int(RECORDING_ID),
                AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
                FFT_ROWS_ARRAY=L_ROWS_ARRAY,
            )

        CONSOLE_LOG("FFT_DB_INSERT_OK", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
            "ROW_COUNT": len(L_ROWS_ARRAY),
        })

    except Exception as L_EXC:
        CONSOLE_LOG("FFT_FATAL_ERROR", {
            "ERROR": _bi.str(L_EXC),
            "TRACE": traceback.format_exc(),
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
        })
