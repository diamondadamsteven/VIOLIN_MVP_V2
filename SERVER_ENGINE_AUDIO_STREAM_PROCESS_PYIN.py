# SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN.py
# ----------------------------------------------------------------------
# pYIN processing for a single audio chunk.
# Responsibilities:
#   • Decode WAV (expected 22.05 kHz mono) to float32
#   • Run librosa.pyin → per-frame f0 + confidence
#   • Convert CHUNK-relative times to ABSOLUTE times
#   • Bulk insert rows into ENGINE_LOAD_HZ with SOURCE_METHOD='PYIN'
# ----------------------------------------------------------------------

import os
import math
import subprocess
import traceback
from pathlib import Path
from typing import Any, Iterable, List, Tuple

import builtins as _bi
import numpy as np

try:
    import librosa  # pYIN
except Exception:  # pragma: no cover
    librosa = None


# ─────────────────────────────────────────────────────────────
# Console logging (ASCII-safe)
# ─────────────────────────────────────────────────────────────
def CONSOLE_LOG(L_MSG: str, L_OBJ: Any = None):
    L_PREFIX = "PROCESS_PYIN"
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

def DB_LOAD_HZ_SERIES(
    L_CONN,
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    SOURCE_METHOD: str,
    HZ_SERIES_ARRAY: Iterable[Tuple[int, int, float, float]],
) -> None:
    """
    Insert rows into ENGINE_LOAD_HZ:
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS, SOURCE_METHOD, HZ, CONFIDENCE)
    """
    L_SQL = """
      INSERT INTO ENGINE_LOAD_HZ
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS, SOURCE_METHOD, HZ, CONFIDENCE)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    DB_BULK_INSERT(
        L_CONN,
        L_SQL,
        (
            (RECORDING_ID, AUDIO_CHUNK_NO, L_S, L_E, SOURCE_METHOD, float(L_HZ), float(L_CONF))
            for (L_S, L_E, L_HZ, L_CONF) in HZ_SERIES_ARRAY
        ),
    )


# ─────────────────────────────────────────────────────────────
# Audio decode (expects 22.05 kHz mono output)
# ─────────────────────────────────────────────────────────────
def AUDIO_DECODE_WAV_TO_FLOAT_MONO_22050(WAV_PATH: Path) -> np.ndarray:
    """
    ffmpeg decode → float32 mono @ 22,050 Hz (no normalization).
    Returns a writable numpy array (C-contiguous).
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
        return L_AUDIO.copy()  # ensure writable buffer
    except subprocess.CalledProcessError as L_EXC:
        CONSOLE_LOG("FFMPEG_DECODE_FAILED", {"path": _bi.str(WAV_PATH), "err": _bi.str(L_EXC)})
        return np.zeros(0, dtype=np.float32)


# ─────────────────────────────────────────────────────────────
# pYIN core (relative series)
# ─────────────────────────────────────────────────────────────
def PYIN_COMPUTE_RELATIVE_SERIES(audio_22k: np.ndarray, sr: int = 22050) -> List[Tuple[int, int, float, float]]:
    """
    Returns per-frame rows relative to the chunk:
      [(START_MS_REL, END_MS_REL, HZ, CONFIDENCE), ...]
    """
    if librosa is None:
        CONSOLE_LOG("LIBROSA_NOT_AVAILABLE")
        return []

    if sr != 22050 or audio_22k.size == 0:
        CONSOLE_LOG("PYIN_BAD_INPUT", {"sr": sr, "size": int(audio_22k.size)})
        return []

    # ~10 ms hop @ 22.05 kHz
    L_HOP = max(1, int(round(sr * 0.010)))
    L_FRAME_LEN = max(L_HOP * 4, 2048)

    def _run_pyin(with_bounds: bool):
        if not with_bounds:
            return librosa.pyin(
                y=audio_22k, sr=sr,
                frame_length=L_FRAME_LEN, hop_length=L_HOP, center=True
            )
        # Fallback with sane bounds (G3..C8)
        try:
            L_FMIN = float(librosa.note_to_hz("G3"))
            L_FMAX = float(librosa.note_to_hz("C8"))
        except Exception:
            L_FMIN, L_FMAX = 196.0, 4186.0
        return librosa.pyin(
            y=audio_22k, sr=sr,
            fmin=L_FMIN, fmax=L_FMAX,
            frame_length=L_FRAME_LEN, hop_length=L_HOP, center=True
        )

    try:
        L_F0, L_VFLAG, L_VPROB = _run_pyin(with_bounds=False)
    except TypeError:
        # Some librosa versions require fmin/fmax
        try:
            L_F0, L_VFLAG, L_VPROB = _run_pyin(with_bounds=True)
        except Exception as L_EXC:
            CONSOLE_LOG("PYIN_FAILED", {"err": _bi.str(L_EXC)})
            return []
    except Exception as L_EXC:
        CONSOLE_LOG("PYIN_FAILED", {"err": _bi.str(L_EXC)})
        return []

    L_ROWS: List[Tuple[int, int, float, float]] = []
    # Convert frame index → time (ms) using hop_length
    for L_I, (L_HZ, L_VOK, L_CONF) in enumerate(zip(L_F0, L_VFLAG, L_VPROB)):
        if not L_VOK or L_HZ is None:
            continue
        if not np.isfinite(L_HZ) or L_HZ <= 0.0:
            continue
        L_START_MS = int(round((L_I * L_HOP) * 1000.0 / sr))
        L_END_MS = L_START_MS + 9  # nominal 10-ms span
        L_ROWS.append((L_START_MS, L_END_MS, float(L_HZ), float(L_CONF)))

    # Quick anomaly stats (optional)
    if L_ROWS:
        L_STARTS = [s for (s, _, _, _) in L_ROWS]
        L_MODS = sorted(set([s % 10 for s in L_STARTS]))
        L_STEPS = sorted(set(np.diff(L_STARTS))) if len(L_STARTS) > 1 else []
        CONSOLE_LOG("PYIN_TIMING_SUMMARY", {
            "count": len(L_ROWS),
            "first_ms": L_STARTS[0],
            "last_ms": L_STARTS[-1],
            "mods_of_10": L_MODS[:6],
            "unique_step_sizes": L_STEPS[:6],
        })

    return L_ROWS


# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY
# ─────────────────────────────────────────────────────────────
def SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN(
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    WAV22050_PATH: str,
    AUDIO_CHUNK_START_MS: int,
) -> None:
    """
    Step-2 calls this if YN_PYIN='Y'.

    Inputs:
      • RECORDING_ID, AUDIO_CHUNK_NO
      • WAV22050_PATH: absolute path to the chunk's mono WAV at 22,050 Hz
      • AUDIO_CHUNK_START_MS: absolute ms offset for this chunk

    Behavior:
      • Decode WAV → float mono 22.05 kHz
      • Run pYIN → relative (start_ms, end_ms, hz, conf)
      • Offset to ABSOLUTE times using AUDIO_CHUNK_START_MS
      • Bulk-insert into ENGINE_LOAD_HZ with SOURCE_METHOD='PYIN'
    """
    try:
        L_WAV = Path(_bi.str(WAV22050_PATH)).resolve()
        if not L_WAV.exists():
            CONSOLE_LOG("WAV_NOT_FOUND", {"path": _bi.str(L_WAV)})
            return

        CONSOLE_LOG("PYIN_BEGIN", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
            "WAV22050_PATH": _bi.str(L_WAV),
            "AUDIO_CHUNK_START_MS": int(AUDIO_CHUNK_START_MS),
        })

        L_AUDIO_22K = AUDIO_DECODE_WAV_TO_FLOAT_MONO_22050(L_WAV)
        if L_AUDIO_22K.size == 0:
            CONSOLE_LOG("DECODE_EMPTY_AUDIO")
            return

        L_ROWS_REL = PYIN_COMPUTE_RELATIVE_SERIES(L_AUDIO_22K, sr=22050)
        if not L_ROWS_REL:
            CONSOLE_LOG("PYIN_NO_ROWS")
            return

        # Convert to ABSOLUTE ms
        L_ROWS_ABS: List[Tuple[int, int, float, float]] = []
        for (L_S_REL, L_E_REL, L_HZ, L_CONF) in L_ROWS_REL:
            L_S_ABS = int(AUDIO_CHUNK_START_MS) + int(L_S_REL)
            L_E_ABS = int(AUDIO_CHUNK_START_MS) + int(L_E_REL)
            L_ROWS_ABS.append((L_S_ABS, L_E_ABS, float(L_HZ), float(L_CONF)))

        with DB_GET_CONN() as L_CONN:
            DB_LOAD_HZ_SERIES(
                L_CONN=L_CONN,
                RECORDING_ID=int(RECORDING_ID),
                AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
                SOURCE_METHOD="PYIN",
                HZ_SERIES_ARRAY=L_ROWS_ABS,
            )

        CONSOLE_LOG("PYIN_DB_INSERT_OK", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
            "ROW_COUNT": len(L_ROWS_ABS),
        })

    except Exception as L_EXC:
        CONSOLE_LOG("PYIN_FATAL_ERROR", {
            "ERROR": _bi.str(L_EXC),
            "TRACE": traceback.format_exc(),
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
        })
