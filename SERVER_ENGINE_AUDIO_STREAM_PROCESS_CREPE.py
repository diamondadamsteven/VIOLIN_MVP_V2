# SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE.py
# ----------------------------------------------------------------------
# CREPE (torchcrepe) processing for a single audio chunk.
# Responsibilities:
#   • Decode WAV (expected 16 kHz mono) to float32
#   • Run torchcrepe → per-10ms f0 + periodicity (confidence)
#   • Convert CHUNK-relative times to ABSOLUTE times
#   • Bulk insert rows into ENGINE_LOAD_HZ with SOURCE_METHOD='CREPE'
# Env:
#   • VIOLIN_ODBC: pyodbc connection string
# ----------------------------------------------------------------------

import os
import math
import hashlib
import subprocess
import traceback
from pathlib import Path
from typing import Any, Iterable, List, Tuple

import builtins as _bi
import numpy as np

# Optional deps (graceful fallback)
try:
    import torch  # type: ignore
except Exception:  # pragma: no cover
    torch = None
try:
    import torchcrepe  # type: ignore
except Exception:  # pragma: no cover
    torchcrepe = None


# ─────────────────────────────────────────────────────────────
# Console logging (ASCII-safe)
# ─────────────────────────────────────────────────────────────
def CONSOLE_LOG(L_MSG: str, L_OBJ: Any = None):
    L_PREFIX = "PROCESS_CREPE"
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
# Audio decode (expects 16 kHz mono output)
# ─────────────────────────────────────────────────────────────
def AUDIO_DECODE_WAV_TO_FLOAT_MONO_16000(WAV_PATH: Path) -> np.ndarray:
    """
    ffmpeg decode → float32 mono @ 16,000 Hz (no normalization).
    Returns writable numpy array (C-contiguous).
    """
    L_CMD = [
        "ffmpeg", "-nostdin", "-v", "error",
        "-i", _bi.str(WAV_PATH),
        "-ac", "1",
        "-ar", "16000",
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
# CREPE core (relative series @ 10 ms hop)
# ─────────────────────────────────────────────────────────────
def CREPE_COMPUTE_RELATIVE_SERIES(audio_16k: np.ndarray, sr: int = 16000) -> List[Tuple[int, int, float, float]]:
    """
    Returns per-frame rows relative to the chunk:
      [(START_MS_REL, END_MS_REL, HZ, CONFIDENCE), ...]
    Uses hop_length=160 (10 ms @ 16 kHz) and viterbi decoder if available.
    """
    if torch is None or torchcrepe is None:
        CONSOLE_LOG("TORCHCREPE_NOT_AVAILABLE")
        return []
    if sr != 16000 or audio_16k.size == 0:
        CONSOLE_LOG("CREPE_BAD_INPUT", {"sr": sr, "size": int(audio_16k.size)})
        return []

    # Fingerprint of audio (debug dedupe)
    try:
        L_SHA1 = hashlib.sha1(audio_16k.tobytes()).hexdigest()[:12]
    except Exception:
        L_SHA1 = "sha1_err"

    L_DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
    L_X = torch.tensor(audio_16k, dtype=torch.float32, device=L_DEVICE).unsqueeze(0)

    L_HOP = 160  # 10 ms @ 16k
    L_DECODER_FN = getattr(torchcrepe.decode, "viterbi", None)
    if L_DECODER_FN is None:
        L_DECODER_FN = torchcrepe.decode.argmax
    L_DECODER_NAME = getattr(L_DECODER_FN, "__name__", str(L_DECODER_FN))

    CONSOLE_LOG("CREPE_BEGIN", {
        "device": L_DEVICE,
        "frames_approx": int(round(audio_16k.shape[0] / float(L_HOP))),
        "audio_sha1": L_SHA1,
        "decoder": L_DECODER_NAME,
    })

    with torch.no_grad():
        L_F0, L_PER = torchcrepe.predict(
            L_X,
            sample_rate=sr,
            hop_length=L_HOP,
            model="full",
            decoder=L_DECODER_FN,
            batch_size=1024,
            device=L_DEVICE,
            return_periodicity=True,
        )

    L_F0 = L_F0.squeeze(0).detach().cpu().numpy()
    L_PER = L_PER.squeeze(0).detach().cpu().numpy()
    L_N = int(min(len(L_F0), len(L_PER)))

    # Vectorized frame start times (ms, relative within chunk)
    L_START_MS = np.round(np.arange(L_N, dtype=np.float64) * L_HOP * 1000.0 / sr).astype(np.int64)

    # Quick anomaly checks (debug)
    if L_N:
        L_MODS = np.unique(L_START_MS % 10)
        L_DIFF = np.unique(np.diff(L_START_MS)) if L_N > 1 else np.array([], dtype=np.int64)
        if (L_MODS.size != 1 or L_MODS[0] != 0) or (L_DIFF.size and not np.all(L_DIFF == 10)):
            CONSOLE_LOG("CREPE_TIMING_ANOMALY", {
                "mods_of_10": L_MODS.tolist(),
                "unique_step_sizes": L_DIFF.tolist()[:6],
                "first_10": L_START_MS[:10].tolist(),
                "last_10": L_START_MS[-10:].tolist(),
            })

    L_ROWS: List[Tuple[int, int, float, float]] = []
    for i in range(L_N):
        hz = float(L_F0[i])
        conf = float(L_PER[i])
        if not (np.isfinite(hz) and hz > 0.0):
            continue
        s_rel = int(L_START_MS[i])
        e_rel = s_rel + 9
        L_ROWS.append((s_rel, e_rel, hz, conf))

    if L_ROWS:
        CONSOLE_LOG("CREPE_RELATIVE_SERIES", {
            "count": len(L_ROWS),
            "first_ms": L_ROWS[0][0],
            "last_ms": L_ROWS[-1][0],
            "audio_sha1": L_SHA1,
        })

    return L_ROWS


# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY
# ─────────────────────────────────────────────────────────────
def SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE(
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    WAV16K_PATH: str,
    AUDIO_CHUNK_START_MS: int,
) -> None:
    """
    Step-2 calls this if YN_CREPE='Y'.

    Inputs:
      • RECORDING_ID, AUDIO_CHUNK_NO
      • WAV16K_PATH: absolute path to the chunk's mono WAV at 16,000 Hz
      • AUDIO_CHUNK_START_MS: absolute ms offset for this chunk

    Behavior:
      • Decode WAV → float mono 16 kHz
      • Run torchcrepe → relative (start_ms, end_ms, hz, conf)
      • Offset to ABSOLUTE times using AUDIO_CHUNK_START_MS
      • Bulk-insert into ENGINE_LOAD_HZ with SOURCE_METHOD='CREPE'
    """
    try:
        L_WAV = Path(_bi.str(WAV16K_PATH)).resolve()
        if not L_WAV.exists():
            CONSOLE_LOG("WAV_NOT_FOUND", {"path": _bi.str(L_WAV)})
            return

        if torch is None or torchcrepe is None:
            CONSOLE_LOG("TORCHCREPE_UNAVAILABLE_SKIP")
            return

        CONSOLE_LOG("CREPE_PROCESS_BEGIN", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
            "WAV16K_PATH": _bi.str(L_WAV),
            "AUDIO_CHUNK_START_MS": int(AUDIO_CHUNK_START_MS),
        })

        L_AUDIO_16K = AUDIO_DECODE_WAV_TO_FLOAT_MONO_16000(L_WAV)
        if L_AUDIO_16K.size == 0:
            CONSOLE_LOG("DECODE_EMPTY_AUDIO")
            return

        L_ROWS_REL = CREPE_COMPUTE_RELATIVE_SERIES(L_AUDIO_16K, sr=16000)
        if not L_ROWS_REL:
            CONSOLE_LOG("CREPE_NO_ROWS")
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
                SOURCE_METHOD="CREPE",
                HZ_SERIES_ARRAY=L_ROWS_ABS,
            )

        CONSOLE_LOG("CREPE_DB_INSERT_OK", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
            "ROW_COUNT": len(L_ROWS_ABS),
        })

    except Exception as L_EXC:
        CONSOLE_LOG("CREPE_FATAL_ERROR", {
            "ERROR": _bi.str(L_EXC),
            "TRACE": traceback.format_exc(),
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
        })
