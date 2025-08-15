# SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_3_STOP.py
# ----------------------------------------------------------------------
# Step-3 of the streaming pipeline (finalize recording):
#   - Load config.json (saved by Step-1) to get AUDIO_STREAM_FILE_NAME
#   - Find all per-chunk 48k WAVs, concatenate in order
#   - Write final 48k PCM WAV named AUDIO_STREAM_FILE_NAME in the recording dir
#   - Call P_ENGINE_RECORD_END
# ----------------------------------------------------------------------

import os
import json
import traceback
from pathlib import Path
from typing import Any, Dict, List, Tuple

import builtins as _bi
import numpy as np
import soundfile as sf  # libsndfile

# ─────────────────────────────────────────────────────────────
# Console logging (ASCII-safe)
# ─────────────────────────────────────────────────────────────
def CONSOLE_LOG(L_MSG: str, L_OBJ: Any = None):
    L_PREFIX = "STEP_3_STOP"
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
# Paths
# ─────────────────────────────────────────────────────────────
BASE_TEMP_DIR = Path(os.getenv("AUDIO_TMP_DIR", "./tmp/active_recordings")).resolve()
BASE_TEMP_DIR.mkdir(parents=True, exist_ok=True)

def FILES_GET_RECORDING_DIR(RECORDING_ID: str) -> Path:
    L_DIR = BASE_TEMP_DIR / str(RECORDING_ID)
    L_DIR.mkdir(parents=True, exist_ok=True)
    return L_DIR

def FILES_GET_CONFIG_JSON_PATH(RECORDING_ID: str) -> Path:
    return FILES_GET_RECORDING_DIR(RECORDING_ID) / "config.json"

def FILES_LIST_CHUNK_WAV48K_PATHS_SORTED(RECORDING_ID: str, FINAL_BASENAME: str) -> List[Path]:
    """
    Looks for per-chunk 48k WAVs produced by Step-1. We prefer a 'chunks' subfolder,
    but will fall back to a recursive search. Sorts by detected chunk number, else name.
    """
    L_REC_DIR = FILES_GET_RECORDING_DIR(RECORDING_ID)
    L_CHUNKS_DIR = L_REC_DIR / "chunks"

    def _extract_chunk_no(L_PATH: Path) -> Tuple[int, str]:
        # Try to pull a number like _chunk_000123_ or _000123_ from the filename
        L_NAME = L_PATH.stem
        import re
        L_M = re.search(r"(?:chunk_)?(\d{3,})", L_NAME)
        if L_M:
            try:
                return (int(L_M.group(1)), L_NAME)
            except Exception:
                pass
        return (10**9, L_NAME)  # big number to push unknowns after numbered ones

    L_CANDIDATES: List[Path] = []
    if L_CHUNKS_DIR.exists():
        L_CANDIDATES.extend([p for p in L_CHUNKS_DIR.glob("**/*_48k.wav") if p.name != FINAL_BASENAME])
    else:
        L_CANDIDATES.extend([p for p in L_REC_DIR.glob("**/*_48k.wav") if p.name != FINAL_BASENAME])

    L_CANDIDATES.sort(key=_extract_chunk_no)
    return L_CANDIDATES

# ─────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────
def DB_GET_CONN():
    import pyodbc  # type: ignore
    L_CONN_STR = os.getenv("VIOLIN_ODBC", "")
    if not L_CONN_STR:
        raise RuntimeError("VIOLIN_ODBC not set (ODBC connection string).")
    return pyodbc.connect(L_CONN_STR, autocommit=True)

def DB_EXEC_SP_ROWS(L_CONN, SP_NAME: str, **PARAMS):
    L_CUR = L_CONN.cursor()
    L_ARGS = list(PARAMS.values())
    L_PLACEHOLDERS = ",".join(["?"] * len(L_ARGS))
    L_SQL = f"EXEC {SP_NAME} {L_PLACEHOLDERS}" if L_PLACEHOLDERS else f"EXEC {SP_NAME}"
    L_CUR.execute(L_SQL, L_ARGS)
    try:
        L_COLS = [c[0] for c in L_CUR.description]  # type: ignore
        return [dict(zip(L_COLS, L_ROW)) for L_ROW in L_CUR.fetchall()]
    except Exception:
        return []

def DB_EXEC_SP_ROW(L_CONN, SP_NAME: str, **PARAMS):
    L_ROWS = DB_EXEC_SP_ROWS(L_CONN, SP_NAME, **PARAMS)
    return L_ROWS[0] if L_ROWS else {}

# ─────────────────────────────────────────────────────────────
# Concatenation & write
# ─────────────────────────────────────────────────────────────
def CONCATENATING_AUDIO_WRITE_FINAL_WAV(
    RECORDING_ID: str,
    AUDIO_STREAM_FILE_NAME: str,
) -> Path:
    """
    Concatenate per-chunk 48k WAVs (float32) and write a single 48k PCM WAV
    named AUDIO_STREAM_FILE_NAME under the recording directory.
    """
    L_REC_DIR = FILES_GET_RECORDING_DIR(RECORDING_ID)
    L_FINAL_PATH = L_REC_DIR / AUDIO_STREAM_FILE_NAME

    L_CHUNK_PATHS = FILES_LIST_CHUNK_WAV48K_PATHS_SORTED(RECORDING_ID, FINAL_BASENAME=L_FINAL_PATH.name)
    CONSOLE_LOG("FINALIZE_FIND_CHUNKS", {"COUNT": len(L_CHUNK_PATHS)})

    if not L_CHUNK_PATHS:
        CONSOLE_LOG("NO_CHUNK_WAVS_FOUND", {"RECORDING_ID": RECORDING_ID})
        # Write a tiny silent WAV to ensure a file exists (optional; can skip if undesired)
        L_SILENT = np.zeros(1, dtype="float32")
        sf.write(L_FINAL_PATH, L_SILENT, 48000, subtype="PCM_16")
        return L_FINAL_PATH

    L_BUFFERS: List[np.ndarray] = []
    for L_P in L_CHUNK_PATHS:
        try:
            L_Y, L_SR = sf.read(L_P, dtype="float32", always_2d=False)
            if L_Y.ndim > 1:
                L_Y = np.mean(L_Y, axis=1).astype("float32")
            if L_SR != 48000:
                CONSOLE_LOG("WARN_CHUNK_NOT_48K", {"PATH": str(L_P), "SR": L_SR})
                # We assume Step-1 already wrote 48k; if not, we still allow mixed SR and resample quickly.
                import librosa
                L_Y = librosa.resample(L_Y, orig_sr=L_SR, target_sr=48000, res_type="kaiser_fast").astype("float32")
            L_BUFFERS.append(L_Y)
        except Exception as L_EXC:
            CONSOLE_LOG("READ_CHUNK_ERROR_SKIP", {"PATH": str(L_P), "ERROR": _bi.str(L_EXC)})

    if not L_BUFFERS:
        CONSOLE_LOG("NO_VALID_BUFFERS", {"RECORDING_ID": RECORDING_ID})
        L_SILENT = np.zeros(1, dtype="float32")
        sf.write(L_FINAL_PATH, L_SILENT, 48000, subtype="PCM_16")
        return L_FINAL_PATH

    L_CONCAT = np.concatenate(L_BUFFERS, axis=0).astype("float32")
    sf.write(L_FINAL_PATH, L_CONCAT, 48000, subtype="PCM_16")
    CONSOLE_LOG("FINAL_WAV_WRITTEN", {"PATH": str(L_FINAL_PATH), "SAMPLES": int(L_CONCAT.shape[0])})
    return L_FINAL_PATH

# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY
# ─────────────────────────────────────────────────────────────
async def SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_3_STOP(RECORDING_ID: str) -> None:
    """
    Saves the full 48k WAV to AUDIO_STREAM_FILE_NAME (from config.json),
    then calls P_ENGINE_RECORD_END.
    """
    try:
        L_CONFIG_PATH = FILES_GET_CONFIG_JSON_PATH(RECORDING_ID)
        if not L_CONFIG_PATH.exists():
            CONSOLE_LOG("CONFIG_JSON_NOT_FOUND", {"PATH": str(L_CONFIG_PATH)})
            return

        L_CONFIG_DICT: Dict[str, Any] = json.loads(L_CONFIG_PATH.read_text(encoding="utf-8"))
        AUDIO_STREAM_FILE_NAME = _bi.str(L_CONFIG_DICT.get("AUDIO_STREAM_FILE_NAME") or "").strip()
        if not AUDIO_STREAM_FILE_NAME:
            CONSOLE_LOG("AUDIO_STREAM_FILE_NAME_MISSING_IN_CONFIG", {"RECORDING_ID": RECORDING_ID})
            return

        # Concatenate all per-chunk WAVs into final WAV
        L_FINAL_PATH = CONCATENATING_AUDIO_WRITE_FINAL_WAV(RECORDING_ID, AUDIO_STREAM_FILE_NAME)

        # Call P_ENGINE_RECORD_END
        try:
            with DB_GET_CONN() as L_CONN:
                _ = DB_EXEC_SP_ROW(
                    L_CONN,
                    "P_ENGINE_RECORD_END",
                    RECORDING_ID=int(RECORDING_ID),
                    AUDIO_STREAM_FILE_NAME=str(AUDIO_STREAM_FILE_NAME),
                )
            CONSOLE_LOG("P_ENGINE_RECORD_END_CALLED", {
                "RECORDING_ID": RECORDING_ID,
                "AUDIO_STREAM_FILE_NAME": AUDIO_STREAM_FILE_NAME,
                "FINAL_WAV": str(L_FINAL_PATH),
            })
        except Exception as L_EXC:
            CONSOLE_LOG("SP_P_ENGINE_RECORD_END_ERROR", {"ERROR": _bi.str(L_EXC), "TRACE": traceback.format_exc()})

    except Exception as L_EXC:
        CONSOLE_LOG("FATAL_STOP_ERROR", {"ERROR": _bi.str(L_EXC), "TRACE": traceback.format_exc()})
