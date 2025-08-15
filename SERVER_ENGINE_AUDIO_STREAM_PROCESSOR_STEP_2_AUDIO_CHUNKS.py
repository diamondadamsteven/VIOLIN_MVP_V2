# SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_2_AUDIO_CHUNKS.py
# ----------------------------------------------------------------------
# Step-2 of the streaming pipeline (per audio chunk):
#   - Load VIOLINIST_ID from config.json (saved by Step-1)
#   - Resample WAV48k -> (48k float, 22.05k float, 16k float)
#   - FFT (compose: only if YN_FFT not null; else call SP to skip; then SP P_ENGINE_ALL_METHOD_FFT)
#           (play/practice: run FFT unconditionally per requirements)
#   - Other processing per flags (ONS, PYIN, CREPE, VOLUME)
#   - Call P_ENGINE_ALL_MASTER
# ----------------------------------------------------------------------

import os
import json
import time
import asyncio
import traceback
from pathlib import Path
from typing import Dict, Any, Tuple, Optional

import builtins as _bi
import numpy as np

# Audio I/O and resampling
import soundfile as sf  # libsndfile
import librosa

# ── Sub-processors (public entries)
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT import SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS import SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN import SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE import SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME import SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME

# ─────────────────────────────────────────────────────────────
# Console logging (ASCII-safe)
# ─────────────────────────────────────────────────────────────
def CONSOLE_LOG(L_MSG: str, L_OBJ: Any = None):
    L_PREFIX = "STEP_2_AUDIO_CHUNKS"
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
# Paths (shared with Step-1 conventions)
# ─────────────────────────────────────────────────────────────
BASE_TEMP_DIR = Path(os.getenv("AUDIO_TMP_DIR", "./tmp/active_recordings")).resolve()
BASE_TEMP_DIR.mkdir(parents=True, exist_ok=True)

def FILES_GET_RECORDING_DIR(RECORDING_ID: str) -> Path:
    L_DIR = BASE_TEMP_DIR / str(RECORDING_ID)
    L_DIR.mkdir(parents=True, exist_ok=True)
    return L_DIR

def FILES_GET_CONFIG_JSON_PATH(RECORDING_ID: str) -> Path:
    return FILES_GET_RECORDING_DIR(RECORDING_ID) / "config.json"

def FILES_GET_CHUNK_WAV_22050_PATH(RECORDING_ID: str, AUDIO_CHUNK_NO: int) -> Path:
    """
    Location for a temporary per-chunk 22,050 Hz WAV written by Step-2
    (needed by the Volume processor’s file-based API).
    """
    return FILES_GET_RECORDING_DIR(RECORDING_ID) / f"chunk_{int(AUDIO_CHUNK_NO):06d}_22050.wav"

# ─────────────────────────────────────────────────────────────
# DB helpers (same conventions as Step-1)
# ─────────────────────────────────────────────────────────────
def DB_GET_CONN():
    """
    Returns an autocommit ODBC connection using env var VIOLIN_ODBC.
    """
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
# Resampling (Step 0)
# ─────────────────────────────────────────────────────────────
def STEP_0_RESAMPLING_AUDIO_RESAMPLE(WAV48K_PATH: str) -> Tuple[np.ndarray, int, np.ndarray, int, np.ndarray, int]:
    """
    Read mono PCM WAV (expected 48k) and produce float32 arrays at:
      - 48,000 Hz (as-is, or resampled if not 48k for safety)
      - 22,050 Hz (for pYIN/vol)
      - 16,000 Hz (for CREPE/ONS pipelines if they want it)
    Returns: (L_AUDIO_DATA_48K, 48000, L_AUDIO_DATA_22K, 22050, L_AUDIO_DATA_16K, 16000)
    """
    L_Y, L_SR = sf.read(WAV48K_PATH, dtype="float32", always_2d=False)
    if L_Y.ndim > 1:
        # Mixdown to mono if needed
        L_Y = np.mean(L_Y, axis=1).astype("float32")

    if L_SR != 48000:
        CONSOLE_LOG("WAV_NOT_48K_RESAMPLING_UPFRONT", {"FOUND_SR": L_SR})
        L_Y_48K = librosa.resample(L_Y, orig_sr=L_SR, target_sr=48000, res_type="kaiser_best").astype("float32")
    else:
        L_Y_48K = L_Y

    L_Y_22K = librosa.resample(L_Y_48K, orig_sr=48000, target_sr=22050, res_type="kaiser_best").astype("float32")
    L_Y_16K = librosa.resample(L_Y_48K, orig_sr=48000, target_sr=16000, res_type="kaiser_best").astype("float32")

    return L_Y_48K, 48000, L_Y_22K, 22050, L_Y_16K, 16000

# ─────────────────────────────────────────────────────────────
# FFT (Step 1) and flag resolution
# ─────────────────────────────────────────────────────────────
async def STEP_1_FFT_IF_NEEDED(
    RECORDING_ID: str,
    AUDIO_CHUNK_NO: int,
    WAV48K_PATH: str,
    COMPOSE_PLAY_OR_PRACTICE: str,
    YN_FFT: Optional[str],
    L_AUDIO_DATA_48K: np.ndarray,
    L_SR_48K: int,
) -> Dict[str, Optional[str]]:
    """
    Compose mode:
      - If YN_FFT is None: call P_ENGINE_ALL_METHOD_COMPOSE_DONT_RUN_FFT
      - Else: run FFT, then call P_ENGINE_ALL_METHOD_FFT
      - Then re-query flags via P_ENGINE_SONG_AUDIO_CHUNK_NO_FOR_COMPOSE_GET

    Play/Practice mode:
      - Run FFT (per your instruction to always run in these modes)

    Returns L_RUN_FLAGS_DICT with YN_RUN_ONS/YN_RUN_PYIN/YN_RUN_CREPE (and YN_RUN_FFT if relevant).
    """
    L_RUN_FLAGS_DICT: Dict[str, Optional[str]] = {
        "YN_RUN_FFT": YN_FFT,
        "YN_RUN_ONS": None,
        "YN_RUN_PYIN": None,
        "YN_RUN_CREPE": None,
    }

    # Run FFT per mode rules
    if COMPOSE_PLAY_OR_PRACTICE == "COMPOSE":
        if YN_FFT is None:
            # Explicit "do not run FFT" path for compose
            CONSOLE_LOG("COMPOSE_FFT_SKIPPED_BY_FLAG", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
            try:
                with DB_GET_CONN() as L_CONN:
                    _ = DB_EXEC_SP_ROW(
                        L_CONN,
                        "P_ENGINE_ALL_METHOD_COMPOSE_DONT_RUN_FFT",
                        RECORDING_ID=int(RECORDING_ID),
                        AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
                    )
            except Exception as L_EXC:
                CONSOLE_LOG("SP_P_ENGINE_ALL_METHOD_COMPOSE_DONT_RUN_FFT_ERROR", {"ERROR": _bi.str(L_EXC)})
        else:
            # Run FFT processor then mark done via SP
            CONSOLE_LOG("COMPOSE_FFT_RUNNING", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
            L_RET = SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT(
                RECORDING_ID=RECORDING_ID,
                AUDIO_CHUNK_NO=AUDIO_CHUNK_NO,
                WAV48K_PATH=WAV48K_PATH,
                AUDIO_DATA_48K=L_AUDIO_DATA_48K,
                SR_48K=L_SR_48K,
            )
            if asyncio.iscoroutine(L_RET):
                await L_RET
            try:
                with DB_GET_CONN() as L_CONN:
                    _ = DB_EXEC_SP_ROW(
                        L_CONN,
                        "P_ENGINE_ALL_METHOD_FFT",
                        RECORDING_ID=int(RECORDING_ID),
                        AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
                    )
            except Exception as L_EXC:
                CONSOLE_LOG("SP_P_ENGINE_ALL_METHOD_FFT_ERROR", {"ERROR": _bi.str(L_EXC)})

        # Refresh run flags for compose chunk
        try:
            with DB_GET_CONN() as L_CONN:
                RES_SET = DB_EXEC_SP_ROW(
                    L_CONN,
                    "P_ENGINE_SONG_AUDIO_CHUNK_NO_FOR_COMPOSE_GET",
                    RECORDING_ID=int(RECORDING_ID),
                    AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
                )
            L_RUN_FLAGS_DICT["YN_RUN_ONS"] = RES_SET.get("YN_RUN_ONS")
            L_RUN_FLAGS_DICT["YN_RUN_PYIN"] = RES_SET.get("YN_RUN_PYIN")
            L_RUN_FLAGS_DICT["YN_RUN_CREPE"] = RES_SET.get("YN_RUN_CREPE")
        except Exception as L_EXC:
            CONSOLE_LOG("SP_P_ENGINE_SONG_AUDIO_CHUNK_NO_FOR_COMPOSE_GET_ERROR", {"ERROR": _bi.str(L_EXC)})

    else:
        # PLAY/PRACTICE → always run FFT per spec
        CONSOLE_LOG("PLAY_PRACTICE_FFT_RUNNING", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
        L_RET = SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT(
            RECORDING_ID=RECORDING_ID,
            AUDIO_CHUNK_NO=AUDIO_CHUNK_NO,
            WAV48K_PATH=WAV48K_PATH,
            AUDIO_DATA_48K=L_AUDIO_DATA_48K,
            SR_48K=L_SR_48K,
        )
        if asyncio.iscoroutine(L_RET):
            await L_RET

        # Use flags passed into Step-2 (from Step-1’s config for play/practice)

    return L_RUN_FLAGS_DICT

# ─────────────────────────────────────────────────────────────
# Other processing (Step 2) + P_ENGINE_ALL_MASTER
# ─────────────────────────────────────────────────────────────
async def STEP_2_ALL_OTHER_PROCESSING(
    RECORDING_ID: str,
    AUDIO_CHUNK_NO: int,
    WAV48K_PATH: str,
    COMPOSE_PLAY_OR_PRACTICE: str,
    YN_ONS: Optional[str],
    YN_PYIN: Optional[str],
    YN_CREPE: Optional[str],
    L_AUDIO_DATA_48K: np.ndarray,
    L_SR_48K: int,
    L_AUDIO_DATA_22K: np.ndarray,
    L_SR_22K: int,
    L_AUDIO_DATA_16K: np.ndarray,
    L_SR_16K: int,
    AUDIO_CHUNK_START_MS: int,
) -> None:

    # ONS (if requested)
    if (YN_ONS or "").upper() == "Y":
        try:
            CONSOLE_LOG("RUN_ONS_BEGIN", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
            L_RET = SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS(
                RECORDING_ID=RECORDING_ID,
                AUDIO_CHUNK_NO=AUDIO_CHUNK_NO,
                WAV48K_PATH=WAV48K_PATH,
                AUDIO_DATA_16K=L_AUDIO_DATA_16K,
                SR_16K=L_SR_16K,
            )
            if asyncio.iscoroutine(L_RET):
                await L_RET
            CONSOLE_LOG("RUN_ONS_END", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
        except Exception as L_EXC:
            CONSOLE_LOG("RUN_ONS_ERROR_NON_FATAL", {"ERROR": _bi.str(L_EXC), "TRACE": traceback.format_exc()})

    # PYIN (if requested)
    if (YN_PYIN or "").upper() == "Y":
        try:
            CONSOLE_LOG("RUN_PYIN_BEGIN", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
            L_RET = SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN(
                RECORDING_ID=RECORDING_ID,
                AUDIO_CHUNK_NO=AUDIO_CHUNK_NO,
                AUDIO_DATA_22K=L_AUDIO_DATA_22K,
                SR_22K=L_SR_22K,
                AUDIO_CHUNK_START_MS=int(AUDIO_CHUNK_START_MS),
            )
            if asyncio.iscoroutine(L_RET):
                await L_RET
            CONSOLE_LOG("RUN_PYIN_END", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
        except Exception as L_EXC:
            CONSOLE_LOG("RUN_PYIN_ERROR_NON_FATAL", {"ERROR": _bi.str(L_EXC), "TRACE": traceback.format_exc()})

    # CREPE (if requested)
    if (YN_CREPE or "").upper() == "Y":
        try:
            CONSOLE_LOG("RUN_CREPE_BEGIN", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
            L_RET = SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE(
                RECORDING_ID=RECORDING_ID,
                AUDIO_CHUNK_NO=AUDIO_CHUNK_NO,
                AUDIO_DATA_16K=L_AUDIO_DATA_16K,
                SR_16K=L_SR_16K,
                AUDIO_CHUNK_START_MS=int(AUDIO_CHUNK_START_MS),
            )
            if asyncio.iscoroutine(L_RET):
                await L_RET
            CONSOLE_LOG("RUN_CREPE_END", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
        except Exception as L_EXC:
            CONSOLE_LOG("RUN_CREPE_ERROR_NON_FATAL", {"ERROR": _bi.str(L_EXC), "TRACE": traceback.format_exc()})

    # VOLUME (always run) — writes a temp 22,050 Hz WAV and calls the volume module
    try:
        CONSOLE_LOG("RUN_VOLUME_BEGIN", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
        L_WAV22050_PATH = FILES_GET_CHUNK_WAV_22050_PATH(RECORDING_ID, AUDIO_CHUNK_NO)
        # Ensure directory exists
        L_WAV22050_PATH.parent.mkdir(parents=True, exist_ok=True)
        # Write mono PCM16 @ 22,050 Hz (module will re-decode via ffmpeg as float32)
        sf.write(_bi.str(L_WAV22050_PATH), L_AUDIO_DATA_22K.astype("float32"), L_SR_22K, subtype="PCM_16")

        L_RET = SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME(
            RECORDING_ID=int(RECORDING_ID),
            AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
            WAV22050_PATH=_bi.str(L_WAV22050_PATH),
            AUDIO_CHUNK_START_MS=int(AUDIO_CHUNK_START_MS),
        )
        if asyncio.iscoroutine(L_RET):
            await L_RET
        CONSOLE_LOG("RUN_VOLUME_END", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
    except Exception as L_EXC:
        CONSOLE_LOG("RUN_VOLUME_ERROR_NON_FATAL", {"ERROR": _bi.str(L_EXC), "TRACE": traceback.format_exc()})

    # Final master SP
    try:
        with DB_GET_CONN() as L_CONN:
            # Need VIOLINIST_ID for P_ENGINE_ALL_MASTER → load from config.json
            L_CONFIG_DICT = json.loads(FILES_GET_CONFIG_JSON_PATH(RECORDING_ID).read_text(encoding="utf-8"))
            VIOLINIST_ID = int(L_CONFIG_DICT.get("VIOLINIST_ID") or 0)

            CONSOLE_LOG("CALL_P_ENGINE_ALL_MASTER", {
                "VIOLINIST_ID": VIOLINIST_ID,
                "RECORDING_ID": RECORDING_ID,
                "COMPOSE_PLAY_OR_PRACTICE": COMPOSE_PLAY_OR_PRACTICE,
                "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO,
            })

            _ = DB_EXEC_SP_ROW(
                L_CONN,
                "P_ENGINE_ALL_MASTER",
                VIOLINIST_ID=int(VIOLINIST_ID),
                RECORDING_ID=int(RECORDING_ID),
                COMPOSE_PLAY_OR_PRACTICE=str(COMPOSE_PLAY_OR_PRACTICE),
                AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
            )
    except Exception as L_EXC:
        CONSOLE_LOG("SP_P_ENGINE_ALL_MASTER_ERROR", {"ERROR": _bi.str(L_EXC), "TRACE": traceback.format_exc()})

# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY (called by Step-1)
# ─────────────────────────────────────────────────────────────
async def SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_2_AUDIO_CHUNKS(
    RECORDING_ID: str,
    AUDIO_CHUNK_NO: int,
    WAV48K_PATH: str,
    AUDIO_CHUNK_START_MS: int,
    AUDIO_CHUNK_END_MS: int,
    COMPOSE_PLAY_OR_PRACTICE: str,
    YN_FFT: Optional[str],
    YN_ONS: Optional[str],
    YN_PYIN: Optional[str],
    YN_CREPE: Optional[str],
) -> None:
    """
    Public entry per your spec. Runs full Step-2 for one audio chunk.
    """
    L_T0 = time.perf_counter()
    CONSOLE_LOG("ENTRY", {
        "RECORDING_ID": RECORDING_ID,
        "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO,
        "WAV48K_PATH": WAV48K_PATH,
        "AUDIO_CHUNK_START_MS": AUDIO_CHUNK_START_MS,
        "AUDIO_CHUNK_END_MS": AUDIO_CHUNK_END_MS,
        "COMPOSE_PLAY_OR_PRACTICE": COMPOSE_PLAY_OR_PRACTICE,
        "YN_FFT": YN_FFT,
        "YN_ONS": YN_ONS,
        "YN_PYIN": YN_PYIN,
        "YN_CREPE": YN_CREPE,
    })

    # ── Step 0: Resample (also load 48k float)
    L_AUDIO_DATA_48K, L_SR_48K, L_AUDIO_DATA_22K, L_SR_22K, L_AUDIO_DATA_16K, L_SR_16K = STEP_0_RESAMPLING_AUDIO_RESAMPLE(WAV48K_PATH)
    L_CHUNK_MS = int(AUDIO_CHUNK_END_MS) - int(AUDIO_CHUNK_START_MS) + 1
    CONSOLE_LOG("RESAMPLE_DONE", {
        "LEN_48K": int(L_AUDIO_DATA_48K.shape[0]),
        "SR_48K": L_SR_48K,
        "LEN_22K": int(L_AUDIO_DATA_22K.shape[0]),
        "SR_22K": L_SR_22K,
        "LEN_16K": int(L_AUDIO_DATA_16K.shape[0]),
        "SR_16K": L_SR_16K,
        "AUDIO_CHUNK_DURATION_IN_MS": L_CHUNK_MS,
    })

    # ── Step 1: FFT (and compose-flag refresh, if applicable)
    L_RUN_FLAGS_DICT = await STEP_1_FFT_IF_NEEDED(
        RECORDING_ID=RECORDING_ID,
        AUDIO_CHUNK_NO=AUDIO_CHUNK_NO,
        WAV48K_PATH=WAV48K_PATH,
        COMPOSE_PLAY_OR_PRACTICE=str(COMPOSE_PLAY_OR_PRACTICE).upper(),
        YN_FFT=YN_FFT,
        L_AUDIO_DATA_48K=L_AUDIO_DATA_48K,
        L_SR_48K=L_SR_48K,
    )

    # When PLAY/PRACTICE, we use flags provided to this entry.
    # When COMPOSE, refresh values may arrive from DB; fall back to provided if missing.
    L_YN_ONS = L_RUN_FLAGS_DICT.get("YN_RUN_ONS") if str(COMPOSE_PLAY_OR_PRACTICE).upper() == "COMPOSE" else YN_ONS
    L_YN_PYIN = L_RUN_FLAGS_DICT.get("YN_RUN_PYIN") if str(COMPOSE_PLAY_OR_PRACTICE).upper() == "COMPOSE" else YN_PYIN
    L_YN_CREPE = L_RUN_FLAGS_DICT.get("YN_RUN_CREPE") if str(COMPOSE_PLAY_OR_PRACTICE).upper() == "COMPOSE" else YN_CREPE

    # ── Step 2: Other processing + P_ENGINE_ALL_MASTER
    await STEP_2_ALL_OTHER_PROCESSING(
        RECORDING_ID=RECORDING_ID,
        AUDIO_CHUNK_NO=AUDIO_CHUNK_NO,
        WAV48K_PATH=WAV48K_PATH,
        COMPOSE_PLAY_OR_PRACTICE=str(COMPOSE_PLAY_OR_PRACTICE).upper(),
        YN_ONS=L_YN_ONS,
        YN_PYIN=L_YN_PYIN,
        YN_CREPE=L_YN_CREPE,
        L_AUDIO_DATA_48K=L_AUDIO_DATA_48K, L_SR_48K=L_SR_48K,
        L_AUDIO_DATA_22K=L_AUDIO_DATA_22K, L_SR_22K=L_SR_22K,
        L_AUDIO_DATA_16K=L_AUDIO_DATA_16K, L_SR_16K=L_SR_16K,
        AUDIO_CHUNK_START_MS=int(AUDIO_CHUNK_START_MS),
    )

    CONSOLE_LOG("EXIT", {
        "RECORDING_ID": RECORDING_ID,
        "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO,
        "ELAPSED_S": round(time.perf_counter() - L_T0, 3),
    })
