# SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_2_AUDIO_CHUNKS.py
# ----------------------------------------------------------------------
# Step-2 of the streaming pipeline (per audio chunk):
#   - Resample WAV48k -> (48k float, 22.05k float, 16k float)
#   - FFT:
#       * COMPOSE: if YN_FFT is None => SP P_ENGINE_ALL_METHOD_COMPOSE_DONT_RUN_FFT
#                  else run FFT then SP P_ENGINE_ALL_METHOD_FFT
#       * PLAY/PRACTICE: always run FFT
#   - Other processing per flags (ONS, PYIN, CREPE, VOLUME) — pass arrays/paths
#   - Call P_ENGINE_ALL_MASTER
#   - Update in-memory metrics and log via DB_LOG_RECORDING_AUDIO_CHUNK
# ----------------------------------------------------------------------

from __future__ import annotations

import time
import asyncio
import traceback
from typing import Dict, Optional, Tuple
from datetime import datetime

import builtins as _bi
import numpy as np
import soundfile as sf
import librosa

# Shared globals & DB helpers
from SERVER_ENGINE_APP_VARIABLES import (
    RECORDING_CONFIG_ARRAY,
    RECORDING_AUDIO_CHUNK_ARRAY,
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    CONSOLE_LOG,
    DB_CONNECT,
    DB_EXEC_SP_NO_RESULT,
    DB_LOG_RECORDING_AUDIO_CHUNK,
)

# Sub-processors (public entries; current signatures)
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT import SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS import SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN import SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE import SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE
from SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME import SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME


# ─────────────────────────────────────────────────────────────
# Resampling
# ─────────────────────────────────────────────────────────────
def _resample_for_step2(wav48k_path: str) -> Tuple[np.ndarray, int, np.ndarray, int, np.ndarray, int]:
    """
    Read mono WAV (expect ~48k) -> float32 arrays at 48k / 22.05k / 16k.
    Returns: (y48k, 48000, y22k, 22050, y16k, 16000)
    """
    y, sr = sf.read(wav48k_path, dtype="float32", always_2d=False)
    if isinstance(y, np.ndarray) and y.ndim > 1:
        y = np.mean(y, axis=1).astype("float32")

    if sr != 48000:
        CONSOLE_LOG("STEP2", "WAV_NOT_48K_RESAMPLING_UPFRONT", {"FOUND_SR": sr, "PATH": wav48k_path})
        y48 = librosa.resample(y, orig_sr=sr, target_sr=48000, res_type="kaiser_best").astype("float32")
    else:
        y48 = y

    y22 = librosa.resample(y48, orig_sr=48000, target_sr=22050, res_type="kaiser_best").astype("float32")
    y16 = librosa.resample(y48, orig_sr=48000, target_sr=16000, res_type="kaiser_best").astype("float32")
    return y48, 48000, y22, 22050, y16, 16000


# ─────────────────────────────────────────────────────────────
# FFT (Step 1) and compose-flag behavior
# ─────────────────────────────────────────────────────────────
async def _run_fft_if_needed(
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    COMPOSE_PLAY_OR_PRACTICE: str,
    YN_FFT: Optional[str],
    AUDIO_CHUNK_START_MS: int,
    AUDIO_ARRAY_22050: np.ndarray,
    SR_22050: int,
) -> Dict[str, Optional[str]]:
    """
    Compose:
      - If YN_FFT is None → SP P_ENGINE_ALL_METHOD_COMPOSE_DONT_RUN_FFT
      - Else run FFT then SP P_ENGINE_ALL_METHOD_FFT
    Play/Practice:
      - Always run FFT
    """
    mode = _bi.str(COMPOSE_PLAY_OR_PRACTICE).upper()

    # Ensure chunk record exists in memory
    chunks = RECORDING_AUDIO_CHUNK_ARRAY.setdefault(RECORDING_ID, {})
    ch = chunks.setdefault(AUDIO_CHUNK_NO, {
        "RECORDING_ID": RECORDING_ID,
        "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO,
    })

    run_flags: Dict[str, Optional[str]] = {
        "YN_RUN_FFT": YN_FFT,
        "YN_RUN_ONS": ch.get("YN_RUN_ONS"),
        "YN_RUN_PYIN": ch.get("YN_RUN_PYIN"),
        "YN_RUN_CREPE": ch.get("YN_RUN_CREPE"),
    }

    if mode == "COMPOSE":
        if YN_FFT is None:
            CONSOLE_LOG("STEP2", "COMPOSE_FFT_SKIPPED_BY_FLAG", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
            try:
                with DB_CONNECT() as conn:
                    DB_EXEC_SP_NO_RESULT(
                        conn,
                        "P_ENGINE_ALL_METHOD_COMPOSE_DONT_RUN_FFT",
                        RECORDING_ID=int(RECORDING_ID),
                        AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
                    )
            except Exception as exc:
                CONSOLE_LOG("STEP2", "SP_P_ENGINE_ALL_METHOD_COMPOSE_DONT_RUN_FFT_ERROR", {"err": _bi.str(exc)})
        else:
            CONSOLE_LOG("STEP2", "COMPOSE_FFT_RUNNING", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
            ch["DT_START_FFT"] = datetime.utcnow()
            t_fft = time.perf_counter()

            # FFT expects AUDIO_DATA_22K + SR_22K
            ret = SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT(
                int(RECORDING_ID),
                int(AUDIO_CHUNK_NO),
                int(AUDIO_CHUNK_START_MS),
                AUDIO_ARRAY_22050,
                int(SR_22050),
            )
            if asyncio.iscoroutine(ret):
                await ret

            ch["FFT_DURATION_IN_MS"] = int(round((time.perf_counter() - t_fft) * 1000))
            try:
                with DB_CONNECT() as conn:
                    DB_EXEC_SP_NO_RESULT(
                        conn,
                        "P_ENGINE_ALL_METHOD_FFT",
                        RECORDING_ID=int(RECORDING_ID),
                        AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
                    )
            except Exception as exc:
                CONSOLE_LOG("STEP2", "SP_P_ENGINE_ALL_METHOD_FFT_ERROR", {"err": _bi.str(exc)})

    else:
        # PLAY / PRACTICE → always run FFT
        CONSOLE_LOG("STEP2", "PLAY_PRACTICE_FFT_RUNNING", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
        ch["DT_START_FFT"] = datetime.utcnow()
        t_fft = time.perf_counter()
        ret = SERVER_ENGINE_AUDIO_STREAM_PROCESS_FFT(
            int(RECORDING_ID),
            int(AUDIO_CHUNK_NO),
            int(AUDIO_CHUNK_START_MS),
            AUDIO_ARRAY_22050,
            int(SR_22050),
        )
        if asyncio.iscoroutine(ret):
            await ret
        ch["FFT_DURATION_IN_MS"] = int(round((time.perf_counter() - t_fft) * 1000))

    return run_flags


# ─────────────────────────────────────────────────────────────
# Other processing (ONS, PYIN, CREPE, VOLUME) + P_ENGINE_ALL_MASTER
# ─────────────────────────────────────────────────────────────
async def _run_others_and_master(
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    COMPOSE_PLAY_OR_PRACTICE: str,
    YN_ONS: Optional[str],
    YN_PYIN: Optional[str],
    YN_CREPE: Optional[str],
    AUDIO_ARRAY_22K: np.ndarray,
    SR_22K: int,
    AUDIO_ARRAY_16K: np.ndarray,
    SR_16K: int,
    AUDIO_CHUNK_START_MS: int,
    WAV48K_PATH: str,
) -> None:

    chunks = RECORDING_AUDIO_CHUNK_ARRAY.setdefault(RECORDING_ID, {})
    ch = chunks.setdefault(AUDIO_CHUNK_NO, {
        "RECORDING_ID": RECORDING_ID,
        "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO,
    })

    # ONS (microservice expects a WAV path; 48k path is acceptable)
    if (YN_ONS or "").upper() == "Y":
        try:
            CONSOLE_LOG("STEP2", "RUN_ONS_BEGIN", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
            ch["DT_START_ONS"] = datetime.utcnow()
            t = time.perf_counter()
            ret = SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS(
                int(RECORDING_ID),
                int(AUDIO_CHUNK_NO),
                _bi.str(WAV48K_PATH),
                int(AUDIO_CHUNK_START_MS),
            )
            if asyncio.iscoroutine(ret):
                await ret
            ch["ONS_DURATION_IN_MS"] = int(round((time.perf_counter() - t) * 1000))
            CONSOLE_LOG("STEP2", "RUN_ONS_END", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
        except Exception as exc:
            CONSOLE_LOG("STEP2", "RUN_ONS_ERROR_NON_FATAL", {"err": _bi.str(exc), "trace": traceback.format_exc()})

    # PYIN
    if (YN_PYIN or "").upper() == "Y":
        try:
            CONSOLE_LOG("STEP2", "RUN_PYIN_BEGIN", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
            ch["DT_START_PYIN"] = datetime.utcnow()
            t = time.perf_counter()
            ret = SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN(
                int(RECORDING_ID),
                int(AUDIO_CHUNK_NO),
                int(AUDIO_CHUNK_START_MS),
                AUDIO_ARRAY_22K,
                int(SR_22K),
            )
            if asyncio.iscoroutine(ret):
                await ret
            ch["PYIN_DURATION_IN_MS"] = int(round((time.perf_counter() - t) * 1000))
            CONSOLE_LOG("STEP2", "RUN_PYIN_END", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
        except Exception as exc:
            CONSOLE_LOG("STEP2", "RUN_PYIN_ERROR_NON_FATAL", {"err": _bi.str(exc), "trace": traceback.format_exc()})

    # CREPE (array-based; computes absolute times from config)
    if (YN_CREPE or "").upper() == "Y":
        try:
            CONSOLE_LOG("STEP2", "RUN_CREPE_BEGIN", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
            ch["DT_START_CREPE"] = datetime.utcnow()
            t = time.perf_counter()
            ret = SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE(
                _bi.str(RECORDING_ID),
                int(AUDIO_CHUNK_NO),
                AUDIO_ARRAY_16K,
                int(SR_16K),
            )
            if asyncio.iscoroutine(ret):
                await ret
            ch["CREPE_DURATION_IN_MS"] = int(round((time.perf_counter() - t) * 1000))
            CONSOLE_LOG("STEP2", "RUN_CREPE_END", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
        except Exception as exc:
            CONSOLE_LOG("STEP2", "RUN_CREPE_ERROR_NON_FATAL", {"err": _bi.str(exc), "trace": traceback.format_exc()})

    # VOLUME (always run)
    try:
        CONSOLE_LOG("STEP2", "RUN_VOLUME_BEGIN", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
        ch["DT_START_VOLUME"] = datetime.utcnow()
        t = time.perf_counter()
        ret = SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME(
            int(RECORDING_ID),
            int(AUDIO_CHUNK_NO),
            int(AUDIO_CHUNK_START_MS),
            AUDIO_ARRAY_22K,
            int(SR_22K),
        )
        if asyncio.iscoroutine(ret):
            await ret
        # If volume processor fills specific *_RECORD_CNT/_DURATION_* fields in `ch`, keep them.
        # Otherwise record a total elapsed as a fallback for 10ms duration:
        ch.setdefault("VOLUME_10_MS_DURATION_IN_MS", int(round((time.perf_counter() - t) * 1000)))
        CONSOLE_LOG("STEP2", "RUN_VOLUME_END", {"AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})
    except Exception as exc:
        CONSOLE_LOG("STEP2", "RUN_VOLUME_ERROR_NON_FATAL", {"err": _bi.str(exc), "trace": traceback.format_exc()})

    # Final master SP
    try:
        with DB_CONNECT() as conn:
            cfg = RECORDING_CONFIG_ARRAY.get(RECORDING_ID, {})
            violinist_id = int(cfg.get("VIOLINIST_ID") or 0)

            CONSOLE_LOG("STEP2", "CALL_P_ENGINE_ALL_MASTER", {
                "VIOLINIST_ID": violinist_id,
                "RECORDING_ID": RECORDING_ID,
                "COMPOSE_PLAY_OR_PRACTICE": COMPOSE_PLAY_OR_PRACTICE,
                "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO,
            })

            ch["DT_START_P_ENGINE_ALL_MASTER"] = datetime.utcnow()
            t = time.perf_counter()
            DB_EXEC_SP_NO_RESULT(
                conn,
                "P_ENGINE_ALL_MASTER",
                VIOLINIST_ID=int(violinist_id),
                RECORDING_ID=int(RECORDING_ID),
                COMPOSE_PLAY_OR_PRACTICE=_bi.str(COMPOSE_PLAY_OR_PRACTICE),
                AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
            )
            ch["P_ENGINE_ALL_MASTER_DURATION_IN_MS"] = int(round((time.perf_counter() - t) * 1000))

    except Exception as exc:
        CONSOLE_LOG("STEP2", "SP_P_ENGINE_ALL_MASTER_ERROR", {"err": _bi.str(exc), "trace": traceback.format_exc()})


# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY (called by Step-1) — reads flags/times from arrays
# ─────────────────────────────────────────────────────────────
async def SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_2_AUDIO_CHUNKS(
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    WAV48K_PATH: str,
) -> None:
    """
    Runs full Step-2 for one audio chunk.
    Reads start/end/flags from RECORDING_AUDIO_CHUNK_ARRAY and mode from RECORDING_CONFIG_ARRAY.
    """
    t0 = time.perf_counter()

    # Look up mode and per-chunk spec from in-memory arrays
    cfg = RECORDING_CONFIG_ARRAY.get(RECORDING_ID, {})
    mode = _bi.str(cfg.get("COMPOSE_PLAY_OR_PRACTICE") or "").upper()

    chunks = RECORDING_AUDIO_CHUNK_ARRAY.setdefault(RECORDING_ID, {})
    ch = chunks.setdefault(AUDIO_CHUNK_NO, {"RECORDING_ID": RECORDING_ID, "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})

    start_ms = int(ch.get("START_MS") or 0)
    end_ms   = int(ch.get("END_MS") or (start_ms - 1))
    ch["AUDIO_CHUNK_DURATION_IN_MS"] = end_ms - start_ms + 1 if end_ms >= start_ms else 0

    # Preserve YN flags already decided in Step-1 (or defaults/compose)
    yn_fft   = ch.get("YN_RUN_FFT")
    yn_ons   = ch.get("YN_RUN_ONS")
    yn_pyin  = ch.get("YN_RUN_PYIN")
    yn_crepe = ch.get("YN_RUN_CREPE")

    # Helpful count if min/max are present
    min_f = ch.get("MIN_AUDIO_STREAM_FRAME_NO")
    max_f = ch.get("MAX_AUDIO_STREAM_FRAME_NO")
    if isinstance(min_f, int) and isinstance(max_f, int) and max_f >= min_f:
        ch.setdefault("CNT_AUDIO_FRAMES", int(max_f - min_f + 1))

    CONSOLE_LOG("STEP2", "ENTRY", {
        "RECORDING_ID": RECORDING_ID,
        "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO,
        "WAV48K_PATH": WAV48K_PATH,
        "AUDIO_CHUNK_START_MS": start_ms,
        "AUDIO_CHUNK_END_MS": end_ms,
        "COMPOSE_PLAY_OR_PRACTICE": mode,
        "YN_FFT": yn_fft, "YN_ONS": yn_ons, "YN_PYIN": yn_pyin, "YN_CREPE": yn_crepe,
    })

    # Step 0: resample
    y48, sr48, y22, sr22, y16, sr16 = _resample_for_step2(WAV48K_PATH)
    CONSOLE_LOG("STEP2", "RESAMPLE_DONE", {
        "LEN_48K": int(y48.shape[0]), "SR_48K": sr48,
        "LEN_22K": int(y22.shape[0]), "SR_22K": sr22,
        "LEN_16K": int(y16.shape[0]), "SR_16K": sr16,
        "AUDIO_CHUNK_DURATION_IN_MS": ch["AUDIO_CHUNK_DURATION_IN_MS"],
    })

    # Step 1: FFT (+ compose handling)
    run_flags = await _run_fft_if_needed(
        RECORDING_ID=RECORDING_ID,
        AUDIO_CHUNK_NO=AUDIO_CHUNK_NO,
        COMPOSE_PLAY_OR_PRACTICE=mode,
        YN_FFT=yn_fft,
        AUDIO_CHUNK_START_MS=start_ms,
        AUDIO_ARRAY_22050=y22,
        SR_22050=sr22,
    )

    # Resolve flags: for COMPOSE we might override with refreshed values (if any),
    # otherwise use the existing chunk flags.
    if mode == "COMPOSE":
        yn_ons   = run_flags.get("YN_RUN_ONS", yn_ons)
        yn_pyin  = run_flags.get("YN_RUN_PYIN", yn_pyin)
        yn_crepe = run_flags.get("YN_RUN_CREPE", yn_crepe)

    # Step 2: other processors + master
    await _run_others_and_master(
        RECORDING_ID=RECORDING_ID,
        AUDIO_CHUNK_NO=AUDIO_CHUNK_NO,
        COMPOSE_PLAY_OR_PRACTICE=mode,
        YN_ONS=yn_ons, YN_PYIN=yn_pyin, YN_CREPE=yn_crepe,
        AUDIO_ARRAY_22K=y22, SR_22K=sr22,
        AUDIO_ARRAY_16K=y16, SR_16K=sr16,
        AUDIO_CHUNK_START_MS=start_ms,
        WAV48K_PATH=WAV48K_PATH,
    )

    # Finalize per-chunk metrics & persist a single log row (AFTER P_ENGINE_ALL_MASTER)
    ch["TOTAL_PROCESSING_DURATION_IN_MS"] = int(round((time.perf_counter() - t0) * 1000))
    DB_LOG_RECORDING_AUDIO_CHUNK(RECORDING_ID, AUDIO_CHUNK_NO)

    CONSOLE_LOG("STEP2", "EXIT", {
        "RECORDING_ID": RECORDING_ID,
        "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO,
        "ELAPSED_MS": ch["TOTAL_PROCESSING_DURATION_IN_MS"],
    })
