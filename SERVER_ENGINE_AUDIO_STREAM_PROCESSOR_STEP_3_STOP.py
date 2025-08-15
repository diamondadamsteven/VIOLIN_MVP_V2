# SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_3_STOP.py
# ----------------------------------------------------------------------
# Step-3 of the streaming pipeline (finalize recording):
#   - Read AUDIO_STREAM_FILE_NAME from in-memory RECORDING_CONFIG_ARRAY
#   - Find all per-chunk 48k WAVs under TEMP_RECORDING_AUDIO_DIR/<RECORDING_ID>, ordered
#   - Concatenate in order -> write final 48k PCM WAV into RECORDING_AUDIO_DIR
#   - Call P_ENGINE_RECORD_END
# ----------------------------------------------------------------------

from __future__ import annotations

import re
import traceback
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import builtins as _bi
import numpy as np
import soundfile as sf

try:
    import librosa  # for safety if any chunk isn’t 48k
except Exception:  # pragma: no cover
    librosa = None

from SERVER_ENGINE_APP_VARIABLES import (
    TEMP_RECORDING_AUDIO_DIR,
    RECORDING_AUDIO_DIR,
    RECORDING_CONFIG_ARRAY,
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    CONSOLE_LOG,
    DB_CONNECT,
    DB_EXEC_SP_NO_RESULT,
)

PREFIX = "STEP_3_STOP"


# ─────────────────────────────────────────────────────────────
# Chunk discovery
# ─────────────────────────────────────────────────────────────
def _extract_chunk_no(path: Path) -> Tuple[int, str]:
    """
    Pull a number like `chunk_000123` or `_000123` from the stem for ordering.
    Unknowns get a big number so they sort last (but still stably by name).
    """
    name = path.stem
    m = re.search(r"(?:chunk_)?(\d{3,})", name)
    if m:
        try:
            return int(m.group(1)), name
        except Exception:
            pass
    return 10**9, name


def list_chunk_wavs_in_order(RECORDING_ID: int, final_basename: str) -> List[Path]:
    """
    Scan TEMP_RECORDING_AUDIO_DIR/<RID> (recursively) for '*_48k.wav'.
    Sort by detected chunk number, else by name.
    """
    rec_dir = TEMP_RECORDING_AUDIO_DIR / str(RECORDING_ID)
    if not rec_dir.exists():
        return []

    candidates = [p for p in rec_dir.rglob("*_48k.wav") if p.name != final_basename]
    candidates.sort(key=_extract_chunk_no)
    return candidates


# ─────────────────────────────────────────────────────────────
# Concatenation & write
# ─────────────────────────────────────────────────────────────
def concatenate_and_write_final_wav(
    RECORDING_ID: int,
    AUDIO_STREAM_FILE_NAME: str,
) -> Path:
    """
    Concatenate per-chunk 48k WAVs (float32) and write a single 48k PCM WAV
    named AUDIO_STREAM_FILE_NAME under RECORDING_AUDIO_DIR.
    """
    final_path = (RECORDING_AUDIO_DIR / AUDIO_STREAM_FILE_NAME).resolve()
    final_path.parent.mkdir(parents=True, exist_ok=True)

    chunk_paths = list_chunk_wavs_in_order(RECORDING_ID, final_path.name)
    CONSOLE_LOG(PREFIX, "FINALIZE_FIND_CHUNKS", {"RECORDING_ID": RECORDING_ID, "COUNT": len(chunk_paths)})

    if not chunk_paths:
        CONSOLE_LOG(PREFIX, "NO_CHUNK_WAVS_FOUND", {"RECORDING_ID": RECORDING_ID})
        # Create a tiny silent WAV so callers have a file to reference
        silent = np.zeros(1, dtype="float32")
        sf.write(final_path, silent, 48000, subtype="PCM_16")
        return final_path

    buffers: List[np.ndarray] = []
    for p in chunk_paths:
        try:
            y, sr = sf.read(p, dtype="float32", always_2d=False)
            if isinstance(y, np.ndarray) and y.ndim > 1:
                y = np.mean(y, axis=1).astype("float32")  # mix to mono
            if sr != 48000:
                CONSOLE_LOG(PREFIX, "WARN_CHUNK_NOT_48K_RESAMPLING", {"PATH": str(p), "SR": sr})
                if librosa is None:
                    CONSOLE_LOG(PREFIX, "LIBROSA_UNAVAILABLE_SKIP_MISMATCHED_CHUNK", {"PATH": str(p)})
                    continue
                y = librosa.resample(y, orig_sr=sr, target_sr=48000, res_type="kaiser_fast").astype("float32")
            buffers.append(y)
        except Exception as exc:
            CONSOLE_LOG(PREFIX, "READ_CHUNK_ERROR_SKIP", {"PATH": str(p), "ERROR": _bi.str(exc)})

    if not buffers:
        CONSOLE_LOG(PREFIX, "NO_VALID_BUFFERS_AFTER_READ", {"RECORDING_ID": RECORDING_ID})
        silent = np.zeros(1, dtype="float32")
        sf.write(final_path, silent, 48000, subtype="PCM_16")
        return final_path

    concat = np.concatenate(buffers, axis=0).astype("float32")
    sf.write(final_path, concat, 48000, subtype="PCM_16")
    CONSOLE_LOG(PREFIX, "FINAL_WAV_WRITTEN", {"PATH": str(final_path), "SAMPLES": int(concat.shape[0])})
    return final_path


# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY
# ─────────────────────────────────────────────────────────────
async def SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_3_STOP(RECORDING_ID: int) -> None:
    """
    Build the final 48k WAV and notify the DB that recording has ended.
    """
    try:
        cfg = RECORDING_CONFIG_ARRAY.get(RECORDING_ID, {})
        audio_stream_file_name = _bi.str(cfg.get("AUDIO_STREAM_FILE_NAME") or "").strip()
        if not audio_stream_file_name:
            # Fallback: deterministic name if the config didn’t provide one
            audio_stream_file_name = f"{RECORDING_ID}.wav"
            CONSOLE_LOG(PREFIX, "AUDIO_STREAM_FILE_NAME_MISSING_USING_FALLBACK", {
                "RECORDING_ID": RECORDING_ID,
                "FALLBACK": audio_stream_file_name,
            })

        # Concatenate all per-chunk WAVs into final WAV (under RECORDING_AUDIO_DIR)
        final_path = concatenate_and_write_final_wav(RECORDING_ID, audio_stream_file_name)

        # Call P_ENGINE_RECORD_END
        try:
            with DB_CONNECT() as conn:
                DB_EXEC_SP_NO_RESULT(
                    conn,
                    "P_ENGINE_RECORD_END",
                    RECORDING_ID=int(RECORDING_ID),
                    AUDIO_STREAM_FILE_NAME=str(audio_stream_file_name),
                )
            CONSOLE_LOG(PREFIX, "P_ENGINE_RECORD_END_CALLED", {
                "RECORDING_ID": RECORDING_ID,
                "AUDIO_STREAM_FILE_NAME": audio_stream_file_name,
                "FINAL_WAV": str(final_path),
            })
        except Exception as exc:
            CONSOLE_LOG(PREFIX, "SP_P_ENGINE_RECORD_END_ERROR", {"ERROR": _bi.str(exc), "TRACE": traceback.format_exc()})

    except Exception as exc:
        CONSOLE_LOG(PREFIX, "FATAL_STOP_ERROR", {"ERROR": _bi.str(exc), "TRACE": traceback.format_exc()})
