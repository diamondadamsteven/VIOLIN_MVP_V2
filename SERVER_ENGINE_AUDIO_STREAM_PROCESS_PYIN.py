# SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN.py

from __future__ import annotations

from typing import Iterable, List, Tuple, Optional
from datetime import datetime
import numpy as np
try:
    import librosa  # type: ignore
except Exception:  # pragma: no cover
    librosa = None  # type: ignore

from SERVER_ENGINE_APP_VARIABLES import (
    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY,  # per-frame metadata (assumed to exist)
    AUDIO_FRAME_MS,
    PYIN_HOP_IN_MS,
    PYIN_OVERLAP_FOR_ACCURACY_OR_SPEED
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    CONSOLE_LOG,
    DB_CONNECT_CTX,
    DB_BULK_INSERT,
    ENGINE_DB_LOG_FUNCTIONS_INS,  # logging decorator
)

PREFIX = "PYIN"

# Row shape for ENGINE_LOAD_HZ inserts (per reading):
# (START_MS, END_MS, HZ, CONFIDENCE)
HZRow = Tuple[int, int, float, float]

# ─────────────────────────────────────────────────────────────
# DB bulk insert (frame-keyed)
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
def ENGINE_LOAD_HZ_INS(
    conn,
    RECORDING_ID: int,
    SOURCE_METHOD: str,          # e.g., "PYIN"
    AUDIO_FRAME_NO: int,
    SAMPLE_RATE: int,            # 22050 for pYIN here
    rows_abs: Iterable[HZRow],
) -> None:
    """
    ENGINE_LOAD_HZ columns:
      (RECORDING_ID, START_MS, END_MS, SOURCE_METHOD, HZ, CONFIDENCE, AUDIO_FRAME_NO, SAMPLE_RATE)
    """
    sql = """
      INSERT INTO ENGINE_LOAD_HZ
      (RECORDING_ID, START_MS, END_MS, SOURCE_METHOD, HZ, CONFIDENCE, AUDIO_FRAME_NO, SAMPLE_RATE)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    DB_BULK_INSERT(
        conn,
        sql,
        (
            (RECORDING_ID, start_ms, end_ms, SOURCE_METHOD, float(hz), float(confidence),
             AUDIO_FRAME_NO, SAMPLE_RATE)
            for (start_ms, end_ms, hz, confidence) in rows_abs
        ),
    )

# ─────────────────────────────────────────────────────────────
# pYIN core: OPTIMIZED version for speed
# ─────────────────────────────────────────────────────────────
def _pyin_relative_rows_optimized(audio_22050: np.ndarray, sample_rate: int = 22050) -> List[HZRow]:
    """
    Optimized PYIN processing with reduced frame_length for faster processing.
    Speed vs accuracy trade-off: 20ms hop, smaller frames.
    """
    if librosa is None:
        CONSOLE_LOG(PREFIX, "LIBROSA_NOT_AVAILABLE")
        return []

    if sample_rate != 22050 or not isinstance(audio_22050, np.ndarray) or audio_22050.size == 0:
        CONSOLE_LOG(PREFIX, "BAD_INPUT", {"sr": int(sample_rate), "size": int(getattr(audio_22050, "size", 0))})
        return []

    # Optimized parameters for speed vs accuracy trade-off
    # 20ms hop = faster processing, slightly less accurate
    hop_length = max(1, int(round(sample_rate * (PYIN_HOP_IN_MS / 1000))))  # 20ms hop for speed
    # frame_length = max(hop_length * 2, 1024)  # Smaller frame for speed
    if PYIN_OVERLAP_FOR_ACCURACY_OR_SPEED == "speed":
        frame_length = max(hop_length * 2, 1024)  # Smaller frame for speed
    elif PYIN_OVERLAP_FOR_ACCURACY_OR_SPEED == "accuracy":
        frame_length = max(hop_length * 4, 2048) # 75% overlap better Hz accuracy
    else:
        CONSOLE_LOG(PREFIX, "INVALID_PYIN_OVERLAP_FOR_ACCURACY_OR_SPEED", {"overlap": PYIN_OVERLAP_FOR_ACCURACY_OR_SPEED})
        return []


    # Run PYIN analysis with optimized parameters
    f0, voiced_flag, voiced_prob = librosa.pyin(
        y=audio_22050, sr=sample_rate,
        fmin=180, fmax=4000,
        frame_length=frame_length, hop_length=hop_length, center=True
    )

    rows_rel: List[HZRow] = []
    for i, (hz, voiced_ok, confidence) in enumerate(zip(f0, voiced_flag, voiced_prob)):
        start_ms_rel = int(round((i * hop_length) * 1000.0 / sample_rate))
        end_ms_rel = start_ms_rel + (PYIN_HOP_IN_MS - 1)  # 20ms span to match hop_length
        rows_rel.append((start_ms_rel, end_ms_rel, float(hz), float(confidence)))

    return rows_rel

# ─────────────────────────────────────────────────────────────
# pYIN core: relative rows @ ~10 ms on 22.05 kHz audio
# ─────────────────────────────────────────────────────────────
def _pyin_relative_rows(audio_22050: np.ndarray, sample_rate: int = 22050) -> List[HZRow]:
    """
    Returns per-frame rows relative to the provided audio buffer:
      [(start_ms_rel, end_ms_rel, hz, confidence), ...] at ~10 ms hop.
    """
    if librosa is None:
        CONSOLE_LOG(PREFIX, "LIBROSA_NOT_AVAILABLE")
        return []

    if sample_rate != 22050 or not isinstance(audio_22050, np.ndarray) or audio_22050.size == 0:
        CONSOLE_LOG(PREFIX, "BAD_INPUT", {"sr": int(sample_rate), "size": int(getattr(audio_22050, "size", 0))})
        return []

    # ~10 ms hop @ 22.05 kHz
    # 20ms hop = faster processing, slightly less accurate
    hop_length = max(1, int(round(sample_rate * (PYIN_HOP_IN_MS / 1000))))  # 20ms hop for speed
    # frame_length = max(hop_length * 2, 1024)  # Smaller frame for speed
    frame_length = max(hop_length * 4, 2048) # 75% overlap better Hz accuracy

    # fmin=180, fmax=4000,
    # Let exceptions bubble to the decorated caller (no local try/except)
    f0, voiced_flag, voiced_prob = librosa.pyin(
        y=audio_22050, sr=sample_rate,
        fmin=180, fmax=4000,
        frame_length=frame_length, hop_length=hop_length, center=True
    )

#     CONSOLE_LOG(PREFIX, "PYIN_DEBUG", {
#     "f0_count": len(f0), 
#     "voiced_count": sum(voiced_flag),
#     "valid_hz_count": sum(1 for hz in f0 if hz and np.isfinite(hz) and hz > 0)
#    })

    rows_rel: List[HZRow] = []
    for i, (hz, voiced_ok, confidence) in enumerate(zip(f0, voiced_flag, voiced_prob)):
        # if not voiced_ok or hz is None:
        #     continue
        # if not np.isfinite(hz) or hz <= 0.0:
        #     continue
        start_ms_rel = int(round((i * hop_length) * 1000.0 / sample_rate))
        end_ms_rel   = start_ms_rel + (PYIN_HOP_IN_MS - 1)  # nominal 10 ms span
        rows_rel.append((start_ms_rel, end_ms_rel, float(hz), float(confidence)))

    if rows_rel:
        starts = [s for (s, _, _, _) in rows_rel]
        mods_of_10 = sorted({s % 10 for s in starts})
        unique_steps = sorted(set(np.diff(starts))) if len(starts) > 1 else []
        CONSOLE_LOG(PREFIX, "TIMING_SUMMARY", {
            "count": len(rows_rel),
            "first_ms": starts[0],
            "last_ms": starts[-1],
            "mods_of_10": mods_of_10[:6],
            "unique_step_sizes": unique_steps[:6],
        })

    return rows_rel

# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY: per-frame PYIN
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def SERVER_ENGINE_AUDIO_STREAM_PROCESS_PYIN(
    RECORDING_ID: int,
    AUDIO_FRAME_NO: int,
    AUDIO_ARRAY_22050: np.ndarray,
) -> int:
    """
    Inputs:
      • RECORDING_ID, AUDIO_FRAME_NO
      • AUDIO_ARRAY_22050: mono float32 at 22,050 Hz
    Returns number of rows inserted.
    """
    SAMPLE_RATE = 22050

    # 100 ms per websocket frame
    START_MS = AUDIO_FRAME_MS * (AUDIO_FRAME_NO - 1)

    # Stamp start
    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_START_PYIN"] = datetime.now()

    # Validate audio
    if not isinstance(AUDIO_ARRAY_22050, np.ndarray) or AUDIO_ARRAY_22050.size == 0:
        CONSOLE_LOG(PREFIX, "EMPTY_AUDIO", {"rid": RECORDING_ID, "frame": AUDIO_FRAME_NO})
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["PYIN_RECORD_CNT"] = 0
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_PYIN"] = datetime.now()
        return 0

    # Compute relative rows then offset to absolute ms using parallel processing
    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_START_PYIN_RELATIVE_ROWS"] = datetime.now()
 
    # Use optimized synchronous PYIN processing
    print(f"PYIN_MAIN: Processing frame {AUDIO_FRAME_NO} with optimized synchronous PYIN...")
    rows_rel = _pyin_relative_rows_optimized(AUDIO_ARRAY_22050.astype(np.float32, copy=False), sample_rate=SAMPLE_RATE)
    print(f"PYIN_MAIN: Completed, got {len(rows_rel) if rows_rel else 0} rows")
 
    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_PYIN_RELATIVE_ROWS"] = datetime.now()

    if not rows_rel:
        CONSOLE_LOG(PREFIX, "NO_ROWS", {"rid": RECORDING_ID, "frame": AUDIO_FRAME_NO, "size": len(AUDIO_ARRAY_22050)})
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["PYIN_RECORD_CNT"] = 0
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_PYIN"] = datetime.now()
        return 0

    rows_abs: List[HZRow] = [
        (START_MS + start_ms_rel, START_MS + end_ms_rel, hz, confidence)
        for (start_ms_rel, end_ms_rel, hz, confidence) in rows_rel
    ]

    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["PYIN_RECORD_CNT"] = len(rows_abs)

    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_START_PYIN_ENGINE_LOAD_HZ_INS"] = datetime.now()

    with DB_CONNECT_CTX() as conn:
        ENGINE_LOAD_HZ_INS(
            conn=conn,
            RECORDING_ID=int(RECORDING_ID),
            SOURCE_METHOD="PYIN",
            AUDIO_FRAME_NO=int(AUDIO_FRAME_NO),
            SAMPLE_RATE=SAMPLE_RATE,
            rows_abs=rows_abs,
        )

    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_PYIN_ENGINE_LOAD_HZ_INS"] = datetime.now()

    CONSOLE_LOG(PREFIX, "DB_INSERT_OK", {
        "rid": RECORDING_ID,
        "frame": AUDIO_FRAME_NO,
        "row_count": len(rows_abs),
    })

    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_PYIN"] = datetime.now()
    return len(rows_abs)
