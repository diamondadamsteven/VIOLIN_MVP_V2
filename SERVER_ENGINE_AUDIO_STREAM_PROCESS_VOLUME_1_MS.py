# SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME_1_MS.py

from __future__ import annotations

from typing import Iterable, List, Tuple
from datetime import datetime
import numpy as np

try:
    import librosa  # type: ignore
except Exception:  # pragma: no cover
    librosa = None  # type: ignore

from SERVER_ENGINE_APP_VARIABLES import (
    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY,  # per-frame metadata (assumed to exist)
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    CONSOLE_LOG,
    DB_CONNECT_CTX,
    DB_BULK_INSERT,
    ENGINE_DB_LOG_FUNCTIONS_INS,  # logging decorator
)

PREFIX = "VOLUME_1_MS"

# (START_MS_ABS, VOLUME_RMS, VOLUME_DB)
Vol1Row = Tuple[int, float, float]

# ─────────────────────────────────────────────────────────────
# DB loader (frame-keyed per-ms series)
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
def ENGINE_LOAD_VOLUME_1_MS_INS(
    conn,
    RECORDING_ID: int,
    AUDIO_FRAME_NO: int,
    SAMPLE_RATE: int,            # 22050
    rows_1ms: Iterable[Vol1Row],
) -> None:
    """
    ENGINE_LOAD_VOLUME_1_MS columns:
      (RECORDING_ID, START_MS, VOLUME, VOLUME_IN_DB, AUDIO_FRAME_NO, SAMPLE_RATE)
    """
    rows_1ms = list(rows_1ms)
    if not rows_1ms:
        return

    sql = """
      INSERT INTO ENGINE_LOAD_VOLUME_1_MS
      (RECORDING_ID, START_MS, VOLUME, VOLUME_IN_DB, AUDIO_FRAME_NO, SAMPLE_RATE)
      VALUES (?, ?, ?, ?, ?, ?)
    """
    DB_BULK_INSERT(
        conn,
        sql,
        (
            (RECORDING_ID, start_ms, float(v_rms), float(v_db), AUDIO_FRAME_NO, SAMPLE_RATE)
            for (start_ms, v_rms, v_db) in rows_1ms
        ),
    )

# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY: per-frame volume (1 ms series with librosa)
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME_1_MS(
    RECORDING_ID: int,
    AUDIO_FRAME_NO: int,
    AUDIO_ARRAY_22050: np.ndarray,
) -> int:
    SAMPLE_RATE = 22050

    # 100 ms per websocket frame → absolute base timestamp
    START_MS_ABS_BASE = 100 * (AUDIO_FRAME_NO - 1)

    # Metadata: stamp start
    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_START_VOLUME_1_MS"] = datetime.now()

    # Validate dependencies/data
    if librosa is None:
        CONSOLE_LOG(PREFIX, "LIBROSA_NOT_AVAILABLE", {"rid": RECORDING_ID, "frame": AUDIO_FRAME_NO})
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["VOLUME_1_MS_RECORD_CNT"] = 0
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_VOLUME_1_MS"] = datetime.now()
        return 0
    if not isinstance(AUDIO_ARRAY_22050, np.ndarray) or AUDIO_ARRAY_22050.size == 0:
        CONSOLE_LOG(PREFIX, "BAD_INPUT", {
            "rid": RECORDING_ID,
            "frame": AUDIO_FRAME_NO,
            "samples": int(getattr(AUDIO_ARRAY_22050, "size", 0)),
        })
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["VOLUME_1_MS_RECORD_CNT"] = 0
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_VOLUME_1_MS"] = datetime.now()
        return 0

    # Ensure float32 mono
    audio = AUDIO_ARRAY_22050.astype(np.float32, copy=False)

    # 1 ms hop → ~22 samples @ 22.05 kHz; small window (~2 ms) for better temporal resolution
    hop_length = max(1, int(round(SAMPLE_RATE * 0.001)))       # ≈ 22
    frame_length = max(hop_length, 2 * hop_length)             # ≈ 44

    # librosa RMS (center=True by default, OK for short frames due to padding)
    rms = librosa.feature.rms(y=audio, frame_length=frame_length, hop_length=hop_length)[0]
    vol_db = 20.0 * np.log10(rms + 1e-6)

    # Frame times (seconds) → absolute ms (ints)
    # Using librosa’s own mapping keeps us consistent with sample-based spacing.
    times_sec = librosa.frames_to_time(np.arange(len(rms)), sr=SAMPLE_RATE, hop_length=hop_length)
    start_ms_abs = np.round(times_sec * 1000.0).astype(np.int64) + START_MS_ABS_BASE

    # Build 1 ms rows (no END_MS column in this table)
    rows_1ms: List[Vol1Row] = [
        (int(start_ms_abs[i]), float(rms[i]), float(vol_db[i]))
        for i in range(len(rms))
    ]

    # Stamp count
    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["VOLUME_1_MS_RECORD_CNT"] = len(rows_1ms)

    # Insert
    with DB_CONNECT_CTX() as conn:
        ENGINE_LOAD_VOLUME_1_MS_INS(
            conn=conn,
            RECORDING_ID=int(RECORDING_ID),
            AUDIO_FRAME_NO=int(AUDIO_FRAME_NO),
            SAMPLE_RATE=SAMPLE_RATE,
            rows_1ms=rows_1ms,
        )

    # Metadata: stamp end
    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_VOLUME_1_MS"] = datetime.now()

    CONSOLE_LOG(PREFIX, "DB_INSERT_OK", {
        "rid": int(RECORDING_ID),
        "frame": int(AUDIO_FRAME_NO),
        "rows_1ms": int(len(rows_1ms)),
        "hop_len": int(hop_length),
        "frame_len": int(frame_length),
    })

    return int(len(rows_1ms))
