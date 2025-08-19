# SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME_10_MS.py
# ----------------------------------------------------------------------
# Volume (10 ms series) for a single 100 ms websocket audio frame.
#   • Input: 22,050 Hz mono float32 array
#   • Compute 10 ms RMS series via librosa.feature.rms
#   • Insert into ENGINE_LOAD_VOLUME_10_MS
#   • All times are ABSOLUTE: START_MS = 100 * (AUDIO_FRAME_NO - 1)
# ----------------------------------------------------------------------

from __future__ import annotations

from typing import Iterable, List, Tuple
from datetime import datetime
import numpy as np

try:
    import librosa  # type: ignore
except Exception:  # pragma: no cover
    librosa = None  # type: ignore

from SERVER_ENGINE_APP_VARIABLES import (
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY,  # per-frame metadata (assumed to exist)
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    CONSOLE_LOG,
    DB_CONNECT_CTX,
    DB_BULK_INSERT,
    ENGINE_DB_LOG_FUNCTIONS_INS,  # logging decorator
)

PREFIX = "VOLUME_10_MS"

# (START_MS, END_MS, VOLUME_RMS, VOLUME_DB)
Vol10Row = Tuple[int, int, float, float]

# ─────────────────────────────────────────────────────────────
# DB loader (frame-keyed)
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
def ENGINE_LOAD_VOLUME_10_MS_INS(
    conn,
    RECORDING_ID: int,
    AUDIO_FRAME_NO: int,
    SAMPLE_RATE: int,            # 22050
    rows_10ms: Iterable[Vol10Row],
) -> None:
    """
    ENGINE_LOAD_VOLUME_10_MS columns:
      (RECORDING_ID, START_MS, END_MS, VOLUME, VOLUME_IN_DB, AUDIO_FRAME_NO, SAMPLE_RATE)
    """
    rows_10ms = list(rows_10ms)
    if not rows_10ms:
        return
    sql = """
      INSERT INTO ENGINE_LOAD_VOLUME_10_MS
      (RECORDING_ID, START_MS, END_MS, VOLUME, VOLUME_IN_DB, AUDIO_FRAME_NO, SAMPLE_RATE)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    DB_BULK_INSERT(
        conn,
        sql,
        (
            (RECORDING_ID, start_ms, end_ms, float(v_rms), float(v_db), AUDIO_FRAME_NO, SAMPLE_RATE)
            for (start_ms, end_ms, v_rms, v_db) in rows_10ms
        ),
    )

# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY: per-frame volume (10 ms via librosa)
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def SERVER_ENGINE_AUDIO_STREAM_PROCESS_VOLUME_10_MS(
    RECORDING_ID: int,
    AUDIO_FRAME_NO: int,
    AUDIO_ARRAY_22050: np.ndarray,
) -> int:
    """
    Inputs:
      • RECORDING_ID, AUDIO_FRAME_NO
      • AUDIO_ARRAY_22050: mono float32 at 22,050 Hz
    Returns: number of 10 ms rows inserted.
    """
    SAMPLE_RATE = 22050
    HOP_MS = 10
    HOP_LENGTH = int((HOP_MS / 1000.0) * SAMPLE_RATE)   # ≈ 220
    FRAME_LENGTH = HOP_LENGTH * 2                        # ≈ 440

    # 100 ms per websocket frame
    START_MS = 100 * (AUDIO_FRAME_NO - 1)

    # Stamp start
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_START_VOLUME_10_MS"] = datetime.now()

    # Validate librosa + audio
    if librosa is None:
        CONSOLE_LOG(PREFIX, "LIBROSA_NOT_AVAILABLE", {"rid": RECORDING_ID, "frame": AUDIO_FRAME_NO})
        ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["VOLUME_10_MS_RECORD_CNT"] = 0
        ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_VOLUME_10_MS"] = datetime.now()
        return 0
    if not isinstance(AUDIO_ARRAY_22050, np.ndarray) or AUDIO_ARRAY_22050.size == 0:
        CONSOLE_LOG(PREFIX, "BAD_INPUT", {
            "rid": RECORDING_ID,
            "frame": AUDIO_FRAME_NO,
            "samples": int(getattr(AUDIO_ARRAY_22050, "size", 0)),
        })
        ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["VOLUME_10_MS_RECORD_CNT"] = 0
        ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_VOLUME_10_MS"] = datetime.now()
        return 0

    # Ensure float32 mono
    audio = AUDIO_ARRAY_22050.astype(np.float32, copy=False)

    # librosa RMS @ 10 ms hop; small window (~20 ms) for stability
    rms = librosa.feature.rms(y=audio, frame_length=FRAME_LENGTH, hop_length=HOP_LENGTH)[0]
    volume_db = 20.0 * np.log10(rms + 1e-6)

    # Frame-aligned absolute times (use simple i*10ms to mirror your sample)
    n = int(len(rms))
    rows_10ms: List[Vol10Row] = []
    for i in range(n):
        start_ms = START_MS + i * HOP_MS
        end_ms = start_ms + (HOP_MS - 1)  # inclusive span
        rows_10ms.append((int(start_ms), int(end_ms), float(rms[i]), float(volume_db[i])))

    # Stamp count
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["VOLUME_10_MS_RECORD_CNT"] = len(rows_10ms)

    # Insert
    with DB_CONNECT_CTX() as conn:
        ENGINE_LOAD_VOLUME_10_MS_INS(
            conn=conn,
            RECORDING_ID=int(RECORDING_ID),
            AUDIO_FRAME_NO=int(AUDIO_FRAME_NO),
            SAMPLE_RATE=SAMPLE_RATE,
            rows_10ms=rows_10ms,
        )

    # Stamp end
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_VOLUME_10_MS"] = datetime.now()

    CONSOLE_LOG(PREFIX, "DB_INSERT_OK", {
        "rid": int(RECORDING_ID),
        "frame": int(AUDIO_FRAME_NO),
        "rows_10ms": int(len(rows_10ms)),
        "hop_len": int(HOP_LENGTH),
        "frame_len": int(FRAME_LENGTH),
    })

    return int(len(rows_10ms))
