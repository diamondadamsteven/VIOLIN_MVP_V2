# SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE.py

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Iterable, List, Tuple, Optional

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

from SERVER_ENGINE_APP_VARIABLES import (
    SPLIT_100_MS_AUDIO_FRAME_ARRAY,               # volatile: raw bytes only
    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY  # durable: metadata only (assumed pre-populated)
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    CONSOLE_LOG,
    DB_CONNECT_CTX,
    DB_BULK_INSERT,
    ENGINE_DB_LOG_FUNCTIONS_INS,  # logging decorator
)

PREFIX = "CREPE"

# ─────────────────────────────────────────────────────────────
# DB loader (bulk insert)
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
def ENGINE_LOAD_HZ_INS(
    conn,
    RECORDING_ID: int,
    SOURCE_METHOD: str,
    AUDIO_FRAME_NO: int,
    SAMPLE_RATE: int,
    rows_abs: Iterable[Tuple[int, int, float, float]],
) -> None:
    """
    Insert rows into ENGINE_LOAD_HZ:
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
            (RECORDING_ID, start_ms, end_ms, SOURCE_METHOD, float(hz), float(conf), AUDIO_FRAME_NO, SAMPLE_RATE)
            for (start_ms, end_ms, hz, conf) in rows_abs
        ),
    )

# ─────────────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────────────
def _pcm16_to_float32_array(pcm: Optional[bytes]) -> Optional[np.ndarray]:
    """Decode mono PCM16 (little-endian) bytes → float32 in [-1, 1]."""
    if not pcm:
        return None
    # removed local try/except; let decorator capture errors upstream
    return np.frombuffer(pcm, dtype=np.int16).astype(np.float32) / 32768.0

# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY: per-frame analyzer
# ─────────────────────────────────────────────────────────────
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE(
    RECORDING_ID: int,
    AUDIO_FRAME_NO: int,
    AUDIO_16000: Optional[np.ndarray] = None,
) -> int:
    """
    Frame-level CREPE processing.
      • Assumes ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO] already exists.
      • Uses provided AUDIO_16000 (float32, mono, 16k) if given; else decodes from WEBSOCKET_AUDIO_FRAME_ARRAY bytes (assumes PCM16@16k).
      • Updates per-frame metadata (DT_START/END_CREPE, CREPE_RECORD_CNT).
      • Bulk-inserts (START_MS, END_MS, HZ, CONFIDENCE) rows into ENGINE_LOAD_HZ.
    Returns the number of inserted rows.
    """
    SAMPLE_RATE = 16000
    HOP = 160                # 10 ms @ 16 kHz for CREPE
    ANALYSIS_HOP_MS = 10     # CREPE hop size

    START_MS = 100 * (AUDIO_FRAME_NO - 1) 

    # Stamp start
    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_START_CREPE"] = datetime.now()

    # Dependencies available?
    if torch is None or torchcrepe is None:
        CONSOLE_LOG(PREFIX, "TORCHCREPE_UNAVAILABLE_SKIP", {"rid": RECORDING_ID, "frame": AUDIO_FRAME_NO})
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["CREPE_RECORD_CNT"] = 0
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_CREPE"] = datetime.now()
        return 0

    # Get/prepare audio @16k mono float32
    if AUDIO_16000 is None:
        b = SPLIT_100_MS_AUDIO_FRAME_ARRAY.get(RECORDING_ID, {}).get(AUDIO_FRAME_NO, {}).get("AUDIO_FRAME_BYTES")
        AUDIO_16000 = _pcm16_to_float32_array(b)

    if AUDIO_16000 is None or getattr(AUDIO_16000, "size", 0) == 0:
        CONSOLE_LOG(PREFIX, "NO_AUDIO", {"rid": RECORDING_ID, "frame": AUDIO_FRAME_NO})
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["CREPE_RECORD_CNT"] = 0
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_CREPE"] = datetime.now()
        return 0

    if isinstance(AUDIO_16000, np.ndarray) and AUDIO_16000.ndim > 1:
        AUDIO_16000 = np.mean(AUDIO_16000, axis=1).astype("float32")
    else:
        AUDIO_16000 = AUDIO_16000.astype("float32", copy=False)

    # removed local try/except; let errors surface to decorator
    audio_sha1 = hashlib.sha1(AUDIO_16000.tobytes()).hexdigest()[:12]

    device = "cuda" if torch.cuda.is_available() else "cpu"
    x = torch.tensor(AUDIO_16000, dtype=torch.float32, device=device).unsqueeze(0)

    decoder_fn = getattr(torchcrepe.decode, "viterbi", None) or torchcrepe.decode.argmax
    decoder_name = getattr(decoder_fn, "__name__", str(decoder_fn))

    CONSOLE_LOG(PREFIX, "BEGIN", {
        "rid": RECORDING_ID,
        "frame": AUDIO_FRAME_NO,
        "device": device,
        "frames_approx": int(round(AUDIO_16000.shape[0] / float(HOP))),
        "audio_sha1": audio_sha1,
        "decoder": decoder_name,
    })

    with torch.no_grad():
        f0, per = torchcrepe.predict(
            x,
            sample_rate=SAMPLE_RATE,
            hop_length=HOP,
            model="full",
            decoder=decoder_fn,
            batch_size=1024,
            device=device,
            return_periodicity=True,
        )

    f0 = f0.squeeze(0).detach().cpu().numpy()
    per = per.squeeze(0).detach().cpu().numpy()
    n = int(min(len(f0), len(per)))

    rows: List[Tuple[int, int, float, float]] = []
    if n > 0:
        rel_starts = np.arange(n, dtype=np.int64) * ANALYSIS_HOP_MS
        START_MS_ARRAY = START_MS + rel_starts
        END_MS_ARRAY   = START_MS_ARRAY + (ANALYSIS_HOP_MS - 1)  # inclusive

        for i in range(n):
            hz = float(f0[i])
            conf = float(per[i])
            if not (np.isfinite(hz) and hz > 0.0):
                continue
            rows.append((int(START_MS_ARRAY[i]), int(END_MS_ARRAY[i]), hz, conf))

    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["CREPE_RECORD_CNT"] = len(rows)

    if not rows:
        CONSOLE_LOG(PREFIX, "NO_ROWS", {"rid": RECORDING_ID, "frame": AUDIO_FRAME_NO})
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_CREPE"] = datetime.now()
        return 0

    with DB_CONNECT_CTX() as conn:
        ENGINE_LOAD_HZ_INS(
            conn=conn,
            RECORDING_ID=RECORDING_ID,
            SOURCE_METHOD="CREPE",
            AUDIO_FRAME_NO=AUDIO_FRAME_NO,
            SAMPLE_RATE=SAMPLE_RATE,
            rows_abs=rows,
        )

    CONSOLE_LOG(PREFIX, "DB_INSERT_OK", {
        "rid": RECORDING_ID,
        "frame": AUDIO_FRAME_NO,
        "row_count": len(rows),
        "audio_sha1": audio_sha1,
    })

    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO]["DT_END_CREPE"] = datetime.now()
    return len(rows)
