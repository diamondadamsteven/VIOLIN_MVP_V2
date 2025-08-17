# SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE.py
# ----------------------------------------------------------------------
# CREPE (torchcrepe) processing for a single audio chunk.
#   • Input: 16 kHz mono float32 audio (from Step-2), plus AUDIO_CHUNK_START_MS
#   • Run torchcrepe → per-10ms f0 + periodicity (confidence)
#   • Convert RELATIVE times to ABSOLUTE using AUDIO_CHUNK_START_MS
#   • Bulk insert rows into ENGINE_LOAD_HZ with SOURCE_METHOD='CREPE'
# ----------------------------------------------------------------------

from __future__ import annotations

import hashlib
import traceback
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

from SERVER_ENGINE_APP_VARIABLES import (
    RECORDING_AUDIO_CHUNK_ARRAY,  # <-- added
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    CONSOLE_LOG,
    DB_CONNECT_CTX,
    DB_BULK_INSERT,
    DB_LOG_FUNCTIONS,  # <-- logging decorator
)

PREFIX = "CREPE"


# ─────────────────────────────────────────────────────────────
# DB loader (bulk insert)
# ─────────────────────────────────────────────────────────────
@DB_LOG_FUNCTIONS()
def _db_load_hz_series(
    conn,
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    SOURCE_METHOD: str,
    HZ_SERIES_ARRAY: Iterable[Tuple[int, int, float, float]],
) -> None:
    """
    Insert rows into ENGINE_LOAD_HZ:
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS, SOURCE_METHOD, HZ, CONFIDENCE)
    """
    sql = """
      INSERT INTO ENGINE_LOAD_HZ
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS, SOURCE_METHOD, HZ, CONFIDENCE)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    DB_BULK_INSERT(
        conn,
        sql,
        (
            (RECORDING_ID, AUDIO_CHUNK_NO, s, e, SOURCE_METHOD, float(hz), float(conf))
            for (s, e, hz, conf) in HZ_SERIES_ARRAY
        ),
    )


# ─────────────────────────────────────────────────────────────
# CREPE core (relative series @ 10 ms hop)
# ─────────────────────────────────────────────────────────────
@DB_LOG_FUNCTIONS()
def _crepe_compute_relative_series(audio_16k: np.ndarray, sr: int = 16000) -> List[Tuple[int, int, float, float]]:
    """
    Returns per-frame rows relative to the chunk:
      [(START_MS_REL, END_MS_REL, HZ, CONFIDENCE), ...]
    Uses hop_length=160 (10 ms @ 16 kHz) and viterbi decoder if available.
    """
    if torch is None or torchcrepe is None:
        CONSOLE_LOG(PREFIX, "TORCHCREPE_NOT_AVAILABLE")
        return []
    if audio_16k is None or getattr(audio_16k, "size", 0) == 0:
        return []
    if sr != 16000:
        CONSOLE_LOG(PREFIX, "BAD_INPUT_SAMPLE_RATE", {"sr": sr, "expected": 16000})
        return []

    # Ensure mono float32
    if isinstance(audio_16k, np.ndarray) and audio_16k.ndim > 1:
        audio_16k = np.mean(audio_16k, axis=1).astype("float32")
    else:
        audio_16k = audio_16k.astype("float32", copy=False)

    # Fingerprint of audio (debug)
    try:
        sha1 = hashlib.sha1(audio_16k.tobytes()).hexdigest()[:12]
    except Exception:
        sha1 = "sha1_err"

    device = "cuda" if torch.cuda.is_available() else "cpu"
    x = torch.tensor(audio_16k, dtype=torch.float32, device=device).unsqueeze(0)

    hop = 160  # 10 ms @ 16k
    decoder_fn = getattr(torchcrepe.decode, "viterbi", None) or torchcrepe.decode.argmax
    decoder_name = getattr(decoder_fn, "__name__", str(decoder_fn))

    CONSOLE_LOG(PREFIX, "CREPE_BEGIN", {
        "device": device,
        "frames_approx": int(round(audio_16k.shape[0] / float(hop))),
        "audio_sha1": sha1,
        "decoder": decoder_name,
    })

    with torch.no_grad():
        f0, per = torchcrepe.predict(
            x,
            sample_rate=sr,
            hop_length=hop,
            model="full",
            decoder=decoder_fn,
            batch_size=1024,
            device=device,
            return_periodicity=True,
        )

    f0 = f0.squeeze(0).detach().cpu().numpy()
    per = per.squeeze(0).detach().cpu().numpy()
    n = int(min(len(f0), len(per)))

    # Vectorized frame start times (ms, relative within chunk)
    start_ms = np.round(np.arange(n, dtype=np.float64) * hop * 1000.0 / sr).astype(np.int64)

    rows: List[Tuple[int, int, float, float]] = []
    for i in range(n):
        hz = float(f0[i])
        conf = float(per[i])
        if not (np.isfinite(hz) and hz > 0.0):
            continue
        s_rel = int(start_ms[i])
        e_rel = s_rel + 9
        rows.append((s_rel, e_rel, hz, conf))

    if rows:
        CONSOLE_LOG(PREFIX, "CREPE_RELATIVE_SERIES", {
            "count": len(rows),
            "first_ms": rows[0][0],
            "last_ms": rows[-1][0],
            "audio_sha1": sha1,
        })

    return rows


# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY (called by Step-2)
# ─────────────────────────────────────────────────────────────
@DB_LOG_FUNCTIONS()
def SERVER_ENGINE_AUDIO_STREAM_PROCESS_CREPE(
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    AUDIO_CHUNK_START_MS: int,
    AUDIO_ARRAY_16000: np.ndarray,
    SAMPLE_RATE_16000: int,
) -> None:
    """
    Called by Step-2 when YN_CREPE='Y'.

    Behavior:
      • Run torchcrepe → relative (start_ms, end_ms, hz, conf)
      • Offset to ABSOLUTE times using AUDIO_CHUNK_START_MS
      • Bulk-insert rows into ENGINE_LOAD_HZ with SOURCE_METHOD='CREPE'
    """
    try:
        if torch is None or torchcrepe is None:
            CONSOLE_LOG(PREFIX, "TORCHCREPE_UNAVAILABLE_SKIP")
            # still stamp zero count for completeness
            chunks = RECORDING_AUDIO_CHUNK_ARRAY.setdefault(int(RECORDING_ID), {})
            ch = chunks.setdefault(int(AUDIO_CHUNK_NO), {"RECORDING_ID": int(RECORDING_ID), "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO)})
            ch["CREPE_RECORD_CNT"] = 0
            return

        CONSOLE_LOG(PREFIX, "PROCESS_BEGIN", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
            "START_MS": int(AUDIO_CHUNK_START_MS),
            "LEN_16K": int(AUDIO_ARRAY_16000.shape[0]) if hasattr(AUDIO_ARRAY_16000, "shape") else None,
            "SR_16K": int(SAMPLE_RATE_16000),
        })

        rel_rows = _crepe_compute_relative_series(AUDIO_ARRAY_16000, sr=int(SAMPLE_RATE_16000))

        # Stamp record count in shared in-memory state (even if zero)
        chunks = RECORDING_AUDIO_CHUNK_ARRAY.setdefault(int(RECORDING_ID), {})
        ch = chunks.setdefault(int(AUDIO_CHUNK_NO), {"RECORDING_ID": int(RECORDING_ID), "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO)})
        ch["CREPE_RECORD_CNT"] = int(len(rel_rows))

        if not rel_rows:
            CONSOLE_LOG(PREFIX, "NO_ROWS")
            return

        base = int(AUDIO_CHUNK_START_MS)
        abs_rows: List[Tuple[int, int, float, float]] = [
            (base + s_rel, base + e_rel, hz, conf) for (s_rel, e_rel, hz, conf) in rel_rows
        ]

        with DB_CONNECT_CTX() as conn:
            _db_load_hz_series(
                conn=conn,
                RECORDING_ID=int(RECORDING_ID),
                AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
                SOURCE_METHOD="CREPE",
                HZ_SERIES_ARRAY=abs_rows,
            )

        CONSOLE_LOG(PREFIX, "DB_INSERT_OK", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
            "ROW_COUNT": len(abs_rows),
        })

    except Exception as exc:
        CONSOLE_LOG(PREFIX, "FATAL_ERROR", {
            "ERROR": _bi.str(exc),
            "TRACE": traceback.format_exc(),
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
        })
