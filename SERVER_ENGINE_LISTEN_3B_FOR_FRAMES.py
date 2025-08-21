# SERVER_ENGINE_LISTEN_3B_FOR_FRAMES.py
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from hashlib import sha256
from typing import Optional, Tuple
import io

import numpy as np
import av

# Prefer polyphase resampling; fall back to librosa; last resort: linear
try:
    from scipy.signal import resample_poly  # type: ignore
except Exception:  # pragma: no cover
    resample_poly = None
try:
    import librosa  # type: ignore
except Exception:  # pragma: no cover
    librosa = None  # type: ignore

# NEW: try libsndfile via soundfile for container decoding (AAC/M4A/WAV/CAF…)
try:
    import soundfile as sf  # type: ignore
except Exception:  # pragma: no cover
    sf = None  # type: ignore

from SERVER_ENGINE_APP_VARIABLES import (
    ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY,       # message rows
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY,   # metadata-only (no bytes)
    WEBSOCKET_AUDIO_FRAME_ARRAY,                 # volatile bytes/arrays
    TEMP_RECORDING_AUDIO_DIR,                    # for raw archive
    ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY,        # per-recording config
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    ENGINE_DB_LOG_FUNCTIONS_INS,  # Start/End/Error logger
    DB_INSERT_TABLE,              # allowlisted insert, fire_and_forget
    schedule_coro,                # loop/thread-safe scheduler
)

# ---------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------
TRANSPORT_SR = 44100         # Fallback SR when treating bytes as PCM16
FRAME_MS     = 100           # Each websocket AUDIO_FRAME_NO spans 100 ms

# ---------------------------------------------------------------------
# Audio helpers (no external deps required for core fallback path)
# ---------------------------------------------------------------------
def pcm16le_bytes_to_float32_mono(pcm: Optional[bytes]) -> Optional[np.ndarray]:
    """Decode little-endian PCM16 bytes → float32 mono in [-1, 1]."""
    if not pcm:
        return None
    # If len(pcm) isn't multiple of 2, frombuffer will raise; guard here
    if (len(pcm) % 2) != 0:
        return None
    return np.frombuffer(pcm, dtype="<i2").astype(np.float32) / 32768.0

def ensure_mono_float(x: np.ndarray) -> np.ndarray:
    """Make mono float32 from (N,), (N,1) or (N,2/+) arrays."""
    if x.ndim == 1:
        y = x.astype(np.float32, copy=False)
    else:
        # average channels
        y = np.mean(x, axis=1).astype(np.float32, copy=False)
    return y

def float32_to_pcm16le_bytes(x: np.ndarray) -> bytes:
    """Clamp float32 [-1,1] → PCM16 (little-endian) bytes."""
    x = np.clip(x, -1.0, 1.0).astype(np.float32, copy=False)
    return (x * 32767.0).astype("<i2", copy=False).tobytes()

def resample_best(x: np.ndarray, src_sr: int, dst_sr: int) -> np.ndarray:
    """
    Resample with best available method:
      1) polyphase (scipy.signal.resample_poly) with AA filtering
      2) librosa.resample('kaiser_best')
      3) linear interpolation (no AA) as a last resort
    """
    if src_sr == dst_sr:
        return x.astype(np.float32, copy=False)

    # 1) Polyphase (preferred)
    if resample_poly is not None:
        from math import gcd
        g = gcd(dst_sr, src_sr)
        up, down = dst_sr // g, src_sr // g
        return resample_poly(x.astype(np.float32, copy=False), up, down).astype(np.float32, copy=False)

    # 2) Librosa (good quality)
    if librosa is not None:
        return librosa.resample(
            x.astype(np.float32, copy=False),
            orig_sr=src_sr, target_sr=dst_sr, res_type="kaiser_best"
        ).astype(np.float32, copy=False)

    # 3) Linear fallback
    n_out = int(round(len(x) * (dst_sr / float(src_sr))))
    if n_out <= 1 or len(x) == 0:
        return np.zeros((0,), dtype=np.float32)
    xp = np.linspace(0.0, 1.0, num=len(x), endpoint=False, dtype=np.float64)
    fp = x.astype(np.float32, copy=False)
    x_new = np.linspace(0.0, 1.0, num=n_out, endpoint=False, dtype=np.float64)
    return np.interp(x_new, xp, fp).astype(np.float32, copy=False)

def decode_bytes_best_effort(pcm_or_container: Optional[bytes]) -> Tuple[Optional[np.ndarray], Optional[int], str]:
    """
    Try to decode as an actual audio file (AAC/M4A/WAV/CAF, etc) using PyAV first.
    Fall back to soundfile (libsndfile), then to raw PCM16LE@44100.
    Returns (mono_float32, sample_rate, encoding_label).
    """
    if not pcm_or_container:
        return None, None, "none"

    # 0) PyAV / FFmpeg — best coverage for AAC/M4A/CAF/etc.
    if av is not None:
        try:
            with av.open(io.BytesIO(pcm_or_container), mode="r") as container:
                # take first audio stream
                astream = next((s for s in container.streams if s.type == "audio"), None)
                if astream is not None:
                    chunks: list[np.ndarray] = []
                    src_sr = int(astream.codec_context.sample_rate or 0)
                    codec_name = (astream.codec_context.name or "unknown")

                    for packet in container.demux(astream):
                        for frame in packet.decode():
                            # Ensure we track the true SR even if stream header lacked it
                            if getattr(frame, "sample_rate", None):
                                src_sr = int(frame.sample_rate)

                            # Convert to float planar/interleaved → mono float32
                            # frame.to_ndarray(format='flt') → shape (C, N)
                            arr = frame.to_ndarray(format="flt")
                            if arr.ndim == 1:
                                y = arr.astype(np.float32, copy=False)
                            else:
                                # average channels to mono
                                y = np.mean(arr, axis=0).astype(np.float32, copy=False)
                            if y.size:
                                chunks.append(y)

                    if chunks:
                        mono = np.concatenate(chunks).astype(np.float32, copy=False)
                        if src_sr <= 0:
                            src_sr = TRANSPORT_SR
                        return mono, src_sr, f"pyav/{codec_name}"
        except Exception:
            # fall through to other decoders
            pass

    # 1) soundfile / libsndfile — works for WAV/AIFF/FLAC (not all AAC/M4A builds)
    if sf is not None:
        try:
            with sf.SoundFile(io.BytesIO(pcm_or_container)) as snd:
                data = snd.read(dtype="float32", always_2d=True)  # (N, C)
                sr = int(snd.samplerate)
                mono = ensure_mono_float(data)
                return mono, sr, f"container/{snd.format or 'unknown'}"
        except Exception:
            pass  # fall through to PCM16

    # 2) Raw PCM16LE @ TRANSPORT_SR fallback
    x = pcm16le_bytes_to_float32_mono(pcm_or_container)
    if x is None:
        return None, None, "decode_failed"
    return x, TRANSPORT_SR, "pcm16"

# ---------------------------------------------------------------------
# Scanner: queue unprocessed FRAME messages
# ---------------------------------------------------------------------
def SERVER_ENGINE_LISTEN_3B_FOR_FRAMES() -> None:
    """
    Find messages where DT_MESSAGE_PROCESS_QUEDED_TO_START is null and MESSAGE_TYPE='FRAME',
    timestamp the queueing, and schedule processing.
    """
    to_launch = []
    for MESSAGE_ID, ROW in list(ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.items()):
        if ROW.get("DT_MESSAGE_PROCESS_QUEDED_TO_START") is None and str(ROW.get("MESSAGE_TYPE", "")).upper() == "FRAME":
            to_launch.append(MESSAGE_ID)

    for MESSAGE_ID in to_launch:
        MSG = ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID)
        if MSG is None:
            continue
        MSG["DT_MESSAGE_PROCESS_QUEDED_TO_START"] = datetime.now()
        schedule_coro(PROCESS_WEBSOCKET_MESSAGE_TYPE_FRAME(MESSAGE_ID))

# ---------------------------------------------------------------------
# Worker: process a single FRAME message
# ---------------------------------------------------------------------
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def PROCESS_WEBSOCKET_MESSAGE_TYPE_FRAME(MESSAGE_ID: int) -> None:
    """
    PROCESS FRAME:
      1) Mark DT_MESSAGE_PROCESS_STARTED
      2) Persist the message row
      3) Read AUDIO_FRAME_BYTES from WEBSOCKET_AUDIO_FRAME_ARRAY (volatile)
      4) Upsert metadata-only frame row and persist
      5) Append PCM16@44.1k to raw archive
      6) Create analyzer arrays (16k & 22.05k) via high-quality resample
      7) Delete the message entry
    """
    MSG = ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID)
    if MSG is None:
        return

    # 1) mark started
    MSG["DT_MESSAGE_PROCESS_STARTED"] = datetime.now()

    # 2) persist message (allowlisted insert; DB path self-logs failures)
    DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_MESSAGE", MSG, fire_and_forget=True)

    # ---- Identify the recording/frame
    RECORDING_ID        = int(MSG.get("RECORDING_ID") or 0)
    AUDIO_FRAME_NO      = int(MSG.get("AUDIO_FRAME_NO") or 0)
    DT_MESSAGE_RECEIVED = MSG.get("DT_MESSAGE_RECEIVED")
    if not isinstance(DT_MESSAGE_RECEIVED, datetime):
        DT_MESSAGE_RECEIVED = datetime.now()

    # 3) get raw bytes from the volatile store (NOT from the message row)
    WEBSOCKET_AUDIO_FRAME_RECORD = WEBSOCKET_AUDIO_FRAME_ARRAY.setdefault(RECORDING_ID, {})
    FRAME_ENTRY = WEBSOCKET_AUDIO_FRAME_RECORD.get(AUDIO_FRAME_NO, {})
    AUDIO_FRAME_BYTES = FRAME_ENTRY.get("AUDIO_FRAME_BYTES")  # may be None if something went wrong

    # 4) upsert metadata-only row (durable) and persist
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD = ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY.setdefault(RECORDING_ID, {})
    FRAME_RECORD = ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD.get(
        AUDIO_FRAME_NO,
        {"RECORDING_ID": RECORDING_ID, "AUDIO_FRAME_NO": AUDIO_FRAME_NO},
    )

    # Absolute timing for transport frame
    START_MS = FRAME_MS * max(AUDIO_FRAME_NO - 1, 0)
    END_MS   = START_MS + (FRAME_MS - 1)
    FRAME_RECORD.setdefault("START_MS", START_MS)
    FRAME_RECORD.setdefault("END_MS", END_MS)

    # Timestamps
    FRAME_RECORD.setdefault("DT_FRAME_RECEIVED", DT_MESSAGE_RECEIVED)
    FRAME_RECORD.setdefault("DT_FRAME_PAIRED_WITH_WEBSOCKETS_METADATA", datetime.now())

    # Size/hash + *detected* encoding label
    if AUDIO_FRAME_BYTES is not None:
        FRAME_RECORD["AUDIO_FRAME_SIZE_BYTES"] = len(AUDIO_FRAME_BYTES)
        FRAME_RECORD["AUDIO_FRAME_SHA256_HEX"] = sha256(AUDIO_FRAME_BYTES).hexdigest()
        # We'll set AUDIO_FRAME_ENCODING below after decode attempt.

    # Compose-mode gating for analyzers
    if ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[RECORDING_ID]["COMPOSE_PLAY_OR_PRACTICE"] == "COMPOSE":
        FRAME_RECORD["YN_RUN_CREPE"] = "Y"
        FRAME_RECORD["YN_RUN_PYIN"]  = "Y"
        if ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[RECORDING_ID]["COMPOSE_YN_FFT"] == "Y":
            FRAME_RECORD["YN_RUN_FFT"] = "Y"
            FRAME_RECORD["YN_RUN_ONS"] = "Y"

    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD[AUDIO_FRAME_NO] = FRAME_RECORD
    DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME", FRAME_RECORD, fire_and_forget=True)

    # 5) Append to archive and 6) create analyzer arrays
    if AUDIO_FRAME_BYTES is not None:
        # NEW: robust decode → mono float32 + sample rate
        X_FLOAT, SRC_SR, enc_label = decode_bytes_best_effort(AUDIO_FRAME_BYTES)
        FRAME_RECORD["AUDIO_FRAME_ENCODING"] = enc_label
        FRAME_RECORD["DT_FRAME_DECODED_FROM_BYTES_INTO_AUDIO_SAMPLES"] = datetime.now()
        ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD[AUDIO_FRAME_NO] = FRAME_RECORD
        DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME", FRAME_RECORD, fire_and_forget=True)

        if (X_FLOAT is not None) and (SRC_SR is not None) and (X_FLOAT.size > 0):
            # Ensure 44.1k anchor for archival file
            X_441 = resample_best(X_FLOAT, SRC_SR, 44100)
            FRAME_RECORD["DT_FRAME_RESAMPLED_TO_44100"] = datetime.now()

            # 5) Append to single raw file per recording
            REC_DIR = (TEMP_RECORDING_AUDIO_DIR / str(RECORDING_ID))
            REC_DIR.mkdir(parents=True, exist_ok=True)
            RAW_PATH: Path = REC_DIR / f"recording_{RECORDING_ID}.pcm16.44100.raw"
            with RAW_PATH.open("ab") as fh:
                fh.write(float32_to_pcm16le_bytes(X_441))
            FRAME_RECORD["DT_FRAME_CONVERTED_TO_PCM16_WITH_SAMPLE_RATE_44100"] = datetime.now()
            FRAME_RECORD["DT_FRAME_APPENDED_TO_RAW_FILE"] = datetime.now()
            ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD[AUDIO_FRAME_NO] = FRAME_RECORD
            DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME", FRAME_RECORD, fire_and_forget=True)

            # 6) Analyzer arrays (float32 mono), stored only in the volatile store
            WEBSOCKET_AUDIO_FRAME_RECORD[AUDIO_FRAME_NO] = WEBSOCKET_AUDIO_FRAME_RECORD.get(AUDIO_FRAME_NO, {})

            # Keep your existing 16k path
            WEBSOCKET_AUDIO_FRAME_RECORD[AUDIO_FRAME_NO]["AUDIO_ARRAY_16000"] = resample_best(X_441, 44100, 16000)
            FRAME_RECORD["DT_FRAME_RESAMPLED_TO_16000"] = datetime.now()

            # NEW: always provide 22.05k for pYIN so later stages don’t crash
            WEBSOCKET_AUDIO_FRAME_RECORD[AUDIO_FRAME_NO]["AUDIO_ARRAY_22050"] = resample_best(X_441, 44100, 22050)
            FRAME_RECORD["DT_FRAME_RESAMPLED_22050"] = datetime.now()

            # Free the transport bytes
            WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][AUDIO_FRAME_NO].pop("AUDIO_FRAME_BYTES", None)

            ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD[AUDIO_FRAME_NO] = FRAME_RECORD
            DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME", FRAME_RECORD, fire_and_forget=True)

    # 7) remove the original message row now that we've captured bytes + meta
    del ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY[MESSAGE_ID]
