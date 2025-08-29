# SERVER_ENGINE_LISTEN_3B_FOR_FRAMES.py
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from hashlib import sha256
from typing import Optional, Tuple
import io
import time
import asyncio

import numpy as np
import av

from SERVER_ENGINE_APP_FUNCTIONS import CONSOLE_LOG

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
    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY,   # metadata-only (no bytes)
    SPLIT_100_MS_AUDIO_FRAME_ARRAY,                 # volatile bytes/arrays
    PRE_SPLIT_AUDIO_FRAME_ARRAY,
    TEMP_RECORDING_AUDIO_DIR,                    # for raw archive
    ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY,        # per-recording config
    AUDIO_BYTES_PER_FRAME,                       # frame size constants
    AUDIO_SAMPLES_PER_FRAME,                     # samples per frame
    AUDIO_SAMPLE_RATE,                           # sample rate
    AUDIO_BYTES_PER_SAMPLE,
    RECORDING_CONFIG_ARRAY,
    ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME_ARRAY
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    ENGINE_DB_LOG_FUNCTIONS_INS,  # Start/End/Error logger
    ENGINE_DB_LOG_TABLE_INS,              # allowlisted insert, fireand_forget
)
from SERVER_ENGINE_AUDIO_PROCESSING_POOL import resample_parallel, wait_for_futures

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
    return (x * 32767.0).astype("<i2", order="C").tobytes()

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
        return np.array([], dtype=np.float32)
    indices = np.linspace(0, len(x) - 1, n_out)
    return np.interp(indices, np.arange(len(x)), x).astype(np.float32, copy=False)

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
async def SERVER_ENGINE_LISTEN_3B_FOR_FRAMES() -> None:
    """
    Find messages where DT_MESSAGE_PROCESS_QUEUED_TO_START is null and MESSAGE_TYPE='FRAME',
    timestamp the queueing, and schedule processing.
    """
    # CONSOLE_LOG("SCANNER", "=== 3B_FOR_FRAMES scanner starting ===")
    MESSAGE_ID_ARRAY = []

    while True:
        MESSAGE_ID_ARRAY.clear()
        for MESSAGE_ID, ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ROW in list(ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.items()):
            if ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ROW.get("DT_MESSAGE_PROCESS_QUEUED_TO_START") is None and \
            str(ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ROW.get("MESSAGE_TYPE", "")).upper() == "FRAME" and \
            ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ROW["RECORDING_ID"]]["DT_PROCESS_WEBSOCKET_START_MESSAGE_DONE"] is not None and \
            (ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ROW["AUDIO_FRAME_NO"] == 1 or 
             ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ROW["AUDIO_FRAME_NO"] == 1 + ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD["RECORDING_ID"]]["MAX_PRE_SPLIT_AUDIO_FRAME_NO_SPLIT"]):
                MESSAGE_ID_ARRAY.append(MESSAGE_ID)

        for MESSAGE_ID in MESSAGE_ID_ARRAY:
            ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY[MESSAGE_ID]["DT_MESSAGE_PROCESS_QUEUED_TO_START"] = datetime.now()
            ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD = ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID)
            if ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD is None:
                continue
            # Create task but don't await it (runs concurrently)
            asyncio.create_task(PROCESS_WEBSOCKET_FRAME_MESSAGE(MESSAGE_ID=MESSAGE_ID))
        
        # if MESSAGE_ID_ARRAY:
        #     CONSOLE_LOG("SCANNER", f"3B_FOR_FRAMES: found {len(MESSAGE_ID_ARRAY)} FRAME messages to process")
        
        # Sleep to prevent excessive CPU usage
        await asyncio.sleep(0.1)  # 100ms delay between scans

# ---------------------------------------------------------------------
# Worker: process a single FRAME message
# ---------------------------------------------------------------------
@ENGINE_DB_LOG_FUNCTIONS_INS()
async def PROCESS_WEBSOCKET_FRAME_MESSAGE(MESSAGE_ID: int) -> None:
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
    # ✅ PERFORMANCE MONITORING: Start timing
    # CONSOLE_LOG("SCANNER", f"PROCESS_WEBSOCKET_FRAME_MESSAGE: {MESSAGE_ID}")
    start_time = time.time()
    
    ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD = ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY.get(MESSAGE_ID)
    # 1) mark started
    ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD["DT_MESSAGE_PROCESS_STARTED"] = datetime.now()

    # 2) persist message (allowlisted insert; DB path self-logs failures)
    ENGINE_DB_LOG_TABLE_INS("ENGINE_DB_LOG_WEBSOCKET_MESSAGE", ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD)

    # ---- Identify the recording/frame
    RECORDING_ID        = ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD["RECORDING_ID"]
    PRE_SPLIT_AUDIO_FRAME_NO      = ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD["AUDIO_FRAME_NO"]
    DT_MESSAGE_RECEIVED = ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD["DT_MESSAGE_RECEIVED"]
    PRE_SPLIT_AUDIO_FRAME_BYTES = PRE_SPLIT_AUDIO_FRAME_ARRAY[RECORDING_ID][PRE_SPLIT_AUDIO_FRAME_NO]["AUDIO_FRAME_BYTES"]

    # 3) get raw bytes from the volatile store (NOT from the message row)
    PRE_SPLIT_AUDIO_FRAME_RECORD = PRE_SPLIT_AUDIO_FRAME_ARRAY.setdefault(RECORDING_ID, {})
    PRE_SPLIT_AUDIO_FRAME_RECORD_2 = PRE_SPLIT_AUDIO_FRAME_RECORD.get(PRE_SPLIT_AUDIO_FRAME_NO, {})
    PRE_SPLIT_AUDIO_FRAME_BYTES = PRE_SPLIT_AUDIO_FRAME_RECORD_2.get("AUDIO_FRAME_BYTES")  # may be None if something went wrong
    PRE_SPLIT_AUDIO_FRAME_DURATION_IN_MS = (len(PRE_SPLIT_AUDIO_FRAME_BYTES) // AUDIO_BYTES_PER_SAMPLE * 1000) // AUDIO_SAMPLE_RATE
    ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME_ARRAY[RECORDING_ID][PRE_SPLIT_AUDIO_FRAME_NO]["PRE_SPLIT_AUDIO_FRAME_DURATION_IN_MS"] = PRE_SPLIT_AUDIO_FRAME_DURATION_IN_MS
   
    ENGINE_DB_LOG_RECORDING_CONFIG_RECORD = ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[RECORDING_ID]
    RECORDING_CONFIG_RECORD = RECORDING_CONFIG_ARRAY[RECORDING_ID]
    
    # Add the chunk to the buffer directly
    RECORDING_CONFIG_RECORD['AUDIO_BYTES'].extend(PRE_SPLIT_AUDIO_FRAME_BYTES)
    ENGINE_DB_LOG_RECORDING_CONFIG_RECORD['TOTAL_BYTES_RECEIVED'] += len(PRE_SPLIT_AUDIO_FRAME_BYTES)
      
    ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_RECORD = ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY.setdefault(RECORDING_ID, {})

    # Keep producing frames while we have enough data
    while len(RECORDING_CONFIG_RECORD['AUDIO_BYTES']) >= AUDIO_BYTES_PER_FRAME:
        # Extract exactly one frame
        SPLIT_100_MS_AUDIO_FRAME_BYTES = bytes(RECORDING_CONFIG_RECORD['AUDIO_BYTES'][:AUDIO_BYTES_PER_FRAME])
        RECORDING_CONFIG_RECORD['AUDIO_BYTES'] = RECORDING_CONFIG_RECORD['AUDIO_BYTES'][AUDIO_BYTES_PER_FRAME:]

        TOTAL_SPLIT_100_MS_FRAMES_PRODUCED = ENGINE_DB_LOG_RECORDING_CONFIG_RECORD['TOTAL_SPLIT_100_MS_FRAMES_PRODUCED'] or 0

        SPLIT_100_MS_AUDIO_FRAME_NO = TOTAL_SPLIT_100_MS_FRAMES_PRODUCED + 1
        
        ENGINE_DB_LOG_RECORDING_CONFIG_RECORD['TOTAL_SPLIT_100_MS_FRAMES_PRODUCED'] += 1
        
        SPLIT_100_MS_AUDIO_FRAME_START_MS = (SPLIT_100_MS_AUDIO_FRAME_NO - 1) * 100
        SPLIT_100_MS_AUDIO_FRAME_END_MS = SPLIT_100_MS_AUDIO_FRAME_START_MS + 99
        
        # Store the frame record directly in the array with all required fields
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][SPLIT_100_MS_AUDIO_FRAME_NO] = {
            "RECORDING_ID": RECORDING_ID,
            "AUDIO_FRAME_NO": SPLIT_100_MS_AUDIO_FRAME_NO,  # Time-based frame number
            "START_MS": SPLIT_100_MS_AUDIO_FRAME_START_MS,  # 100ms per frame
            "END_MS": SPLIT_100_MS_AUDIO_FRAME_END_MS,
            "AUDIO_FRAME_SIZE_BYTES": len(SPLIT_100_MS_AUDIO_FRAME_BYTES),
            "AUDIO_FRAME_SHA256_HEX": sha256(SPLIT_100_MS_AUDIO_FRAME_BYTES).hexdigest(),
            "NOTE": f"Time-based frame: {SPLIT_100_MS_AUDIO_FRAME_START_MS}-{SPLIT_100_MS_AUDIO_FRAME_END_MS}ms (from client frame {PRE_SPLIT_AUDIO_FRAME_NO})",
            # Add default analyzer flags
            "YN_RUN_FFT": "N",
            "YN_RUN_PYIN": "N", 
            "YN_RUN_CREPE": "N",
            "YN_RUN_ONS": "N",
            # Add the missing fields that Scanner 6 needs
            "DT_PROCESSING_QUEUED_TO_START": None,
            "DT_PROCESSING_START": None,
            "DT_PROCESSING_END": None
        }
        
        # Store volatile data in the separate array
        SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][SPLIT_100_MS_AUDIO_FRAME_NO] = {
            "RECORDING_ID": RECORDING_ID,
            "AUDIO_FRAME_NO": SPLIT_100_MS_AUDIO_FRAME_NO,
            "AUDIO_FRAME_BYTES": SPLIT_100_MS_AUDIO_FRAME_BYTES
        }
        
        # Compose-mode gating for analyzers
        if ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[RECORDING_ID]["COMPOSE_PLAY_OR_PRACTICE"].upper() == "COMPOSE":
            ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][SPLIT_100_MS_AUDIO_FRAME_NO]["YN_RUN_CREPE"] = "Y"
            ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][SPLIT_100_MS_AUDIO_FRAME_NO]["YN_RUN_PYIN"] = "Y"
            if ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[RECORDING_ID].get("COMPOSE_YN_RUN_FFT", "N") == "Y":
                ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][SPLIT_100_MS_AUDIO_FRAME_NO]["YN_RUN_FFT"] = "Y"
                ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][SPLIT_100_MS_AUDIO_FRAME_NO]["YN_RUN_ONS"] = "Y"
        
       
        # Process the aligned frame bytes
        X_FLOAT, SRC_SR, enc_label = decode_bytes_best_effort(SPLIT_100_MS_AUDIO_FRAME_BYTES)
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][SPLIT_100_MS_AUDIO_FRAME_NO]["AUDIO_FRAME_ENCODING"] = enc_label
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][SPLIT_100_MS_AUDIO_FRAME_NO]["DT_FRAME_DECODED_FROM_BYTES_INTO_AUDIO_SAMPLES"] = datetime.now()
        
        # Ensure 44.1k anchor for archival file using parallel processing
        X_441_future = resample_parallel(X_FLOAT, SRC_SR, 44100)
        if hasattr(X_441_future, 'result'):
            X_441 = X_441_future.result(timeout=30)
        else:
            X_441 = X_441_future  # Fallback to direct result
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][SPLIT_100_MS_AUDIO_FRAME_NO]["DT_FRAME_RESAMPLED_TO_44100"] = datetime.now()

        # 5) Append to single raw file per recording
        REC_DIR = (TEMP_RECORDING_AUDIO_DIR / str(RECORDING_ID))
        REC_DIR.mkdir(parents=True, exist_ok=True)
        RAW_PATH: Path = REC_DIR / f"recording_{RECORDING_ID}.pcm16.44100.raw"
        with RAW_PATH.open("ab") as fh:
            fh.write(float32_to_pcm16le_bytes(X_441))
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][SPLIT_100_MS_AUDIO_FRAME_NO]["DT_FRAME_CONVERTED_TO_PCM16_WITH_SAMPLE_RATE_44100"] = datetime.now()
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][SPLIT_100_MS_AUDIO_FRAME_NO]["DT_FRAME_APPENDED_TO_RAW_FILE"] = datetime.now()

        # 6) Analyzer arrays (float32 mono), stored only in the volatile store
        # Keep your existing 16k path using parallel processing
        AUDIO_ARRAY_16000_future = resample_parallel(X_441, 44100, 16000)
        if hasattr(AUDIO_ARRAY_16000_future, 'result'):
            AUDIO_ARRAY_16000 = AUDIO_ARRAY_16000_future.result(timeout=30)
        else:
            AUDIO_ARRAY_16000 = AUDIO_ARRAY_16000_future  # Fallback to direct result
        SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][SPLIT_100_MS_AUDIO_FRAME_NO]["AUDIO_ARRAY_16000"] = AUDIO_ARRAY_16000
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][SPLIT_100_MS_AUDIO_FRAME_NO]["DT_FRAME_RESAMPLED_TO_16000"] = datetime.now()

        # NEW: always provide 22.05k for pYIN so later stages don't crash using parallel processing
        AUDIO_ARRAY_22050_future = resample_parallel(X_441, 44100, 22050)
        if hasattr(AUDIO_ARRAY_22050_future, 'result'):
            AUDIO_ARRAY_22050 = AUDIO_ARRAY_22050_future.result(timeout=30)
        else:
            AUDIO_ARRAY_22050 = AUDIO_ARRAY_22050_future  # Fallback to direct result
        SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][SPLIT_100_MS_AUDIO_FRAME_NO]["AUDIO_ARRAY_22050"] = AUDIO_ARRAY_22050
        ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][SPLIT_100_MS_AUDIO_FRAME_NO]["DT_FRAME_RESAMPLED_22050"] = datetime.now()

       # Free the transport bytes
        SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][SPLIT_100_MS_AUDIO_FRAME_NO].pop("AUDIO_FRAME_BYTES", None)

        # ENGINE_DB_LOG_TABLE_INS("ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME", ENGINE_DB_LOG_SPLIT_100_MS_AUDIO_FRAME_ARRAY[RECORDING_ID][SPLIT_100_MS_AUDIO_FRAME_NO])
    
    # 7) remove the original message row now that we've captured bytes + meta
    del ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY[MESSAGE_ID]

    ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[RECORDING_ID]["MAX_PRE_SPLIT_AUDIO_FRAME_NO_SPLIT"] = PRE_SPLIT_AUDIO_FRAME_NO
    ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME_ARRAY[RECORDING_ID][PRE_SPLIT_AUDIO_FRAME_NO]["DT_FRAME_SPLIT_INTO_100_MS_FRAMES"] = datetime.now()
    ENGINE_DB_LOG_TABLE_INS("ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME", ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME_ARRAY[RECORDING_ID][PRE_SPLIT_AUDIO_FRAME_NO])

    # ✅ PERFORMANCE MONITORING: Log function execution time
    execution_time = time.time() - start_time
    

