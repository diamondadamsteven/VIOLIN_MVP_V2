# SERVER_ENGINE_LISTEN_3B_FOR_FRAMES.py
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from hashlib import sha256
from typing import Optional, Tuple
import io
import time

import numpy as np
import av

from SERVER_ENGINE_APP_FUNCTIONS import CONSOLE_LOG
from SERVER_ENGINE_AUDIO_FRAME_ALIGNMENT import process_audio_chunk, flush_recording_audio

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
    DB_INSERT_TABLE_BULK,         # allowlisted bulk insert, fire_and_forget
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
    # ✅ PERFORMANCE MONITORING: Start timing
    start_time = time.time()
    
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

    # 4) Initialize the metadata structure for this recording
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD = ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY.setdefault(RECORDING_ID, {})

    # 5) NEW: Use audio frame alignment system to process the chunk
    if AUDIO_FRAME_BYTES is not None:
        # Process the audio chunk through the alignment system
        complete_frames = process_audio_chunk(RECORDING_ID, AUDIO_FRAME_NO, AUDIO_FRAME_BYTES)
        
        # ✅ PERFORMANCE OPTIMIZATION: Collect frames for batch insert
        frames_to_insert = []
        
        # Process each complete frame that was produced
        for aligned_frame_no, aligned_frame_bytes in complete_frames:
            CONSOLE_LOG("FRAME_3B", "processing_aligned_frame", {
                "rid": RECORDING_ID,
                "client_frame": AUDIO_FRAME_NO,
                "aligned_frame": aligned_frame_no,
                "time_range": f"{aligned_frame_no * 100}-{(aligned_frame_no * 100) + 99}ms",
                "aligned_bytes": len(aligned_frame_bytes),
                "aligned_samples": len(aligned_frame_bytes) // 2,  # PCM16 = 2 bytes per sample
                "aligned_duration_ms": (len(aligned_frame_bytes) // 2 * 1000) // 44100
            })
            
            # Create frame record for the aligned frame
            # aligned_frame_no is now time-based: Frame 0 = 0-99ms, Frame 1 = 100-199ms, etc.
            start_ms = aligned_frame_no * 100
            end_ms = (aligned_frame_no * 100) + 99
            
            aligned_frame_record = {
                "RECORDING_ID": RECORDING_ID,
                "AUDIO_FRAME_NO": aligned_frame_no,  # Time-based frame number
                "START_MS": start_ms,  # 100ms per frame
                "END_MS": end_ms,
                "DT_FRAME_RECEIVED": DT_MESSAGE_RECEIVED,
                "DT_FRAME_PAIRED_WITH_WEBSOCKETS_METADATA": datetime.now(),
                "AUDIO_FRAME_SIZE_BYTES": len(aligned_frame_bytes),
                "AUDIO_FRAME_SHA256_HEX": sha256(aligned_frame_bytes).hexdigest(),
                "NOTE": f"Time-based frame: {start_ms}-{end_ms}ms (from client frame {AUDIO_FRAME_NO})"
            }
            
            # Compose-mode gating for analyzers
            if ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[RECORDING_ID]["COMPOSE_PLAY_OR_PRACTICE"] == "COMPOSE":
                aligned_frame_record["YN_RUN_CREPE"] = "Y"
                aligned_frame_record["YN_RUN_PYIN"] = "Y"
                if ENGINE_DB_LOG_RECORDING_CONFIG_ARRAY[RECORDING_ID]["COMPOSE_YN_FFT"] == "Y":
                    aligned_frame_record["YN_RUN_FFT"] = "Y"
                    aligned_frame_record["YN_RUN_ONS"] = "Y"
            
            # Store the aligned frame record
            ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD[aligned_frame_no] = aligned_frame_record
            # ✅ PERFORMANCE OPTIMIZATION: Collect for batch insert instead of individual inserts
            frames_to_insert.append(aligned_frame_record)
            
            # Process the aligned frame bytes
            X_FLOAT, SRC_SR, enc_label = decode_bytes_best_effort(aligned_frame_bytes)
            aligned_frame_record["AUDIO_FRAME_ENCODING"] = enc_label
            aligned_frame_record["DT_FRAME_DECODED_FROM_BYTES_INTO_AUDIO_SAMPLES"] = datetime.now()
            
            # CRITICAL FIX: Always create audio arrays, even if decode fails
            # This ensures STAGE6_FRAMES doesn't crash when trying to access missing arrays
            if RECORDING_ID not in WEBSOCKET_AUDIO_FRAME_ARRAY:
                WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID] = {}
            if aligned_frame_no not in WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID]:
                WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][aligned_frame_no] = {}
            
            # DIAGNOSTIC: Log decode results for each frame
            CONSOLE_LOG("FRAME_3B", "debug_decode_result", {
                "rid": RECORDING_ID,
                "frame": aligned_frame_no,
                "X_FLOAT_exists": X_FLOAT is not None,
                "X_FLOAT_size": X_FLOAT.size if X_FLOAT is not None else 0,
                "SRC_SR": SRC_SR,
                "enc_label": enc_label,
                "aligned_bytes": len(aligned_frame_bytes)
            })
            
            # CRITICAL FIX: Create deep copies to prevent reference sharing corruption
            # Store a copy in the metadata structure
            ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD[aligned_frame_no] = aligned_frame_record.copy()
            # ✅ PERFORMANCE OPTIMIZATION: Update the frame in batch collection
            # Find and update the existing frame in frames_to_insert with a copy
            for i, frame in enumerate(frames_to_insert):
                if frame["AUDIO_FRAME_NO"] == aligned_frame_no:
                    frames_to_insert[i] = aligned_frame_record.copy()
                    break
            
            # 6) Analyzer arrays (float32 mono), stored only in the volatile store
            # Store in WEBSOCKET_AUDIO_FRAME_ARRAY for STAGE6_FRAMES to access
            # CRITICAL FIX: Always create audio arrays, regardless of decode success
            if (X_FLOAT is not None) and (SRC_SR is not None) and (X_FLOAT.size > 0):
                # Ensure 44.1k anchor for archival file
                X_441 = resample_best(X_FLOAT, SRC_SR, 44100)
                aligned_frame_record["DT_FRAME_RESAMPLED_TO_44100"] = datetime.now()
                
                # 5) Append to single raw file per recording
                REC_DIR = (TEMP_RECORDING_AUDIO_DIR / str(RECORDING_ID))
                REC_DIR.mkdir(parents=True, exist_ok=True)
                RAW_PATH: Path = REC_DIR / f"recording_{RECORDING_ID}.pcm16.44100.raw"
                with RAW_PATH.open("ab") as fh:
                    fh.write(float32_to_pcm16le_bytes(X_441))
                aligned_frame_record["DT_FRAME_CONVERTED_TO_PCM16_WITH_SAMPLE_RATE_44100"] = datetime.now()
                aligned_frame_record["DT_FRAME_APPENDED_TO_RAW_FILE"] = datetime.now()
                
                # Create real audio arrays from decoded audio
                WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][aligned_frame_no]["AUDIO_ARRAY_16000"] = resample_best(X_441, 44100, 16000)
                aligned_frame_record["DT_FRAME_RESAMPLED_TO_16000"] = datetime.now()
                
                WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][aligned_frame_no]["AUDIO_ARRAY_22050"] = resample_best(X_441, 44100, 22050)
                aligned_frame_record["DT_FRAME_RESAMPLED_22050"] = datetime.now()
                
                CONSOLE_LOG("FRAME_3B", "debug_audio_arrays_created", {
                    "rid": RECORDING_ID,
                    "frame": aligned_frame_no,
                    "note": "Created real audio arrays from decoded audio",
                    "created_22050": "AUDIO_ARRAY_22050" in WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][aligned_frame_no],
                    "created_16000": "AUDIO_ARRAY_16000" in WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][aligned_frame_no],
                    "frame_keys": list(WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][aligned_frame_no].keys())
                })
            else:
                # DIAGNOSTIC: Log why we couldn't create real audio arrays
                CONSOLE_LOG("FRAME_3B", "debug_audio_arrays_failed", {
                    "rid": RECORDING_ID,
                    "frame": aligned_frame_no,
                    "X_FLOAT_exists": X_FLOAT is not None,
                    "X_FLOAT_size": X_FLOAT.size if X_FLOAT is not None else 0,
                    "SRC_SR": SRC_SR,
                    "reason": "Decode failed or empty audio"
                })
                
                # Create placeholder arrays with zeros to prevent KeyError
                # Use 100ms of silence at 16kHz and 22.05kHz
                placeholder_16k = np.zeros(1600, dtype=np.float32)  # 100ms * 16kHz = 1600 samples
                placeholder_22k = np.zeros(2205, dtype=np.float32)  # 100ms * 22.05kHz = 2205 samples
                
                WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][aligned_frame_no]["AUDIO_ARRAY_16000"] = placeholder_16k
                WEBSOCKET_AUDIO_FRAME_ARRAY[RECORDING_ID][aligned_frame_no]["AUDIO_ARRAY_22050"] = placeholder_22k
                
                CONSOLE_LOG("FRAME_3B", "debug_placeholder_arrays_created", {
                    "rid": RECORDING_ID,
                    "frame": aligned_frame_no,
                    "note": "Created placeholder arrays due to decode failure",
                    "placeholder_16k_size": placeholder_16k.size,
                    "placeholder_22k_size": placeholder_22k.size
                })
            
            # CRITICAL FIX: Create deep copies to prevent reference sharing corruption
            # Store a copy in the metadata structure
            ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_RECORD[aligned_frame_no] = aligned_frame_record.copy()
            # ✅ PERFORMANCE OPTIMIZATION: Update the frame in batch collection
            # Find and update the existing frame in frames_to_insert with a copy
            for i, frame in enumerate(frames_to_insert):
                if frame["AUDIO_FRAME_NO"] == aligned_frame_no:
                    frames_to_insert[i] = aligned_frame_record.copy()
                    break
        
        # ✅ PERFORMANCE OPTIMIZATION: Batch insert all frames at once
        if frames_to_insert:
            CONSOLE_LOG("FRAME_3B", "batch_insert_frames", {
                "rid": RECORDING_ID,
                "frames_count": len(frames_to_insert),
                "note": "Using batch insert for performance"
            })
            DB_INSERT_TABLE_BULK("ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME", frames_to_insert, fire_and_forget=True)
        
        # CRITICAL FIX: Don't corrupt the aligned frame data!
        # The AUDIO_FRAME_BYTES cleanup was happening in the wrong location
        # and was overwriting the audio arrays we just created
        
    else:
        # DIAGNOSTIC: Log why we have no audio bytes
        CONSOLE_LOG("FRAME_3B", "debug_no_audio_bytes", {
            "rid": RECORDING_ID,
            "frame": AUDIO_FRAME_NO,
            "FRAME_ENTRY_keys": list(FRAME_ENTRY.keys()) if FRAME_ENTRY else [],
            "WEBSOCKET_AUDIO_FRAME_RECORD_keys": list(WEBSOCKET_AUDIO_FRAME_RECORD.keys()),
            "reason": "No AUDIO_FRAME_BYTES found in frame entry"
        })

    # 7) remove the original message row now that we've captured bytes + meta
    del ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY[MESSAGE_ID]
    
    # ✅ PERFORMANCE MONITORING: Log function execution time
    execution_time = time.time() - start_time
    if execution_time > 0.1:  # Log if takes more than 100ms
        CONSOLE_LOG("FRAME_3B", "performance_warning", {
            "rid": RECORDING_ID,
            "frame": AUDIO_FRAME_NO,
            "execution_time_ms": round(execution_time * 1000, 1),
            "frames_processed": len(frames_to_insert) if 'frames_to_insert' in locals() else 0,
            "note": "Function execution time exceeded 100ms"
        })
    

