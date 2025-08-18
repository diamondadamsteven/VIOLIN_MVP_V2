# SERVER_ENGINE_LISTEN_5_CONCATENATE.py
from __future__ import annotations
from datetime import datetime
from io import BytesIO
import wave
import audioop
import asyncio
import inspect
import os
import tempfile
import subprocess
from typing import List, Optional

import numpy as np  # for PCM→float arrays

from SERVER_ENGINE_APP_VARIABLES import (
    TEMP_RECORDING_AUDIO_DIR,
    RECORDING_AUDIO_FRAME_ARRAY,
    RECORDING_AUDIO_CHUNK_ARRAY,
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    DB_LOG_FUNCTIONS,
    CONSOLE_LOG,
    DB_LOG_ENGINE_DB_LOG_STEPS,   # contextual DB step logging (can be sampled if needed)
    schedule_coro,                 # loop-safe scheduler
)

# Canonical intermediate decode rate (what we decode each micro-chunk to)
ORIG_SAMPLE_RATE = 44100
SAMPLE_WIDTH     = 2       # 16-bit PCM
CHANNELS         = 1

# ─────────────────────────────────────────────────────────────
# Small helpers
# ─────────────────────────────────────────────────────────────
def _frames_to_bytes(frames: List[bytes]) -> bytes:
    return b"".join(frames)

def _bytes_to_wav_bytes(pcm: bytes, sample_rate: int) -> bytes:
    bio = BytesIO()
    with wave.open(bio, "wb") as wf:
        wf.setnchannels(CHANNELS)
        wf.setsampwidth(SAMPLE_WIDTH)
        wf.setframerate(sample_rate)
        wf.writeframes(pcm)
    return bio.getvalue()

def _log_step(step: str,
              recording_id: Optional[int] = None,
              chunk_no: Optional[int] = None,
              frame_no: Optional[int] = None):
    """Helper to write to ENGINE_DB_LOG_STEPS with a real-time DT_ADDED."""
    try:
        fn_name = inspect.currentframe().f_back.f_code.co_name if inspect.currentframe() else "_"
    except Exception:
        fn_name = "_"
    DB_LOG_ENGINE_DB_LOG_STEPS(
        DT_ADDED=datetime.now(),
        STEP_NAME=step,
        PYTHON_FUNCTION_NAME=fn_name,
        PYTHON_FILE_NAME=os.path.basename(__file__),
        RECORDING_ID=recording_id,
        AUDIO_CHUNK_NO=chunk_no,
        FRAME_NO=frame_no,
    )

def _pcm16_to_float32_array(pcm: bytes) -> np.ndarray:
    """Decode PCM16 mono → float32 [-1, 1]."""
    if not pcm:
        return np.zeros(0, dtype=np.float32)
    arr = np.frombuffer(pcm, dtype=np.int16).astype(np.float32) / 32768.0
    return arr

def _looks_like_mp4_m4a(b: bytes) -> bool:
    """
    Very light heuristic: MP4 family usually has 'ftyp' within first 32 bytes.
    Also common brands: isom, mp42, M4A, etc.
    """
    if not b or len(b) < 12:
        return False
    head = b[:64]
    return (b"ftyp" in head) or (b"isom" in head) or (b"mp42" in head) or (b"M4A" in head)

def _ffmpeg_available() -> bool:
    try:
        subprocess.run(
            ["ffmpeg", "-version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return True
    except Exception:
        return False

def _ffmpeg_decode_to_pcm16_mono(frame_bytes: bytes, target_sr: int = ORIG_SAMPLE_RATE) -> Optional[bytes]:
    """
    Use ffmpeg to decode compressed audio (e.g., AAC in M4A) → PCM16 mono @ target_sr.
    Returns raw s16le bytes, or None if decode fails.
    """
    if not frame_bytes:
        return None
    # Create a temp dir per call (auto-cleaned by context manager)
    try:
        with tempfile.TemporaryDirectory(prefix="frame_decode_") as tdir:
            in_path  = os.path.join(tdir, "in.m4a")
            out_path = os.path.join(tdir, "out.raw")  # s16le

            with open(in_path, "wb") as f:
                f.write(frame_bytes)

            # ffmpeg -y -hide_banner -loglevel error -i in.m4a -ac 1 -ar 44100 -f s16le out.raw
            cmd = [
                "ffmpeg", "-y",
                "-hide_banner", "-loglevel", "error",
                "-i", in_path,
                "-ac", "1",
                "-ar", str(int(target_sr)),
                "-f", "s16le",
                out_path,
            ]
            proc = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=False)
            if proc.returncode != 0:
                # Emit a small, bounded error to console for visibility
                err = (proc.stderr or b"")[:200].decode("utf-8", "replace")
                CONSOLE_LOG("CONCAT", "ffmpeg_decode_failed", {"rc": proc.returncode, "err": err})
                return None

            with open(out_path, "rb") as f:
                return f.read()
    except Exception as e:
        CONSOLE_LOG("CONCAT", "ffmpeg_exception", {"err": str(e)})
        return None

def _safe_resample_pcm(
    pcm: bytes,
    from_rate: int,
    to_rate: int,
    *,
    RECORDING_ID: Optional[int] = None,
    AUDIO_CHUNK_NO: Optional[int] = None,
    CONTEXT: Optional[str] = None,
) -> bytes:
    """
    Resample PCM with audioop.ratecv, trimming partial frames if needed.
    """
    if from_rate == to_rate:
        return pcm

    frame_size = SAMPLE_WIDTH * CHANNELS
    remainder = len(pcm) % frame_size
    if remainder != 0:
        _log_step(
            step=f"PCM misaligned: len={len(pcm)}, frame_size={frame_size}, remainder={remainder} ({CONTEXT})",
            recording_id=RECORDING_ID,
            chunk_no=AUDIO_CHUNK_NO,
        )
        pcm = pcm[:len(pcm) - remainder]

    if not pcm:
        _log_step(
            step=f"PCM empty before resample {from_rate}->{to_rate} ({CONTEXT})",
            recording_id=RECORDING_ID,
            chunk_no=AUDIO_CHUNK_NO,
        )
        return b""

    try:
        converted, _ = audioop.ratecv(pcm, SAMPLE_WIDTH, CHANNELS, from_rate, to_rate, None)
        return converted
    except Exception as e:
        _log_step(
            step=f"ratecv error {from_rate}->{to_rate}: {e} ({CONTEXT})",
            recording_id=RECORDING_ID,
            chunk_no=AUDIO_CHUNK_NO,
        )
        # Fail safe: return original pcm
        return pcm

# ─────────────────────────────────────────────────────────────
# Stage 5: scan & schedule
# ─────────────────────────────────────────────────────────────
def SERVER_ENGINE_LISTEN_5_CONCATENATE() -> None:
    """
    For chunks with DT_COMPLETE_FRAMES_RECEIVED set and
    DT_START_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK is null,
    launch CONCATENATE_FRAMES_INTO_AN_AUDIO_CHUNK.
    """
    to_launch = []
    for rid, chunks in list(RECORDING_AUDIO_CHUNK_ARRAY.items()):
        for chno, ch in list(chunks.items()):
            if ch.get("DT_COMPLETE_FRAMES_RECEIVED") and ch.get("DT_START_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK") is None:
                to_launch.append((rid, chno))
    for rid, chno in to_launch:
        schedule_coro(CONCATENATE_FRAMES_INTO_AN_AUDIO_CHUNK(RECORDING_ID=rid, AUDIO_CHUNK_NO=chno))

# ─────────────────────────────────────────────────────────────
# Stage 5: concatenate + decode + resample + persist
# ─────────────────────────────────────────────────────────────
@DB_LOG_FUNCTIONS()
async def CONCATENATE_FRAMES_INTO_AN_AUDIO_CHUNK(RECORDING_ID: int, AUDIO_CHUNK_NO: int) -> None:
    """
    Step 1) Mark DT_START_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK
    Step 2) Decode EACH micro-chunk to PCM16 mono @ 44.1k and concatenate
    Step 3) Mark DT_COMPLETE_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK
    Step 4) Convert to WAV (44.1k) and save to temp dir
    Step 5) Resample once to 16k and 22,050 Hz
    Step 6) Expose float32 arrays for analyzers
    Step 7) Delete frames from RECORDING_AUDIO_FRAME_ARRAY
    Step 8) Mark DT_AUDIO_CHUNK_PREPARATION_COMPLETE
    """
    now = datetime.now()
    ch = RECORDING_AUDIO_CHUNK_ARRAY[RECORDING_ID][AUDIO_CHUNK_NO]
    ch["DT_START_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK"] = now

    # Gather frames in order
    lo = int(ch["MIN_AUDIO_STREAM_FRAME_NO"])
    hi = int(ch["MAX_AUDIO_STREAM_FRAME_NO"])
    frames_map = RECORDING_AUDIO_FRAME_ARRAY.get(RECORDING_ID, {})

    ff_ok = _ffmpeg_available()
    if not ff_ok:
        CONSOLE_LOG("CONCAT", "ffmpeg_not_found_on_path", {})

    decoded_pcm_chunks: List[bytes] = []
    for i in range(lo, hi + 1):
        frame = frames_map.get(i)
        if not frame:
            continue
        raw = frame.get("AUDIO_FRAME_DATA") or b""

        # Decide path: decode with ffmpeg if it looks like MP4/M4A, else assume PCM16
        use_decode = _looks_like_mp4_m4a(raw) and ff_ok

        if use_decode:
            pcm = _ffmpeg_decode_to_pcm16_mono(raw, target_sr=ORIG_SAMPLE_RATE)
            if pcm is None or len(pcm) == 0:
                # Fallback: treat as PCM (will sound wrong if actually AAC)
                _log_step(f"ffmpeg decode failed; fallback to raw PCM (frame {i})",
                          recording_id=RECORDING_ID, chunk_no=AUDIO_CHUNK_NO, frame_no=i)
                pcm = raw
        else:
            # If it's not AAC (or ffmpeg missing), assume it's already PCM16 mono @ ORIG_SAMPLE_RATE.
            pcm = raw

        decoded_pcm_chunks.append(pcm)

    # Concatenate decoded PCM16 @ 44.1k
    pcm_44k = _frames_to_bytes(decoded_pcm_chunks)

    # Mark concatenation complete
    ch["DT_COMPLETE_FRAMES_CONCATENATED_INTO_AN_AUDIO_CHUNK"] = datetime.now()

    # Write WAV (44.1k) for quick listening/debug
    try:
        wav_bytes = _bytes_to_wav_bytes(pcm_44k, ORIG_SAMPLE_RATE)
        out_path = TEMP_RECORDING_AUDIO_DIR / f"rec_{RECORDING_ID}_chunk_{AUDIO_CHUNK_NO}.wav"
        out_path.write_bytes(wav_bytes)
        ch["DT_AUDIO_CHUNK_CONVERTED_TO_WAV"] = datetime.now()
        ch["DT_AUDIO_CHUNK_WAV_SAVED_TO_FILE"] = datetime.now()
    except Exception as e:
        CONSOLE_LOG("CONCAT", "wav_write_failed", {"rid": int(RECORDING_ID), "chunk": int(AUDIO_CHUNK_NO), "err": str(e)})

    # Single resample pass to 16k & 22.05k for analyzers
    pcm_16k = _safe_resample_pcm(
        pcm_44k, ORIG_SAMPLE_RATE, 16000,
        RECORDING_ID=RECORDING_ID, AUDIO_CHUNK_NO=AUDIO_CHUNK_NO, CONTEXT=f"{lo}-{hi}"
    )
    ch["AUDIO_CHUNK_DATA_16K"] = pcm_16k
    ch["DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_16K"] = datetime.now()

    pcm_22050 = _safe_resample_pcm(
        pcm_44k, ORIG_SAMPLE_RATE, 22050,
        RECORDING_ID=RECORDING_ID, AUDIO_CHUNK_NO=AUDIO_CHUNK_NO, CONTEXT=f"{lo}-{hi}"
    )
    ch["AUDIO_CHUNK_DATA_22050"] = pcm_22050
    ch["DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_22050"] = datetime.now()

    # Float32 arrays for analyzers
    ch["AUDIO_ARRAY_16000"]  = _pcm16_to_float32_array(pcm_16k)
    ch["SAMPLE_RATE_16000"]  = 16000
    ch["AUDIO_ARRAY_22050"]  = _pcm16_to_float32_array(pcm_22050)
    ch["SAMPLE_RATE_22050"]  = 22050

    # Cleanup frames
    for i in range(lo, hi + 1):
        frames_map.pop(i, None)

    # Done
    ch["DT_AUDIO_CHUNK_PREPARATION_COMPLETE"] = datetime.now()
