# SERVER_ENGINE_LISTEN_5_CONCATENATE.py
from __future__ import annotations
from datetime import datetime
from io import BytesIO
import wave
import audioop
from typing import List

from SERVER_ENGINE_APP_VARIABLES import (
    TEMP_RECORDING_AUDIO_DIR,
    RECORDING_AUDIO_FRAME_ARRAY,
    RECORDING_AUDIO_CHUNK_ARRAY,
)

from SERVER_ENGINE_APP_FUNCTIONS import (
    DB_LOG_FUNCTIONS,
    CONSOLE_LOG,
    schedule_coro,   # ← loop-safe scheduler (now lives in APP_FUNCTIONS)
)

ORIG_SAMPLE_RATE = 44100   # assumption; adjust if your client sends a different rate
SAMPLE_WIDTH     = 2       # 16-bit PCM
CHANNELS         = 1

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

def _resample_pcm(pcm: bytes, from_rate: int, to_rate: int) -> bytes:
    if from_rate == to_rate or not pcm:
        return pcm
    # audioop.ratecv returns (converted_bytes, state)
    converted, _ = audioop.ratecv(pcm, SAMPLE_WIDTH, CHANNELS, from_rate, to_rate, None)
    return converted

def SERVER_ENGINE_LISTEN_5_CONCATENATE() -> None:
    """
    Step 1) For each chunk with DT_COMPLETE_FRAMES_RECEIVED set and
            DT_START_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK is null,
            launch CONCATENATE_FRAMES_INTO_AN_AUDIO_CHUNK (asynchronously).
    """
    to_launch = []
    for rid, chunks in list(RECORDING_AUDIO_CHUNK_ARRAY.items()):
        for chno, ch in list(chunks.items()):
            if ch.get("DT_COMPLETE_FRAMES_RECEIVED") and ch.get("DT_START_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK") is None:
                to_launch.append((rid, chno))

    # Use loop-safe scheduler so this works whether we're on the main loop or a worker thread
    for rid, chno in to_launch:
        schedule_coro(CONCATENATE_FRAMES_INTO_AN_AUDIO_CHUNK(RECORDING_ID=rid, AUDIO_CHUNK_NO=chno))

@DB_LOG_FUNCTIONS()
async def CONCATENATE_FRAMES_INTO_AN_AUDIO_CHUNK(RECORDING_ID: int, AUDIO_CHUNK_NO: int) -> None:
    """
    Step 1) Mark DT_START_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK
    Step 2) Concatenate frames to AUDIO_CHUNK_DATA (local)
    Step 3) Mark DT_COMPLETE_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK
    Step 4) Convert to WAV
    Step 5) Mark DT_AUDIO_CHUNK_CONVERTED_TO_WAV
    Step 6) Save the WAV to TEMP_RECORDING_AUDIO_DIR
    Step 7) Mark DT_AUDIO_CHUNK_WAV_SAVED_TO_FILE
    Step 8) Convert to 16k -> set DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_16K and AUDIO_CHUNK_DATA_16K
    Step 9) Convert to 22050 -> set DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_22050 and AUDIO_CHUNK_DATA_22050
    Step 10) Delete the frames from RECORDING_AUDIO_FRAME_ARRAY
    Step 11) Mark DT_AUDIO_CHUNK_PREPARATION_COMPLETE
    """
    now = datetime.now()
    ch = RECORDING_AUDIO_CHUNK_ARRAY[RECORDING_ID][AUDIO_CHUNK_NO]
    ch["DT_START_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK"] = now

    # Step 2: gather frames in order
    lo = int(ch["MIN_AUDIO_STREAM_FRAME_NO"])
    hi = int(ch["MAX_AUDIO_STREAM_FRAME_NO"])
    frames_map = RECORDING_AUDIO_FRAME_ARRAY.get(RECORDING_ID, {})
    ordered = [frames_map[i]["AUDIO_FRAME_DATA"] or b"" for i in range(lo, hi + 1) if i in frames_map]
    pcm = _frames_to_bytes(ordered)

    # Step 3: mark complete concatenation
    ch["DT_COMPLETE_FRAMES_CONCATENATED_INTO_AUDIO_CHUNK"] = datetime.now()

    # Step 4: create WAV bytes at original rate
    wav_bytes = _bytes_to_wav_bytes(pcm, ORIG_SAMPLE_RATE)
    ch["DT_AUDIO_CHUNK_CONVERTED_TO_WAV"] = datetime.now()

    # Step 6: save file
    out_path = TEMP_RECORDING_AUDIO_DIR / f"rec_{RECORDING_ID}_chunk_{AUDIO_CHUNK_NO}.wav"
    out_path.write_bytes(wav_bytes)
    ch["DT_AUDIO_CHUNK_WAV_SAVED_TO_FILE"] = datetime.now()

    # Step 8: 16k resample
    pcm_16k = _resample_pcm(pcm, ORIG_SAMPLE_RATE, 16000)
    ch["AUDIO_CHUNK_DATA_16K"] = pcm_16k
    ch["DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_16K"] = datetime.now()

    # Step 9: 22050 resample
    pcm_22050 = _resample_pcm(pcm, ORIG_SAMPLE_RATE, 22050)
    ch["AUDIO_CHUNK_DATA_22050"] = pcm_22050
    ch["DT_AUDIO_CHUNK_CONVERTED_TO_SAMPLE_RATE_22050"] = datetime.now()

    # Step 10: delete frames for this range to free memory
    for i in range(lo, hi + 1):
        frames_map.pop(i, None)

    # Step 11: mark prep complete
    ch["DT_AUDIO_CHUNK_PREPARATION_COMPLETE"] = datetime.now()
