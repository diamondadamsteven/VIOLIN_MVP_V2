# SERVER_ENGINE_AUDIO_STREAM_PROCESSOR.py
# ------------------------------------------------------------
# Processor for streamed frames → engine chunks, feature loads,
# and final export. Uses Option A microservice for Onsets&Frames.
# ------------------------------------------------------------

import os
import json
import math
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List, Iterable

import numpy as np
import pyodbc
import requests
import pretty_midi

# Optional deps for pitch / DSP
try:
    import librosa
except Exception:
    librosa = None

# torchcrepe (recommended CREPE implementation)
try:
    import torch
    import torchcrepe
except Exception:
    torch = None
    torchcrepe = None


def LOG(msg, obj=None):
    prefix = "SERVER_ENGINE_AUDIO_STREAM_PROCESSOR"
    if obj is None:
        print(f"{prefix} - {msg}", flush=True)
    else:
        print(f"{prefix} - {msg} {obj}", flush=True)

# =========================
# DB CONFIG
# =========================
DB_CONN_STR = os.getenv(
    "DB_CONN_STR",
    "DRIVER={ODBC Driver 17 for SQL Server};SERVER=104.40.11.248,3341;"
    "DATABASE=VIOLIN;UID=violin;PWD=Test123!;TrustServerCertificate=yes",
)

def _GET_CONN():
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._GET_CONN")
    return pyodbc.connect(DB_CONN_STR, autocommit=True)

def _EXEC_PROC(CONN, PROC_NAME: str, PARAMS: dict):
    print(f"SERVER_ENGINE_AUDIO_STREAM_PROCESSOR - Calling sp {PROC_NAME} {PARAMS}", flush=True)
    CUR = CONN.cursor()
    PLACEHOLDERS = ", ".join(f"@{K} = ?" for K in PARAMS.keys())
    SQL = f"EXEC {PROC_NAME} {PLACEHOLDERS}"
    CUR.execute(SQL, tuple(PARAMS.values()))
    return None

# =========================
# STATE – Per Recording
# =========================
# FRAMES: {RID: {FRAME_NO: {"start_ms":int,"end_ms":int,"path":str,"overlap_ms":int}}}
FRAMES: Dict[str, Dict[int, Dict[str, Any]]] = {}

# Global context per recording (hinted by listener START)
# CONTEXT[RID] = {
#   "VIOLINIST_ID": int,
#   "COMPOSE_PLAY_OR_PRACTICE": str,
#   "AUDIO_STREAM_FILE_NAME": Optional[str],
# }
CONTEXT: Dict[str, Dict[str, Any]] = {}

# COMPOSE mode runtime params
# COMPOSE_PARAMS[RID] = {"CHUNK_MS": int, "YN_RUN_FFT": 'Y'|'N', "NEXT_CHUNK_NO": int}
COMPOSE_PARAMS: Dict[str, Dict[str, Any]] = {}

# PLAY/PRACTICE plan per recording (list of dict rows with flags)
# Each row: { "AUDIO_CHUNK_NO": int, "START_MS": int, "END_MS": int,
#             "YN_RUN_FFT": 'Y'|'N', "YN_RUN_ONS": 'Y'|'N',
#             "YN_RUN_PYIN": 'Y'|'N', "YN_RUN_CREPE": 'Y'|'N' }
PLAY_PLAN: Dict[str, List[Dict[str, Any]]] = {}
# Current index into PLAY_PLAN per recording
PLAY_PLAN_INDEX: Dict[str, int] = {}

# Make sure we call P_ENGINE_ALL_BEFORE once
DID_BEFORE: set = set()

# =========================
# O&F microservice (Option A)
# =========================
OAF_HOST = os.getenv("OAF_HOST", "127.0.0.1")
OAF_PORT = int(os.getenv("OAF_PORT", "9077"))
OAF_URL  = f"http://{OAF_HOST}:{OAF_PORT}"

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", ".")).resolve()
TMP_CHUNKS_DIR = PROJECT_ROOT / "tmp" / "chunks"
TMP_CHUNKS_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# ffmpeg helpers (decode/resample)
# =========================
def _read_wav_as_float_mono(in_wav: Path, target_sr: int) -> np.ndarray:
    """
    Decode to mono float32 via ffmpeg pipe at target_sr.
    """
    cmd = [
        "ffmpeg", "-nostdin", "-v", "error",
        "-i", str(in_wav),
        "-ac", "1",
        "-ar", str(target_sr),
        "-f", "f32le",
        "pipe:1",
    ]
    try:
        raw = subprocess.check_output(cmd)
        audio = np.frombuffer(raw, dtype=np.float32)
        return audio.copy()  # ensure writable
    except subprocess.CalledProcessError as e:
        LOG("ffmpeg decode failed", {"path": str(in_wav), "err": str(e)})
        return np.zeros(0, dtype=np.float32)

def _export_resampled_wav(in_wav: Path, out_wav: Path, target_sr: int) -> bool:
    """
    Create a resampled mono PCM16 WAV file at target_sr.
    """
    out_wav.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg", "-nostdin", "-y", "-v", "error",
        "-i", str(in_wav),
        "-ac", "1",
        "-ar", str(target_sr),
        "-c:a", "pcm_s16le",
        str(out_wav),
    ]
    try:
        subprocess.check_call(cmd)
        return out_wav.exists() and out_wav.stat().st_size > 44
    except subprocess.CalledProcessError as e:
        LOG("ffmpeg resample failed", {"in": str(in_wav), "out": str(out_wav), "err": str(e)})
        return False

# =========================
# Context hint from listener
# =========================
def REGISTER_RECORDING_CONTEXT_HINT(RECORDING_ID: str, **kwargs):
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR.REGISTER_RECORDING_CONTEXT_HINT",
        {"RECORDING_ID": RECORDING_ID, **kwargs})
    CONN = _GET_CONN()
    CUR = CONN.cursor()

    CUR.execute("EXEC P_ENGINE_ALL_RECORDING_PARAMETERS_GET @RECORDING_ID = ?", (int(RECORDING_ID),))
    ROW = CUR.fetchone()
    ctx = {
      "VIOLINIST_ID": ROW.VIOLINIST_ID,
      "COMPOSE_PLAY_OR_PRACTICE": ROW.COMPOSE_PLAY_OR_PRACTICE,
      "AUDIO_STREAM_FILE_NAME": ROW.AUDIO_STREAM_FILE_NAME
    }

    CONTEXT[RECORDING_ID] = ctx
    for k, v in kwargs.items():
        ctx[k] = v

    CUR.close()
    CONN.close()

# =========================
# DB Context & Plans
# =========================
def STEP_2_LOAD_COMPOSE_PARAMS(CONN, RECORDING_ID: str):
    """
    P_ENGINE_SONG_AUDIO_CHUNK_FOR_COMPOSE_GET @RECORDING_ID
      -> AUDIO_CHUNK_DURATION_IN_MS, YN_RUN_FFT
    """
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR.STEP_2_LOAD_COMPOSE_PARAMS",
        {"RECORDING_ID": RECORDING_ID})

    if RECORDING_ID in COMPOSE_PARAMS:
        return

    CUR = CONN.cursor()
    print("SERVER_ENGINE_AUDIO_STREAM_PROCESSOR - Calling sp P_ENGINE_SONG_AUDIO_CHUNK_FOR_COMPOSE_GET "
          f"{{'RECORDING_ID': {int(RECORDING_ID)}}}", flush=True)
    CUR.execute("EXEC P_ENGINE_SONG_AUDIO_CHUNK_FOR_COMPOSE_GET @RECORDING_ID = ?", (int(RECORDING_ID),))
    ROW = CUR.fetchone()
    if not ROW:
        # Default if nothing returned
        COMPOSE_PARAMS[RECORDING_ID] = {"CHUNK_MS": 600, "YN_RUN_FFT": "Y", "NEXT_CHUNK_NO": 1}
        return

    CHUNK_MS = int(ROW.AUDIO_CHUNK_DURATION_IN_MS)
    YN_RUN_FFT = str(ROW.YN_RUN_FFT or "N").upper()
    COMPOSE_PARAMS[RECORDING_ID] = {"CHUNK_MS": CHUNK_MS, "YN_RUN_FFT": YN_RUN_FFT, "NEXT_CHUNK_NO": 1}

def STEP_3_NEXT_COMPOSE_FLAGS(CONN, RECORDING_ID: str, AUDIO_CHUNK_NO: int) -> Dict[str, str]:
    """
    P_ENGINE_SONG_AUDIO_CHUNK_NO_FOR_COMPOSE_GET @RECORDING_ID, @AUDIO_CHUNK_NO
       -> YN_RUN_ONS, YN_RUN_PYIN, YN_RUN_CREPE
    """
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR.STEP_3_NEXT_COMPOSE_FLAGS",
        {"RECORDING_ID": RECORDING_ID, "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO})

    CUR = CONN.cursor()
    print("SERVER_ENGINE_AUDIO_STREAM_PROCESSOR - Calling sp P_ENGINE_SONG_AUDIO_CHUNK_NO_FOR_COMPOSE_GET "
          f"{{'RECORDING_ID': {int(RECORDING_ID)}, 'AUDIO_CHUNK_NO': {AUDIO_CHUNK_NO}}}", flush=True)
    CUR.execute(
        "EXEC P_ENGINE_SONG_AUDIO_CHUNK_NO_FOR_COMPOSE_GET @RECORDING_ID = ?, @AUDIO_CHUNK_NO = ?",
        (int(RECORDING_ID), int(AUDIO_CHUNK_NO))
    )
    ROW = CUR.fetchone()
    if not ROW:
        return {"YN_RUN_ONS": "N", "YN_RUN_PYIN": "N", "YN_RUN_CREPE": "N"}
    return {
        "YN_RUN_ONS": str(getattr(ROW, "YN_RUN_ONS", "N") or "N").upper(),
        "YN_RUN_PYIN": str(getattr(ROW, "YN_RUN_PYIN", "N") or "N").upper(),
        "YN_RUN_CREPE": str(getattr(ROW, "YN_RUN_CREPE", "N") or "N").upper(),
    }

def STEP_4_LOAD_PLAY_PLAN(CONN, RECORDING_ID: str):
    """
    P_ENGINE_SONG_AUDIO_CHUNK_FOR_PLAY_AND_PRACTICE_GET @RECORDING_ID
       -> rows: AUDIO_CHUNK_NO, START_MS, END_MS, YN_RUN_FFT, YN_RUN_ONS, YN_RUN_PYIN, YN_RUN_CREPE
    """
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR.STEP_4_LOAD_PLAY_PLAN",
        {"RECORDING_ID": RECORDING_ID})

    if RECORDING_ID in PLAY_PLAN:
        return

    CUR = CONN.cursor()
    print("SERVER_ENGINE_AUDIO_STREAM_PROCESSOR - Calling sp P_ENGINE_SONG_AUDIO_CHUNK_FOR_PLAY_AND_PRACTICE_GET "
          f"{{'RECORDING_ID': {int(RECORDING_ID)}}}", flush=True)
    CUR.execute("EXEC P_ENGINE_SONG_AUDIO_CHUNK_FOR_PLAY_AND_PRACTICE_GET @RECORDING_ID = ?", (int(RECORDING_ID),))
    PLAN = []
    for ROW in CUR.fetchall():
        PLAN.append({
            "AUDIO_CHUNK_NO": int(ROW.AUDIO_CHUNK_NO),
            "START_MS": int(ROW.START_MS),
            "END_MS": int(ROW.END_MS),
            "YN_RUN_FFT": str(ROW.YN_RUN_FFT or "N").upper(),
            "YN_RUN_ONS": str(ROW.YN_RUN_ONS or "N").upper(),
            "YN_RUN_PYIN": str(ROW.YN_RUN_PYIN or "N").upper(),
            "YN_RUN_CREPE": str(ROW.YN_RUN_CREPE or "N").upper(),
        })
    PLAN.sort(key=lambda r: r["AUDIO_CHUNK_NO"])
    PLAY_PLAN[RECORDING_ID] = PLAN
    PLAY_PLAN_INDEX[RECORDING_ID] = 0
    LOG("Loaded PLAY_PLAN", {"count": len(PLAN)})

# =========================
# Coverage & Export helpers
# =========================
def _WINDOW_COVERED(RID: str, START_MS: int, END_MS: int) -> bool:
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._WINDOW_COVERED",
        {"RID": RID, "START_MS": START_MS, "END_MS": END_MS})
    if RID not in FRAMES:
        return False
    frames = FRAMES[RID]
    spans = sorted((d["start_ms"], d["end_ms"]) for d in frames.values())
    needed = START_MS
    for s, e in spans:
        if e < needed:
            continue
        if s > needed:
            return False
        needed = max(needed, e + 1)
        if needed > END_MS:
            return True
    return needed > END_MS

def _EXPORT_CHUNK_WAV_FROM_FRAMES(RID: str, START_MS: int, END_MS: int, OUT_WAV: Path) -> bool:
    """
    Uses ffmpeg concat + trim to export an exact window as WAV mono 48k.
    This avoids decoding to numpy first (good for O&F Option A).
    """
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._EXPORT_CHUNK_WAV_FROM_FRAMES",
        {"RID": RID, "START_MS": START_MS, "END_MS": END_MS, "OUT": str(OUT_WAV)})

    frames = FRAMES.get(RID, {})
    if not frames:
        return False

    # Use a concat list in the recording's temp dir
    any_path = next(iter(frames.values()))["path"]
    temp_dir = Path(any_path).parent

    ordered = sorted(frames.values(), key=lambda d: (d["start_ms"], d["end_ms"], d["path"]))
    concat_list = temp_dir / "_concat_all.txt"
    with concat_list.open("w", encoding="utf-8") as f:
        for d in ordered:
            p = Path(d["path"]).resolve()
            f.write(f"file '{p.as_posix()}'\n")

    # Build a trimmed WAV from the concatenated stream
    start_sec = START_MS / 1000.0
    dur_sec = max(0.0, (END_MS - START_MS + 1) / 1000.0)

    OUT_WAV.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0",
        "-i", str(concat_list),
        "-ss", f"{start_sec:.3f}",
        "-t", f"{dur_sec:.3f}",
        "-ac", "1",
        "-ar", "48000",
        "-c:a", "pcm_s16le",
        str(OUT_WAV),
    ]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        ok = OUT_WAV.exists() and OUT_WAV.stat().st_size > 44  # > WAV header
    except subprocess.CalledProcessError:
        ok = False
    finally:
        try:
            concat_list.unlink(missing_ok=True)
        except Exception:
            pass
    return ok

# =========================
# DB Load helpers
# =========================
def _BULK_INSERT(CONN, SQL: str, ROWS: Iterable[tuple]):
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._BULK_INSERT")
    ROWS = list(ROWS)
    if not ROWS:
        return
    CUR = CONN.cursor()
    CUR.fast_executemany = True
    CUR.executemany(SQL, ROWS)

def _LOAD_NOTE(CONN, RECORDING_ID: int, AUDIO_CHUNK_NO: int,
               NOTE_ROWS: Iterable[Tuple[int, int, int, int, str]]):
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._LOAD_NOTE",
        {"RECORDING_ID": RECORDING_ID, "CHUNK": AUDIO_CHUNK_NO})
    SQL = """
      INSERT INTO ENGINE_LOAD_NOTE
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS,
       NOTE_MIDI_PITCH_NO, VOLUME_MIDI_VELOCITY_NO, SOURCE_METHOD)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    PACK = (
        (RECORDING_ID, AUDIO_CHUNK_NO, s, e, midi, vel, src)
        for (s, e, midi, vel, src) in NOTE_ROWS
    )
    _BULK_INSERT(CONN, SQL, PACK)

def _LOAD_HZ(CONN, RECORDING_ID: int, AUDIO_CHUNK_NO: int,
             SOURCE_METHOD: str,
             HZ_SERIES: Iterable[Tuple[int, int, float, float]]):
    """
    Insert many per-10ms rows:
      HZ_SERIES = [(start_ms, end_ms, hz, confidence), ...]
      start_ms/end_ms are absolute within the recording timeline.
    """
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._LOAD_HZ",
        {"RECORDING_ID": RECORDING_ID, "CHUNK": AUDIO_CHUNK_NO, "SRC": SOURCE_METHOD})
    rows = list(HZ_SERIES)
    if not rows:
        return
    SQL = """
      INSERT INTO ENGINE_LOAD_HZ
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS, SOURCE_METHOD, HZ, CONFIDENCE)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    PACK = (
        (RECORDING_ID, AUDIO_CHUNK_NO, s, e, SOURCE_METHOD, float(hz), float(conf))
        for (s, e, hz, conf) in rows
    )
    _BULK_INSERT(CONN, SQL, PACK)

def _LOAD_FFT_FRAMES(CONN, RECORDING_ID: int, AUDIO_CHUNK_NO: int,
                     FFT_ROWS: Iterable[Tuple[int, int, int, float, float, float, float]]):
    """
    Insert per-100ms FFT rows. Each row is:
      (FRAME_START_MS, FRAME_END_MS, FFT_BUCKET_NO, HZ_START, HZ_END,
       FFT_BUCKET_SIZE_IN_HZ, FFT_VALUE)
    """
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._LOAD_FFT_FRAMES",
        {"RECORDING_ID": RECORDING_ID, "CHUNK": AUDIO_CHUNK_NO})
    SQL = """
      INSERT INTO ENGINE_LOAD_FFT
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS,
       FFT_BUCKET_NO, HZ_START, HZ_END, FFT_BUCKET_SIZE_IN_HZ, FFT_VALUE)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    PACK = (
        (RECORDING_ID, AUDIO_CHUNK_NO, s, e, bno, hz0, hz1, bsz, val)
        for (s, e, bno, hz0, hz1, bsz, val) in FFT_ROWS
    )
    _BULK_INSERT(CONN, SQL, PACK)

def _LOAD_VOLUME(CONN, RECORDING_ID: int, AUDIO_CHUNK_NO: int, START_MS: int,
                 VOL_AGG: Optional[Tuple[float, float]]):
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._LOAD_VOLUME",
        {"RECORDING_ID": RECORDING_ID, "CHUNK": AUDIO_CHUNK_NO})
    if not VOL_AGG:
        return
    SQL = """
      INSERT INTO ENGINE_LOAD_VOLUME
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, VOLUME, VOLUME_IN_DB)
      VALUES (?, ?, ?, ?, ?)
    """
    _BULK_INSERT(CONN, SQL, [(RECORDING_ID, AUDIO_CHUNK_NO, START_MS, VOL_AGG[0], VOL_AGG[1])])

def _LOAD_VOLUME_10MS(CONN, RECORDING_ID: int, AUDIO_CHUNK_NO: int,
                      VOL_SERIES: Iterable[Tuple[int, int, float, float]]):
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._LOAD_VOLUME_10MS",
        {"RECORDING_ID": RECORDING_ID, "CHUNK": AUDIO_CHUNK_NO})
    rows = list(VOL_SERIES)
    if not rows:
        return
    SQL = """
      INSERT INTO ENGINE_LOAD_VOLUME_10_MS
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS, VOLUME, VOLUME_IN_DB)
      VALUES (?, ?, ?, ?, ?, ?)
    """
    PACK = (
        (RECORDING_ID, AUDIO_CHUNK_NO, s, e, v, vdb) for (s, e, v, vdb) in rows
    )
    _BULK_INSERT(CONN, SQL, PACK)

# =========================
# Feature implementations
# =========================
def _RUN_ONSETS_AND_FRAMES_MICROSERVICE(absolute_wav_path: Path) -> Optional[Path]:
    """
    Calls the Option A microservice (FastAPI in Docker) to transcribe absolute WAV.
    Returns absolute MIDI path on success.
    """
    try:
        resp = requests.post(
            f"{OAF_URL}/transcribe",
            json={"audio_path": str(absolute_wav_path)},
            timeout=120,
        )
        if resp.ok:
            data = resp.json()
            if data.get("ok"):
                midi_path = Path(data["midi_path"]).resolve()
                return midi_path if midi_path.exists() else None
            else:
                LOG("O&F microservice returned error", data)
        else:
            LOG("O&F microservice HTTP error", {"status": resp.status_code, "text": resp.text})
    except Exception as exc:
        LOG("O&F microservice call failed", str(exc))
    return None

def _PARSE_MIDI_TO_NOTES(midi_path: Path) -> List[Tuple[int, int, int, int, str]]:
    """
    Returns note rows: [(START_MS, END_MS, MIDI, VELOCITY, 'ONS'), ...]
    """
    pm = pretty_midi.PrettyMIDI(str(midi_path))
    rows = []
    for inst in pm.instruments:
        for n in inst.notes:
            s = int(round(n.start * 1000.0))
            e = int(round(n.end   * 1000.0))
            rows.append((s, e, int(n.pitch), int(n.velocity), "ONS"))
    return rows

def _COMPUTE_ONS_VIA_MICROSERVICE(chunk_wav_path: Path) -> List[Tuple[int, int, int, int, str]]:
    """
    Wrapper that was referenced upstream but not defined:
    runs microservice and parses MIDI to engine note rows.
    """
    midi = _RUN_ONSETS_AND_FRAMES_MICROSERVICE(chunk_wav_path)
    if not midi:
        return []
    try:
        return _PARSE_MIDI_TO_NOTES(midi)
    except Exception as e:
        LOG("Parse MIDI failed", str(e))
        return []

def _COMPUTE_FFT(audio_22k: np.ndarray, sr: int, base_start_ms: int) -> List[Tuple[int, int, int, float, float, float, float]]:
    """
    100 ms window / 100 ms hop @ 22,050 Hz.
    Returns rows:
      (FRAME_START_MS, FRAME_END_MS, FFT_BUCKET_NO, HZ_START, HZ_END, BUCKET_SIZE_HZ, VALUE)
    VALUE is per-frame max-normalized magnitude.
    """
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._COMPUTE_FFT",
        {"sr": sr, "len": int(audio_22k.shape[0])})
    if sr <= 0 or audio_22k.size == 0:
        return []

    window_ms = 100
    hop_ms = 100
    win = int(round(sr * (window_ms / 1000.0)))
    hop = int(round(sr * (hop_ms / 1000.0)))
    if win <= 0 or hop <= 0 or audio_22k.size < win:
        return []

    # Frequency bucket size in Hz
    bucket_hz = sr / float(win)

    rows: List[Tuple[int, int, int, float, float, float, float]] = []
    n_frames = 1 + (audio_22k.size - win) // hop
    for i in range(n_frames):
        start = i * hop
        end = start + win
        seg = audio_22k[start:end]

        # Hann to reduce leakage
        seg = seg * np.hanning(seg.shape[0])
        # rfft: real FFT -> N/2+1 bins
        spec = np.fft.rfft(seg)
        mag = np.abs(spec)

        # Per-frame max normalization (avoid div-by-zero)
        m = float(mag.max()) if mag.size else 0.0
        if m > 0:
            mag = mag / m

        # Frame timing in ms (relative to chunk, then offset by base_start_ms)
        frame_start_ms = base_start_ms + int(round((start / sr) * 1000.0))
        frame_end_ms   = base_start_ms + int(round((end   / sr) * 1000.0))

        # Bin 0..N/2; map bin → [hz_start, hz_end)
        for bno in range(mag.shape[0]):
            hz0 = bno * bucket_hz
            hz1 = (bno + 1) * bucket_hz
            rows.append((
                frame_start_ms, frame_end_ms,
                bno, float(hz0), float(hz1), float(bucket_hz), float(mag[bno])
            ))

    return rows

def _COMPUTE_PYIN_SERIES(audio_22k: np.ndarray, sr: int) -> List[Tuple[int, int, float, float]]:
    """
    pYIN at 22.05 kHz → per-10ms series relative to the chunk:
      returns [(start_ms, end_ms, hz, confidence), ...]
    """
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._COMPUTE_PYIN_SERIES",
        {"sr": sr, "len": int(audio_22k.shape[0])})

    if librosa is None or sr != 22050 or audio_22k.size == 0:
        if librosa is None:
            LOG("pYIN unavailable: librosa not installed")
        else:
            LOG("pYIN expects 22050 Hz non-empty input", {"sr": sr, "size": int(audio_22k.size)})
        return []

    hop_length = max(1, int(round(sr * 0.010)))  # ~10 ms
    frame_length = max(hop_length * 4, 2048)

    try:
        fmin = librosa.note_to_hz("G3")   # ~196 Hz
        fmax = librosa.note_to_hz("C8")   # ~4186 Hz
    except Exception:
        fmin, fmax = 196.0, 4186.0

    try:
        f0, voiced_flag, voiced_prob = librosa.pyin(
            y=audio_22k, sr=sr, fmin=fmin, fmax=fmax,
            frame_length=frame_length, hop_length=hop_length, center=True
        )
    except Exception as e:
        LOG("pYIN failed", str(e))
        return []

    rows: List[Tuple[int, int, float, float]] = []
    for i, (hz, vflag, vprob) in enumerate(zip(f0, voiced_flag, voiced_prob)):
        if vflag and hz is not None and np.isfinite(hz) and hz > 0.0:
            start_ms = int(round((i * hop_length) * 1000.0 / sr))
            end_ms   = start_ms + 9
            rows.append((start_ms, end_ms, float(hz), float(vprob)))
    return rows

def _COMPUTE_CREPE_SERIES(audio_16k: np.ndarray, sr: int) -> List[Tuple[int, int, float, float]]:
    """
    torchcrepe at 16 kHz → per-10ms series relative to the chunk:
      returns [(start_ms, end_ms, hz, confidence), ...]
    """
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._COMPUTE_CREPE_SERIES",
        {"sr": sr, "len": int(audio_16k.shape[0])})

    if torchcrepe is None or torch is None or sr != 16000 or audio_16k.size == 0:
        if torchcrepe is None or torch is None:
            LOG("CREPE unavailable: torchcrepe/torch not installed")
        else:
            LOG("CREPE expects 16000 Hz non-empty input", {"sr": sr, "size": int(audio_16k.size)})
        return []

    device = "cuda" if torch.cuda.is_available() else "cpu"
    x = torch.tensor(audio_16k, dtype=torch.float32, device=device).unsqueeze(0)  # (1, T)

    hop_length = 160  # 10 ms @ 16k

    # Violin-safe range
    try:
        fmin = float(librosa.note_to_hz("G3")) if librosa else 196.0
        fmax = float(librosa.note_to_hz("C8")) if librosa else 4186.0
    except Exception:
        fmin, fmax = 196.0, 4186.0

    # ---- IMPORTANT: pass a CALLABLE decoder, not a string ----
    decoder_fn = getattr(torchcrepe.decode, "viterbi", None)
    if decoder_fn is None:
        # Fallback to argmax if viterbi is unavailable
        decoder_fn = torchcrepe.decode.argmax

    with torch.no_grad():
        f0, periodicity = torchcrepe.predict(
            x,
            sample_rate=sr,
            hop_length=hop_length,
            fmin=fmin, fmax=fmax,
            model="full",
            decoder=decoder_fn,          # <<< fixed
            batch_size=1024,
            device=device,
            return_periodicity=True
        )

    f0 = f0.squeeze(0).detach().cpu().numpy()
    pr = periodicity.squeeze(0).detach().cpu().numpy()

    rows: List[Tuple[int, int, float, float]] = []
    for i, (hz, conf) in enumerate(zip(f0, pr)):
        if np.isfinite(hz) and hz > 0.0:
            start_ms = int(round(i * hop_length * 1000.0 / sr))
            end_ms   = start_ms + 9
            rows.append((start_ms, end_ms, float(hz), float(conf)))
    return rows

def _COMPUTE_VOLUME(audio: np.ndarray, sr: int, base_start_ms: int) -> Tuple[Optional[Tuple[float, float]], List[Tuple[int, int, float, float]]]:
    """
    Aggregate RMS & dB for the chunk, and a 10 ms series to match ENGINE_LOAD_VOLUME_10_MS:
      returns ( (avg_rms, avg_db), [ (start_ms, end_ms, rms, db), ... ] )
    """
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._COMPUTE_VOLUME", {"sr": sr, "len": int(audio.shape[0])})
    if sr <= 0 or audio.size == 0:
        return None, []

    # 25 ms window / 10 ms hop
    hop_ms = 10
    win_ms = 25
    hop = max(1, int(round(sr * (hop_ms / 1000.0))))
    win = max(1, int(round(sr * (win_ms / 1000.0))))

    series: List[Tuple[int, int, float, float]] = []

    if librosa is not None:
        try:
            rms = librosa.feature.rms(y=audio, frame_length=win, hop_length=hop, center=True)[0]
            # frame times in seconds from chunk start (aligned to hop)
            times = librosa.frames_to_time(np.arange(len(rms)), sr=sr, hop_length=hop, n_fft=win)
            for r, t in zip(rms, times):
                s_ms = base_start_ms + int(round(t * 1000.0))
                e_ms = s_ms + (hop_ms - 1)
                v = float(r)
                vdb = float(20.0 * math.log10(max(v, 1e-12)))
                series.append((s_ms, e_ms, v, vdb))
        except Exception as e:
            LOG("librosa RMS failed, falling back to numpy", str(e))

    if not series:
        # Numpy fallback with Hann window
        i = 0
        hann = np.hanning(win) if win > 1 else None
        N = audio.size
        while i + win <= N:
            seg = audio[i:i+win]
            if hann is not None:
                seg = seg * hann
            v = float(np.sqrt(np.mean(seg * seg))) if seg.size else 0.0
            vdb = float(20.0 * math.log10(max(v, 1e-12)))
            s_ms = base_start_ms + int(round((i / sr) * 1000.0))
            e_ms = s_ms + (hop_ms - 1)
            series.append((s_ms, e_ms, v, vdb))
            i += hop

    if not series:
        return None, []

    avg_rms = float(np.mean([v for (_, _, v, _) in series]))
    avg_db  = float(np.mean([vdb for (_, _, _, vdb) in series]))
    return (avg_rms, avg_db), series

# =========================
# Main entry – per frame
# =========================
async def PROCESS_AUDIO_STREAM(
    RECORDING_ID: str,
    FRAME_NO: int,
    FRAME_START_MS: int,
    FRAME_END_MS: int,
    FRAME_DURATION_IN_MS: int,
    COUNTDOWN_OVERLAP_MS: int,
    AUDIO_STREAM_FILE_PATH: str,
):
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR.PROCESS_AUDIO_STREAM",
        {"RECORDING_ID": RECORDING_ID, "FRAME_NO": FRAME_NO, "PATH": AUDIO_STREAM_FILE_PATH})
    RID = str(RECORDING_ID)
    CONN = _GET_CONN()
    try:
        if RID not in CONTEXT:
            REGISTER_RECORDING_CONTEXT_HINT(RID)
        CTX = CONTEXT[RID]
        MODE = str(CTX["COMPOSE_PLAY_OR_PRACTICE"]).upper()

        # Register frame
        FRAMES.setdefault(RID, {})[int(FRAME_NO)] = {
            "start_ms": int(FRAME_START_MS),
            "end_ms": int(FRAME_END_MS),
            "path": str(AUDIO_STREAM_FILE_PATH),
            "overlap_ms": int(COUNTDOWN_OVERLAP_MS or 0),
        }

        if MODE == "COMPOSE":
            # Load compose params once
            STEP_2_LOAD_COMPOSE_PARAMS(CONN, RID)
            params = COMPOSE_PARAMS[RID]
            CHUNK_MS = int(params["CHUNK_MS"])
            YN_FFT = params["YN_RUN_FFT"]

            # Emit as many complete CHUNK_MS windows as we can
            while True:
                AUDIO_CHUNK_NO = params["NEXT_CHUNK_NO"]
                start_ms = (AUDIO_CHUNK_NO - 1) * CHUNK_MS
                end_ms = start_ms + CHUNK_MS - 1

                if not _WINDOW_COVERED(RID, start_ms, end_ms):
                    break

                # Export 48k WAV for the exact window
                chunk_wav_48k = TMP_CHUNKS_DIR / f"{RID}_compose_{AUDIO_CHUNK_NO:06d}_48k.wav"
                if not _EXPORT_CHUNK_WAV_FROM_FRAMES(RID, start_ms, end_ms, chunk_wav_48k):
                    break

                # Prepare 22.05k and 16k versions (decode or file)
                audio_22k = _read_wav_as_float_mono(chunk_wav_48k, 22050)
                audio_16k = _read_wav_as_float_mono(chunk_wav_48k, 16000)

                # For O&F microservice, create a 16k file path
                chunk_wav_16k = TMP_CHUNKS_DIR / f"{RID}_compose_{AUDIO_CHUNK_NO:06d}_16k.wav"
                _export_resampled_wav(chunk_wav_48k, chunk_wav_16k, 16000)

                # FFT (per 100 ms, 22.05k)
                if (YN_FFT or "N").upper() == "Y":
                    fft_rows = _COMPUTE_FFT(audio_22k, 22050, start_ms)
                    _LOAD_FFT_FRAMES(CONN, int(RID), AUDIO_CHUNK_NO, fft_rows)
                    _EXEC_PROC(CONN, "P_ENGINE_ALL_METHOD_FFT", {
                        "RECORDING_ID": int(RID),
                        "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO,
                        "COMPOSE_PLAY_OR_PRACTICE": "COMPOSE",
                    })
                else:
                    _EXEC_PROC(CONN, "P_ENGINE_ALL_METHOD_COMPOSE_DONT_RUN_FFT", {
                        "RECORDING_ID": int(RID),
                        "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO,
                    })

                # Per-chunk flags for ONS/PYIN/CREPE
                flags = STEP_3_NEXT_COMPOSE_FLAGS(CONN, RID, AUDIO_CHUNK_NO)

                if flags.get("YN_RUN_ONS", "N") == "Y":
                    note_rows = _COMPUTE_ONS_VIA_MICROSERVICE(chunk_wav_16k)
                    _LOAD_NOTE(CONN, int(RID), AUDIO_CHUNK_NO, note_rows)

                if flags.get("YN_RUN_PYIN", "N") == "Y":
                    py_series_rel = _COMPUTE_PYIN_SERIES(audio_22k, 22050)
                    py_series_abs = [(start_ms + rs, start_ms + re, hz, conf)
                                     for (rs, re, hz, conf) in py_series_rel]
                    _LOAD_HZ(CONN, int(RID), AUDIO_CHUNK_NO, "PYIN", py_series_abs)

                if flags.get("YN_RUN_CREPE", "N") == "Y":
                    cr_series_rel = _COMPUTE_CREPE_SERIES(audio_16k, 16000)
                    cr_series_abs = [(start_ms + rs, start_ms + re, hz, conf)
                                     for (rs, re, hz, conf) in cr_series_rel]
                    _LOAD_HZ(CONN, int(RID), AUDIO_CHUNK_NO, "CREPE", cr_series_abs)

                # Volume (computed at 22.05k, per 10 ms)
                vol_agg, vol_series = _COMPUTE_VOLUME(audio_22k, 22050, start_ms)
                _LOAD_VOLUME(CONN, int(RID), AUDIO_CHUNK_NO, start_ms, vol_agg)
                _LOAD_VOLUME_10MS(CONN, int(RID), AUDIO_CHUNK_NO, vol_series)

                # Master
                _EXEC_PROC(CONN, "P_ENGINE_ALL_MASTER", {
                    "VIOLINIST_ID": int(CTX["VIOLINIST_ID"]),
                    "RECORDING_ID": int(RID),
                    "COMPOSE_PLAY_OR_PRACTICE": "COMPOSE",
                    "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO
                    # "YN_RECORDING_STOPPED": None,
                })

                # Advance
                params["NEXT_CHUNK_NO"] = AUDIO_CHUNK_NO + 1

        else:
            # PLAY or PRACTICE
            STEP_4_LOAD_PLAY_PLAN(CONN, RID)
            plan = PLAY_PLAN[RID]
            idx = PLAY_PLAN_INDEX.get(RID, 0)

            while idx < len(plan):
                row = plan[idx]
                start_ms = row["START_MS"]
                end_ms = row["END_MS"]
                AUDIO_CHUNK_NO = row["AUDIO_CHUNK_NO"]

                if not _WINDOW_COVERED(RID, start_ms, end_ms):
                    break

                # Export exact window to 48k WAV
                chunk_wav_48k = TMP_CHUNKS_DIR / f"{RID}_{MODE.lower()}_{AUDIO_CHUNK_NO:06d}_48k.wav"
                if not _EXPORT_CHUNK_WAV_FROM_FRAMES(RID, start_ms, end_ms, chunk_wav_48k):
                    break

                # Prepare 22.05k & 16k
                audio_22k = _read_wav_as_float_mono(chunk_wav_48k, 22050)
                audio_16k = _read_wav_as_float_mono(chunk_wav_48k, 16000)
                chunk_wav_16k = TMP_CHUNKS_DIR / f"{RID}_{MODE.lower()}_{AUDIO_CHUNK_NO:06d}_16k.wav"
                _export_resampled_wav(chunk_wav_48k, chunk_wav_16k, 16000)

                # FFT
                if row.get("YN_RUN_FFT", "N") == "Y":
                    fft_rows = _COMPUTE_FFT(audio_22k, 22050, start_ms)
                    _LOAD_FFT_FRAMES(CONN, int(RID), AUDIO_CHUNK_NO, fft_rows)

                # ONS
                if row.get("YN_RUN_ONS", "N") == "Y":
                    note_rows = _COMPUTE_ONS_VIA_MICROSERVICE(chunk_wav_16k)
                    _LOAD_NOTE(CONN, int(RID), AUDIO_CHUNK_NO, note_rows)

                # PYIN / CREPE
                if row.get("YN_RUN_PYIN", "N") == "Y":
                    py_series_rel = _COMPUTE_PYIN_SERIES(audio_22k, 22050)
                    py_series_abs = [(start_ms + rs, start_ms + re, hz, conf)
                                     for (rs, re, hz, conf) in py_series_rel]
                    _LOAD_HZ(CONN, int(RID), AUDIO_CHUNK_NO, "PYIN", py_series_abs)
                if row.get("YN_RUN_CREPE", "N") == "Y":
                    cr_series_rel = _COMPUTE_CREPE_SERIES(audio_16k, 16000)
                    cr_series_abs = [(start_ms + rs, start_ms + re, hz, conf)
                                     for (rs, re, hz, conf) in cr_series_rel]
                    _LOAD_HZ(CONN, int(RID), AUDIO_CHUNK_NO, "CREPE", cr_series_abs)

                # Volume (per 10 ms)
                vol_agg, vol_series = _COMPUTE_VOLUME(audio_22k, 22050, start_ms)
                _LOAD_VOLUME(CONN, int(RID), AUDIO_CHUNK_NO, start_ms, vol_agg)
                _LOAD_VOLUME_10MS(CONN, int(RID), AUDIO_CHUNK_NO, vol_series)

                # Master
                _EXEC_PROC(CONN, "P_ENGINE_ALL_MASTER", {
                    "VIOLINIST_ID": int(CTX["VIOLINIST_ID"]),
                    "RECORDING_ID": int(RID),
                    "COMPOSE_PLAY_OR_PRACTICE": MODE,
                    "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO
                    # "YN_RECORDING_STOPPED": None,
                })

                idx += 1
                PLAY_PLAN_INDEX[RID] = idx

    finally:
        CONN.close()

# =========================
# Finalize on STOP
# =========================
def _CHOOSE_EXPORT_PATH(RECORDING_ID: str, AUDIO_STREAM_FILE_NAME: Optional[str]) -> str:
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._CHOOSE_EXPORT_PATH",
        {"RECORDING_ID": RECORDING_ID, "AUDIO_STREAM_FILE_NAME": AUDIO_STREAM_FILE_NAME})
    out_root = PROJECT_ROOT / "tmp" / "recordings"
    out_root.mkdir(parents=True, exist_ok=True)

    if not AUDIO_STREAM_FILE_NAME:
        return str(out_root / f"{RECORDING_ID}.wav")

    name = Path(AUDIO_STREAM_FILE_NAME).name
    stem = Path(name).stem
    return str(out_root / f"{stem}.wav")

def FINALIZE_RECORDING_EXPORT(RECORDING_ID: str, AUDIO_STREAM_FILE_NAME: Optional[str]) -> Optional[str]:
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR.FINALIZE_RECORDING_EXPORT",
        {"RECORDING_ID": RECORDING_ID})
    RID = str(RECORDING_ID)
    frames = FRAMES.get(RID, {})
    if not frames:
        return None

    any_path = next(iter(frames.values()))["path"]
    temp_dir = Path(any_path).parent

    ordered = sorted(frames.values(), key=lambda d: (d["start_ms"], d["end_ms"], d["path"]))
    concat_list = temp_dir / "_concat_final.txt"
    with concat_list.open("w", encoding="utf-8") as f:
        for d in ordered:
            p = Path(d["path"]).resolve()
            f.write(f"file '{p.as_posix()}'\n")

    out_path = _CHOOSE_EXPORT_PATH(RID, AUDIO_STREAM_FILE_NAME)

    cmd = [
        "ffmpeg", "-y",
        "-f", "concat", "-safe", "0",
        "-i", str(concat_list),
        "-ac", "1",
        "-ar", "48000",
        "-c:a", "pcm_s16le",
        out_path,
    ]
    LOG("Running ffmpeg concat → wav", {"out": out_path})

    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    finally:
        try:
            concat_list.unlink(missing_ok=True)
        except Exception:
            pass

    return out_path

async def PROCESS_STOP_RECORDING(RECORDING_ID: str):
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR.PROCESS_STOP_RECORDING",
        {"RECORDING_ID": RECORDING_ID})
    RID = str(RECORDING_ID)
    CONN = _GET_CONN()
    try:
        if RID not in CONTEXT:
            REGISTER_RECORDING_CONTEXT_HINT(RID)
        CTX = CONTEXT[RID]

        final_path = FINALIZE_RECORDING_EXPORT(RID, CTX.get("AUDIO_STREAM_FILE_NAME"))
        LOG("Final WAV path", {"path": final_path})

        _EXEC_PROC(CONN, "P_ENGINE_RECORD_END", {
            "RECORDING_ID": int(RID)
        })

    finally:
        CONN.close()
        FRAMES.pop(RID, None)
        CONTEXT.pop(RID, None)
        COMPOSE_PARAMS.pop(RID, None)
        PLAY_PLAN.pop(RID, None)
        PLAY_PLAN_INDEX.pop(RID, None)
        if RID in DID_BEFORE:
            DID_BEFORE.remove(RID)
        LOG("Processor state cleared", {"RECORDING_ID": RID})
