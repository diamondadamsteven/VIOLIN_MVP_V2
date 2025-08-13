# SERVER_ENGINE_AUDIO_STREAM_PROCESSOR.py
# ------------------------------------------------------------
# Processor for streamed frames → engine chunks, feature loads,
# and final export. Uses Option A microservice for Onsets&Frames.
# ------------------------------------------------------------

import os
import json
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List, Iterable

import numpy as np
import pyodbc
import requests
import pretty_midi

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
#   "YN_RUN_FFT": 'Y'|'N',
#   "YN_RUN_ONS": 'Y'|'N',
#   "YN_RUN_PYIN": 'Y'|'N',
#   "YN_RUN_CREPE": 'Y'|'N',
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
# def STEP_1_GET_RECORDING_CONTEXT(CONN, RECORDING_ID: str) -> Dict[str, Any]:
#     LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR.STEP_1_GET_RECORDING_CONTEXT",
#         {"RECORDING_ID": RECORDING_ID})
#     if RECORDING_ID in CONTEXT:
#         return CONTEXT[RECORDING_ID]

#     # Replace this block with your real proc P_ENGINE_ALL_RECORDING_PARAMETERS_GET
#     CUR = CONN.cursor()
#     CUR.execute("EXEC P_ENGINE_ALL_RECORDING_PARAMETERS_GET @RECORDING_ID = ?", (int(RECORDING_ID),))
#     ROW = CUR.fetchone()
#     ctx = {
#       "VIOLINIST_ID": ROW.VIOLINIST_ID,
#       "COMPOSE_PLAY_OR_PRACTICE": ROW.COMPOSE_PLAY_OR_PRACTICE,
#       "AUDIO_STREAM_FILE_NAME": ROW.AUDIO_STREAM_FILE_NAME
#     }

#     CONTEXT[RECORDING_ID] = ctx
#     return ctx

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

def _LOAD_HZ(CONN, RECORDING_ID: int, AUDIO_CHUNK_NO: int, START_MS: int, END_MS: int,
             SOURCE_METHOD: str, HZ_ROWS: Iterable[Tuple[float, float]]):
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._LOAD_HZ",
        {"RECORDING_ID": RECORDING_ID, "CHUNK": AUDIO_CHUNK_NO, "SRC": SOURCE_METHOD})
    SQL = """
      INSERT INTO ENGINE_LOAD_HZ
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS, SOURCE_METHOD, HZ, CONFIDENCE)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    PACK = (
        (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS, SOURCE_METHOD, hz, conf)
        for (hz, conf) in HZ_ROWS
    )
    _BULK_INSERT(CONN, SQL, PACK)

def _LOAD_FFT(CONN, RECORDING_ID: int, AUDIO_CHUNK_NO: int, START_MS: int, END_MS: int,
              FFT_ROWS: Iterable[Tuple[int, float, float, float, float]]):
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._LOAD_FFT",
        {"RECORDING_ID": RECORDING_ID, "CHUNK": AUDIO_CHUNK_NO})
    SQL = """
      INSERT INTO ENGINE_LOAD_FFT
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS,
       FFT_BUCKET_NO, HZ_START, HZ_END, FFT_BUCKET_SIZE_IN_HZ, FFT_VALUE)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    PACK = (
        (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS, bno, hz0, hz1, bsz, val)
        for (bno, hz0, hz1, bsz, val) in FFT_ROWS
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
    SQL = """
      INSERT INTO ENGINE_LOAD_VOLUME_10_MS
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS, VOLUME, VOLUME_IN_DB)
      VALUES (?, ?, ?, ?, ?, ?)
    """
    PACK = (
        (RECORDING_ID, AUDIO_CHUNK_NO, s, e, v, vdb) for (s, e, v, vdb) in VOL_SERIES
    )
    _BULK_INSERT(CONN, SQL, PACK)

# =========================
# Feature implementations (placeholders where needed)
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
            timeout=120,  # large enough for O&F on small chunks
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

def _COMPUTE_ONS_VIA_MICROSERVICE(CHUNK_WAV: Path) -> List[Tuple[int, int, int, int, str]]:
    midi = _RUN_ONSETS_AND_FRAMES_MICROSERVICE(CHUNK_WAV)
    if not midi:
        return []
    return _PARSE_MIDI_TO_NOTES(midi)

def _COMPUTE_FFT(AUDIO: np.ndarray, SR: int):
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._COMPUTE_FFT")
    return []

def _COMPUTE_PYIN(AUDIO: np.ndarray, SR: int):
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._COMPUTE_PYIN")
    return []

def _COMPUTE_CREPE(AUDIO: np.ndarray, SR: int):
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._COMPUTE_CREPE")
    return []

def _COMPUTE_VOLUME(AUDIO: np.ndarray, SR: int):
    LOG("Start function SERVER_ENGINE_AUDIO_STREAM_PROCESSOR._COMPUTE_VOLUME")
    return None, []

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
        # CTX = STEP_1_GET_RECORDING_CONTEXT(CONN, RID)
        # MODE = str(CTX["COMPOSE_PLAY_OR_PRACTICE"]).upper()
        if RID not in CONTEXT:
            REGISTER_RECORDING_CONTEXT_HINT (RID)
        CTX = CONTEXT[RID]
        MODE = str(CTX["COMPOSE_PLAY_OR_PRACTICE"]).upper()

        # if RID not in DID_BEFORE:
        #     _EXEC_PROC(CONN, "P_ENGINE_ALL_BEFORE", {"RECORDING_ID": int(RID)})
        #     DID_BEFORE.add(RID)

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
                # chunk 1 starts at 0..CHUNK_MS-1
                start_ms = (AUDIO_CHUNK_NO - 1) * CHUNK_MS
                end_ms = start_ms + CHUNK_MS - 1

                if not _WINDOW_COVERED(RID, start_ms, end_ms):
                    break

                # Export exact chunk window to WAV (for O&F microservice)
                chunk_wav = TMP_CHUNKS_DIR / f"{RID}_compose_{AUDIO_CHUNK_NO:06d}.wav"
                ok = _EXPORT_CHUNK_WAV_FROM_FRAMES(RID, start_ms, end_ms, chunk_wav)
                if not ok:
                    # Should not happen if coverage was true, but guard anyway
                    break

                # FFT
                if (YN_FFT or "N").upper() == "Y":
                    # If/when FFT computes bucket rows, load via _LOAD_FFT and call method:
                    FFT_ROWS = _COMPUTE_FFT(np.zeros(1, dtype=np.float32), 48000)
                    _LOAD_FFT(CONN, int(RID), AUDIO_CHUNK_NO, start_ms, end_ms, FFT_ROWS)
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
                    NOTE_ROWS = _COMPUTE_ONS_VIA_MICROSERVICE(chunk_wav)
                    _LOAD_NOTE(CONN, int(RID), AUDIO_CHUNK_NO, NOTE_ROWS)
                    # _EXEC_PROC(CONN, "P_ENGINE_ALL_METHOD_ONS", {
                    #     "RECORDING_ID": int(RID),
                    #     "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO,
                    # })

                if flags.get("YN_RUN_PYIN", "N") == "Y":
                    HZ_ROWS = _COMPUTE_PYIN(np.zeros(1, dtype=np.float32), 48000)
                    _LOAD_HZ(CONN, int(RID), AUDIO_CHUNK_NO, start_ms, end_ms, "PYIN", HZ_ROWS)

                if flags.get("YN_RUN_CREPE", "N") == "Y":
                    HZ_ROWS = _COMPUTE_CREPE(np.zeros(1, dtype=np.float32), 48000)
                    _LOAD_HZ(CONN, int(RID), AUDIO_CHUNK_NO, start_ms, end_ms, "CREPE", HZ_ROWS)

                # if flags.get("YN_RUN_PYIN", "N") == "Y" or flags.get("YN_RUN_CREPE", "N") == "Y":
                #     _EXEC_PROC(CONN, "P_ENGINE_ALL_METHOD_CREPE_AND_PYIN", {
                #         "RECORDING_ID": int(RID),
                #         "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO,
                #     })

                # Volume (placeholder)
                VOL_AGG, VOL_SERIES = _COMPUTE_VOLUME(np.zeros(1, dtype=np.float32), 48000)
                _LOAD_VOLUME(CONN, int(RID), AUDIO_CHUNK_NO, start_ms, VOL_AGG)
                _LOAD_VOLUME_10MS(CONN, int(RID), AUDIO_CHUNK_NO, VOL_SERIES)

                # Master
                _EXEC_PROC(CONN, "P_ENGINE_ALL_MASTER", {
                    "VIOLINIST_ID": int(CTX["VIOLINIST_ID"]),
                    "RECORDING_ID": int(RID),
                    "COMPOSE_PLAY_OR_PRACTICE": "COMPOSE",
                    "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO,
                    "YN_RECORDING_STOPPED": None,
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

                # Export exact window to WAV for O&F
                chunk_wav = TMP_CHUNKS_DIR / f"{RID}_{MODE.lower()}_{AUDIO_CHUNK_NO:06d}.wav"
                ok = _EXPORT_CHUNK_WAV_FROM_FRAMES(RID, start_ms, end_ms, chunk_wav)
                if not ok:
                    break

                # FFT
                if row.get("YN_RUN_FFT", "N") == "Y":
                    FFT_ROWS = _COMPUTE_FFT(np.zeros(1, dtype=np.float32), 48000)
                    _LOAD_FFT(CONN, int(RID), AUDIO_CHUNK_NO, start_ms, end_ms, FFT_ROWS)

                # ONS
                if row.get("YN_RUN_ONS", "N") == "Y":
                    NOTE_ROWS = _COMPUTE_ONS_VIA_MICROSERVICE(chunk_wav)
                    _LOAD_NOTE(CONN, int(RID), AUDIO_CHUNK_NO, NOTE_ROWS)
                    # _EXEC_PROC(CONN, "P_ENGINE_ALL_METHOD_ONS", {
                    #     "RECORDING_ID": int(RID),
                    #     "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO,
                    # })

                # PYIN / CREPE
                did_pitch = False
                if row.get("YN_RUN_PYIN", "N") == "Y":
                    HZ_ROWS = _COMPUTE_PYIN(np.zeros(1, dtype=np.float32), 48000)
                    _LOAD_HZ(CONN, int(RID), AUDIO_CHUNK_NO, start_ms, end_ms, "PYIN", HZ_ROWS)
                    did_pitch = True
                if row.get("YN_RUN_CREPE", "N") == "Y":
                    HZ_ROWS = _COMPUTE_CREPE(np.zeros(1, dtype=np.float32), 48000)
                    _LOAD_HZ(CONN, int(RID), AUDIO_CHUNK_NO, start_ms, end_ms, "CREPE", HZ_ROWS)
                    did_pitch = True
                # if did_pitch:
                #     _EXEC_PROC(CONN, "P_ENGINE_ALL_METHOD_CREPE_AND_PYIN", {
                #         "RECORDING_ID": int(RID),
                #         "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO,
                #     })

                # Volume (placeholder)
                VOL_AGG, VOL_SERIES = _COMPUTE_VOLUME(np.zeros(1, dtype=np.float32), 48000)
                _LOAD_VOLUME(CONN, int(RID), AUDIO_CHUNK_NO, start_ms, VOL_AGG)
                _LOAD_VOLUME_10MS(CONN, int(RID), AUDIO_CHUNK_NO, VOL_SERIES)

                # Master
                _EXEC_PROC(CONN, "P_ENGINE_ALL_MASTER", {
                    "VIOLINIST_ID": int(CTX["VIOLINIST_ID"]),
                    "RECORDING_ID": int(RID),
                    "COMPOSE_PLAY_OR_PRACTICE": MODE,
                    "AUDIO_CHUNK_NO": AUDIO_CHUNK_NO,
                    "YN_RECORDING_STOPPED": None,
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
#         CTX = STEP_1_GET_RECORDING_CONTEXT(CONN, RID)
#         MODE = str(CTX["COMPOSE_PLAY_OR_PRACTICE"]).upper()
        if RID not in CONTEXT:
            REGISTER_RECORDING_CONTEXT_HINT (RID)
        CTX = CONTEXT[RID]

        final_path = FINALIZE_RECORDING_EXPORT(RID, CTX.get("AUDIO_STREAM_FILE_NAME"))
        LOG("Final WAV path", {"path": final_path})

        _EXEC_PROC(CONN, "P_ENGINE_RECORD_END", {
            "RECORDING_ID": int(RID)
        })

#         _EXEC_PROC(CONN, "P_ENGINE_ALL_MASTER", {
#             "VIOLINIST_ID": int(CTX["VIOLINIST_ID"]),
#             "RECORDING_ID": int(RID),
#             "COMPOSE_PLAY_OR_PRACTICE": MODE,
#             "AUDIO_CHUNK_NO": None,
#             "YN_RECORDING_STOPPED": "Y",
#         })

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
