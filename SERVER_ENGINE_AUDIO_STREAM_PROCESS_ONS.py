# SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS.py
# ----------------------------------------------------------------------
# Onsets & Frames processing for a single audio chunk.
# Responsibilities:
#   • Call O&F microservice with a WAV path (16 kHz mono is fine)
#   • Parse returned MIDI into (START_MS, END_MS, MIDI, VELOCITY)
#   • Convert CHUNK-relative times to ABSOLUTE by adding AUDIO_CHUNK_START_MS
#   • Bulk insert into ENGINE_LOAD_NOTE with SOURCE_METHOD='ONS'
#
# NOTE: Step-2 decides whether to call this (via YN_ONS) and will run
#       P_ENGINE_ALL_MASTER afterwards; this module only loads rows.
# ----------------------------------------------------------------------

import os
import json
import traceback
from pathlib import Path
from typing import Any, Iterable, List, Tuple

import builtins as _bi
import requests

try:
    import pretty_midi  # MIDI parsing
except Exception:  # pragma: no cover
    pretty_midi = None

# ─────────────────────────────────────────────────────────────
# Console logging (ASCII-safe)
# ─────────────────────────────────────────────────────────────
def CONSOLE_LOG(L_MSG: str, L_OBJ: Any = None):
    L_PREFIX = "PROCESS_ONS"
    try:
        if L_OBJ is None:
            print(f"{L_PREFIX} - {L_MSG}", flush=True)
        else:
            print(f"{L_PREFIX} - {L_MSG} {L_OBJ}", flush=True)
    except Exception:
        try:
            L_S = f"{L_PREFIX} - {L_MSG} {L_OBJ}".encode("utf-8", "replace").decode("ascii", "ignore")
            print(L_S, flush=True)
        except Exception:
            print(f"{L_PREFIX} - {L_MSG}", flush=True)

# ─────────────────────────────────────────────────────────────
# DB helpers (ODBC)
# ─────────────────────────────────────────────────────────────
def DB_GET_CONN():
    import pyodbc  # type: ignore
    L_CONN_STR = os.getenv("VIOLIN_ODBC", "")
    if not L_CONN_STR:
        raise RuntimeError("VIOLIN_ODBC not set (ODBC connection string).")
    return pyodbc.connect(L_CONN_STR, autocommit=True)

def DB_BULK_INSERT(L_CONN, L_SQL: str, L_ROWS: Iterable[tuple]) -> None:
    L_ROWS_LIST = list(L_ROWS)
    if not L_ROWS_LIST:
        return
    L_CUR = L_CONN.cursor()
    L_CUR.fast_executemany = True
    L_CUR.executemany(L_SQL, L_ROWS_LIST)

def DB_LOAD_NOTE_ROWS(
    L_CONN,
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    NOTE_ROWS_ARRAY: Iterable[Tuple[int, int, int, int, str]],
) -> None:
    """
    Insert rows into ENGINE_LOAD_NOTE:
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS,
       NOTE_MIDI_PITCH_NO, VOLUME_MIDI_VELOCITY_NO, SOURCE_METHOD)
    """
    L_SQL = """
      INSERT INTO ENGINE_LOAD_NOTE
      (RECORDING_ID, AUDIO_CHUNK_NO, START_MS, END_MS,
       NOTE_MIDI_PITCH_NO, VOLUME_MIDI_VELOCITY_NO, SOURCE_METHOD)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    DB_BULK_INSERT(
        L_CONN,
        L_SQL,
        (
            (RECORDING_ID, AUDIO_CHUNK_NO, L_S, L_E, L_MIDI, L_VEL, L_SRC)
            for (L_S, L_E, L_MIDI, L_VEL, L_SRC) in NOTE_ROWS_ARRAY
        ),
    )

# ─────────────────────────────────────────────────────────────
# Microservice call (Onsets & Frames)
# ─────────────────────────────────────────────────────────────
def ONS_MICROSERVICE_URL() -> str:
    L_HOST = os.getenv("OAF_HOST", "127.0.0.1")
    L_PORT = int(os.getenv("OAF_PORT", "9077"))
    return f"http://{L_HOST}:{L_PORT}"

def ONS_RUN_MICROSERVICE_AND_GET_MIDI_PATH(WAV_PATH: Path) -> Path | None:
    """
    POST {url}/transcribe  with  {"audio_path": "<abs path>"}
    Expects JSON: {"ok": true, "midi_path": "<abs path>"}
    """
    L_URL = ONS_MICROSERVICE_URL().rstrip("/") + "/transcribe"
    L_PAYLOAD = {"audio_path": _bi.str(WAV_PATH)}
    CONSOLE_LOG("ONS_HTTP_POST", {"url": L_URL, "payload": L_PAYLOAD})
    L_RESP = requests.post(L_URL, json=L_PAYLOAD, timeout=120)
    if not L_RESP.ok:
        CONSOLE_LOG("ONS_HTTP_ERROR", {"status": L_RESP.status_code, "text": L_RESP.text[:300]})
        return None
    try:
        L_DATA = L_RESP.json()
    except Exception:
        CONSOLE_LOG("ONS_BAD_JSON", {"text": L_RESP.text[:300]})
        return None
    if not L_DATA.get("ok"):
        CONSOLE_LOG("ONS_SERVICE_NOT_OK", L_DATA)
        return None
    L_MIDI = Path(_bi.str(L_DATA.get("midi_path", ""))).resolve()
    if not L_MIDI.exists():
        CONSOLE_LOG("ONS_MIDI_NOT_FOUND", {"midi_path": _bi.str(L_MIDI)})
        return None
    return L_MIDI

# ─────────────────────────────────────────────────────────────
# MIDI → note rows (relative to the chunk WAV)
# ─────────────────────────────────────────────────────────────
def ONS_PARSE_MIDI_TO_NOTE_ROWS_RELATIVE(MIDI_PATH: Path) -> List[Tuple[int, int, int, int]]:
    """
    Returns rows (relative to the given WAV):
      [(START_MS_REL, END_MS_REL, MIDI_PITCH, VELOCITY), ...]
    """
    if pretty_midi is None:
        CONSOLE_LOG("PRETTY_MIDI_MISSING")
        return []
    try:
        L_PM = pretty_midi.PrettyMIDI(_bi.str(MIDI_PATH))
    except Exception as L_EXC:
        CONSOLE_LOG("PRETTY_MIDI_LOAD_FAILED", {"err": _bi.str(L_EXC)})
        return []

    L_ROWS: List[Tuple[int, int, int, int]] = []
    for L_INST in L_PM.instruments:
        for L_N in L_INST.notes:
            L_S = int(round(float(L_N.start) * 1000.0))
            L_E = int(round(float(L_N.end)   * 1000.0))
            L_ROWS.append((L_S, L_E, int(L_N.pitch), int(L_N.velocity)))
    return L_ROWS

# ─────────────────────────────────────────────────────────────
# PUBLIC ENTRY
# ─────────────────────────────────────────────────────────────
def SERVER_ENGINE_AUDIO_STREAM_PROCESS_ONS(
    RECORDING_ID: int,
    AUDIO_CHUNK_NO: int,
    WAV16K_PATH: str,
    AUDIO_CHUNK_START_MS: int,
) -> None:
    """
    Step-2 calls this if YN_ONS='Y'.

    Inputs:
      • RECORDING_ID, AUDIO_CHUNK_NO
      • WAV16K_PATH: absolute path to the chunk's mono WAV (16 kHz is fine)
      • AUDIO_CHUNK_START_MS: absolute ms offset for this chunk

    Behavior:
      • Calls microservice to transcribe WAV → MIDI
      • Parses MIDI → relative rows
      • Offsets to ABSOLUTE times using AUDIO_CHUNK_START_MS
      • Bulk-inserts into ENGINE_LOAD_NOTE with SOURCE_METHOD='ONS'
    """
    try:
        L_WAV = Path(_bi.str(WAV16K_PATH)).resolve()
        if not L_WAV.exists():
            CONSOLE_LOG("WAV_NOT_FOUND", {"path": _bi.str(L_WAV)})
            return

        CONSOLE_LOG("ONS_BEGIN", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
            "WAV16K_PATH": _bi.str(L_WAV),
            "AUDIO_CHUNK_START_MS": int(AUDIO_CHUNK_START_MS),
        })

        L_MIDI_PATH = ONS_RUN_MICROSERVICE_AND_GET_MIDI_PATH(L_WAV)
        if not L_MIDI_PATH:
            CONSOLE_LOG("ONS_SKIPPED_NO_MIDI")
            return

        L_ROWS_REL = ONS_PARSE_MIDI_TO_NOTE_ROWS_RELATIVE(L_MIDI_PATH)
        CONSOLE_LOG("ONS_ROWS_RELATIVE", {"count": len(L_ROWS_REL)})

        if not L_ROWS_REL:
            return

        # Convert to ABSOLUTE ms and add SOURCE_METHOD
        L_ROWS_ABS_WITH_SRC: List[Tuple[int, int, int, int, str]] = []
        for (L_S_REL, L_E_REL, L_MIDI, L_VEL) in L_ROWS_REL:
            L_S_ABS = int(AUDIO_CHUNK_START_MS) + int(L_S_REL)
            L_E_ABS = int(AUDIO_CHUNK_START_MS) + int(L_E_REL)
            L_ROWS_ABS_WITH_SRC.append((L_S_ABS, L_E_ABS, int(L_MIDI), int(L_VEL), "ONS"))

        with DB_GET_CONN() as L_CONN:
            DB_LOAD_NOTE_ROWS(
                L_CONN=L_CONN,
                RECORDING_ID=int(RECORDING_ID),
                AUDIO_CHUNK_NO=int(AUDIO_CHUNK_NO),
                NOTE_ROWS_ARRAY=L_ROWS_ABS_WITH_SRC,
            )

        CONSOLE_LOG("ONS_DB_INSERT_OK", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
            "ROW_COUNT": len(L_ROWS_ABS_WITH_SRC),
        })

    except Exception as L_EXC:
        CONSOLE_LOG("ONS_FATAL_ERROR", {
            "ERROR": _bi.str(L_EXC),
            "TRACE": traceback.format_exc(),
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(AUDIO_CHUNK_NO),
        })
