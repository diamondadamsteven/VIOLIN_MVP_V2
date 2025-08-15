# SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_1_CONCATENATE.py
# ----------------------------------------------------------------------
# Step-1 of the streaming pipeline:
#   - Load per-recording CONFIG from DB (once) -> config.json
#   - Watch temp folder for arriving .m4a frames
#   - For each complete audio-chunk, concatenate frames -> 48k mono WAV
#   - Delete consumed .m4a frames
#   - Invoke Step-2 processing on each WAV
#   - When STOP marker exists and all complete chunks are processed, invoke Step-3
# ----------------------------------------------------------------------

import os
import json
import asyncio
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import builtins as _bi
import traceback

# Call the public entries directly (no aliasing)
from SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_2_AUDIO_CHUNKS import (
    SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_2_AUDIO_CHUNKS,
)
from SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_3_STOP import (
    SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_3_STOP,
)

# ─────────────────────────────────────────────────────────────
# Console logging (ASCII-safe)
# ─────────────────────────────────────────────────────────────
def CONSOLE_LOG(L_MSG: str, L_OBJ: Any = None):
    L_PREFIX = "STEP_1_CONCATENATE"
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
# Paths & markers
# ─────────────────────────────────────────────────────────────
BASE_TEMP_DIR = Path(os.getenv("AUDIO_TMP_DIR", "./tmp/active_recordings")).resolve()
BASE_TEMP_DIR.mkdir(parents=True, exist_ok=True)

def FILES_GET_RECORDING_DIR(RECORDING_ID: str) -> Path:
    L_DIR = BASE_TEMP_DIR / str(RECORDING_ID)
    L_DIR.mkdir(parents=True, exist_ok=True)
    return L_DIR

def FILES_GET_STOP_MARKER_PATH(RECORDING_ID: str) -> Path:
    return FILES_GET_RECORDING_DIR(RECORDING_ID) / "_STOP"

def FILES_GET_CONFIG_JSON_PATH(RECORDING_ID: str) -> Path:
    return FILES_GET_RECORDING_DIR(RECORDING_ID) / "config.json"

def FILES_GET_AUDIO_CHUNKS_CATALOG_JSON_PATH(RECORDING_ID: str) -> Path:
    # Catalog of already-built chunks (replaces prior "*index*.json" naming)
    return FILES_GET_RECORDING_DIR(RECORDING_ID) / "audio_chunks_catalog.json"

def FILES_GET_CHUNK_WAV48K_PATH(RECORDING_ID: str, AUDIO_CHUNK_NO: int) -> Path:
    return FILES_GET_RECORDING_DIR(RECORDING_ID) / f"chunk_{AUDIO_CHUNK_NO:06d}_48k.wav"

def FILES_GET_FRAME_PATH(RECORDING_ID: str, FRAME_NO: int) -> Path:
    return FILES_GET_RECORDING_DIR(RECORDING_ID) / f"{FRAME_NO:08d}.m4a"

# ─────────────────────────────────────────────────────────────
# DB helpers (swap with your shared helpers if desired)
# ─────────────────────────────────────────────────────────────
def DB_GET_CONN():
    """
    Returns an autocommit ODBC connection using env var VIOLIN_ODBC.
    """
    import pyodbc  # type: ignore
    L_CONN_STR = os.getenv("VIOLIN_ODBC", "")
    if not L_CONN_STR:
        raise RuntimeError("VIOLIN_ODBC not set (ODBC connection string).")
    return pyodbc.connect(L_CONN_STR, autocommit=True)

def DB_EXEC_SP_ROWS(L_CONN, SP_NAME: str, **PARAMS) -> List[Dict[str, Any]]:
    """
    Execute stored procedure and return rows as list of dicts.
    Uses positional "?" parameters in the call order of supplied kwargs.
    """
    L_CUR = L_CONN.cursor()
    L_ARGS = list(PARAMS.values())
    L_PLACEHOLDERS = ",".join(["?"] * len(L_ARGS))
    L_SQL = f"EXEC {SP_NAME} {L_PLACEHOLDERS}" if L_PLACEHOLDERS else f"EXEC {SP_NAME}"
    L_CUR.execute(L_SQL, L_ARGS)
    try:
        L_COLS = [c[0] for c in L_CUR.description]  # type: ignore
        return [dict(zip(L_COLS, L_ROW)) for L_ROW in L_CUR.fetchall()]
    except Exception:
        return []  # SP may not return a result set

def DB_EXEC_SP_ROW(L_CONN, SP_NAME: str, **PARAMS) -> Dict[str, Any]:
    L_ROWS = DB_EXEC_SP_ROWS(L_CONN, SP_NAME, **PARAMS)
    return L_ROWS[0] if L_ROWS else {}

# ─────────────────────────────────────────────────────────────
# PUBLIC: STEP-1 config load for a recording
# ─────────────────────────────────────────────────────────────
async def STEP_1_NEW_RECORDING_STARTED(RECORDING_ID: str) -> None:
    """
    1) P_ENGINE_ALL_RECORDING_PARAMETERS_GET
    2) If COMPOSE -> P_ENGINE_SONG_AUDIO_CHUNK_FOR_COMPOSE_GET
       Else (PLAY/PRACTICE) -> P_ENGINE_SONG_AUDIO_CHUNK_FOR_PLAY_AND_PRACTICE_GET
    3) Save config.json into the recording temp dir (ALL-CAPS keys; *_ARRAY/*_DICT suffixes).
    """
    CONSOLE_LOG("STEP_1_NEW_RECORDING_STARTED.begin", {"RECORDING_ID": RECORDING_ID})

    def DB_LOAD_RECORDING_CONFIG(L_RECORDING_ID: str) -> Dict[str, Any]:
        with DB_GET_CONN() as L_CONN:
            RES_SET_P_ENGINE_ALL_RECORDING_PARAMETERS_GET = DB_EXEC_SP_ROW(
                L_CONN, "P_ENGINE_ALL_RECORDING_PARAMETERS_GET", RECORDING_ID=int(L_RECORDING_ID)
            )

            L_COMPOSE_PLAY_OR_PRACTICE = _bi.str(
                (RES_SET_P_ENGINE_ALL_RECORDING_PARAMETERS_GET.get("COMPOSE_PLAY_OR_PRACTICE") or "")
            ).upper().strip()

            VIOLINIST_ID = RES_SET_P_ENGINE_ALL_RECORDING_PARAMETERS_GET.get("VIOLINIST_ID")
            AUDIO_STREAM_FILE_NAME = RES_SET_P_ENGINE_ALL_RECORDING_PARAMETERS_GET.get("AUDIO_STREAM_FILE_NAME")
            AUDIO_STREAM_FRAME_SIZE_IN_MS = RES_SET_P_ENGINE_ALL_RECORDING_PARAMETERS_GET.get("AUDIO_STREAM_FRAME_SIZE_IN_MS")

            L_CONFIG_DICT: Dict[str, Any] = {
                "RECORDING_ID": L_RECORDING_ID,
                "VIOLINIST_ID": VIOLINIST_ID,
                "COMPOSE_PLAY_OR_PRACTICE": L_COMPOSE_PLAY_OR_PRACTICE,
                "AUDIO_STREAM_FILE_NAME": AUDIO_STREAM_FILE_NAME,
                "AUDIO_STREAM_FRAME_SIZE_IN_MS": AUDIO_STREAM_FRAME_SIZE_IN_MS,
            }

            if L_COMPOSE_PLAY_OR_PRACTICE == "COMPOSE":
                RES_SET_P_ENGINE_SONG_AUDIO_CHUNK_FOR_COMPOSE_GET = DB_EXEC_SP_ROW(
                    L_CONN, "P_ENGINE_SONG_AUDIO_CHUNK_FOR_COMPOSE_GET", RECORDING_ID=int(L_RECORDING_ID)
                )
                L_CONFIG_DICT["COMPOSE_DICT"] = {
                    "AUDIO_CHUNK_DURATION_IN_MS": int(RES_SET_P_ENGINE_SONG_AUDIO_CHUNK_FOR_COMPOSE_GET.get("AUDIO_CHUNK_DURATION_IN_MS")),
                    "CNT_FRAMES_PER_AUDIO_CHUNK": int(RES_SET_P_ENGINE_SONG_AUDIO_CHUNK_FOR_COMPOSE_GET.get("CNT_FRAMES_PER_AUDIO_CHUNK")),
                    "YN_RUN_FFT": (RES_SET_P_ENGINE_SONG_AUDIO_CHUNK_FOR_COMPOSE_GET.get("YN_RUN_FFT") or None),
                }
            else:
                RES_SET_P_ENGINE_SONG_AUDIO_CHUNK_FOR_PLAY_AND_PRACTICE_GET_ARRAY = DB_EXEC_SP_ROWS(
                    L_CONN, "P_ENGINE_SONG_AUDIO_CHUNK_FOR_PLAY_AND_PRACTICE_GET", RECORDING_ID=int(L_RECORDING_ID)
                )
                L_AUDIO_CHUNKS_ARRAY: List[Dict[str, Any]] = []
                for L_ROW in RES_SET_P_ENGINE_SONG_AUDIO_CHUNK_FOR_PLAY_AND_PRACTICE_GET_ARRAY:
                    L_AUDIO_CHUNKS_ARRAY.append({
                        "AUDIO_CHUNK_NO": int(L_ROW.get("AUDIO_CHUNK_NO")),
                        "AUDIO_CHUNK_DURATION_IN_MS": int(L_ROW.get("AUDIO_CHUNK_DURATION_IN_MS")),
                        "START_MS": int(L_ROW.get("START_MS")),
                        "END_MS": int(L_ROW.get("END_MS")),
                        "MIN_AUDIO_STREAM_FRAME_NO": int(L_ROW.get("MIN_AUDIO_STREAM_FRAME_NO")),
                        "MAX_AUDIO_STREAM_FRAME_NO": int(L_ROW.get("MAX_AUDIO_STREAM_FRAME_NO")),
                        "YN_RUN_FFT": (L_ROW.get("YN_RUN_FFT") or None),
                        "YN_RUN_ONS": (L_ROW.get("YN_RUN_ONS") or None),
                        "YN_RUN_PYIN": (L_ROW.get("YN_RUN_PYIN") or None),
                        "YN_RUN_CREPE": (L_ROW.get("YN_RUN_CREPE") or None),
                    })
                L_CONFIG_DICT["PLAY_PRACTICE_DICT"] = {"AUDIO_CHUNKS_ARRAY": L_AUDIO_CHUNKS_ARRAY}

            FILES_GET_CONFIG_JSON_PATH(L_RECORDING_ID).write_text(json.dumps(L_CONFIG_DICT, indent=2), encoding="utf-8")
            return L_CONFIG_DICT

    try:
        L_CONFIG_DICT = await asyncio.to_thread(DB_LOAD_RECORDING_CONFIG, RECORDING_ID)
        CONSOLE_LOG("CONFIG_SAVED", {"CONFIG_PATH": _bi.str(FILES_GET_CONFIG_JSON_PATH(RECORDING_ID))})
        CONSOLE_LOG("CONFIG_SUMMARY", {
            "COMPOSE_PLAY_OR_PRACTICE": L_CONFIG_DICT.get("COMPOSE_PLAY_OR_PRACTICE"),
            "AUDIO_STREAM_FRAME_SIZE_IN_MS": L_CONFIG_DICT.get("AUDIO_STREAM_FRAME_SIZE_IN_MS"),
            "COMPOSE_PRESENT": "COMPOSE_DICT" in L_CONFIG_DICT,
            "PLAY_PRACTICE_AUDIO_CHUNKS_COUNT":
                len(L_CONFIG_DICT.get("PLAY_PRACTICE_DICT", {}).get("AUDIO_CHUNKS_ARRAY", []))
                if L_CONFIG_DICT.get("COMPOSE_PLAY_OR_PRACTICE") in ("PLAY", "PRACTICE") else None,
        })
    except Exception as L_EXC:
        CONSOLE_LOG("STEP_1_NEW_RECORDING_STARTED.error", {"ERROR": _bi.str(L_EXC), "TRACE": traceback.format_exc()})
        raise

# ─────────────────────────────────────────────────────────────
# PUBLIC: STEP-2 chunk loop driver (concatenate & dispatch)
# ─────────────────────────────────────────────────────────────
async def STEP_2_CREATE_AUDIO_CHUNKS(RECORDING_ID: str) -> None:
    """
    Iterate audio-chunk-by-audio-chunk; when all frames for a chunk are present:
      - Concatenate -> WAV 48k mono
      - Delete consumed .m4a frames
      - Call Step-2 processing (public entry)
    When STOP marker exists and all complete chunks processed, call Step-3 (public entry).
    """
    CONSOLE_LOG("STEP_2_CREATE_AUDIO_CHUNKS.begin", {"RECORDING_ID": RECORDING_ID})
    L_CONFIG_PATH = FILES_GET_CONFIG_JSON_PATH(RECORDING_ID)

    # Load CONFIG
    try:
        L_CONFIG_DICT = json.loads(L_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as L_EXC:
        CONSOLE_LOG("CONFIG_LOAD_FAILED", {"ERROR": _bi.str(L_EXC), "CONFIG_PATH": _bi.str(L_CONFIG_PATH)})
        raise

    L_COMPOSE_PLAY_OR_PRACTICE = _bi.str(L_CONFIG_DICT.get("COMPOSE_PLAY_OR_PRACTICE") or "").upper()
    L_AUDIO_CHUNKS_CATALOG_ARRAY = SAVING_AUDIO_LOAD_AUDIO_CHUNKS_CATALOG_ARRAY(RECORDING_ID)
    L_NEXT_AUDIO_CHUNK_NO = SAVING_AUDIO_NEXT_CHUNK_NO_FROM_CATALOG(L_AUDIO_CHUNKS_CATALOG_ARRAY)
    CONSOLE_LOG("STARTING_AT_AUDIO_CHUNK", {"NEXT_AUDIO_CHUNK_NO": L_NEXT_AUDIO_CHUNK_NO})

    try:
        while True:
            (L_FRAME_RANGE_TUPLE,
             L_TIMING_TUPLE,
             L_RUN_FLAGS_DICT) = CONFIG_GET_CHUNK_SPEC(L_CONFIG_DICT, L_COMPOSE_PLAY_OR_PRACTICE, L_NEXT_AUDIO_CHUNK_NO)

            if L_FRAME_RANGE_TUPLE is None:
                # No such chunk in CONFIG (PLAY/PRACTICE end). If STOP seen, we can finish.
                if FILES_GET_STOP_MARKER_PATH(RECORDING_ID).exists():
                    CONSOLE_LOG("STOP_SEEN_NO_FURTHER_AUDIO_CHUNKS")
                    break
                await asyncio.sleep(0.1)
                continue

            L_MIN_AUDIO_STREAM_FRAME_NO, L_MAX_AUDIO_STREAM_FRAME_NO = L_FRAME_RANGE_TUPLE
            L_AUDIO_CHUNK_START_MS, L_AUDIO_CHUNK_END_MS, L_AUDIO_CHUNK_DURATION_IN_MS = L_TIMING_TUPLE

            L_READY = await FRAMES_AWAIT_RANGE_ON_DISK(RECORDING_ID, L_MIN_AUDIO_STREAM_FRAME_NO, L_MAX_AUDIO_STREAM_FRAME_NO)
            if not L_READY:
                # STOP marker hit before frames completed -> bail
                CONSOLE_LOG("STOP_BEFORE_AUDIO_CHUNK_COMPLETE", {"AUDIO_CHUNK_NO": L_NEXT_AUDIO_CHUNK_NO})
                break

            L_WAV48K_PATH = FILES_GET_CHUNK_WAV48K_PATH(RECORDING_ID, L_NEXT_AUDIO_CHUNK_NO)
            L_FRAME_FILE_PATHS_ARRAY = [FILES_GET_FRAME_PATH(RECORDING_ID, L_I) for L_I in range(L_MIN_AUDIO_STREAM_FRAME_NO, L_MAX_AUDIO_STREAM_FRAME_NO + 1)]
            await CONVERTING_AUDIO_STEP_1_CONCAT_M4A_TO_WAV48K(L_FRAME_FILE_PATHS_ARRAY, L_WAV48K_PATH)

            # Delete consumed frames
            for L_FRAME_FILE_PATH in L_FRAME_FILE_PATHS_ARRAY:
                try:
                    L_FRAME_FILE_PATH.unlink(missing_ok=True)
                except Exception as L_E:
                    CONSOLE_LOG("DELETE_FRAME_FAILED_NON_FATAL", {"FILE": _bi.str(L_FRAME_FILE_PATH), "ERROR": _bi.str(L_E)})

            # Update audio-chunks catalog JSON
            L_AUDIO_CHUNKS_CATALOG_ARRAY.append({"AUDIO_CHUNK_NO": L_NEXT_AUDIO_CHUNK_NO, "WAV48K_PATH": _bi.str(L_WAV48K_PATH)})
            SAVING_AUDIO_SAVE_AUDIO_CHUNKS_CATALOG_ARRAY(RECORDING_ID, L_AUDIO_CHUNKS_CATALOG_ARRAY)

            # Call Step-2 processing (public entry), await if coroutine
            L_RES = SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_2_AUDIO_CHUNKS(
                RECORDING_ID=RECORDING_ID,
                AUDIO_CHUNK_NO=L_NEXT_AUDIO_CHUNK_NO,
                WAV48K_PATH=_bi.str(L_WAV48K_PATH),
                AUDIO_CHUNK_START_MS=L_AUDIO_CHUNK_START_MS,
                AUDIO_CHUNK_END_MS=L_AUDIO_CHUNK_END_MS,
                COMPOSE_PLAY_OR_PRACTICE=L_COMPOSE_PLAY_OR_PRACTICE,
                YN_FFT=L_RUN_FLAGS_DICT.get("YN_RUN_FFT"),
                YN_ONS=L_RUN_FLAGS_DICT.get("YN_RUN_ONS"),
                YN_PYIN=L_RUN_FLAGS_DICT.get("YN_RUN_PYIN"),
                YN_CREPE=L_RUN_FLAGS_DICT.get("YN_RUN_CREPE"),
            )
            if asyncio.iscoroutine(L_RES):
                await L_RES

            L_NEXT_AUDIO_CHUNK_NO += 1

            # If PLAY/PRACTICE: after final configured audio-chunk & STOP present -> exit
            if L_COMPOSE_PLAY_OR_PRACTICE in ("PLAY", "PRACTICE"):
                L_TOTAL_CHUNKS = len(L_CONFIG_DICT.get("PLAY_PRACTICE_DICT", {}).get("AUDIO_CHUNKS_ARRAY", []))
                if L_NEXT_AUDIO_CHUNK_NO > L_TOTAL_CHUNKS and FILES_GET_STOP_MARKER_PATH(RECORDING_ID).exists():
                    CONSOLE_LOG("ALL_AUDIO_CHUNKS_DONE_AND_STOP_SEEN")
                    break

            await asyncio.sleep(0)

    finally:
        # Call Step-3 (public entry), await if coroutine
        try:
            L_RES3 = SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_3_STOP(RECORDING_ID=RECORDING_ID)
            if asyncio.iscoroutine(L_RES3):
                await L_RES3
        except Exception as L_EXC:
            CONSOLE_LOG("STEP_3_STOP_ERROR", {"RECORDING_ID": RECORDING_ID, "ERROR": _bi.str(L_EXC), "TRACE": traceback.format_exc()})
        CONSOLE_LOG("STEP_2_CREATE_AUDIO_CHUNKS.end", {"RECORDING_ID": RECORDING_ID})

# ─────────────────────────────────────────────────────────────
# Helpers: audio-chunks catalog, config interpretation, waiting, ffmpeg
# ─────────────────────────────────────────────────────────────
def SAVING_AUDIO_LOAD_AUDIO_CHUNKS_CATALOG_ARRAY(RECORDING_ID: str) -> List[Dict[str, Any]]:
    L_JSON_PATH = FILES_GET_AUDIO_CHUNKS_CATALOG_JSON_PATH(RECORDING_ID)
    if L_JSON_PATH.exists():
        try:
            return json.loads(L_JSON_PATH.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []

def SAVING_AUDIO_SAVE_AUDIO_CHUNKS_CATALOG_ARRAY(RECORDING_ID: str, L_CATALOG_ARRAY: List[Dict[str, Any]]) -> None:
    FILES_GET_AUDIO_CHUNKS_CATALOG_JSON_PATH(RECORDING_ID).write_text(json.dumps(L_CATALOG_ARRAY, indent=2), encoding="utf-8")

def SAVING_AUDIO_NEXT_CHUNK_NO_FROM_CATALOG(L_CATALOG_ARRAY: List[Dict[str, Any]]) -> int:
    if not L_CATALOG_ARRAY:
        return 1
    return max(int(L_X.get("AUDIO_CHUNK_NO", 0)) for L_X in L_CATALOG_ARRAY) + 1

def CONFIG_GET_CHUNK_SPEC(
    L_CONFIG_DICT: Dict[str, Any],
    L_COMPOSE_PLAY_OR_PRACTICE: str,
    AUDIO_CHUNK_NO: int
) -> Tuple[Optional[Tuple[int, int]], Tuple[int, int, int], Dict[str, Optional[str]]]:
    """
    Returns:
      - frame_range: (MIN_AUDIO_STREAM_FRAME_NO, MAX_AUDIO_STREAM_FRAME_NO) or None if not in config
      - timing: (AUDIO_CHUNK_START_MS, AUDIO_CHUNK_END_MS, AUDIO_CHUNK_DURATION_IN_MS)
      - run_flags: dict with YN_RUN_FFT/ONS/PYIN/CREPE (may be None)
    """
    if L_COMPOSE_PLAY_OR_PRACTICE == "COMPOSE":
        L_COMPOSE_DICT = L_CONFIG_DICT.get("COMPOSE_DICT", {})
        CNT_FRAMES_PER_AUDIO_CHUNK = int(L_COMPOSE_DICT.get("CNT_FRAMES_PER_AUDIO_CHUNK"))
        AUDIO_CHUNK_DURATION_IN_MS = int(L_COMPOSE_DICT.get("AUDIO_CHUNK_DURATION_IN_MS"))

        L_MIN_AUDIO_STREAM_FRAME_NO = (AUDIO_CHUNK_NO - 1) * CNT_FRAMES_PER_AUDIO_CHUNK + 1
        L_MAX_AUDIO_STREAM_FRAME_NO = L_MIN_AUDIO_STREAM_FRAME_NO + CNT_FRAMES_PER_AUDIO_CHUNK - 1

        AUDIO_CHUNK_START_MS = (AUDIO_CHUNK_NO - 1) * AUDIO_CHUNK_DURATION_IN_MS
        AUDIO_CHUNK_END_MS = AUDIO_CHUNK_START_MS + AUDIO_CHUNK_DURATION_IN_MS - 1

        L_FLAGS = {
            "YN_RUN_FFT": L_COMPOSE_DICT.get("YN_RUN_FFT"),
            "YN_RUN_ONS": None,
            "YN_RUN_PYIN": None,
            "YN_RUN_CREPE": None,
        }
        return (L_MIN_AUDIO_STREAM_FRAME_NO, L_MAX_AUDIO_STREAM_FRAME_NO), (
            AUDIO_CHUNK_START_MS, AUDIO_CHUNK_END_MS, AUDIO_CHUNK_DURATION_IN_MS
        ), L_FLAGS

    # PLAY / PRACTICE
    L_AUDIO_CHUNKS_ARRAY = L_CONFIG_DICT.get("PLAY_PRACTICE_DICT", {}).get("AUDIO_CHUNKS_ARRAY", [])
    L_IDX = AUDIO_CHUNK_NO - 1
    if L_IDX < 0 or L_IDX >= len(L_AUDIO_CHUNKS_ARRAY):
        return None, (0, 0, 0), {}

    L_ROW = L_AUDIO_CHUNKS_ARRAY[L_IDX]
    L_FRAME_RANGE = (int(L_ROW["MIN_AUDIO_STREAM_FRAME_NO"]), int(L_ROW["MAX_AUDIO_STREAM_FRAME_NO"]))
    AUDIO_CHUNK_START_MS = int(L_ROW["START_MS"])
    AUDIO_CHUNK_END_MS = int(L_ROW["END_MS"])
    AUDIO_CHUNK_DURATION_IN_MS = int(L_ROW["AUDIO_CHUNK_DURATION_IN_MS"])
    L_FLAGS = {
        "YN_RUN_FFT": L_ROW.get("YN_RUN_FFT"),
        "YN_RUN_ONS": L_ROW.get("YN_RUN_ONS"),
        "YN_RUN_PYIN": L_ROW.get("YN_RUN_PYIN"),
        "YN_RUN_CREPE": L_ROW.get("YN_RUN_CREPE"),
    }
    return L_FRAME_RANGE, (AUDIO_CHUNK_START_MS, AUDIO_CHUNK_END_MS, AUDIO_CHUNK_DURATION_IN_MS), L_FLAGS

async def FRAMES_AWAIT_RANGE_ON_DISK(RECORDING_ID: str, MIN_AUDIO_STREAM_FRAME_NO: int, MAX_AUDIO_STREAM_FRAME_NO: int, POLL_MS: int = 100) -> bool:
    """
    Wait until every .m4a for [MIN_AUDIO_STREAM_FRAME_NO..MAX_AUDIO_STREAM_FRAME_NO] exists.
    If STOP marker appears first, return False.
    """
    L_NEEDED_SET = {L_I for L_I in range(MIN_AUDIO_STREAM_FRAME_NO, MAX_AUDIO_STREAM_FRAME_NO + 1)}
    L_STOP_PATH = FILES_GET_STOP_MARKER_PATH(RECORDING_ID)

    while L_NEEDED_SET:
        L_MISSING_ARRAY = [L_I for L_I in L_NEEDED_SET if not FILES_GET_FRAME_PATH(RECORDING_ID, L_I).exists()]
        if not L_MISSING_ARRAY:
            return True
        if L_STOP_PATH.exists():
            return False
        await asyncio.sleep(POLL_MS / 1000.0)

async def CONVERTING_AUDIO_STEP_1_CONCAT_M4A_TO_WAV48K(FRAME_FILE_PATHS_ARRAY: List[Path], OUT_WAV_PATH: Path) -> None:
    """
    Use ffmpeg concat demuxer to decode+concatenate .m4a frames -> 48k mono PCM WAV.
    """
    L_LIST_PATH = OUT_WAV_PATH.with_suffix(".concat.txt")
    L_TEXT = "".join([f"file '{CONVERTING_AUDIO_ESCAPE_FFMPEG_PATH(str(L_FRAME))}'\n" for L_FRAME in FRAME_FILE_PATHS_ARRAY])
    L_LIST_PATH.write_text(L_TEXT, encoding="utf-8")

    L_CMD = [
        "ffmpeg",
        "-v", "error",
        "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", str(L_LIST_PATH),
        "-ac", "1",
        "-ar", "48000",
        "-c:a", "pcm_s16le",
        str(OUT_WAV_PATH),
    ]
    CONSOLE_LOG("FFMPEG_CONCAT_TO_WAV48K", {"FRAMES": len(FRAME_FILE_PATHS_ARRAY), "OUT_WAV_PATH": str(OUT_WAV_PATH)})

    L_PROC = await asyncio.create_subprocess_exec(
        *L_CMD,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    L_STDOUT, L_STDERR = await L_PROC.communicate()
    if L_PROC.returncode != 0:
        CONSOLE_LOG("FFMPEG_ERROR", {"CODE": L_PROC.returncode, "STDERR": (L_STDERR or b'').decode('utf-8', 'ignore')})
        try:
            OUT_WAV_PATH.unlink(missing_ok=True)
        except Exception:
            pass
        raise RuntimeError(f"ffmpeg concat failed (rc={L_PROC.returncode})")

    try:
        L_LIST_PATH.unlink(missing_ok=True)
    except Exception:
        pass

def CONVERTING_AUDIO_ESCAPE_FFMPEG_PATH(L_PATH_STR: str) -> str:
    # concat demuxer quoting; escape single quotes in path
    return L_PATH_STR.replace("'", "'\\''")

# ─────────────────────────────────────────────────────────────
# Optional local harness
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    L_PARSER = argparse.ArgumentParser()
    L_PARSER.add_argument("--rid", required=True, help="RECORDING_ID")
    L_PARSER.add_argument("--only-config", action="store_true", help="Only load config and exit")
    L_ARGS = L_PARSER.parse_args()

    async def _MAIN():
        await STEP_1_NEW_RECORDING_STARTED(L_ARGS.rid)
        if L_ARGS.only_config:
            return
        await STEP_2_CREATE_AUDIO_CHUNKS(L_ARGS.rid)

    asyncio.run(_MAIN())
