# SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_1_CONCATENATE.py
# ----------------------------------------------------------------------
# Step-1 of the streaming pipeline:
#   - Load per-recording CONFIG from DB (once) -> populate in-memory arrays
#   - Watch temp folder for arriving .m4a frames
#   - For each complete audio-chunk, concatenate frames -> 48k mono WAV
#   - Delete consumed .m4a frames
#   - Invoke Step-2 processing on each WAV (Step-2 reads globals; no config.json)
#   - When STOP marker exists and all complete chunks are processed, invoke Step-3
# ----------------------------------------------------------------------

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

import builtins as _bi
import traceback

# Shared globals & helpers
from SERVER_ENGINE_APP_VARIABLES import (
    TEMP_RECORDING_AUDIO_DIR,
    RECORDING_CONFIG_ARRAY,
    RECORDING_AUDIO_CHUNK_ARRAY,
    RECORDING_AUDIO_FRAME_ARRAY,
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    CONSOLE_LOG,
    DB_CONNECT,
    DB_EXEC_SP_SINGLE_ROW,
    DB_EXEC_SP_MULTIPLE_ROWS,
    # NEW: DB logs per our rules
    DB_LOG_RECORDING_CONFIG,
    DB_LOG_ENGINE_DB_AUDIO_FRAME_TRANSFER,
)

# Step-2/3 public entries — assumed refactored to read globals
from SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_2_AUDIO_CHUNKS import (
    SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_2_AUDIO_CHUNKS,
)
from SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_3_STOP import (
    SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_3_STOP,
)

PREFIX = "STEP_1_CONCATENATE"

# ─────────────────────────────────────────────────────────────
# Local path helpers (file-only; intentionally not centralized)
# ─────────────────────────────────────────────────────────────
def _rec_dir(rid_int: int) -> Path:
    d = TEMP_RECORDING_AUDIO_DIR / str(rid_int)
    d.mkdir(parents=True, exist_ok=True)
    return d

def _stop_marker_path(rid_int: int) -> Path:
    return _rec_dir(rid_int) / "_STOP"

def _chunk_wav48k_path(rid_int: int, chunk_no: int) -> Path:
    return _rec_dir(rid_int) / f"chunk_{chunk_no:06d}_48k.wav"

def _frame_path(rid_int: int, frame_no: int) -> Path:
    return _rec_dir(rid_int) / f"{frame_no:08d}.m4a"


# ─────────────────────────────────────────────────────────────
# PUBLIC: STEP-1 config load for a recording
# ─────────────────────────────────────────────────────────────
async def STEP_1_NEW_RECORDING_STARTED(RECORDING_ID: int | str) -> None:
    """
    1) P_ENGINE_ALL_RECORDING_PARAMETERS_GET
    2) If COMPOSE -> P_ENGINE_SONG_AUDIO_CHUNK_FOR_COMPOSE_GET
       Else (PLAY/PRACTICE) -> P_ENGINE_SONG_AUDIO_CHUNK_FOR_PLAY_AND_PRACTICE_GET
    3) Populate in-memory arrays (no files written), then DB_LOG_RECORDING_CONFIG.
    """
    rid = int(RECORDING_ID)
    CONSOLE_LOG(PREFIX, "STEP_1_NEW_RECORDING_STARTED.begin", {"RECORDING_ID": rid})

    def _db_load() -> Dict[str, Any]:
        with DB_CONNECT() as conn:
            base = DB_EXEC_SP_SINGLE_ROW(
                conn, "P_ENGINE_ALL_RECORDING_PARAMETERS_GET",
                RECORDING_ID=rid
            )
            mode = _bi.str((base.get("COMPOSE_PLAY_OR_PRACTICE") or "")).upper().strip()

            cfg: Dict[str, Any] = {
                "RECORDING_ID": rid,
                "VIOLINIST_ID": base.get("VIOLINIST_ID"),
                "COMPOSE_PLAY_OR_PRACTICE": mode,
                "AUDIO_STREAM_FILE_NAME": base.get("AUDIO_STREAM_FILE_NAME"),
                "AUDIO_STREAM_FRAME_SIZE_IN_MS": base.get("AUDIO_STREAM_FRAME_SIZE_IN_MS"),
            }

            if mode == "COMPOSE":
                row = DB_EXEC_SP_SINGLE_ROW(
                    conn, "P_ENGINE_SONG_AUDIO_CHUNK_FOR_COMPOSE_GET",
                    RECORDING_ID=rid
                )
                cfg["COMPOSE_DICT"] = {
                    "AUDIO_CHUNK_DURATION_IN_MS": int(row.get("AUDIO_CHUNK_DURATION_IN_MS")),
                    "CNT_FRAMES_PER_AUDIO_CHUNK": int(row.get("CNT_FRAMES_PER_AUDIO_CHUNK")),
                    "YN_RUN_FFT": (row.get("YN_RUN_FFT") or None),
                }
            else:
                rows = DB_EXEC_SP_MULTIPLE_ROWS(
                    conn, "P_ENGINE_SONG_AUDIO_CHUNK_FOR_PLAY_AND_PRACTICE_GET",
                    RECORDING_ID=rid
                )
                chunks: Dict[int, Dict[str, Any]] = {}
                for r in rows:
                    cno = int(r.get("AUDIO_CHUNK_NO"))
                    chunks[cno] = {
                        "RECORDING_ID": rid,
                        "AUDIO_CHUNK_NO": cno,
                        "AUDIO_CHUNK_DURATION_IN_MS": int(r.get("AUDIO_CHUNK_DURATION_IN_MS")),
                        "START_MS": int(r.get("START_MS")),
                        "END_MS": int(r.get("END_MS")),
                        "MIN_AUDIO_STREAM_FRAME_NO": int(r.get("MIN_AUDIO_STREAM_FRAME_NO")),
                        "MAX_AUDIO_STREAM_FRAME_NO": int(r.get("MAX_AUDIO_STREAM_FRAME_NO")),
                        "YN_RUN_FFT": (r.get("YN_RUN_FFT") or None),
                        "YN_RUN_ONS": (r.get("YN_RUN_ONS") or None),
                        "YN_RUN_PYIN": (r.get("YN_RUN_PYIN") or None),
                        "YN_RUN_CREPE": (r.get("YN_RUN_CREPE") or None),
                    }
                cfg["PLAY_PRACTICE_CHUNKS"] = chunks
            return cfg

    try:
        cfg = await asyncio.to_thread(_db_load)

        # Ensure per-recording directory exists (for frames/WAVs)
        _rec_dir(rid)

        # Populate in-memory config (merge if already seeded)
        rec_cfg = RECORDING_CONFIG_ARRAY.setdefault(rid, {"RECORDING_ID": rid})
        rec_cfg.update({
            "VIOLINIST_ID": cfg.get("VIOLINIST_ID"),
            "COMPOSE_PLAY_OR_PRACTICE": cfg.get("COMPOSE_PLAY_OR_PRACTICE"),
            "AUDIO_STREAM_FILE_NAME": cfg.get("AUDIO_STREAM_FILE_NAME"),
            "AUDIO_STREAM_FRAME_SIZE_IN_MS": cfg.get("AUDIO_STREAM_FRAME_SIZE_IN_MS"),
        })
        # Ensure COMPOSE specifics live under config for compose mode
        if cfg.get("COMPOSE_PLAY_OR_PRACTICE") == "COMPOSE" and "COMPOSE_DICT" in cfg:
            rec_cfg["COMPOSE_DICT"] = cfg["COMPOSE_DICT"]
        rec_cfg.setdefault("DT_RECORDING_START", datetime.utcnow())

        # Populate per-chunk specs in memory
        mode = cfg.get("COMPOSE_PLAY_OR_PRACTICE")
        if mode in ("PLAY", "PRACTICE"):
            RECORDING_AUDIO_CHUNK_ARRAY[rid] = cfg.get("PLAY_PRACTICE_CHUNKS", {})
        elif mode == "COMPOSE":
            # Compose chunks are uniform; Step-1 will generate each chunk spec on the fly.
            RECORDING_AUDIO_CHUNK_ARRAY.setdefault(rid, {})

        # ← DB_LOG_RECORDING_CONFIG only here (Step-1), after SPs per rules
        DB_LOG_RECORDING_CONFIG(rid)

        CONSOLE_LOG(PREFIX, "CONFIG_LOADED", {
            "RECORDING_ID": rid,
            "MODE": mode,
            "PLAY_PRACTICE_CHUNKS": len(RECORDING_AUDIO_CHUNK_ARRAY.get(rid, {})) if mode in ("PLAY", "PRACTICE") else None,
        })
    except Exception as exc:
        CONSOLE_LOG(PREFIX, "STEP_1_NEW_RECORDING_STARTED.error", {
            "ERROR": _bi.str(exc),
            "TRACE": traceback.format_exc(),
        })
        raise


# ─────────────────────────────────────────────────────────────
# PUBLIC: STEP-2 chunk loop driver (concatenate & dispatch)
# ─────────────────────────────────────────────────────────────
async def STEP_2_CREATE_AUDIO_CHUNKS(RECORDING_ID: int | str) -> None:
    """
    Iterate audio-chunk-by-audio-chunk; when all frames for a chunk are present:
      - Concatenate -> WAV 48k mono
      - Delete consumed .m4a frames (and stamp/DB-log per-frame removals)
      - Call Step-2 processing (public entry)
    When STOP marker exists and all complete chunks processed, call Step-3 (public entry).
    """
    rid = int(RECORDING_ID)
    CONSOLE_LOG(PREFIX, "STEP_2_CREATE_AUDIO_CHUNKS.begin", {"RECORDING_ID": rid})

    mode = _bi.str(RECORDING_CONFIG_ARRAY.get(rid, {}).get("COMPOSE_PLAY_OR_PRACTICE") or "").upper()
    if not mode:
        raise RuntimeError(f"No config in memory for RECORDING_ID={rid}")

    next_chunk_no = _next_chunk_no_from_memory(rid, mode)
    CONSOLE_LOG(PREFIX, "STARTING_AT_AUDIO_CHUNK", {"NEXT_AUDIO_CHUNK_NO": next_chunk_no})

    try:
        while True:
            frame_range, timing, flags = _chunk_spec_from_memory(rid, mode, next_chunk_no)
            if frame_range is None:
                # No such chunk (PLAY/PRACTICE end). If STOP seen, finish.
                if _stop_marker_path(rid).exists():
                    CONSOLE_LOG(PREFIX, "STOP_SEEN_NO_FURTHER_AUDIO_CHUNKS")
                    break
                await asyncio.sleep(0.1)
                continue

            min_frame, max_frame = frame_range
            start_ms, end_ms, chunk_ms = timing

            ready = await _await_frames_on_disk(rid, min_frame, max_frame)
            if not ready:
                CONSOLE_LOG(PREFIX, "STOP_BEFORE_AUDIO_CHUNK_COMPLETE", {"AUDIO_CHUNK_NO": next_chunk_no})
                break

            wav48 = _chunk_wav48k_path(rid, next_chunk_no)
            frame_paths = [_frame_path(rid, i) for i in range(min_frame, max_frame + 1)]

            # Mark chunk ready-to-start time (all frames received)
            ch_map = RECORDING_AUDIO_CHUNK_ARRAY.setdefault(rid, {})
            ch = ch_map.setdefault(next_chunk_no, {
                "RECORDING_ID": rid,
                "AUDIO_CHUNK_NO": next_chunk_no,
                "AUDIO_CHUNK_DURATION_IN_MS": chunk_ms,
                "START_MS": start_ms,
                "END_MS": end_ms,
                "MIN_AUDIO_STREAM_FRAME_NO": min_frame,
                "MAX_AUDIO_STREAM_FRAME_NO": max_frame,
                "YN_RUN_FFT": flags.get("YN_RUN_FFT"),
                "YN_RUN_ONS": flags.get("YN_RUN_ONS"),
                "YN_RUN_PYIN": flags.get("YN_RUN_PYIN"),
                "YN_RUN_CREPE": flags.get("YN_RUN_CREPE"),
            })
            ch["DT_COMPLETE_FRAMES_RECEIVED"] = datetime.utcnow()

            # Per-frame: stamp concatenation time & chunk number (before concat)
            frames = RECORDING_AUDIO_FRAME_ARRAY.setdefault(rid, {})
            cat_time = datetime.utcnow()
            for i in range(min_frame, max_frame + 1):
                fr = frames.setdefault(i, {"RECORDING_ID": rid, "FRAME_NO": i})
                fr.setdefault("DT_FRAME_CONCATENATED_TO_AUDIO_CHUNK", cat_time)
                fr["AUDIO_CHUNK_NO"] = next_chunk_no

            # Concat to WAV
            await _concat_m4a_to_wav48k(frame_paths, wav48)

            # Purge frames: delete files, stamp removal, then DB log transfer per frame
            for i, f in zip(range(min_frame, max_frame + 1), frame_paths):
                try:
                    f.unlink(missing_ok=True)
                except Exception as e:
                    CONSOLE_LOG(PREFIX, "DELETE_FRAME_FAILED_NON_FATAL", {"FILE": _bi.str(f), "ERROR": _bi.str(e)})
                fr = frames.setdefault(i, {"RECORDING_ID": rid, "FRAME_NO": i})
                if not fr.get("DT_FRAME_REMOVED_FROM_MEMORY"):
                    fr["DT_FRAME_REMOVED_FROM_MEMORY"] = datetime.utcnow()
                    # DB log per-frame transfer ONLY here (after concat & purge)
                    DB_LOG_ENGINE_DB_AUDIO_FRAME_TRANSFER(rid, i)

            # Call Step-2 (reads globals)
            res = SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_2_AUDIO_CHUNKS(
                RECORDING_ID=rid,
                AUDIO_CHUNK_NO=next_chunk_no,
                WAV48K_PATH=_bi.str(wav48),
            )
            if asyncio.iscoroutine(res):
                await res

            next_chunk_no += 1

            # If PLAY/PRACTICE: after final configured audio-chunk & STOP present -> exit
            if mode in ("PLAY", "PRACTICE"):
                total = len(RECORDING_AUDIO_CHUNK_ARRAY.get(rid, {}))
                if next_chunk_no > total and _stop_marker_path(rid).exists():
                    CONSOLE_LOG(PREFIX, "ALL_AUDIO_CHUNKS_DONE_AND_STOP_SEEN")
                    break

            await asyncio.sleep(0)

    finally:
        # Finalize (Step-3 will read globals and the per-chunk WAVs)
        try:
            res3 = SERVER_ENGINE_AUDIO_STREAM_PROCESSOR_STEP_3_STOP(RECORDING_ID=rid)
            if asyncio.iscoroutine(res3):
                await res3
        except Exception as exc:
            CONSOLE_LOG(PREFIX, "STEP_3_STOP_ERROR", {
                "RECORDING_ID": rid,
                "ERROR": _bi.str(exc),
                "TRACE": traceback.format_exc()
            })
        CONSOLE_LOG(PREFIX, "STEP_2_CREATE_AUDIO_CHUNKS.end", {"RECORDING_ID": rid})


# ─────────────────────────────────────────────────────────────
# Helpers: specs from memory, waiting, ffmpeg
# ─────────────────────────────────────────────────────────────
def _next_chunk_no_from_memory(rid: int, mode: str) -> int:
    if mode == "COMPOSE":
        # For compose, start at 1 and proceed indefinitely until STOP
        return 1
    chunks = RECORDING_AUDIO_CHUNK_ARRAY.get(rid, {})
    if not chunks:
        return 1
    return max(chunks.keys()) + 1  # resume if partially processed

def _chunk_spec_from_memory(
    rid: int,
    mode: str,
    chunk_no: int
) -> Tuple[Optional[Tuple[int, int]], Tuple[int, int, int], Dict[str, Optional[str]]]:
    """
    Returns:
      - frame_range: (MIN_FRAME_NO, MAX_FRAME_NO) or None if not in config (PLAY/PRACTICE end)
      - timing: (START_MS, END_MS, DURATION_MS)
      - flags: dict with YN_RUN_FFT/ONS/PYIN/CREPE (may be None)
    """
    cfg = RECORDING_CONFIG_ARRAY.get(rid, {})
    if mode == "COMPOSE":
        cd = cfg.get("COMPOSE_DICT", {})
        cnt_frames = int(cd.get("CNT_FRAMES_PER_AUDIO_CHUNK"))
        dur_ms = int(cd.get("AUDIO_CHUNK_DURATION_IN_MS"))

        min_f = (chunk_no - 1) * cnt_frames + 1
        max_f = min_f + cnt_frames - 1

        start_ms = (chunk_no - 1) * dur_ms
        end_ms = start_ms + dur_ms - 1

        flags = {
            "YN_RUN_FFT": cd.get("YN_RUN_FFT"),
            "YN_RUN_ONS": None,
            "YN_RUN_PYIN": None,
            "YN_RUN_CREPE": None,
        }
        return (min_f, max_f), (start_ms, end_ms, dur_ms), flags

    # PLAY / PRACTICE
    row = RECORDING_AUDIO_CHUNK_ARRAY.get(rid, {}).get(chunk_no)
    if not row:
        return None, (0, 0, 0), {}
    frame_range = (int(row["MIN_AUDIO_STREAM_FRAME_NO"]), int(row["MAX_AUDIO_STREAM_FRAME_NO"]))
    start_ms = int(row["START_MS"])
    end_ms = int(row["END_MS"])
    dur_ms = int(row["AUDIO_CHUNK_DURATION_IN_MS"])
    flags = {
        "YN_RUN_FFT": row.get("YN_RUN_FFT"),
        "YN_RUN_ONS": row.get("YN_RUN_ONS"),
        "YN_RUN_PYIN": row.get("YN_RUN_PYIN"),
        "YN_RUN_CREPE": row.get("YN_RUN_CREPE"),
    }
    return frame_range, (start_ms, end_ms, dur_ms), flags

async def _await_frames_on_disk(
    rid: int,
    min_frame_no: int,
    max_frame_no: int,
    poll_ms: int = 100
) -> bool:
    """
    Wait until every .m4a for [MIN..MAX] exists.
    If STOP marker appears first, return False.
    """
    needed = {i for i in range(min_frame_no, max_frame_no + 1)}
    stop_path = _stop_marker_path(rid)

    while needed:
        missing = [i for i in needed if not _frame_path(rid, i).exists()]
        if not missing:
            return True
        if stop_path.exists():
            return False
        await asyncio.sleep(poll_ms / 1000.0)

async def _concat_m4a_to_wav48k(
    frame_paths: List[Path],
    out_wav: Path
) -> None:
    """
    Use ffmpeg concat demuxer to decode+concatenate .m4a frames -> 48k mono PCM WAV.
    """
    lst = out_wav.with_suffix(".concat.txt")
    text = "".join([f"file '{_escape_ffmpeg_path(str(p))}'\n" for p in frame_paths])
    lst.write_text(text, encoding="utf-8")

    cmd = [
        "ffmpeg", "-v", "error", "-y",
        "-f", "concat", "-safe", "0",
        "-i", str(lst),
        "-ac", "1", "-ar", "48000",
        "-c:a", "pcm_s16le",
        str(out_wav),
    ]
    CONSOLE_LOG(PREFIX, "FFMPEG_CONCAT_TO_WAV48K", {"FRAMES": len(frame_paths), "OUT_WAV_PATH": str(out_wav)})

    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        CONSOLE_LOG(PREFIX, "FFMPEG_ERROR", {"CODE": proc.returncode, "STDERR": (stderr or b'').decode('utf-8', 'ignore')})
        try:
            out_wav.unlink(missing_ok=True)
        except Exception:
            pass
        raise RuntimeError(f"ffmpeg concat failed (rc={proc.returncode})")

    try:
        lst.unlink(missing_ok=True)
    except Exception:
        pass

def _escape_ffmpeg_path(s: str) -> str:
    # concat demuxer quoting; escape single quotes in path
    return s.replace("'", "'\\''")


# Back-compat alias name (optional to keep imports compiling elsewhere)
STEP_2_AUDIO_CHUNKS = STEP_2_CREATE_AUDIO_CHUNKS

# Local harness
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--rid", required=True, help="RECORDING_ID (int)")
    parser.add_argument("--only-config", action="store_true", help="Only load config and exit")
    args = parser.parse_args()

    async def _main():
        rid = int(args.rid)
        await STEP_1_NEW_RECORDING_STARTED(rid)
        if args.only_config:
            return
        await STEP_2_CREATE_AUDIO_CHUNKS(rid)

    asyncio.run(_main())
