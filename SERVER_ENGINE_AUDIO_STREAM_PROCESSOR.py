# SERVER_ENGINE_AUDIO_STREAM_PROCESSOR.py
# ------------------------------------------------------------
# Processing side for VIOLIN_MVP audio engine.
# - Accepts per-chunk calls from the listener (HTTP or WS)
# - Computes precise global offsets using countdown boundary flags
# - Loads features (FFT/ONS/PYIN/CREPE), then runs master aggregation
# - Finalizes on STOP
#
# NOTE: Replace the example stored-procedure names with your actual ones.
# ------------------------------------------------------------

import os
from typing import Dict, Any, Optional

import pyodbc

# =========================
# DB CONFIG (owned here)
# =========================
DB_CONN_STR = os.getenv(
    "DB_CONN_STR",
    "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;"
    "DATABASE=VIOLIN;UID=sa;PWD=your_password;TrustServerCertificate=yes",
)

def _GET_CONN():
    return pyodbc.connect(DB_CONN_STR, autocommit=True)

def _EXEC_PROC(CONN, PROC_NAME: str, PARAMS: dict):
    CUR = CONN.cursor()
    PLACEHOLDERS = ", ".join(f"@{K} = ?" for K in PARAMS.keys())
    SQL = f"EXEC {PROC_NAME} {PLACEHOLDERS}"
    CUR.execute(SQL, tuple(PARAMS.values()))
    # If you need result sets, fetch here.
    return None

# =========================
# ALIGNMENT STATE
# =========================
# For precise time-zero alignment once the boundary frame arrives.
# Key: str(RECORDING_ID)
# Value: {
#   "STREAMED_CHUNK_DURATION_IN_MS": int,
#   "T0_CHUNK_NO": int,
#   "T0_OFFSET_MS_WITHIN_CHUNK": int,
#   "T0_GLOBAL_MS": int
# }
ALIGNMENT_BY_RECORDING_ID: Dict[str, Dict[str, int]] = {}

# =========================
# RUNTIME DURATION LOOKUP
# =========================
def _RESOLVE_CHUNK_DURATION_MS(
    RECORDING_ID: str,
    RUNTIME_INFO: Optional[Dict[str, Any]],
    CONN,
) -> int:
    """
    Prefer duration supplied by listener via RUNTIME_INFO.
    Else try DB. Else fall back to 600.
    """
    # 1) From listener runtime cache
    if RUNTIME_INFO and "STREAMED_CHUNK_DURATION_IN_MS" in RUNTIME_INFO:
        try:
            return int(RUNTIME_INFO["STREAMED_CHUNK_DURATION_IN_MS"])
        except Exception:
            pass

    # 2) (Optional) From DB — replace with your real lookup SP
    # try:
    #     CUR = CONN.cursor()
    #     CUR.execute("EXEC P_ENGINE_GET_RECORDING_RUNTIME_INFO @RECORDING_ID = ?", (int(RECORDING_ID),))
    #     ROW = CUR.fetchone()
    #     if ROW and ROW.CHUNK_MS is not None:
    #         return int(ROW.CHUNK_MS)
    # except Exception:
    #     pass

    # 3) Safe default
    return 600

def _ENSURE_ALIGNMENT(
    RECORDING_ID: str,
    STREAMED_CHUNK_DURATION_IN_MS: int,
    STREAMED_CHUNK_NO: int,
    COUNTDOWN_ZERO_IN_THIS_CHUNK: Optional[str],
    COUNTDOWN_ZERO_OFFSET_MS_IN_CHUNK: Optional[int],
):
    """
    If this frame marks the countdown boundary, compute and store global t0.
    """
    if RECORDING_ID in ALIGNMENT_BY_RECORDING_ID:
        return

    if (COUNTDOWN_ZERO_IN_THIS_CHUNK or "").upper() == "Y":
        OFFSET = int(COUNTDOWN_ZERO_OFFSET_MS_IN_CHUNK or 0)
        T0_GLOBAL_MS = STREAMED_CHUNK_NO * STREAMED_CHUNK_DURATION_IN_MS + OFFSET
        ALIGNMENT_BY_RECORDING_ID[RECORDING_ID] = {
            "STREAMED_CHUNK_DURATION_IN_MS": int(STREAMED_CHUNK_DURATION_IN_MS),
            "T0_CHUNK_NO": int(STREAMED_CHUNK_NO),
            "T0_OFFSET_MS_WITHIN_CHUNK": int(OFFSET),
            "T0_GLOBAL_MS": int(T0_GLOBAL_MS),
        }

def _GLOBAL_OFFSET_MS(
    RECORDING_ID: str,
    STREAMED_CHUNK_NO: int,
    STREAMED_CHUNK_DURATION_IN_MS: int,
) -> int:
    """
    Global offset (ms) for the *start* of this frame.
    If t0 is known, shift so t0 == 0. If not, return pre-alignment ms.
    """
    RAW = STREAMED_CHUNK_NO * STREAMED_CHUNK_DURATION_IN_MS
    ALIGN = ALIGNMENT_BY_RECORDING_ID.get(RECORDING_ID)
    if not ALIGN:
        return RAW
    return RAW - ALIGN["T0_GLOBAL_MS"]

# =========================
# MAIN ENTRY: PER-CHUNK
# =========================
async def PROCESS_AUDIO_STREAM(
    RECORDING_ID: str,
    STREAMED_CHUNK_NO: int,
    AUDIO_CHUNK_FILE_PATH: str,
    COUNTDOWN_ZERO_IN_THIS_CHUNK: Optional[str] = None,
    COUNTDOWN_ZERO_OFFSET_MS_IN_CHUNK: Optional[int] = None,
    RUNTIME_INFO: Optional[Dict[str, Any]] = None,
):
    """
    Called after the listener saves a frame to disk.
    Computes precise alignment, then kicks your feature loaders + master proc.
    """
    CONN = _GET_CONN()
    try:
        STREAMED_CHUNK_DURATION_IN_MS = _RESOLVE_CHUNK_DURATION_MS(RECORDING_ID, RUNTIME_INFO, CONN)

        # One-time alignment lock-in (when boundary frame arrives)
        _ENSURE_ALIGNMENT(
            RECORDING_ID=RECORDING_ID,
            STREAMED_CHUNK_DURATION_IN_MS=STREAMED_CHUNK_DURATION_IN_MS,
            STREAMED_CHUNK_NO=STREAMED_CHUNK_NO,
            COUNTDOWN_ZERO_IN_THIS_CHUNK=COUNTDOWN_ZERO_IN_THIS_CHUNK,
            COUNTDOWN_ZERO_OFFSET_MS_IN_CHUNK=COUNTDOWN_ZERO_OFFSET_MS_IN_CHUNK,
        )

        CHUNK_START_MS = _GLOBAL_OFFSET_MS(
            RECORDING_ID=RECORDING_ID,
            STREAMED_CHUNK_NO=STREAMED_CHUNK_NO,
            STREAMED_CHUNK_DURATION_IN_MS=STREAMED_CHUNK_DURATION_IN_MS,
        )

        # ----------------- YOUR PIPELINE -----------------
        # Load per-feature (replace with your actual procs)
        _EXEC_PROC(CONN, "P_ENGINE_LOAD_FFT", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(STREAMED_CHUNK_NO),
            "FILE_PATH": AUDIO_CHUNK_FILE_PATH,
            "CHUNK_START_MS": int(CHUNK_START_MS),
        })
        _EXEC_PROC(CONN, "P_ENGINE_LOAD_ONS", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(STREAMED_CHUNK_NO),
            "FILE_PATH": AUDIO_CHUNK_FILE_PATH,
            "CHUNK_START_MS": int(CHUNK_START_MS),
        })
        _EXEC_PROC(CONN, "P_ENGINE_LOAD_PYIN", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(STREAMED_CHUNK_NO),
            "FILE_PATH": AUDIO_CHUNK_FILE_PATH,
            "CHUNK_START_MS": int(CHUNK_START_MS),
        })
        _EXEC_PROC(CONN, "P_ENGINE_LOAD_CREPE", {
            "RECORDING_ID": int(RECORDING_ID),
            "AUDIO_CHUNK_NO": int(STREAMED_CHUNK_NO),
            "FILE_PATH": AUDIO_CHUNK_FILE_PATH,
            "CHUNK_START_MS": int(CHUNK_START_MS),
        })

        # Aggregate / decide / write client-facing tables
        _EXEC_PROC(CONN, "P_ENGINE_ALL_MASTER", {
            "RECORDING_ID": int(RECORDING_ID),
        })

        # (Optional) If you plan to fill missing frames with silence:
        # - Detect gaps here by tracking last processed frame per RECORDING_ID
        # - Synthesize a silent segment and run loaders with CHUNK_START_MS set appropriately
        # For now, we rely on WS ACK + client resend.

    finally:
        CONN.close()

# =========================
# FINALIZE ON STOP
# =========================
async def PROCESS_STOP_RECORDING(RECORDING_ID: str):
    """
    Finalize a take. Stitching, summary writes, cleanup.
    """
    CONN = _GET_CONN()
    try:
        # Example finalize (replace with your concrete proc if needed)
        # _EXEC_PROC(CONN, "P_CLIENT_RECORD_END", {"RECORDING_ID": int(RECORDING_ID)})

        # Clear alignment state for this recording
        if RECORDING_ID in ALIGNMENT_BY_RECORDING_ID:
            del ALIGNMENT_BY_RECORDING_ID[RECORDING_ID]

    finally:
        CONN.close()
