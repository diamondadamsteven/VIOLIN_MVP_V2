# SERVER_ENGINE_LISTEN_2_FOR_WS_MESSAGES.py
from __future__ import annotations

import asyncio
import json
from datetime import datetime
from hashlib import sha256
from typing import Any, Dict, List, Optional

from fastapi import WebSocket, WebSocketDisconnect

from SERVER_ENGINE_APP_VARIABLES import (
    ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY,
    ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY,
    ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY,  # metadata only (no bytes)
    WEBSOCKET_AUDIO_FRAME_ARRAY,                # raw bytes only (volatile)
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    DB_INSERT_TABLE,   # allowlisted insert; supports fire_and_forget=True
    CONSOLE_LOG,
)

# ──────────────────────────────────────────────────────────────
# Internal state
# ──────────────────────────────────────────────────────────────
# Per-connection FIFO: TEXT “FRAME” announces (RECORDING_ID, AUDIO_FRAME_NO)
# -> the *next* BINARY message is paired with the oldest announce
_PENDING_BY_CONN: Dict[int, List[Dict[str, Any]]] = {}

_NEXT_MSG_ID = 1
def _alloc_msg_id() -> int:
    global _NEXT_MSG_ID
    mid = _NEXT_MSG_ID
    _NEXT_MSG_ID += 1
    return mid


# ──────────────────────────────────────────────────────────────
# Tiny helpers (1-responsibility each)
# ──────────────────────────────────────────────────────────────
def _persist_message(row: Dict[str, Any]) -> None:
    """Fire-and-forget DB insert of a websocket message row."""
    try:
        DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_MESSAGE", row, fire_and_forget=True)
    except Exception as e:
        CONSOLE_LOG("DB_INSERT_MESSAGE", "schedule_failed", {"err": str(e), "row_keys": list(row.keys())})


def _save_bytes_and_metadata(
    recording_id: int,
    audio_frame_no: int,
    raw_bytes: bytes,
    dt_received: datetime,
    websocket_connection_id: int,
) -> Dict[str, Any]:
    """
    1) Store raw bytes in the volatile cache (WEBSOCKET_AUDIO_FRAME_ARRAY)
    2) Build/update the durable metadata map (ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY)
    3) Persist ONLY metadata to DB
    """
    # 1) volatile audio bytes
    frames_for_rec = WEBSOCKET_AUDIO_FRAME_ARRAY.setdefault(recording_id, {})
    frames_for_rec[audio_frame_no] = {
        "RECORDING_ID": recording_id,
        "AUDIO_FRAME_NO": audio_frame_no,
        "AUDIO_FRAME_BYTES": raw_bytes,
    }

    # 2) durable metadata
    meta_row = {
        "RECORDING_ID": recording_id,
        "AUDIO_FRAME_NO": audio_frame_no,
        "START_MS": None,
        "END_MS": None,
        "DT_FRAME_RECEIVED": dt_received,
        "DT_FRAME_PAIRED_WITH_WEBSOCKETS_METADATA": dt_received,
        "AUDIO_FRAME_SIZE_BYTES": len(raw_bytes),
        "AUDIO_FRAME_ENCODING": "raw",
        "AUDIO_FRAME_SHA256_HEX": sha256(raw_bytes).hexdigest(),
        "WEBSOCKET_CONNECTION_ID": websocket_connection_id,  # ignored by DB if not allowlisted
    }
    meta_map = ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME_ARRAY.setdefault(recording_id, {})
    meta_map[audio_frame_no] = meta_row

    # 3) persist metadata (never the bytes)
    try:
        DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME", meta_row, fire_and_forget=True)
    except Exception as e:
        CONSOLE_LOG("DB_INSERT_AUDIO_META", "schedule_failed", {
            "rec_id": recording_id, "frame_no": audio_frame_no, "err": str(e)
        })

    return meta_row


# ──────────────────────────────────────────────────────────────
# Main receive loop
# ──────────────────────────────────────────────────────────────
async def SERVER_ENGINE_LISTEN_2_FOR_WS_MESSAGES(ws: WebSocket, WEBSOCKET_CONNECTION_ID: int) -> None:
    """
    Contract:
      • TEXT with MESSAGE_TYPE=FRAME arrives first → we queue its (recording_id, frame_no) as an “announce”
      • The *next* BINARY frame is paired with the oldest announce
      • Non-FRAME TEXT gets logged/persisted (e.g., START/STOP/etc.)
      • STOP → socket closed + connection row stamped
      • All DB writes are fire-and-forget to keep the loop snappy
    """
    _PENDING_BY_CONN[WEBSOCKET_CONNECTION_ID] = []

    try:
        while True:
            raw = await ws.receive()
            now = datetime.now()

            # ── Disconnect
            if raw.get("type") == "websocket.disconnect":
                conn = ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY.get(WEBSOCKET_CONNECTION_ID)
                if conn is not None:
                    conn["DT_CONNECTION_CLOSED"] = now
                    try:
                        DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_CONNECTION", conn, fire_and_forget=True)
                    except Exception as e:
                        CONSOLE_LOG("WS_CONN_DB", "close_insert_failed", {
                            "conn_id": WEBSOCKET_CONNECTION_ID, "err": str(e)
                        })
                break

            # ── TEXT
            text = raw.get("text")
            if text is not None:
                try:
                    payload = json.loads(text)
                except Exception:
                    payload = {"MESSAGE_TYPE": "TEXT", "TEXT": text}

                message_type = str(payload.get("MESSAGE_TYPE") or payload.get("type", "")).upper()
                recording_id = int(payload.get("RECORDING_ID") or 0)
                audio_frame_no = payload.get("AUDIO_FRAME_NO") or payload.get("FRAME_NO")
                audio_frame_no = int(audio_frame_no) if audio_frame_no is not None else None

                if message_type == "FRAME":
                    # queue the announce; the next BINARY will consume this
                    _PENDING_BY_CONN[WEBSOCKET_CONNECTION_ID].append({
                        "RECORDING_ID": recording_id,
                        "AUDIO_FRAME_NO": audio_frame_no,
                        "DT_MESSAGE_RECEIVED": now,
                    })
                    continue

                # normal (non-FRAME) control/message → log & persist
                msg_id = _alloc_msg_id()
                msg_row = {
                    "MESSAGE_ID": msg_id,
                    "DT_MESSAGE_RECEIVED": now,
                    "RECORDING_ID": recording_id,
                    "MESSAGE_TYPE": message_type or "TEXT",
                    "AUDIO_FRAME_NO": audio_frame_no,
                    "DT_MESSAGE_PROCESS_STARTED": None,
                    "WEBSOCKET_CONNECTION_ID": WEBSOCKET_CONNECTION_ID,
                }
                ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY[msg_id] = msg_row
                _persist_message(msg_row)

                if message_type == "STOP":
                    # graceful close
                    try:
                        await ws.close()
                    except Exception:
                        pass
                    conn = ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY.get(WEBSOCKET_CONNECTION_ID)
                    if conn is not None:
                        conn["DT_CONNECTION_CLOSED"] = now
                        try:
                            DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_CONNECTION", conn, fire_and_forget=True)
                        except Exception as e:
                            CONSOLE_LOG("WS_CONN_DB", "close_insert_failed", {
                                "conn_id": WEBSOCKET_CONNECTION_ID, "err": str(e)
                            })
                    break

            # ── BINARY (audio payload)
            elif raw.get("bytes") is not None:
                raw_bytes: bytes = raw["bytes"]

                if _PENDING_BY_CONN[WEBSOCKET_CONNECTION_ID]:
                    announce = _PENDING_BY_CONN[WEBSOCKET_CONNECTION_ID].pop(0)
                    recording_id = int(announce.get("RECORDING_ID") or 0)
                    audio_frame_no = int(announce.get("AUDIO_FRAME_NO") or 0)
                    dt_received = announce.get("DT_MESSAGE_RECEIVED", now)
                else:
                    # orphaned audio (no announce)
                    recording_id = 0
                    audio_frame_no = 0
                    dt_received = now

                # log the FRAME message itself
                msg_id = _alloc_msg_id()
                frame_msg_row = {
                    "MESSAGE_ID": msg_id,
                    "DT_MESSAGE_RECEIVED": dt_received,
                    "RECORDING_ID": recording_id,
                    "MESSAGE_TYPE": "FRAME",
                    "AUDIO_FRAME_NO": audio_frame_no,
                    "DT_MESSAGE_PROCESS_STARTED": None,
                    "WEBSOCKET_CONNECTION_ID": WEBSOCKET_CONNECTION_ID,
                }
                ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY[msg_id] = frame_msg_row
                _persist_message(frame_msg_row)

                # store raw bytes + metadata (and persist metadata)
                _save_bytes_and_metadata(
                    recording_id=recording_id,
                    audio_frame_no=audio_frame_no,
                    raw_bytes=raw_bytes,
                    dt_received=dt_received,
                    websocket_connection_id=WEBSOCKET_CONNECTION_ID,
                )

            # ── Other WS control frames (ignore)
            else:
                continue

    except WebSocketDisconnect:
        conn = ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY.get(WEBSOCKET_CONNECTION_ID)
        if conn is not None:
            conn["DT_CONNECTION_CLOSED"] = datetime.now()
            try:
                DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_CONNECTION", conn, fire_and_forget=True)
            except Exception as e:
                CONSOLE_LOG("WS_CONN_DB", "close_insert_failed", {
                    "conn_id": WEBSOCKET_CONNECTION_ID, "err": str(e)
                })
    finally:
        _PENDING_BY_CONN.pop(WEBSOCKET_CONNECTION_ID, None)
