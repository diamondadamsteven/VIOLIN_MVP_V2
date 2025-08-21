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
    ENGINE_DB_LOG_FUNCTIONS_INS,  # centralized Start/End/Error logging
)

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
    DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_MESSAGE", row, fire_and_forget=True)


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
    DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_AUDIO_FRAME", meta_row, fire_and_forget=True)

    return meta_row


async def _receive_next_binary(ws: WebSocket) -> Optional[bytes]:
    """
    Block until the next binary WS message (skip over non-binary control/text),
    or return None on disconnect.
    """
    while True:
        evt = await ws.receive()
        if evt.get("type") == "websocket.disconnect":
            return None
        if evt.get("bytes") is not None:
            return evt["bytes"]
        # Skip any stray control/text frames that may arrive between header and bytes.


# ──────────────────────────────────────────────────────────────
# Main receive loop
# ──────────────────────────────────────────────────────────────
# @ENGINE_DB_LOG_FUNCTIONS_INS()
async def SERVER_ENGINE_LISTEN_2_FOR_WS_MESSAGES(ws: WebSocket, WEBSOCKET_CONNECTION_ID: int) -> None:
    """
    Contract (paired receive):
      • When TEXT {MESSAGE_TYPE:'FRAME', RECORDING_ID, FRAME_NO} arrives,
        we synchronously await the very next BINARY and pair them atomically.
      • Non-FRAME TEXT (START/STOP/etc.) is logged immediately.
      • STOP → socket closed + connection row stamped.
      • If a BINARY arrives without a prior FRAME header, we treat it as orphaned (RID=0, FRAME_NO=0).
    """
    while True:
        raw = await ws.receive()
        now = datetime.now()

        # ── Disconnect
        if raw.get("type") == "websocket.disconnect":
            conn = ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY.get(WEBSOCKET_CONNECTION_ID)
            if conn is not None:
                conn["DT_CONNECTION_CLOSED"] = now
                DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_CONNECTION", conn, fire_and_forget=True)
            break

        # ── TEXT
        text = raw.get("text")
        if text is not None:
            payload = json.loads(text)

            message_type = str(payload.get("MESSAGE_TYPE") or payload.get("type", "")).upper()
            recording_id = int(payload.get("RECORDING_ID") or 0)
            audio_frame_no = payload.get("AUDIO_FRAME_NO") or payload.get("FRAME_NO")
            audio_frame_no = int(audio_frame_no) if audio_frame_no is not None else None

            if message_type == "FRAME":
                # PAIRING: wait for the very next binary and only then log/enqueue
                bin_bytes = await _receive_next_binary(ws)
                if bin_bytes is None:
                    # peer disconnected before sending the binary – treat like a dropped frame
                    break

                # log the FRAME message itself (now that we HAVE bytes)
                msg_id = _alloc_msg_id()
                frame_msg_row = {
                    "MESSAGE_ID": msg_id,
                    "DT_MESSAGE_RECEIVED": now,
                    "RECORDING_ID": recording_id,
                    "MESSAGE_TYPE": "FRAME",
                    "AUDIO_FRAME_NO": int(audio_frame_no or 0),
                    "DT_MESSAGE_PROCESS_STARTED": None,
                    "WEBSOCKET_CONNECTION_ID": WEBSOCKET_CONNECTION_ID,
                }
                ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY[msg_id] = frame_msg_row
                _persist_message(frame_msg_row)

                # store raw bytes + metadata (and persist metadata)
                _save_bytes_and_metadata(
                    recording_id=recording_id,
                    audio_frame_no=int(audio_frame_no or 0),
                    raw_bytes=bin_bytes,
                    dt_received=now,
                    websocket_connection_id=WEBSOCKET_CONNECTION_ID,
                )
                continue

            # normal (non-FRAME) control/message → log & persist
            msg_id = _alloc_msg_id()
            msg_row = {
                "MESSAGE_ID": msg_id,
                "DT_MESSAGE_RECEIVED": now,
                "RECORDING_ID": recording_id,
                "MESSAGE_TYPE": message_type or "TEXT",
                "AUDIO_FRAME_NO": int(audio_frame_no or 0) if audio_frame_no is not None else None,
                "DT_MESSAGE_PROCESS_STARTED": None,
                "WEBSOCKET_CONNECTION_ID": WEBSOCKET_CONNECTION_ID,
            }
            ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY[msg_id] = msg_row
            _persist_message(msg_row)

            if message_type == "STOP":
                # graceful close
                await ws.close()
                conn = ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY.get(WEBSOCKET_CONNECTION_ID)
                if conn is not None:
                    conn["DT_CONNECTION_CLOSED"] = now
                    DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_CONNECTION", conn, fire_and_forget=True)
                break

        # ── BINARY (audio payload) received WITHOUT a preceding FRAME header
        elif raw.get("bytes") is not None:
            bin_bytes: bytes = raw["bytes"]
            # orphaned audio (no header) → record with RID=0/FRAME=0 for visibility
            msg_id = _alloc_msg_id()
            frame_msg_row = {
                "MESSAGE_ID": msg_id,
                "DT_MESSAGE_RECEIVED": now,
                "RECORDING_ID": 0,
                "MESSAGE_TYPE": "FRAME",
                "AUDIO_FRAME_NO": 0,
                "DT_MESSAGE_PROCESS_STARTED": None,
                "WEBSOCKET_CONNECTION_ID": WEBSOCKET_CONNECTION_ID,
            }
            ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY[msg_id] = frame_msg_row
            _persist_message(frame_msg_row)

            _save_bytes_and_metadata(
                recording_id=0,
                audio_frame_no=0,
                raw_bytes=bin_bytes,
                dt_received=now,
                websocket_connection_id=WEBSOCKET_CONNECTION_ID,
            )

        # ── Other WS control frames (ignore)
        else:
            continue
