# SERVER_ENGINE_LISTEN_2_FOR_WS_MESSAGES.py
from __future__ import annotations
import json
import base64
from datetime import datetime
from typing import Any, Dict, Optional
from fastapi import WebSocket, WebSocketDisconnect

from SERVER_ENGINE_APP_VARIABLES import (
    RECORDING_WEBSOCKET_MESSAGE_ARRAY,
    RECORDING_AUDIO_FRAME_ARRAY,
    RECORDING_WEBSOCKET_CONNECTION_ARRAY,
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    DB_LOG_FUNCTIONS,
    DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE,
    CONSOLE_LOG,
)

# Monotonic allocator for message ids
_next_msg_id = 1
def _alloc_message_id() -> int:
    global _next_msg_id
    mid = _next_msg_id
    _next_msg_id += 1
    return mid

def _decode_audio_from_payload(payload: Dict[str, Any]) -> Optional[bytes]:
    """
    If FRAME payload includes audio:
      - 'AUDIO_FRAME_BASE64': base64-encoded mono PCM (16-bit) bytes
      - or 'AUDIO_FRAME_HEX': hex string
      - or 'AUDIO_FRAME_BYTES': already bytes (rare)
    Returns bytes or None.
    """
    if "AUDIO_FRAME_BASE64" in payload and payload["AUDIO_FRAME_BASE64"]:
        try:
            return base64.b64decode(payload["AUDIO_FRAME_BASE64"])
        except Exception:
            return None
    if "AUDIO_FRAME_HEX" in payload and payload["AUDIO_FRAME_HEX"]:
        try:
            return bytes.fromhex(payload["AUDIO_FRAME_HEX"])
        except Exception:
            return None
    if "AUDIO_FRAME_BYTES" in payload and payload["AUDIO_FRAME_BYTES"]:
        b = payload["AUDIO_FRAME_BYTES"]
        return b if isinstance(b, (bytes, bytearray)) else None
    return None

@DB_LOG_FUNCTIONS()
async def SERVER_ENGINE_LISTEN_2_FOR_WS_MESSAGES(
    ws: WebSocket,
    WEBSOCKET_CONNECTION_ID: int,
) -> None:
    """
    Step 1) Receive the WebSocket message from the client
    Step 2) Insert into RECORDING_WEBSOCKET_MESSAGE_ARRAY with fields:
           DT_MESSAGE_RECEIVED, RECORDING_ID, MESSAGE_TYPE, AUDIO_FRAME_NO
    Notes:
      • If FRAME carries audio, we stash it temporarily at the message entry
        under key 'AUDIO_FRAME_BYTES' (non-breaking: TypedDict allows extra keys).
      • If STOP is received, we close the ws here (and set DT_CONNECTION_CLOSED).
    """
    try:
        while True:
            raw = await ws.receive()
            dt_now = datetime.now()

            # Messages could be text JSON or direct bytes. Prefer text->JSON.
            payload: Dict[str, Any] = {}
            if "text" in raw and raw["text"] is not None:
                try:
                    payload = json.loads(raw["text"])
                except Exception:
                    # In case client sent raw text like "PING"
                    payload = {"MESSAGE_TYPE": "TEXT", "TEXT": raw["text"]}
            elif "bytes" in raw and raw["bytes"] is not None:
                # If client sends binary frames directly, you can decide how to pair
                payload = {"MESSAGE_TYPE": "FRAME_BYTES", "AUDIO_FRAME_BYTES": raw["bytes"]}

            msg_type = str(payload.get("MESSAGE_TYPE", "")).upper()
            recording_id = int(payload.get("RECORDING_ID", 0) or 0)
            frame_no     = payload.get("AUDIO_FRAME_NO")
            frame_no     = int(frame_no) if frame_no is not None else None

            # Prepare message record
            message_id = _alloc_message_id()
            msg = {
                "MESSAGE_ID": message_id,
                "RECORDING_ID": recording_id,
                "MESSAGE_TYPE": msg_type,
                "AUDIO_FRAME_NO": frame_no,
                "DT_MESSAGE_RECEIVED": dt_now,
                "DT_MESSAGE_PROCESS_STARTED": None,
            }

            # If this is an audio frame, attach optional bytes for downstream processor
            if msg_type in ("FRAME", "FRAME_BYTES"):
                b = _decode_audio_from_payload(payload)
                if b is not None:
                    # Non-breaking extra field to carry bytes to the processor
                    msg["AUDIO_FRAME_BYTES"] = bytes(b)

            RECORDING_WEBSOCKET_MESSAGE_ARRAY[message_id] = msg

            # Optional: immediate DB log of receipt (process step will also log "started")
            # DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE(message_id)

            # If STOP is received, close the WS here (engine "stop" processing also updates arrays)
            if msg_type == "STOP":
                try:
                    await ws.close()
                except Exception:
                    pass
                # Mark connection closed
                if WEBSOCKET_CONNECTION_ID in RECORDING_WEBSOCKET_CONNECTION_ARRAY:
                    RECORDING_WEBSOCKET_CONNECTION_ARRAY[WEBSOCKET_CONNECTION_ID]["DT_CONNECTION_CLOSED"] = dt_now
                    # Log the closed state for audit
                    try:
                        DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE(message_id)
                    except Exception:
                        pass
                break

    except WebSocketDisconnect:
        # Client dropped unexpectedly; mark the connection as closed (time now)
        if WEBSOCKET_CONNECTION_ID in RECORDING_WEBSOCKET_CONNECTION_ARRAY:
            RECORDING_WEBSOCKET_CONNECTION_ARRAY[WEBSOCKET_CONNECTION_ID]["DT_CONNECTION_CLOSED"] = datetime.now()
