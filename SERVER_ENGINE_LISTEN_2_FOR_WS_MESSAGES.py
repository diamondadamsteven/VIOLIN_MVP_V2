# SERVER_ENGINE_LISTEN_2_FOR_WS_MESSAGES.py
from __future__ import annotations
import asyncio
import json
import base64
from datetime import datetime
from typing import Dict, Any, List, Optional
from fastapi import WebSocket, WebSocketDisconnect

from SERVER_ENGINE_APP_VARIABLES import (
    RECORDING_WEBSOCKET_MESSAGE_ARRAY,
    RECORDING_WEBSOCKET_CONNECTION_ARRAY,
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    DB_LOG_FUNCTIONS,
    DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE,
    CONSOLE_LOG,
)

# --- Internal State
_PENDING_FRAME_META: Dict[int, List[Dict[str, Any]]] = {}
_NEXT_MSG_ID = 1

def _alloc_msg_id() -> int:
    """Allocate a sequential message ID."""
    global _NEXT_MSG_ID
    mid = _NEXT_MSG_ID
    _NEXT_MSG_ID += 1
    return mid

def _maybe_decode_audio(payload: Dict[str, Any]) -> Optional[bytes]:
    """
    Try to extract audio bytes from a FRAME payload.
    Supported encodings: base64, hex, raw bytes.
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
    if "AUDIO_FRAME_BYTES" in payload and isinstance(payload["AUDIO_FRAME_BYTES"], (bytes, bytearray)):
        return bytes(payload["AUDIO_FRAME_BYTES"])
    return None

async def _persist_message_async(message_id: int) -> None:
    """
    Off-WS-path persistence to DB for a message already stored in memory.
    Uses a background thread to avoid blocking the event loop.
    """
    try:
        await asyncio.to_thread(DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE, message_id)
    except Exception as e:
        # Keep the WS loop resilient; note to console.
        CONSOLE_LOG("DB_LOG_ENGINE_DB_WEBSOCKET_MESSAGE", "failed",
                    {"mid": message_id, "err": str(e)})

def _persist_message_fire_and_forget(message_id: int) -> None:
    """
    Schedule DB persistence without awaiting (fire-and-forget).
    Safe to call from inside the WS receive loop.
    """
    try:
        asyncio.create_task(_persist_message_async(message_id))
    except Exception:
        # If no running loop (unlikely inside FastAPI), ignore.
        pass


async def SERVER_ENGINE_LISTEN_2_FOR_WS_MESSAGES(ws: WebSocket, WEBSOCKET_CONNECTION_ID: int) -> None:
    """
    Receive loop for websocket messages:
      • TEXT frame with MESSAGE_TYPE=FRAME queues metadata for the next BINARY frame.
      • TEXT frame with MESSAGE_TYPE=STOP closes the socket.
      • BINARY frame is paired with pending FRAME metadata (or saved as raw if no meta).
      • Every message stored in memory is also persisted to DB off the WS path.
      • Clean exit on disconnect.
    """
    _PENDING_FRAME_META[WEBSOCKET_CONNECTION_ID] = []

    try:
        while True:
            raw = await ws.receive()

            # --- Disconnect event
            if raw.get("type") == "websocket.disconnect":
                if WEBSOCKET_CONNECTION_ID in RECORDING_WEBSOCKET_CONNECTION_ARRAY:
                    RECORDING_WEBSOCKET_CONNECTION_ARRAY[WEBSOCKET_CONNECTION_ID]["DT_CONNECTION_CLOSED"] = datetime.now()
                break

            now = datetime.now()

            # --- TEXT MESSAGE
            if raw.get("text") is not None:
                try:
                    payload = json.loads(raw["text"])
                except Exception:
                    payload = {"MESSAGE_TYPE": "TEXT", "TEXT": raw["text"]}

                mtype = str(payload.get("MESSAGE_TYPE") or payload.get("type", "")).upper()
                recording_id = int(payload.get("RECORDING_ID") or 0)
                frame_no = payload.get("AUDIO_FRAME_NO") or payload.get("FRAME_NO")
                frame_no = int(frame_no) if frame_no is not None else None

                if mtype == "FRAME":
                    # Queue FRAME metadata until paired with BINARY
                    _PENDING_FRAME_META[WEBSOCKET_CONNECTION_ID].append({
                        "RECORDING_ID": recording_id,
                        "AUDIO_FRAME_NO": frame_no,
                        "DT_MESSAGE_RECEIVED": now,
                    })
                    continue

                # Normal non-frame message → log + persist
                mid = _alloc_msg_id()
                RECORDING_WEBSOCKET_MESSAGE_ARRAY[mid] = {
                    "MESSAGE_ID": mid,
                    "DT_MESSAGE_RECEIVED": now,
                    "RECORDING_ID": recording_id,
                    "MESSAGE_TYPE": mtype,
                    "AUDIO_FRAME_NO": frame_no,
                    "DT_MESSAGE_PROCESS_STARTED": None,
                }
                _persist_message_fire_and_forget(mid)

                # Stop request → close socket and exit
                if mtype == "STOP":
                    try:
                        await ws.close()
                    except Exception:
                        pass
                    if WEBSOCKET_CONNECTION_ID in RECORDING_WEBSOCKET_CONNECTION_ARRAY:
                        RECORDING_WEBSOCKET_CONNECTION_ARRAY[WEBSOCKET_CONNECTION_ID]["DT_CONNECTION_CLOSED"] = now
                    break

            # --- BINARY MESSAGE (audio frame data)
            elif raw.get("bytes") is not None:
                b = raw["bytes"]
                if _PENDING_FRAME_META[WEBSOCKET_CONNECTION_ID]:
                    meta = _PENDING_FRAME_META[WEBSOCKET_CONNECTION_ID].pop(0)
                    mid = _alloc_msg_id()
                    RECORDING_WEBSOCKET_MESSAGE_ARRAY[mid] = {
                        "MESSAGE_ID": mid,
                        "DT_MESSAGE_RECEIVED": meta["DT_MESSAGE_RECEIVED"],
                        "RECORDING_ID": int(meta["RECORDING_ID"] or 0),
                        "MESSAGE_TYPE": "FRAME",
                        "AUDIO_FRAME_NO": int(meta["AUDIO_FRAME_NO"] or 0),
                        "DT_MESSAGE_PROCESS_STARTED": None,
                        "AUDIO_FRAME_BYTES": b,
                    }
                else:
                    # orphaned binary frame
                    mid = _alloc_msg_id()
                    RECORDING_WEBSOCKET_MESSAGE_ARRAY[mid] = {
                        "MESSAGE_ID": mid,
                        "DT_MESSAGE_RECEIVED": now,
                        "RECORDING_ID": 0,
                        "MESSAGE_TYPE": "FRAME",
                        "AUDIO_FRAME_NO": 0,
                        "DT_MESSAGE_PROCESS_STARTED": None,
                        "AUDIO_FRAME_BYTES": b,
                    }
                _persist_message_fire_and_forget(mid)

            # --- PING / keepalive / other control frames
            else:
                pass

    except WebSocketDisconnect:
        if WEBSOCKET_CONNECTION_ID in RECORDING_WEBSOCKET_CONNECTION_ARRAY:
            RECORDING_WEBSOCKET_CONNECTION_ARRAY[WEBSOCKET_CONNECTION_ID]["DT_CONNECTION_CLOSED"] = datetime.now()
    finally:
        _PENDING_FRAME_META.pop(WEBSOCKET_CONNECTION_ID, None)
