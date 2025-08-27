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
    ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME_ARRAY,  # metadata only (no bytes)
    PRE_SPLIT_AUDIO_FRAME_ARRAY,                # raw bytes only (volatile)
)
from SERVER_ENGINE_APP_FUNCTIONS import (
    DB_INSERT_TABLE,   # allowlisted insert; supports fire_and_forget=True
    CONSOLE_LOG,
    ENGINE_DB_LOG_FUNCTIONS_INS,  # centralized Start/End/Error logging
)

L_MESSAGE_ID = 0

# ──────────────────────────────────────────────────────────────
# Main receive loop
# ──────────────────────────────────────────────────────────────
# @ENGINE_DB_LOG_FUNCTIONS_INS()
async def SERVER_ENGINE_LISTEN_2_FOR_WS_MESSAGES(WEBSOCKET_MESSAGE: WebSocket, WEBSOCKET_CONNECTION_ID: int) -> None:
    """
    Contract (paired receive):
      • When TEXT {MESSAGE_TYPE:'FRAME', RECORDING_ID, FRAME_NO} arrives,
        we synchronously await the very next BINARY and pair them atomically.
      • Non-FRAME TEXT (START/STOP/etc.) is logged immediately.
      • STOP → socket closed + connection row stamped.
      • If a BINARY arrives without a prior FRAME header, we treat it as orphaned (RID=0, FRAME_NO=0).
    """

    global L_MESSAGE_ID
    
    while True:
        RAW_WEBSOCKET_MESSAGE = await WEBSOCKET_MESSAGE.receive()
        now = datetime.now()

        # ── Disconnect
        if RAW_WEBSOCKET_MESSAGE.get("type") == "websocket.disconnect":
            ENGINE_DB_LOG_WEBSOCKET_CONNECTION_RECORD = ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY.get(WEBSOCKET_CONNECTION_ID)
            ENGINE_DB_LOG_WEBSOCKET_CONNECTION_RECORD["DT_CONNECTION_CLOSED"] = now
            DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_CONNECTION", ENGINE_DB_LOG_WEBSOCKET_CONNECTION_RECORD, fire_and_forget=True)

        # ── TEXT
        WEBSOCKET_MESSAGE_TEXT = RAW_WEBSOCKET_MESSAGE.get("text")
        if WEBSOCKET_MESSAGE_TEXT is not None:
            WEBSOCKET_MESSAGE_JSON = json.loads(WEBSOCKET_MESSAGE_TEXT)

            MESSAGE_TYPE = str(WEBSOCKET_MESSAGE_JSON.get("MESSAGE_TYPE") or 
                               WEBSOCKET_MESSAGE_JSON.get("type", "")).upper()
            RECORDING_ID = int(WEBSOCKET_MESSAGE_JSON.get("RECORDING_ID") or 0)

            if MESSAGE_TYPE == "FRAME":              
                AUDIO_FRAME_NO = int(WEBSOCKET_MESSAGE_JSON.get("AUDIO_FRAME_NO") or
                                    WEBSOCKET_MESSAGE_JSON.get("FRAME_NO"))
                # PAIRING: wait for the very next binary and only then log/enqueue
                while True:
                    RAW_WEBSOCKET_MESSAGE_2 = await WEBSOCKET_MESSAGE.receive()
                    if RAW_WEBSOCKET_MESSAGE_2.get("type") == "websocket.disconnect":
                        break
                    if RAW_WEBSOCKET_MESSAGE_2.get("bytes") is not None: 
                        AUDIO_FRAME_BYTES = RAW_WEBSOCKET_MESSAGE_2["bytes"]
                        break

                # log the FRAME message itself (now that we HAVE bytes)
                L_MESSAGE_ID = L_MESSAGE_ID + 1
                ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD = {
                    "MESSAGE_ID": L_MESSAGE_ID,
                    "DT_MESSAGE_RECEIVED": now,
                    "RECORDING_ID": RECORDING_ID,
                    "MESSAGE_TYPE": "FRAME",
                    "AUDIO_FRAME_NO": AUDIO_FRAME_NO,
                    "DT_MESSAGE_PROCESS_STARTED": None,
                    "WEBSOCKET_CONNECTION_ID": WEBSOCKET_CONNECTION_ID,
                }
                ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY[L_MESSAGE_ID] = ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD
                DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_MESSAGE", ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD, fire_and_forget=True)

                PRE_SPLIT_AUDIO_FRAME_RECORD = PRE_SPLIT_AUDIO_FRAME_ARRAY.setdefault(RECORDING_ID, {})
                PRE_SPLIT_AUDIO_FRAME_RECORD[AUDIO_FRAME_NO] = {
                    "RECORDING_ID": RECORDING_ID,
                    "AUDIO_FRAME_NO": AUDIO_FRAME_NO,
                    "AUDIO_FRAME_BYTES": AUDIO_FRAME_BYTES,
                }

                ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME_RECORD = ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME_ARRAY.setdefault(RECORDING_ID, {})
                ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME_RECORD[AUDIO_FRAME_NO] = {
                    "RECORDING_ID": RECORDING_ID,
                    "AUDIO_FRAME_NO": AUDIO_FRAME_NO,
                    "START_MS": None,
                    "END_MS": None,
                    "DT_FRAME_RECEIVED": now,
                    "DT_FRAME_PAIRED_WITH_WEBSOCKETS_METADATA": now,
                    "AUDIO_FRAME_SIZE_BYTES": len(AUDIO_FRAME_BYTES),
                    "AUDIO_FRAME_ENCODING": "raw",
                    "AUDIO_FRAME_SHA256_HEX": sha256(AUDIO_FRAME_BYTES).hexdigest(),
                    "WEBSOCKET_CONNECTION_ID": WEBSOCKET_CONNECTION_ID,  # ignored by DB if not allowlisted
                }
                # 3) persist metadata (never the bytes)
                DB_INSERT_TABLE("ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME", ENGINE_DB_LOG_PRE_SPLIT_AUDIO_FRAME_RECORD, fire_and_forget=True)

            else:  #NON-FRAME
                L_MESSAGE_ID =  L_MESSAGE_ID + 1
                ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD = {
                "MESSAGE_ID": L_MESSAGE_ID,
                "DT_MESSAGE_RECEIVED": now,
                "RECORDING_ID": RECORDING_ID,
                "MESSAGE_TYPE": MESSAGE_TYPE or "TEXT",
                "AUDIO_FRAME_NO": None,
                "DT_MESSAGE_PROCESS_STARTED": None,
                "WEBSOCKET_CONNECTION_ID": WEBSOCKET_CONNECTION_ID,
                }
                ENGINE_DB_LOG_WEBSOCKET_MESSAGE_ARRAY[L_MESSAGE_ID] = ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD
                DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_MESSAGE", ENGINE_DB_LOG_WEBSOCKET_MESSAGE_RECORD, fire_and_forget=True)

            if MESSAGE_TYPE == "STOP":
                # graceful close
                await WEBSOCKET_MESSAGE.close()
                ENGINE_DB_LOG_WEBSOCKET_CONNECTION_RECORD = ENGINE_DB_LOG_WEBSOCKET_CONNECTION_ARRAY.get(WEBSOCKET_CONNECTION_ID)
                ENGINE_DB_LOG_WEBSOCKET_CONNECTION_RECORD["DT_CONNECTION_CLOSED"] = now
                DB_INSERT_TABLE("ENGINE_DB_LOG_WEBSOCKET_CONNECTION", ENGINE_DB_LOG_WEBSOCKET_CONNECTION_RECORD, fire_and_forget=True)
                break

        else:
            continue
