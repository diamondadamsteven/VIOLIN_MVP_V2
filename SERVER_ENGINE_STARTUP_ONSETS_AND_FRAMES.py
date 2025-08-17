from __future__ import annotations

import sys
import json
import asyncio
import subprocess
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

import builtins as _bi
import traceback
import os

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.websockets import WebSocketState
from starlette.types import ASGIApp, Scope, Receive, Send  # for logging WS Origin


from SERVER_ENGINE_APP_FUNCTIONS import (
    CONSOLE_LOG,
    DB_LOG_FUNCTIONS,  # <<< logging decorator
)

PREFIX = "SERVER_ENGINE_STARTUP_ONSETS_AND_FRAMES"

# ─────────────────────────────────────────────────────────────
# Onsets & Frames Docker management (kept here; called at startup)
# ─────────────────────────────────────────────────────────────
OAF_IMAGE = os.getenv("OAF_IMAGE", "violin/oaf:latest")
OAF_CONTAINER = os.getenv("OAF_CONTAINER", "violin_oaf_server")
# Host exposes microservice at 127.0.0.1:OAF_PORT -> container:9077
OAF_PORT = int(os.getenv("OAF_PORT", "9077"))

# use the shared project root from app-variables
PROJECT_ROOT = PROJECT_ROOT_DIR
CHECKPOINT_DIR = PROJECT_ROOT_DIR / "onsets-frames"   # local default; no env dependency

@DB_LOG_FUNCTIONS()
def SERVER_ENGINE_STARTUP_ONSETS_AND_FRAMES():
    """Ensures the O&F Docker container is up and serving DOCKER_ONSETS_AND_FRAMES_SERVER.py on 9077."""
    CONSOLE_LOG(PREFIX, "STEP_1_ENSURE_OAF_CONTAINER_RUNNING")
    try:
        res = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", OAF_CONTAINER],
            capture_output=True, text=True
        )
        if res.returncode == 0 and "true" in (res.stdout or "").strip():
            CONSOLE_LOG(PREFIX, "OAF_CONTAINER_ALREADY_RUNNING", {"container": OAF_CONTAINER})
            return
    except Exception as e:
        CONSOLE_LOG(PREFIX, "DOCKER_INSPECT_FAILED_WILL_RUN", _bi.str(e))

    CONSOLE_LOG(PREFIX, "STARTING_OAF_CONTAINER", {"image": OAF_IMAGE, "name": OAF_CONTAINER})
    cmd = [
        "docker", "run", "-d", "--rm",
        "--name", OAF_CONTAINER,
        "-p", f"127.0.0.1:{OAF_PORT}:9077",
        "-v", f"{PROJECT_ROOT}:/data",
        "-v", f"{CHECKPOINT_DIR}:/model",
        "-w", "/data",
        OAF_IMAGE,
        "python", "DOCKER_ONSETS_AND_FRAMES_SERVER.py"
    ]
    subprocess.run(cmd, check=True)
