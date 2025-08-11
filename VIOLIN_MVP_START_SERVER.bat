@echo off
cd /d "%~dp0"

echo Activating virtual environment...
call venv\Scripts\activate.bat || (
  echo Failed to activate virtual environment.
  pause
  exit /b
)

REM Start the main FastAPI backend (port 8000)
start "FastAPI Backend" cmd /k python -m uvicorn SERVER_VIOLIN_MVP_START:app --host 0.0.0.0 --port 8000 --reload

REM Start the Server Engine WebSocket Listener (port 7070)
start "Server Engine Listener" cmd /k python -m uvicorn SERVER_ENGINE_AUDIO_STREAM_LISTENER:APP --host 0.0.0.0 --port 7070 --reload

pause
