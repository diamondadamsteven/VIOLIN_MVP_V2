@echo off
cd /d "%~dp0"

echo Activating virtual environment...
call .venv\Scripts\activate.bat

echo Starting FastAPI server with uvicorn...
uvicorn SERVER_VIOLIN_MVP_START:app --host 0.0.0.0 --port 8000 --reload


pause
