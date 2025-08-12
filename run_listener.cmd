@echo off
cd /d "%~dp0"
call venv\Scripts\activate.bat
echo [listener] starting... >>"C:\Users\diamo\VIOLIN_MVP\listener_server.log"
python -m uvicorn SERVER_ENGINE_AUDIO_STREAM_LISTENER:APP --host 0.0.0.0 --port 7070 --reload >>"C:\Users\diamo\VIOLIN_MVP\listener_server.log" 2>> "C:\Users\diamo\VIOLIN_MVP\listener_server.log"
