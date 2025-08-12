@echo off
cd /d "%~dp0"
call venv\Scripts\activate.bat
echo [backend] starting... >>"C:\Users\diamo\VIOLIN_MVP\backend_server.log"
python -m uvicorn SERVER_VIOLIN_MVP_START:app --host 0.0.0.0 --port 8000 --reload >>"C:\Users\diamo\VIOLIN_MVP\backend_server.log" 2>> "C:\Users\diamo\VIOLIN_MVP\backend_server.log"
