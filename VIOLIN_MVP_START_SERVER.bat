@echo off 
setlocal EnableExtensions EnableDelayedExpansion
pushd "%~dp0"

rem ============================
rem Config
rem ============================
set "NOW=%date:~10,4%-%date:~4,2%-%date:~7,2%_%time:~0,2%-%time:~3,2%-%time:~6,2%.%time:~9,2%"
set "NOW=%NOW: =0%"

set "LOG_FILE=%~dp0VIOLIN_MVP_START_SERVER_output.txt"
set "BACKEND_LOG_FILE=%~dp0VIOLIN_MVP_START_SERVER_backend_output.txt"
set "LISTENER_LOG_FILE=%~dp0VIOLIN_MVP_START_SERVER_listener_output.log"

set "OAF_CONTAINER=violin_oaf_server"
set "OAF_IMAGE=violin/oaf:latest"
set "OAF_IMAGE_FALLBACK=tensorflow/magenta"
set "OAF_PORT=9077"

for %%I in (.) do set "PROJECT_ROOT=%%~fI"
set "CHECKPOINT_DIR=%PROJECT_ROOT%\onsets-frames"
set "SKIP_OAF_START=0"

rem ============================
rem Logging helper
rem ============================
if exist "%LOG_FILE%" del "%LOG_FILE%" >nul 2>&1
if exist "%BACKEND_LOG_FILE%" del "%BACKEND_LOG_FILE%" >nul 2>&1
if exist "%LISTENER_LOG_FILE%" del "%LISTENER_LOG_FILE%" >nul 2>&1
call :LOG "======== SERVER STARTUP LOG %DATE% %TIME% ========"

rem 1) venv
echo venv
call :LOG "Activating virtual environment..."
call .venv\Scripts\activate.bat >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
  call :LOG "[Error] Failed to activate virtual environment."
  pause & popd & exit /b 1
)

rem 2) Firewall (7070)
echo "Firewall"
call :LOG "Ensuring Windows Firewall allows inbound TCP 7070..."
powershell -NoProfile -ExecutionPolicy Bypass ^
  -Command "try{ if(-not (Get-NetFirewallRule -DisplayName 'VIOLIN_MVP_WS_7070' -ErrorAction SilentlyContinue)){ New-NetFirewallRule -DisplayName 'VIOLIN_MVP_WS_7070' -Direction Inbound -Action Allow -Protocol TCP -LocalPort 7070 | Out-Null; 'Firewall rule created.' } else { 'Firewall rule already exists.' } } catch { 'Firewall step error: ' + $_.Exception.Message }" >> "%LOG_FILE%" 2>&1

rem 3) O&F container (optional)
echo "O&F Container"
if "%SKIP_OAF_START%"=="0" (
  call :LOG "Launching Onsets_And_Frames container via start_oaf.ps1..."
  start "Onsets_And_Frames (Docker)" /D "%PROJECT_ROOT%" powershell -NoExit -ExecutionPolicy Bypass -File "%~dp0start_oaf.ps1" ^
    -ContainerName "%OAF_CONTAINER%" ^
    -PreferredImage "%OAF_IMAGE%" ^
    -FallbackImage "%OAF_IMAGE_FALLBACK%" ^
    -HostPort %OAF_PORT% ^
    -ProjectRoot "%PROJECT_ROOT%" ^
    -CheckpointDir "%CHECKPOINT_DIR%"
) else (
  call :LOG "[Info] SKIP_OAF_START=1 — skipping O&F start."
)

rem 4) Backend
echo Backend
call :LOG "Starting FastAPI backend on :8000 ..."
start "FastAPI Backend" /D "%PROJECT_ROOT%" cmd /k ^
  .venv\Scripts\python.exe -m uvicorn SERVER_VIOLIN_MVP_START:app --host 0.0.0.0 --port 8000 --reload ^>"%LOG_SERVER_VIOLIN_MVP_START" 2^>^&1

rem 5) Listener (single process runs WS + orchestrator)
echo Listener
call :LOG "Starting Server Engine Listener on :7070 ..."
start "Server Engine Listener" /D "%PROJECT_ROOT%" cmd /k ^
  .venv\Scripts\python.exe -m uvicorn SERVER_ENGINE_ORCHESTRATOR:APP --host 0.0.0.0 --port 7070 --reload ^>"%LOG_SERVER_ENGINE_ORCHESTRATOR" 2^>^&1

echo.
echo "Servers launched."
echo   Backend:  http://localhost:8000
echo   WS:       ws://localhost:7070/ws/stream
echo   O&F:      http://127.0.0.1:%OAF_PORT%
echo.
echo "(Close the two console windows to stop them. To stop O&F: docker stop %OAF_CONTAINER%)"
echo.
echo Startup complete. See logs:
echo   %LOG_FILE%
echo.
pause
popd
endlocal
exit /b 0

:LOG
>> "%LOG_FILE%" echo %DATE% %TIME% ^| %~1 %~2 %~3 %~4 %~5 %~6 %~7 %~8 %~9
goto :eof
