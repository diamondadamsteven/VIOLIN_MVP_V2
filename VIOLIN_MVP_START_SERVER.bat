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

rem Per-listener logs will be placed here:
set "LISTENERS_LOG_DIR=%~dp0VIOLIN_MVP_LISTENER_LOGS"
if not exist "%LISTENERS_LOG_DIR%" mkdir "%LISTENERS_LOG_DIR%"

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
rem Wipe old listener logs so we start fresh
if exist "%LISTENERS_LOG_DIR%\*.log" del /q "%LISTENERS_LOG_DIR%\*.log" >nul 2>&1

call :LOG "======== SERVER STARTUP LOG %DATE% %TIME% ========"

rem 1) venv
echo venv
call :LOG "Activating virtual environment..."
call venv\Scripts\activate.bat >> "%LOG_FILE%" 2>&1
if errorlevel 1 (
  call :LOG "[Error] Failed to activate virtual environment."
  pause & popd & exit /b 1
)

rem 2) Firewall
echo "Firewall"
call :LOG "Ensuring Windows Firewall allows inbound TCP 7070..."
powershell -NoProfile -ExecutionPolicy Bypass ^
  -Command "try{ if(-not (Get-NetFirewallRule -DisplayName 'VIOLIN_MVP_WS_7070' -ErrorAction SilentlyContinue)){ New-NetFirewallRule -DisplayName 'VIOLIN_MVP_WS_7070' -Direction Inbound -Action Allow -Protocol TCP -LocalPort 7070 | Out-Null; 'Firewall rule created.' } else { 'Firewall rule already exists.' } } catch { 'Firewall step error: ' + $_.Exception.Message }" >> "%LOG_FILE%" 2>&1

rem 3) O&F container (new PS window)
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
  python -m uvicorn SERVER_VIOLIN_MVP_START:app --host 0.0.0.0 --port 8000 --reload ^>"%BACKEND_LOG_FILE%" 2^>^&1

rem 5) Listeners (multiple worker scripts)
echo Listeners
call :LOG "Starting listener workers..."

rem Space-separated list of listener worker files to launch
set "LISTENER_FILES= ^
SERVER_ENGINE_LISTEN_1_FOR_WS_CONNECTIONS.py ^
SERVER_ENGINE_LISTEN_2_FOR_WS_MESSAGES.py ^
SERVER_ENGINE_LISTEN_3A_FOR_START.py ^
SERVER_ENGINE_LISTEN_3B_FOR_FRAMES.py ^
SERVER_ENGINE_LISTEN_3C_FOR_STOP.py ^
SERVER_ENGINE_LISTEN_4_FOR_AUDIO_CHUNKS_TO_PREPARE.py ^
SERVER_ENGINE_LISTEN_5_CONCATENATE.py ^
SERVER_ENGINE_LISTEN_6_FOR_AUDIO_CHUNKS_TO_PROCESS.py ^
SERVER_ENGINE_LISTEN_7_FOR_FINISHED_RECORDINGS.py"

for %%F in (%LISTENER_FILES%) do (
  set "FILE=%%F"
  set "BASE=%%~nF"
  call :LOG "Launching !FILE! ..."
  rem Each listener gets a fresh (overwritten) log
  start "Listener - !BASE!" /D "%PROJECT_ROOT%" cmd /k ^
    set PYTHONUNBUFFERED=1 ^& python "!FILE!" ^>"%LISTENERS_LOG_DIR%\!BASE!_output.log" 2^>^&1
)

echo.
echo "Servers launched."
echo   Backend:  http://localhost:8000
echo   Listeners: %LISTENERS_LOG_DIR% (one log per worker)
echo   O&F:      http://127.0.0.1:%OAF_PORT%
echo.
echo "(Close the spawned console windows to stop them. To stop O&F: docker stop %OAF_CONTAINER%)"
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
