@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"

rem ============================
rem Config / logging (overwrite)
rem ============================
set "LOG=%~dp0VIOLIN_MVP_STOP_SERVER_output.txt"
if exist "%LOG%" del "%LOG%" >nul 2>&1
call :TEE "========== STOPPING VIOLIN_MVP  %DATE% %TIME% =========="

rem Optional: warn if not admin (helps when some kills resist)
net session >nul 2>&1
if errorlevel 1 (
  call :TEE "[Note] Not running as Administrator. If any processes resist termination, right-click this .bat and 'Run as administrator'."
)

rem ============================
rem 1) Stop the Onsets & Frames container
rem ============================
call :TEE "Stopping Docker container: violin_oaf_server ..."
docker stop violin_oaf_server >> "%LOG%" 2>&1

rem ============================
rem 2) Kill anything bound to ports 8000 (backend) and 7070 (listener)
rem ============================
call :KILL_BY_PORT 8000 "FastAPI backend (:8000)"
call :KILL_BY_PORT 7070 "WS listener (:7070)"

rem ============================
rem 3) Extra: kill uvicorn by command line match
rem ============================
call :TEE "Stopping uvicorn processes by command line match (fallback)..."
powershell -NoProfile -ExecutionPolicy Bypass ^
  -Command "Get-CimInstance Win32_Process | Where-Object { ($_.Name -like 'python*' -or $_.Name -like 'uvicorn*') -and $_.CommandLine -match 'uvicorn.*SERVER_VIOLIN_MVP_START:app' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }" >> "%LOG%" 2>&1

powershell -NoProfile -ExecutionPolicy Bypass ^
  -Command "Get-CimInstance Win32_Process | Where-Object { ($_.Name -like 'python*' -or $_.Name -like 'uvicorn*') -and $_.CommandLine -match 'uvicorn.*SERVER_ENGINE_AUDIO_STREAM_LISTENER:APP' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }" >> "%LOG%" 2>&1

rem ============================
rem 4) Close leftover windows by title (the two shells you still see)
rem ============================
call :CLOSE_WINDOWS_BY_TITLE "FastAPI Backend"
call :CLOSE_WINDOWS_BY_TITLE "Server Engine Listener"
call :CLOSE_WINDOWS_BY_TITLE "Onsets_And_Frames (Docker)"

rem PowerShell fallback: close any window whose title contains our labels
powershell -NoProfile -ExecutionPolicy Bypass ^
  -Command "$labels = 'FastAPI Backend','Server Engine Listener','Onsets_And_Frames (Docker)'; Get-Process | Where-Object { $_.MainWindowTitle -and ($labels | ForEach-Object { $_ }) -contains $_.MainWindowTitle } | ForEach-Object { Stop-Process -Id $_.Id -Force }" >> "%LOG%" 2>&1

rem Broader fallback: kill cmd.exe / powershell.exe whose titles match (wildcards)
for %%T in ("FastAPI Backend" "Server Engine Listener" "Onsets_And_Frames (Docker)") do (
  taskkill /FI "WINDOWTITLE eq %%~T" /IM cmd.exe /T /F >> "%LOG%" 2>&1
  taskkill /FI "WINDOWTITLE eq %%~T" /IM powershell.exe /T /F >> "%LOG%" 2>&1
)

rem ============================
rem 5) Final sweep: conhost children (sometimes linger)
rem ============================
for %%P in (conhost.exe) do taskkill /IM %%P /F >> "%LOG%" 2>&1

rem ============================
rem 6) Summary & auto-exit (no pause)
rem ============================
call :TEE "All components requested to stop."
call :TEE "Log written to: %LOG%"
call :TEE "========================================================"

endlocal & exit /b 0


rem ======= helpers =======

:KILL_BY_PORT
rem %1 = port, %2 = label
set "_PORT=%~1"
set "_LABEL=%~2"
call :TEE "Checking for processes listening on :%_PORT% (%_LABEL%)..."

powershell -NoProfile -ExecutionPolicy Bypass ^
  -Command "Get-NetTCPConnection -State Listen -LocalPort %_PORT% -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess | Sort-Object -Unique" > "%TEMP%\pids_%_PORT%.txt" 2>>"%LOG%"

set "FOUND=0"
for /f "usebackq delims=" %%P in ("%TEMP%\pids_%_PORT%.txt") do (
  if not "%%P"=="" (
    set "FOUND=1"
    call :TEE " - Killing PID %%P bound to :%_PORT% ..."
    taskkill /F /PID %%P >> "%LOG%" 2>&1
  )
)
del "%TEMP%\pids_%_PORT%.txt" >nul 2>&1
if "%FOUND%"=="0" call :TEE " - No listener found on :%_PORT%."
exit /b 0

:CLOSE_WINDOWS_BY_TITLE
rem %1 = exact window title we used in 'start "Title" cmd /k ...'
set "_TTL=%~1"
call :TEE "Closing window titled: %_TTL% ..."
taskkill /FI "WINDOWTITLE eq %_TTL%" /IM cmd.exe /T /F >> "%LOG%" 2>&1
taskkill /FI "WINDOWTITLE eq %_TTL%" /IM powershell.exe /T /F >> "%LOG%" 2>&1
exit /b 0

:TEE
>> "%LOG%" echo %DATE% %TIME% ^| %*
exit /b 0
