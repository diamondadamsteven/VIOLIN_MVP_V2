@echo off
cd /d %~dp0

:: Display current folder
echo ----------------------------------
echo Pushing changes for VIOLIN_MVP_V2...
echo ----------------------------------

:: Stage all changes
git add .

:: Prompt for commit message
set /p commitmsg=Enter commit message: 
if "%commitmsg%"=="" set commitmsg=Quick update

:: Commit
git commit -m "%commitmsg%"

:: Push to GitHub
git push https://github.com/DIAMONDADAMSTEVEN/VIOLIN_MVP_V2.git main

:: Done
echo ----------------------------------
echo Push complete!
pause
