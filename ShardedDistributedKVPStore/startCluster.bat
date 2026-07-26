@echo off
setlocal enabledelayedexpansion

set "CONF=%~dp0cluster.conf"

if not exist "%CONF%" (
    echo Cannot find %CONF%
    pause
    exit /b 1
)

:: Kill any previously running node windows before starting fresh ones
echo Closing any existing node processes...
for /f "tokens=1,2,3" %%A in (%CONF%) do (
    taskkill /FI "WINDOWTITLE eq Node %%A*" /T /F >nul 2>&1
)

:: Give Windows a moment to actually release the ports/handles
timeout /t 2 /nobreak >nul

:: Read the config and spin up each node in a dedicated window
for /f "tokens=1,2,3" %%A in (%CONF%) do (
    echo Starting Node %%A %%B %%C
    start "Node %%A" cmd /k mvn exec:java -Dexec.mainClass=Node "-Dexec.args=%%A %%B %%C"
)

pause