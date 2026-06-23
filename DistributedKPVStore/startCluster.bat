@echo off
setlocal

set "CONF=%~dp0cluster.conf"

if not exist "%CONF%" (
    echo Cannot find %CONF%
    pause
    exit /b 1
)

:: Read the config and spin up each node in a dedicated window
for /f "tokens=1,2" %%A in (%CONF%) do (
    echo Starting Node %%A %%B
    start "Node %%A" cmd /k "mvn exec:java -Dexec.mainClass=Node -Dexec.args="%%A %%B""
)

pause