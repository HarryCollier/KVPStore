@echo off
setlocal

set "CONF=%~dp0cluster.conf"

if not exist "%CONF%" (
    echo Cannot find %CONF%
    pause
    exit /b 1
)

for /f "tokens=1,2" %%A in (%CONF%) do (
    echo Starting Node %%A %%B
    start "" java Node %%A %%B
)

pause