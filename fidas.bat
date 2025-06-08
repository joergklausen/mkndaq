@echo off
setlocal 

REM === Configuration ===
set PROJECT_DIR=C:\Users\mkn\Documents\git\mkndaq
set PYTHON_EXE=%PROJECT_DIR%\.venv\Scripts\python.exe
set SCRIPT_NAME=__fidas__.py
set SCRIPT=%PROJECT_DIR%\%SCRIPT_NAME%
set LOG_FILE=C:\Users\mkn\Documents\mkndaq\fidas-start.log

REM === Get timestamp ===
for /f %%a in ('wmic os get localdatetime ^| find "."') do set ldt=%%a
set TIMESTAMP=%ldt:~0,4%-%ldt:~4,2%-%ldt:~6,2% %ldt:~8,2%:%ldt:~10,2%:%ldt:~12,2%

call :log "Attempting to start %SCRIPT_NAME%"

REM === Check if script is already running ===
tasklist /FI "IMAGENAME eq python.exe" /V | findstr /I "%SCRIPT_NAME%" >nul

if %ERRORLEVEL%==0 (
    call :log "Script %SCRIPT_NAME% is already running. Skipping startup."
    exit /b 0
)

cd /d "%PROJECT_DIR%"

call :log "Starting script %SCRIPT_NAME%"

"%PYTHON_EXE%" "%SCRIPT%"
rem echo "%PYTHON_EXE%" "%SCRIPT%"
rem pause >nul
exit /b 0

:log
set MSG=[%TIMESTAMP%] %~1
echo %MSG%
echo %MSG% >> "%LOG_FILE%"
goto :eof
