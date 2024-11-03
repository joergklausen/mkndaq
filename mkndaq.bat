@echo off
set root=c:/users/mkn/Documents
set git=%root%/git/mkndaq
set venv=%git%/.venv/Scripts
set config=%git%/dist/mkndaq.yml
set log=%root%/mkndaq/mkndaq-start.log
set script=%git%/__main__.py

rem record execution of mkndaq.bat to logfile.
echo %date:~6,4%-%date:~3,2%-%date:~0,2% %time:~0,8% mkndaq.bat started. >> %log%

@echo off
SETLOCAL EnableExtensions

echo script: %script%

echo activate virtual environment and run script
cmd /k "%venv%/activate.bat && %venv%/python %script%"