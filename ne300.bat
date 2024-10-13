@echo off
set root=c:/users/mkn/Documents
set git=%root%/git/mkndaq
set venv=%git%/.venv/Scripts
set config=%git%/dist/mkndaq.yml
set log=%root%/mkndaq/ne300-start.log
rem set script=%git%/mkndaq/ne300.py -c %config%
set script=%git%/ne300.py

rem record execution of mkndaq.bat to logfile.
echo %date:~6,4%-%date:~3,2%-%date:~0,2% %time:~0,8% mkndaq.bat started. >> %log%

@echo off
SETLOCAL EnableExtensions

echo script: %script%

echo activate virtual environment and run script
cmd /k "%venv%/activate.bat && %venv%/python %script%"

rem FOR /F %%x IN ('tasklist /NH /FI "IMAGENAME eq %b%"') DO IF %%x == %b% goto ProcessFound
rem goto ProcessNotFound

rem :ProcessFound
rem rem echo %b% is running, no further action taken.
rem echo %date:~6,4%-%date:~3,2%-%date:~0,2% %time:~0,8% %b% running. >> %f%
rem goto END

rem :ProcessNotFound
rem rem echo Starting %b% ...
rem echo %date:~6,4%-%date:~3,2%-%date:~0,2% %time:~0,8% %b% started. >> %f%
rem rem open CLI and run batch file, return to CLI
rem cmd /k %d%
rem goto END

rem :END
rem echo Finished!
