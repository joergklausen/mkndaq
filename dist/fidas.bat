@echo off
rem set a=c:/users/mkn/mkndaq
set a=c:/users/mkn/Documents/git/mkndaq
set b=__fidas__.py
set c=mkndaq.yml
set d=%a%/%b% -c %a%/dist/%c%
set e=mkndaq-start.log
set f=%a%/dist/%e%

rem record execution of __fidas__.py to logfile.
rem echo %date:~6,4%-%date:~3,2%-%date:~0,2% %time:~0,8% __fidas__.py started. >> %f%

@echo off
SETLOCAL EnableExtensions
FOR /F %%x IN ('tasklist /NH /FI "IMAGENAME eq %b%"') DO IF %%x == %b% goto ProcessFound
goto ProcessNotFound

:ProcessFound
rem echo %b% is running, no further action taken.
echo %date:~6,4%-%date:~3,2%-%date:~0,2% %time:~0,8% %b% running. >> %f%
goto END

:ProcessNotFound
rem echo Starting %b% ...
call C:\Users\mkn\Documents\git\mkndaq\.venv\Scripts\activate.bat
echo %date:~6,4%-%date:~3,2%-%date:~0,2% %time:~0,8% .venv activated. >> %f%
cd /d C:\Users\mkn\Documents\git\mkndaq
python -u C:\Users\mkn\Documents\git\mkndaq\__fidas__.py

echo %date:~6,4%-%date:~3,2%-%date:~0,2% %time:~0,8% %b% started. >> %f%
rem open CLI and run batch file, return to CLI
cmd /k %d%
goto END

:END
echo Finished!
