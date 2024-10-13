@echo off
rem set a=c:/users/mkn/mkndaq
set a=c:/users/mkn/Documents/git/mkndaq/dist
set b=mkndaq.exe
set c=mkndaq.yml
set d=%a%/%b% -c %a%/%c%
set e=mkndaq-start.log
set f=%a%/%e%

rem record execution of mkndaq.bat to logfile.
rem echo %date:~6,4%-%date:~3,2%-%date:~0,2% %time:~0,8% mkndaq.bat started. >> %f%

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
echo %date:~6,4%-%date:~3,2%-%date:~0,2% %time:~0,8% %b% started. >> %f%
rem open CLI and run batch file, return to CLI
cmd /k %d%
goto END

:END
echo Finished!
