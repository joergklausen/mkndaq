@echo off

set a=c:/users/jkl/public/git/mkndaq/dist
set b=mkndaq.exe 
set c=mkndaq.cfg
set d=%a%/%b% -c %a%/%c%

rem open CLI and run batch file, return to CLI
cmd /k %d%