@echo off

set a=c:/users/jkl/public/git/gaw-mkn-daq
set b=dist/mkndaq.exe 
set c=mkndaq/mkndaq.cfg
set d=%a%/%b% -c %a%/%c%

rem open CLI and run batch file, return to CLI
cmd /k %d%