import time, os, sys
from shutil import move
from config import _ADDR_bulletin,_MHSQCT

sys.path.insert(0,'/home/admin')
from version import _STATIONname
from version import TYPE
time_bulletin=time.strftime('%Y%m%d%H%M')

content= "500\n" + _MHSQCT +" LSSW " + time_bulletin[6:] + '\n\nkkk zzzztttt ycoml1s0\n' + _STATIONname + ' ' + time_bulletin + ' 1'  


namefile=_MHSQCT + "." + time_bulletin + ".500"
f=open(_ADDR_bulletin+"." +namefile ,'w')
f.write(content)
f.close()
os.chmod(_ADDR_bulletin+"." +namefile,0777)
move(_ADDR_bulletin+"." +namefile , _ADDR_bulletin+namefile )

print "Bulletin DWH QCT created"
