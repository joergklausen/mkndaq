#------------------------------------------------------------------------------------------------
# file : reset.py
# author : Grandjean Jacques
# version : 1.0
# date : 1.07.2018
# description : reset current file,...
#------------------------------------------------------------------------------------------------

# modules import
import os
import shutil
import sys
sys.path.insert(0,'/home/admin')
import version

os.chdir('/')

if (version.TYPE == 'URANET_AQUA'):
	shutil.copyfile('/SCRIPT/current_base_AQUA.txt','/DATA/'+version.PROJECT+'/current.txt')
	cmd = 'crontab /SCRIPT/crontab-bak-AQUA'
	os.system(cmd)
elif (version.TYPE == 'URANET_AIR'):
	shutil.copyfile('/SCRIPT/current_base_AIR.txt','/DATA/'+version.PROJECT+'/current.txt')
	cmd = 'crontab /SCRIPT/crontab-bak-AIR'
	os.system(cmd)
elif (version.TYPE == 'URANET_IODE'):
	shutil.copyfile('/SCRIPT/current_base_IODE.txt','/DATA/'+version.PROJECT+'/current.txt')
	cmd = 'crontab /SCRIPT/crontab-bak-IODE'
	os.system(cmd) 
if (version.TYPE == 'REGA'):
	shutil.copyfile('/SCRIPT/current_base.txt','/DATA/'+version.PROJECT+'/current.txt')
	cmd = 'crontab /SCRIPT/crontab-bak'
	os.system(cmd)
if (version.TYPE == 'ONESNOW'):
	shutil.copyfile('/SCRIPT/current_base.txt','/DATA/'+version.PROJECT+'/current.txt')
	cmd = 'crontab /SCRIPT/crontab-bak'
	os.system(cmd)
if (version.TYPE == 'LOGGER'):
	shutil.copyfile('/SCRIPT/current_base.txt','/DATA/'+version.PROJECT+'/current.txt')
	cmd = 'crontab /SCRIPT/crontab-bak'
	os.system(cmd)
