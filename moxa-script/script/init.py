#------------------------------------------------------------------------------------------------
# file : init.py
# author : Grandjean Jacques
# version : 1.0
# date : 14.12.2017
# description : configure cron, watchdog, folder, link,...
#------------------------------------------------------------------------------------------------

# modules import
import os
import shutil
import sys
sys.path.insert(0,'/home/admin')
import version

os.chdir('/')

for link in ('SCRIPT','DATA','bulletin') :
	if os.path.islink(link):
		os.remove(link)

cmd1 = 'umount /mnt/sd-mmcblk1p1'
cmd2 = 'mount -t vfat -o rw,umask=000 /dev/mmcblk1p1 /mnt/sd-mmcblk1p1'
os.system(cmd1)
os.system(cmd2)

os.symlink('/media/sd-mmcblk1p1','/DATA')

if os.path.isdir('/DATA/' + version.PROJECT):
	shutil.rmtree('/DATA/' + version.PROJECT)
	
os.makedirs('/DATA/' + version.PROJECT)
os.makedirs('/DATA/' + version.PROJECT + '/bulletin')
os.makedirs('/DATA/' + version.PROJECT + '/data')
os.makedirs('/DATA/' + version.PROJECT + '/log')

os.symlink('/home/admin/' + version.PROJECT + '/script','/SCRIPT')
os.symlink('/DATA/' + version.PROJECT + '/bulletin','/bulletin')

for root, dirs, files in os.walk('/DATA'):													# adjust permission rights of new folder and files
	for d in dirs:
		os.chmod(os.path.join(root, d), 0777)
	for f in files:
		os.chmod(os.path.join(root, f), 0777)

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
elif (version.TYPE == 'KENYA'):
	shutil.copyfile('/SCRIPT/current_base.txt','/DATA/'+version.PROJECT+'/current.txt')
	cmd = 'crontab /SCRIPT/crontab-bak'
	os.system(cmd) 
#	shutil.copyfile('/SCRIPT/initab-bak','/etc/inittab')
	os.system(cmd) 
else:
	shutil.copyfile('/SCRIPT/current_base.txt','/DATA/'+version.PROJECT+'/current.txt')
	cmd = 'crontab /SCRIPT/crontab-bak'
	os.system(cmd) 

shutil.copyfile('/SCRIPT/watchdog-bak','/etc/watchdog.conf')


