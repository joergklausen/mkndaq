#------------------------------------------------------------------------------------------------
# file : get.py
# author : Grandjean Jacques
# version : 1.0
# date : 14.12.2017
# description : download all files from server depending station and type. prepare and config linuxbox
#------------------------------------------------------------------------------------------------

# modules import
import os
import sys
import shutil 
sys.path.insert(0,'/home/admin')
import version

# definition variables
version_soft = 'DEV_KENYA_2.1'
version_description = 'LinuxBox logger for MCH instrument'
interface='INTERFACE_NAIROBI'
source_folder = '/home/admin/'
temp_script_folder = source_folder + version.PROJECT + '/script_tmp/'
old_script_folder = source_folder + version.PROJECT + '/oldscript/'
script_folder = source_folder + version.PROJECT + '/script/'
ftp_user = 'admin'
ftp_pass = 'admin2009'
ftp_server = '10.182.129.100'
			
print 'Install new software : %s' %(version_soft)

if (os.path.isfile(source_folder+'version.py')) :														# check is version.py exist

	if not os.path.exists(temp_script_folder) :															# check if SARA temp script folders exist																
		os.makedirs(temp_script_folder)																		# if not, create it and add 777 permission for all		
	if not os.path.exists(old_script_folder) :															# check if SARA old script folders exist																								
		os.makedirs(old_script_folder)																		# if not, create it and add 777 permission for all	
	if not os.path.exists(script_folder) :																# check if SARA script folders exist
		os.makedirs(script_folder)																			# if not, create it and add 777 permission for all

	print '\tDownload files from server ...'
	os.chdir(temp_script_folder)																		# load all script's files from server (temporary folder)
	cmd = 'lftp %s:%s@%s -e "mget script/%s/*;quit"' %(ftp_user,ftp_pass,ftp_server,version_soft)
	os.system(cmd)
	
	for root, dirs, files in os.walk(source_folder):													# adjust permission rights of new folder and files
		for d in dirs:
			os.chmod(os.path.join(root, d), 0777)
		for f in files:
			os.chmod(os.path.join(root, f), 0777)
	
	print '\tUpdate info station on server ...'
	
	cmd = """sed -i 's/^\VERSION=.*/\VERSION=\"%s\"/' /home/admin/version.py""" %version_soft
	os.system(cmd)
	cmd = """sed -i 's/^\DESCRIPTION=.*/\DESCRIPTION=\"%s\"/' /home/admin/version.py""" %version_description
	os.system(cmd)


	cmd = 'lftp %s:%s@%s -e "put -O stations/%s/ ' %(ftp_user,ftp_pass,ftp_server, version._STATIONname) + source_folder + 'version.py -o version_%s.py; quit"' %(version.TYPE)
	os.system(cmd)
	
	for f in os.listdir(script_folder):																	# move old script (if exist) to old script folder
		target = os.path.join(old_script_folder, f)
		try:
			shutil.rmtree(target)
		except:
			try:
				os.unlink(target)
			except:
				pass
		shutil.move(os.path.join(script_folder, f), target)
	
	for f in os.listdir(temp_script_folder):															# move new scripts to script folder
		target = os.path.join(script_folder, f)
		try:
			shutil.rmtree(target)
		except:
			try:
				os.unlink(target)
			except:
				pass
		shutil.move(os.path.join(temp_script_folder, f), target)
		
	print '\tInitialization ...'	
	cmd = '/usr/bin/python ' + script_folder + 'init.py'												# load configuration LinuxBox
	os.system(cmd)
	
	cmd = '/usr/bin/python ' + script_folder + 'getConfig.py'											# load configuration Software
	os.system(cmd)
	
	cmd = '/usr/bin/python ' + script_folder + 'upgrade.py %s' %(interface)											# load configuration Software
	os.system(cmd)
	
	print 'done.'
	
else :
	print 'No version file found. Contact your administrator. Installation aborted.'


