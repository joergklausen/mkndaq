import os
import sys
import shutil
sys.path.insert(0,'/home/admin')
import version
# definition variables

if len(sys.argv) > 1 :
	version_soft = sys.argv[1]
	source_folder = '/home/admin/tmp_'+version_soft
	ftp_user = 'admin'
	ftp_pass = 'admin2009'
	ftp_server = '10.182.129.100'

	try :
		if not os.path.exists(source_folder) :															# check if SARA temp script folders exist																
			os.makedirs(source_folder)	
			os.chdir(source_folder)		
			cmd = 'lftp %s:%s@%s -e "get script/%s/get.py ;quit"' %(ftp_user,ftp_pass,ftp_server,version_soft)
			os.system(cmd)
			cmd = '/usr/bin/python get.py'
			os.system(cmd)
			shutil.rmtree(source_folder)
			print 'upgrade %s done.' %version_soft
	except Exception as e:
		print 'Error. Upgrade aborted.'
		print e.message, e.args
else :
	print 'argument missing'


