import sys,ftplib,os
sys.path.insert(0,'/home/admin')
from version import _STATIONname
from version import TYPE

try :
	ftp=ftplib.FTP("10.182.129.100")
	ftp.login("admin","admin2009")
	ftp.cwd("stations/"+_STATIONname)
	os.chdir("/SCRIPT")
	ftp.retrbinary('RETR config_'+TYPE+'.py', open('config_tmp.py', 'wb').write)
	ftp.quit()
	if (os.path.isfile('config.py')):
		os.rename('config.py', 'oldconfig.py')
	os.rename('config_tmp.py','config.py')
except ftplib.all_errors, e :
	print "download config aborted, %s" %(e)
	

