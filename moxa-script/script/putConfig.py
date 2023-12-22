import sys,ftplib,os
sys.path.insert(0,'/home/admin')
from version import _STATIONname
from version import TYPE

try :
   ftp=ftplib.FTP("10.182.129.100")
   ftp.login("admin","admin2009")
   ftp.cwd("stations/"+_STATIONname)
   os.chdir("/SCRIPT")
   file=open('config.py','rb')
   ftp.storbinary('STOR config_'+TYPE+'.py', file)
   file.close()
   ftp.quit()
except ftplib.all_errors, e :
   print "upload config aborted, %s" %(e)