import sys
sys.path.insert(0,'/home/admin')
from version import _STATION_IPMODEM
import subprocess
from subprocess import Popen, PIPE
import os
import netifaces
import time

def is_interface_up(interface):
	try:
		addr = netifaces.ifaddresses(interface)
		return netifaces.AF_INET in addr
	except Exception,e :
		print interface+ " not valid or disabled"
		return False

def pingOK (sHOST):
	try:
#		print "try ping "+sHOST + " network ..."
		response = os.system("ping -c 1 "+ sHOST)
		if response == 0:
#			print "ping success"
			return True
		else :
#			print "ping error"
			return False
	except Exception,e :
		print e
		return False
	


print "test if SMN connection working ..."	
if not pingOK('10.182.128.10'):
	print "Communication not working... check connections\n"
	try:
		if is_interface_up('eth0'):
			print "eth0 up"
			if pingOK(_STATION_IPMODEM):
				print "\t subnetwork is working. GSM bug"
			else :
				print "Please check connection.\nReboot"
				time.sleep(10)
				os.system('shutdown -r now')
		else :
			print "eth0 not working. Please check connection.\nReboot"
			time.sleep(10)
			os.system('shutdown -r now')
			

	except Exception,e :
		print e
		print "error, try again in 2 minutes."
	
