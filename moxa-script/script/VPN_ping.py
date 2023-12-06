import sys
sys.path.insert(0,'/home/admin')
from version import _VPN_user, _VPN_password, _VPN_host
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
	print "Not working... check connections\n"
	try:
		if is_interface_up('eth0'):
			print "eth0 up"
			if not is_interface_up('tun0'):
				print "tun0 VPN down"
				print "\n... test ADSL connection ..."
				if pingOK('172.217.18.35'):
					print "\t ADSL is working."
					print "\t... connect to smn VPN ..."
					subprocess.Popen(['/bin/sh','/SCRIPT/VPN_connect.sh', _VPN_user, _VPN_password, _VPN_host])
					print "\t... done."
				else :
					print "ADSL not working. Please check connection.\nReboot if full hour"
					if (time.strftime("%M")=="13"):
						os.system('shutdown -r now')

			else :
				print "tun0 VPN up"
				print "ADSL not working. Please check connection.\nReboot if full hour"
				if (time.strftime("%M")=="13"):
					os.system('shutdown -r now')
		else :
			print "eth0 down. ADSL not working. Please check connection.\nReboot if full hour"
			if (time.strftime("%M")=="13"):
				os.system('shutdown -r now')
			

	except Exception,e :
		print e
		print "error connection VPN, try again in 2 minutes."
	
