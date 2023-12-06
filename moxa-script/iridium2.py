import time
import ConfigParser 
import serial
import os
import re
from config import _MHSDWH, _LIST_VAR_IRIDIUM
from datetime import datetime, timedelta

import socket
socket.setdefaulttimeout(1)

import sys
sys.path.insert(0,'/home/admin')
from version import PROJECT

try:	
	ser=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
except socket.timeout, e :
	print ("TCP socket error")

def SendToIRI(msg):
	try:
		ser.connect(("10.182.255.2",4002))
		ser.send(msg)
		ser.close()
	except :
		print ("send command error")
	return

def GetMsg(content):
	msg='\x02'+time_file.strftime('%Y.%m.%d,%H:%M')[:-1]+"0,"
	try :
		list_iridium = _LIST_VAR_IRIDIUM.split()
		list_variable=content[3].split()
		list_value=content[4].split()
		
		for var in list_iridium :
			if list_variable.count(var)==1:
				msg=msg+list_value[list_variable.index(var)]+","
			else :
				msg=msg+'?,' 
		msg=msg+'\x03'
	except :
		msg='\x02'+time_file.strftime('%Y.%m.%d,%H:%M')[:-1]+"0,"
		msg=msg+','.join("?" for x in range(len(_LIST_VAR_IRIDIUM.split())))+',\x03'
	return msg

	
time_file=datetime.now()
print time_file.strftime('%Y%m%d%H%M')
FindFile=False
for filename in os.listdir("/DATA/"+PROJECT+"/data"):
	if re.match(_MHSDWH+'.'+time_file.strftime('%Y%m%d%H%M')[:-1]+"0.001",filename):
		print "file exist, send by iridium"
		SendToIRI(GetMsg(open("/DATA/"+PROJECT+"/data/"+_MHSDWH+'.'+time_file.strftime('%Y%m%d%H%M')[:-1]+"0.001").readlines()))
		FindFile=True
		

if not (FindFile):
	print "time iridium test"
	msg='\x02'+time_file.strftime('%Y.%m.%d,%H:%M')[:-1]+"0,"
	msg=msg+','.join("?" for x in range(len(_LIST_VAR_IRIDIUM.split())))+',\x03'
	SendToIRI(msg)
