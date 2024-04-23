
# get data from MCH Sensors, save into a file (program derived from Onesnow project)
# author : MeteoSwiss / Grandjean Jacques 
# date : 18.09.2018
# version : 0.1

import serial
import time
import shlex
import socket
from threading import *
socket.setdefaulttimeout(1)
import os
from shutil import move, copyfile
from subprocess import Popen, PIPE, STDOUT

from config import _ADDR_bulletin, _ADDR_data, _ADDR_data_info, _MHSRAW, _MHSDWH, _INIT_TIME_BULLETIN_RAW,_INIT_TIME_BULLETIN_DWH, _REPEAT_TIME_BULLETIN_DWH,_REPEAT_TIME_BULLETIN_RAW,\
	_NOTE_SENSOR_1, _NOTE_SENSOR_2, _NOTE_SENSOR_3, _NOTE_SENSOR_4,_NOTE_SENSOR_5, _NOTE_SENSOR_6, _NOTE_SENSOR_7, _NOTE_SENSOR_8,_NOTE_SENSOR_9, _NOTE_SENSOR_10,\
	_TYPE_SENSOR_1, _TYPE_SENSOR_2, _TYPE_SENSOR_3, _TYPE_SENSOR_4,_TYPE_SENSOR_5, _TYPE_SENSOR_6, _TYPE_SENSOR_7, _TYPE_SENSOR_8,_TYPE_SENSOR_9, _TYPE_SENSOR_10,\
	_STATUS_SENSOR_1,_STATUS_SENSOR_2,_STATUS_SENSOR_3,_STATUS_SENSOR_4,_STATUS_SENSOR_5,_STATUS_SENSOR_6,_STATUS_SENSOR_7,_STATUS_SENSOR_8,_STATUS_SENSOR_9,_STATUS_SENSOR_10,_STATUS_BULLETIN_DWH,_STATUS_BULLETIN_RAW,\
	_STATUS_RAW_SENSOR_1,_STATUS_RAW_SENSOR_2,_STATUS_RAW_SENSOR_3,_STATUS_RAW_SENSOR_4,_STATUS_RAW_SENSOR_5,_STATUS_RAW_SENSOR_6,_STATUS_RAW_SENSOR_7,_STATUS_RAW_SENSOR_8,_STATUS_RAW_SENSOR_9,_STATUS_RAW_SENSOR_10,\
	_STATUS_DWH_SENSOR_1,_STATUS_DWH_SENSOR_2,_STATUS_DWH_SENSOR_3,_STATUS_DWH_SENSOR_4,_STATUS_DWH_SENSOR_5,_STATUS_DWH_SENSOR_6,_STATUS_DWH_SENSOR_7,_STATUS_DWH_SENSOR_8,_STATUS_DWH_SENSOR_9,_STATUS_DWH_SENSOR_10,\
	_PORT_SENSOR_1,_PORT_SENSOR_2,_PORT_SENSOR_3,_PORT_SENSOR_4,_PORT_SENSOR_5,_PORT_SENSOR_6,_PORT_SENSOR_7,_PORT_SENSOR_8,_PORT_SENSOR_9,_PORT_SENSOR_10,\
	_PROTOCOLE_SENSOR_1,_PROTOCOLE_SENSOR_2, _PROTOCOLE_SENSOR_3,_PROTOCOLE_SENSOR_4,_PROTOCOLE_SENSOR_5,_PROTOCOLE_SENSOR_6, _PROTOCOLE_SENSOR_7,_PROTOCOLE_SENSOR_8,_PROTOCOLE_SENSOR_9,_PROTOCOLE_SENSOR_10,\
	_BAUDRATE_SENSOR_1,_BAUDRATE_SENSOR_2,_BAUDRATE_SENSOR_3,_BAUDRATE_SENSOR_4,_BAUDRATE_SENSOR_5,_BAUDRATE_SENSOR_6,_BAUDRATE_SENSOR_7,_BAUDRATE_SENSOR_8,_BAUDRATE_SENSOR_9,_BAUDRATE_SENSOR_10,\
	_NBITS_SENSOR_1,_NBITS_SENSOR_2,_NBITS_SENSOR_3,_NBITS_SENSOR_4,_NBITS_SENSOR_5,_NBITS_SENSOR_6,_NBITS_SENSOR_7,_NBITS_SENSOR_8,_NBITS_SENSOR_9,_NBITS_SENSOR_10,\
	_PARITY_SENSOR_1,_PARITY_SENSOR_2,_PARITY_SENSOR_3,_PARITY_SENSOR_4,_PARITY_SENSOR_5,_PARITY_SENSOR_6,_PARITY_SENSOR_7,_PARITY_SENSOR_8,_PARITY_SENSOR_9,_PARITY_SENSOR_10,\
	_STOPBIT_SENSOR_1,_STOPBIT_SENSOR_2,_STOPBIT_SENSOR_3,_STOPBIT_SENSOR_4,_STOPBIT_SENSOR_5,_STOPBIT_SENSOR_6,_STOPBIT_SENSOR_7,_STOPBIT_SENSOR_8,_STOPBIT_SENSOR_9,_STOPBIT_SENSOR_10,\
	_TIMEOUT_SENSOR_1,_TIMEOUT_SENSOR_2,_TIMEOUT_SENSOR_3,_TIMEOUT_SENSOR_4,_TIMEOUT_SENSOR_5,_TIMEOUT_SENSOR_6,_TIMEOUT_SENSOR_7,_TIMEOUT_SENSOR_8,_TIMEOUT_SENSOR_9,_TIMEOUT_SENSOR_10,\
	_SIZE_SENSOR_1,_SIZE_SENSOR_2,_SIZE_SENSOR_3,_SIZE_SENSOR_4,_SIZE_SENSOR_5,_SIZE_SENSOR_6,_SIZE_SENSOR_7,_SIZE_SENSOR_8,_SIZE_SENSOR_9,_SIZE_SENSOR_10,\
	_INIT_TIME_SENSOR_1,_INIT_TIME_SENSOR_2,_INIT_TIME_SENSOR_3,_INIT_TIME_SENSOR_4,_INIT_TIME_SENSOR_5,_INIT_TIME_SENSOR_6,_INIT_TIME_SENSOR_7,_INIT_TIME_SENSOR_8,_INIT_TIME_SENSOR_9,_INIT_TIME_SENSOR_10,\
	_REPEAT_TIME_SENSOR_1,_REPEAT_TIME_SENSOR_2,_REPEAT_TIME_SENSOR_3,_REPEAT_TIME_SENSOR_4,_REPEAT_TIME_SENSOR_5,_REPEAT_TIME_SENSOR_6,_REPEAT_TIME_SENSOR_7,_REPEAT_TIME_SENSOR_8,_REPEAT_TIME_SENSOR_9,_REPEAT_TIME_SENSOR_10,\
	_CMD_SENSOR_1,_CMD_SENSOR_2,_CMD_SENSOR_3,_CMD_SENSOR_4,_CMD_SENSOR_5,_CMD_SENSOR_6,_CMD_SENSOR_7,_CMD_SENSOR_8,_CMD_SENSOR_9,_CMD_SENSOR_10,\
	_INIT_VAR_SENSOR_1,_INIT_VAR_SENSOR_2,_INIT_VAR_SENSOR_3,_INIT_VAR_SENSOR_4,_INIT_VAR_SENSOR_5,_INIT_VAR_SENSOR_6,_INIT_VAR_SENSOR_7,_INIT_VAR_SENSOR_8,_INIT_VAR_SENSOR_9,_INIT_VAR_SENSOR_10,\
	_HOST_SENSOR_1,_HOST_SENSOR_2,_HOST_SENSOR_3,_HOST_SENSOR_4,_HOST_SENSOR_5,_HOST_SENSOR_6,_HOST_SENSOR_7,_HOST_SENSOR_8,_HOST_SENSOR_9,_HOST_SENSOR_10,\
	_DESC_SENSOR_1,_DESC_SENSOR_2,_DESC_SENSOR_3,_DESC_SENSOR_4,_DESC_SENSOR_5,_DESC_SENSOR_6,_DESC_SENSOR_7,_DESC_SENSOR_8,_DESC_SENSOR_9,_DESC_SENSOR_10,\
	_VAR_LIST_SENSOR_1,_VAR_LIST_SENSOR_2,_VAR_LIST_SENSOR_3,_VAR_LIST_SENSOR_4,_VAR_LIST_SENSOR_5,_VAR_LIST_SENSOR_6,_VAR_LIST_SENSOR_7,_VAR_LIST_SENSOR_8,_VAR_LIST_SENSOR_9,_VAR_LIST_SENSOR_10,\
	_SLOPE_ANALYSER,_OFFSET_ANALYSER,_OFFSET_PRESSURE,_SLOPE_PRECIP


import sys
sys.path.insert(0, '/home/admin/')
from version import _STATIONname, _STATIONnumber,_STATION_IPLOGGER
import ConfigParser
current=ConfigParser.SafeConfigParser()

buffer_sensor_1=""
buffer_sensor_2=""
buffer_sensor_3=""
buffer_sensor_4=""
buffer_sensor_5=""
buffer_sensor_6=""
buffer_sensor_7=""
buffer_sensor_8=""
buffer_sensor_9=""
buffer_sensor_10=""
DWH_value_sensor_1=""
DWH_value_sensor_2=""
DWH_value_sensor_3=""
DWH_value_sensor_4=""
DWH_value_sensor_5=""
DWH_value_sensor_6=""
DWH_value_sensor_7=""
DWH_value_sensor_8=""
DWH_value_sensor_9=""
DWH_value_sensor_10=""
TemperatureDisplay="9999"
DewPointDisplay="9999"
HumidityDisplay="9999"
PressureDisplay="9999"
WindSpeedDisplay="9999"
WindDirDisplay="9999"
PrecipitationDisplay="9999"
OzoneDisplay="9999"


screen_lock=Semaphore(value=1)

def nbit_five():
	return serial.FIVEBITS
def nbit_six():
	return serial.SIXTBITS
def nbit_seven():
	return serial.SEVENBITS
def nbit_eight():
	return serial.EIGHTBITS
serial_nbit = {"5": nbit_five,
		   "6": nbit_six,
		   "7": nbit_seven,
           "8": nbit_eight,}
		   
		   
def parity_none():
	return serial.PARITY_NONE
def parity_even():
	return serial.PARITY_EVEN
def parity_odd():
	return serial.PARITY_ODD
def parity_mark():
	return serial.PARITY_MARK
def parity_space():
	return serial.PARITY_SPACE
serial_parity = {"NONE": parity_none,
           "EVEN": parity_even,
		   "ODD": parity_odd,
		   "MARK": parity_mark,
		   "SPACE": parity_space,}
		   
def stopbit_one():
	return serial.STOPBITS_ONE
def stopbit_one_point_five():
	return serial.STOPBITS_ONE_POINT_FIVE
def stopbit_two():
	return serial.STOPBITS_TWO
serial_stopbit = {"1": stopbit_one,
           "1.5": stopbit_one_point_five,
		   "2": stopbit_two,}
	

#--------------------------------------------
def ProcessSHM30_standard(VALUE,BUFFER): 
	if (VALUE!="") : 
		screen_lock.acquire()
		print VALUE
		screen_lock.release()
	RAW_value=VALUE
	DWH_value=VALUE.replace("<","").replace(">","").replace("+","")
	DWH_value=DWH_value.split(" ")[0] + " " + DWH_value.split(" ")[1] + " " + DWH_value.split(" ")[2] + " " + DWH_value.split(" ")[3]
	return BUFFER+time.strftime("\n\t\t\t<%Y-%m-%d/%H:%M:%S>") + RAW_value + time.strftime("<\\%Y-%m-%d/%H:%M:%S>"), DWH_value
	
def ProcessPT100_standard(VALUE,BUFFER):
	if (VALUE!="") : 
		screen_lock.acquire()
		print VALUE
		screen_lock.release()
	RAW_value=VALUE
	DWH_value=VALUE.split(",")[2]
	return BUFFER+time.strftime("\n\t\t\t<%Y-%m-%d/%H:%M:%S>") + RAW_value + time.strftime("<\\%Y-%m-%d/%H:%M:%S>"), DWH_value
			
def ProcessApogee_1x(VALUE,BUFFER):			
	if (VALUE!="") :
		screen_lock.acquire()
		print VALUE
		screen_lock.release()
	RAW_value=VALUE
	DWH_value=VALUE.split(",")[0] + " " + VALUE.split(",")[1] + " "
	return BUFFER+time.strftime("\n\t\t\t<%Y-%m-%d/%H:%M:%S>") + RAW_value + time.strftime("<\\%Y-%m-%d/%H:%M:%S>"), DWH_value
 
def ProcessApogee_2x(VALUE,BUFFER):	 
	if (VALUE!="") :
		screen_lock.acquire()
		print VALUE
		screen_lock.release()
	RAW_value=VALUE
	DWH_value=VALUE.split(",")[0] + " " + VALUE.split(",")[1] + " " + VALUE.split(",")[2] + " " + VALUE.split(",")[3] + " "
	return BUFFER+time.strftime("\n\t\t\t<%Y-%m-%d/%H:%M:%S>") + RAW_value + time.strftime("<\\%Y-%m-%d/%H:%M:%S>"), DWH_value
			
def ProcessWS600_msgM0(VALUE,BUFFER):
	global TemperatureDisplay
	global DewPointDisplay
	global HumidityDisplay
	global PressureDisplay
	global WindSpeedDisplay
	global WindDirDisplay
	global PrecipitationDisplay

	if (VALUE!="") :
		screen_lock.acquire()
		print VALUE
		screen_lock.release()	
		RAW_value=VALUE.replace("\n","").replace("\r","")
		TemperatureDisplay=RAW_value.replace(";"," ").split()[1]
		DewPointDisplay=RAW_value.replace(";"," ").split()[1]
		HumidityDisplay=RAW_value.replace(";"," ").split()[4]
		PressureDisplay=str(round(float(RAW_value.replace(";"," ").split()[5]) + float(_OFFSET_PRESSURE),1))
		WindSpeedDisplay=RAW_value.replace(";"," ").split()[6]
		WindDirDisplay=RAW_value.replace(";"," ").split()[7]
		PrecipitationDisplay=str(round(float(RAW_value.replace(";"," ").split()[8]) * float(_SLOPE_PRECIP),1))
		DWH_value=VALUE.replace("<","").replace(">","").replace("+","").replace("\r","").replace("\n","").replace(";"," ")
		DWH_value=DWH_value.split(" ")[1] + " " + DWH_value.split(" ")[4] + " " + DWH_value.split(" ")[5] + " " + DWH_value.split(" ")[6] + " " + DWH_value.split(" ")[7] + " " + DWH_value.split(" ")[8]
	else :
		RAW_value="no data"
		DWH_value=""
		TemperatureDisplay="9999"
		DewPointDisplay="9999"
		HumidityDisplay="9999"
		PressureDisplay="9999"
		WindSpeedDisplay="9999"
		WindDirDisplay="9999"
		PrecipitationDisplay="9999"
	return BUFFER+time.strftime("\n\t\t\t<%Y-%m-%d/%H:%M:%S>") + RAW_value + time.strftime("<\\%Y-%m-%d/%H:%M:%S>"), DWH_value

def ProcessWS600_msgM2(VALUE,BUFFER):
	if (VALUE!="") :
		screen_lock.acquire()
		print VALUE
		screen_lock.release()
		RAW_value=VALUE.replace("\n","").replace("\r","")
		DWH_value=VALUE.replace("<","").replace(">","").replace("+","").replace("\r","").replace("\n","").replace(";"," ")
		DWH_value=DWH_value.split(" ")[3]
	else :
		RAW_value="no data"
		DWH_value=""
	return BUFFER+time.strftime("\n\t\t\t<%Y-%m-%d/%H:%M:%S>") + RAW_value + time.strftime("<\\%Y-%m-%d/%H:%M:%S>"), DWH_value

def ProcessWS600_msgM4(VALUE,BUFFER):
	if (VALUE!="") :
		screen_lock.acquire()
		print VALUE
		screen_lock.release()
		RAW_value=VALUE.replace("\n","").replace("\r","")
		DWH_value=VALUE.replace("<","").replace(">","").replace("+","").replace("\r","").replace("\n","").replace(";"," ")
		DWH_value=DWH_value.split(" ")[5]
	else :
		RAW_value="no data"
		DWH_value=""
	return BUFFER+time.strftime("\n\t\t\t<%Y-%m-%d/%H:%M:%S>") + RAW_value + time.strftime("<\\%Y-%m-%d/%H:%M:%S>"), DWH_value

def ProcessWS600_R1(VALUE,BUFFER):
	DWH_value=""
	return BUFFER, DWH_value
	
	
def ProcessPWD22_standard(VALUE,BUFFER):
	if (VALUE!="") :
		screen_lock.acquire()
		print VALUE
		screen_lock.release()
	RAW_value=VALUE.replace("\n","").replace("\r","")
	DWH_value=VALUE
#			DWH_value=VALUE.replace("<","").replace(">","").replace("+","").replace("\r","").replace("\n","").replace(";"," ")
#			DWH_value=DWH_value.split(" ")[1] + " " + DWH_value.split(" ")[3] + " " + DWH_value.split(" ")[4] + DWH_value.split(" ")[4] + " " + DWH_value.split(" ")[5] + " " + DWH_value.split(" ")[6] + DWH_value.split(" ")[7] + DWH_value.split(" ")[8] + " " + DWH_value.split(" ")[9] + " " + DWH_value.split(" ")[10] +" "
	return BUFFER+time.strftime("\n\t\t\t<%Y-%m-%d/%H:%M:%S>") + RAW_value + time.strftime("<\\%Y-%m-%d/%H:%M:%S>"), DWH_value

def ProcessRotronic_RDD(VALUE,BUFFER):
	if (VALUE!="") :
		screen_lock.acquire()
		print VALUE
		screen_lock.release()
		RAW_value=VALUE.replace("\n","").replace("\r","")
		DWH_value=VALUE.split(";")[5].replace(" ","") + " " + VALUE.split(";")[1].replace(" ","")
	else :
		RAW_value="no data"
		DWH_value=""
	return BUFFER+time.strftime("\n\t\t\t<%Y-%m-%d/%H:%M:%S>") + RAW_value + time.strftime("<\\%Y-%m-%d/%H:%M:%S>"), DWH_value

def ProcessOzone_min(VALUE,BUFFER):
	global OzoneDisplay
	if (VALUE!="") :
		screen_lock.acquire()
		print VALUE
		screen_lock.release()
		RAW_value="%.2f" %((float(VALUE.split()[1])*float(_SLOPE_ANALYSER))+float(_OFFSET_ANALYSER))
		DWH_value=RAW_value
		OzoneDisplay=str(int(float(DWH_value)))
	else :
		RAW_value="no data"
		DWH_value=""
		OzoneDisplay="9999"

	fileozone="O3_"+time.strftime("%Y%m%d")+'.txt'
	output=open(_ADDR_bulletin+"."+fileozone,'a')
	output.write(time.strftime("\n%Y-%m-%d %H:%M:%S ") + RAW_value)
	output.close()		
	return BUFFER+time.strftime("\n\t\t\t<%Y-%m-%d/%H:%M:%S>") + RAW_value + time.strftime("<\\%Y-%m-%d/%H:%M:%S>"), DWH_value

def ProcessOzone_day(VALUE,BUFFER):
	if (VALUE!="") :
		screen_lock.acquire()
		print VALUE
		screen_lock.release()
		RAW_value=VALUE
		DWH_value=""
	else :
		RAW_value="no data"
		DWH_value=""
		
	if time.strftime("%H%M") == "2359":
		print("\ncreate bulletin desc Ozone")
		fileozone="O3_"+time.strftime("%Y%m%d")+'.txt'
		os.chmod(_ADDR_bulletin+"."+fileozone,0777)
		move(_ADDR_bulletin+"."+fileozone, _ADDR_bulletin+fileozone)	
		time.sleep(60)
		RAW_value="Time_Resolution 300 No_Photometer 2 Applied_Soft_Slope " +_SLOPE_ANALYSER + " Applied_Soft_Offset " + _OFFSET_ANALYSER + "\n\t\t\t" + time.strftime("%Y-%m-%d %H:%M:%S ") + VALUE +"\n"		
		fileozone="O3_"+time.strftime("%Y%m%d")+'.txt'
		output=open(_ADDR_bulletin+"."+fileozone,'w')
		output.write(RAW_value)
		output.close()
	return BUFFER+time.strftime("\n\t\t\t<%Y-%m-%d/%H:%M:%S>") + RAW_value + time.strftime("<\\%Y-%m-%d/%H:%M:%S>"), DWH_value

			
def ProcessCL31_profil(VALUE,BUFFER):
	if (VALUE!="") :
		screen_lock.acquire()
		print VALUE
		screen_lock.release()
	RAW_value=VALUE
	DWH_value=VALUE
#			DWH_value=VALUE.replace("<","").replace(">","").replace("+","").replace("\r","").replace("\n","").replace(";"," ")
#			DWH_value=DWH_value.split(" ")[1] + " " + DWH_value.split(" ")[3] + " " + DWH_value.split(" ")[4] + DWH_value.split(" ")[4] + " " + DWH_value.split(" ")[5] + " " + DWH_value.split(" ")[6] + DWH_value.split(" ")[7] + DWH_value.split(" ")[8] + " " + DWH_value.split(" ")[9] + " " + DWH_value.split(" ")[10] +" "
	return BUFFER+time.strftime("\n\t\t\t<%Y-%m-%d/%H:%M:%S>") + RAW_value + time.strftime("<\\%Y-%m-%d/%H:%M:%S>"), DWH_value
			
ProcessSensorData = {"SHM30_standard": ProcessSHM30_standard,
		"PT100_standard": ProcessPT100_standard,
		"Apogee_SDI_1x": ProcessApogee_1x,
		"Apogee_SDI_2x": ProcessApogee_2x,
		"WS600_M0": ProcessWS600_msgM0,
		"WS600_M2": ProcessWS600_msgM2,
		"WS600_M4": ProcessWS600_msgM4,
		"WS600_R1": ProcessWS600_R1,
		"CL31_profil": ProcessCL31_profil,
		"Rotronic_RDD": ProcessRotronic_RDD,
		"Ozone_day": ProcessOzone_day,
		"Ozone_min": ProcessOzone_min,
		"PWD22_standard":ProcessPWD22_standard,}	   
		 
def config_port(NOTE,PORT,PROTOCOLE,HOST,BAUDRATE,TYPE,NBITS,PARITY,STOPBIT,TIMEOUT):
	screen_lock.acquire()
	print("try connect to\n\tport = %s\n\tprotocole = %s\n\thost = %s\n\tbaudrate = %s" %(PORT,PROTOCOLE,HOST,BAUDRATE))
	screen_lock.release()
	if PORT in ("/dev/ttyM0", "/dev/ttyM1") and PROTOCOLE in ("RS232","RS485-2w","RS422/485-4w") :
		if PROTOCOLE=="RS232" : PROTOCOLE_INT="0"
		elif PROTOCOLE=="RS485-2w" : PROTOCOLE_INT="1"
		elif PROTOCOLE=="RS422/485-4w" : PROTOCOLE_INT="2"
		cmd = "setinterface %s %s" % (PORT, PROTOCOLE_INT)
		p = Popen(shlex.split(cmd))
		p.wait()
		if (PORT=="/dev/ttyM0"): PORT_ser="/dev/ttyO5"
		else : PORT_ser="/dev/ttyO1"

	#configure serial connection
		ser = serial.Serial(
			port=PORT_ser,
			baudrate=BAUDRATE,
			parity=PARITY,
			stopbits=STOPBIT,
			bytesize=NBITS,
			timeout=TIMEOUT)
	#close active connection
		if (ser.isOpen()):
			ser.close()

	elif PROTOCOLE == "serial_over_IP" :
	#open TCP PORT communication
		ser=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		ser.connect((HOST,int(PORT)))
		ser.settimeout(float(TIMEOUT))
	else:
		screen_lock.acquire()
		print "erreur config on %s, port %s, protocole %s" %(HOST,PORT,BAUDRATE)
		screen_lock.release()
		ser=""
	return ser


def get_value(NOTE,CMD,PORT,PROTOCOLE,HOST,DESC,BAUDRATE,TYPE,NBITS,PARITY,STOPBIT,TIMEOUT,SIZE):
	screen_lock.acquire()
	print("\nGet value from %s " %DESC)
	screen_lock.release()
	ser=config_port(NOTE,PORT,PROTOCOLE,HOST,BAUDRATE,TYPE,NBITS,PARITY,STOPBIT,TIMEOUT)
	if PROTOCOLE in ("RS232","RS485-2w","RS422/485-4w"):
		value=""
		nb_try=1
		while (value=="" and nb_try<=3):
			ser.open()
			ser.write(CMD)
			value = ser.read(SIZE)
			ser.close()
			nb_try=nb_try+1
		if value=="" :
			screen_lock.acquire()
			print "timeout connection %s %s" %(TYPE,NOTE)
			screen_lock.release()			

	elif PROTOCOLE == "serial_over_IP" :
		value=""
		nb_try=0
		while (value=="" and nb_try<=3):
			try:	
				ser.send(CMD)
				watchdog=False
				while not (watchdog):
						try:
							value=value+ser.recv(int(SIZE))
						except socket.timeout, e :
							watchdog=True				
				ser.close()
			except socket.timeout, e :
				screen_lock.acquire()
				print ("send commande error")
				screen_lock.release()
			nb_try=nb_try+1	
	return value

	
	
def bulletinDWH(value_sensor_1,value_sensor_2,value_sensor_3,value_sensor_4,value_sensor_5,value_sensor_6,value_sensor_7,value_sensor_8,value_sensor_9,value_sensor_10):
	header="001\n%s LSSW %s\n\n" %(_MHSDWH, time.strftime("%d%H%M"))
#	print header
	namefile="%s.%s.001" %(_MHSDWH, time.strftime("%Y%m%d%H%M"))
	f=open(_ADDR_bulletin+"."+namefile,'w')
	f.write(header)
	list_variable = "iii zzzztttt"
	list_value = _STATIONnumber + " " +time.strftime("%Y%m%d%H%M")
	if (_STATUS_SENSOR_1=="1" and _STATUS_DWH_SENSOR_1=="1"):
		list_variable=list_variable  + " " + _VAR_LIST_SENSOR_1
		list_value=list_value + " " + value_sensor_1
	if (_STATUS_SENSOR_2=="1" and _STATUS_DWH_SENSOR_2=="1"):
		list_variable=list_variable + " " + _VAR_LIST_SENSOR_2
		list_value=list_value + " " + value_sensor_2
	if (_STATUS_SENSOR_3=="1" and _STATUS_DWH_SENSOR_3=="1"):
		list_variable=list_variable + " " + _VAR_LIST_SENSOR_3
		list_value=list_value + " " + value_sensor_3
	if (_STATUS_SENSOR_4=="1" and _STATUS_DWH_SENSOR_4=="1"):
		list_variable=list_variable + " " + _VAR_LIST_SENSOR_4
		list_value=list_value + " " + value_sensor_4
	if (_STATUS_SENSOR_5=="1" and _STATUS_DWH_SENSOR_5=="1"):
		list_variable=list_variable + " " + _VAR_LIST_SENSOR_5
		list_value=list_value + " " + value_sensor_5
	if (_STATUS_SENSOR_6=="1" and _STATUS_DWH_SENSOR_6=="1"):
		list_variable=list_variable + " " + _VAR_LIST_SENSOR_6
		list_value=list_value + " " + value_sensor_6	
	if (_STATUS_SENSOR_7=="1" and _STATUS_DWH_SENSOR_7=="1"):
		list_variable=list_variable + " " + _VAR_LIST_SENSOR_7
		list_value=list_value + " " + value_sensor_7
	if (_STATUS_SENSOR_8=="1" and _STATUS_DWH_SENSOR_8=="1"):
		list_variable=list_variable + " " + _VAR_LIST_SENSOR_8
		list_value=list_value + " " + value_sensor_8
	if (_STATUS_SENSOR_9=="1" and _STATUS_DWH_SENSOR_9=="1"):
		list_variable=list_variable + " " + _VAR_LIST_SENSOR_9
		list_value=list_value + " " + value_sensor_9
	if (_STATUS_SENSOR_10=="1" and _STATUS_DWH_SENSOR_10=="1"):
		list_variable=list_variable + " " + _VAR_LIST_SENSOR_10
		list_value=list_value + " " + value_sensor_10
		
	f.write(list_variable+'\n')
	f.write(list_value+'\n')
	f.close()
	os.chmod(_ADDR_bulletin+'.'+namefile,0777)
	move(_ADDR_bulletin+'.'+namefile, _ADDR_bulletin+namefile)
	copyfile(_ADDR_bulletin+namefile, _ADDR_data+namefile)

	screen_lock.acquire()
	print "Bulletin DWH created"
	screen_lock.release()
	return
	
	
def bulletinRAW(buffer_sensor_1,buffer_sensor_2,buffer_sensor_3,buffer_sensor_4,buffer_sensor_5,buffer_sensor_6,buffer_sensor_7,buffer_sensor_8,buffer_sensor_9,buffer_sensor_10):
	header="001\n%s LSSW %s\n\niii zzzztttt message\n%s %s\n" %(_MHSRAW, time.strftime("%d%H%M"), _STATIONnumber, time.strftime("%Y%m%d%H%M")) 
#	print header
	namefile="%s.%s.001" %(_MHSRAW, time.strftime("%Y%m%d%H%M"))
	f=open(_ADDR_bulletin+"."+namefile,'w')
	f.write(header)
	f.write('<bulletin>\n')
	f.write('\t<info>\n')
	f.write('\t\tstation="%s"\n' % _STATIONname)
	f.write('\t\tid="%s"\n'  % _STATIONnumber)
	f.write('\t\thost="%s"\n'  % _STATION_IPLOGGER)
	f.write('\t\tdate="%s"\n' % time.strftime("%Y-%m-%dT%H:%MZ"))
	f.write('\t</info>\n')
	if (_STATUS_SENSOR_1=="1" and _STATUS_RAW_SENSOR_1=="1"):
		f.write('\t<sensor>\n')
		f.write('\t\t<description>\n')
		f.write('\t\t\tremark="%s"\n' % _DESC_SENSOR_1)
		f.write('\t\t\ttype="%s"\n' % _TYPE_SENSOR_1)
		f.write('\t\t\tnote"%s"\n' % _NOTE_SENSOR_1)
		f.write('\t\t\tprotocole"%s"\n' % _PROTOCOLE_SENSOR_1)
		f.write('\t\t\tbaudrate="%s"\n' % _BAUDRATE_SENSOR_1)
		f.write('\t\t\tport="%s"\n' % _PORT_SENSOR_1)
		f.write('\t\t\thost="%s"\n' % _HOST_SENSOR_1)
		f.write('\t\t\tcommand="%s"\n' % repr(_CMD_SENSOR_1))
		f.write('\t\t</description>\n')
		f.write('\t\t<data>')
		if(buffer_sensor_1!="") : f.write(buffer_sensor_1)
		else :f.write("\t\t\tno data")
		f.write('\n\t\t</data>\n')
		f.write('\t</sensor>\n')
	if (_STATUS_SENSOR_2=="1" and _STATUS_RAW_SENSOR_2=="1"):
		f.write('\t<sensor>\n')
		f.write('\t\t<description>\n')
		f.write('\t\t\tremark="%s"\n' % _DESC_SENSOR_2)
		f.write('\t\t\ttype="%s"\n' % _TYPE_SENSOR_2)
		f.write('\t\t\tnote="%s"\n' % _NOTE_SENSOR_2)
		f.write('\t\t\tprotocole="%s"\n' % _PROTOCOLE_SENSOR_2)
		f.write('\t\t\tbaudrate="%s"\n' % _BAUDRATE_SENSOR_2)
		f.write('\t\t\tport="%s"\n' % _PORT_SENSOR_2)
		f.write('\t\t\thost="%s"\n' % _HOST_SENSOR_2)
		f.write('\t\t\tcommand="%s"\n' % repr(_CMD_SENSOR_2))
		f.write('\t\t</description>\n')
		f.write('\t\t<data>')
		if(buffer_sensor_2!="") : f.write(buffer_sensor_2)
		else :f.write("\t\t\tno data")
		f.write('\n\t\t</data>\n')
		f.write('\t</sensor>\n')
	if (_STATUS_SENSOR_3=="1" and _STATUS_RAW_SENSOR_3=="1"):
		f.write('\t<sensor>\n')
		f.write('\t\t<description>\n')
		f.write('\t\t\tremark="%s"\n' % _DESC_SENSOR_3)
		f.write('\t\t\ttype="%s"\n' % _TYPE_SENSOR_3)
		f.write('\t\t\tnote="%s"\n' % _NOTE_SENSOR_3)
		f.write('\t\t\tprotocole="%s"\n' % _PROTOCOLE_SENSOR_3)
		f.write('\t\t\tbaudrate="%s"\n' % _BAUDRATE_SENSOR_3)
		f.write('\t\t\tport="%s"\n' % _PORT_SENSOR_3)
		f.write('\t\t\thost="%s"\n' % _HOST_SENSOR_3)
		f.write('\t\t\tcommand="%s"\n' % repr(_CMD_SENSOR_3))
		f.write('\t\t</description>\n')
		f.write('\t\t<data>')
		if(buffer_sensor_3!="") : f.write(buffer_sensor_3)
		else :f.write("\t\t\tno data")
		f.write('\n\t\t</data>\n')
		f.write('\t</sensor>\n')
	if (_STATUS_SENSOR_4=="1"  and _STATUS_RAW_SENSOR_4=="1"):
		f.write('\t<sensor>\n')
		f.write('\t\t<description>\n')
		f.write('\t\t\tremark="%s"\n' % _DESC_SENSOR_4)
		f.write('\t\t\ttype="%s"\n' % _TYPE_SENSOR_4)
		f.write('\t\t\tnote="%s"\n' % _NOTE_SENSOR_4)
		f.write('\t\t\tprotocole="%s"\n' % _PROTOCOLE_SENSOR_4)
		f.write('\t\t\tbaudrate="%s"\n' % _BAUDRATE_SENSOR_4)
		f.write('\t\t\tport="%s"\n' % _PORT_SENSOR_4)
		f.write('\t\t\thost="%s"\n' % _HOST_SENSOR_4)
		f.write('\t\t\tcommand="%s"\n' % repr(_CMD_SENSOR_4))
		f.write('\t\t</description>\n')
		f.write('\t\t<data>')
		if(buffer_sensor_4!="") : f.write(buffer_sensor_4)
		else :f.write("\t\t\tno data")
		f.write('\n\t\t</data>\n')
		f.write('\t</sensor>\n')
	if (_STATUS_SENSOR_5=="1" and _STATUS_RAW_SENSOR_5=="1"):
		f.write('\t<sensor>\n')
		f.write('\t\t<description>\n')
		f.write('\t\t\tremark="%s"\n' % _DESC_SENSOR_5)
		f.write('\t\t\ttype="%s"\n' % _TYPE_SENSOR_5)
		f.write('\t\t\tnote="%s"\n' % _NOTE_SENSOR_5)
		f.write('\t\t\tprotocole="%s"\n' % _PROTOCOLE_SENSOR_5)
		f.write('\t\t\tbaudrate="%s"\n' % _BAUDRATE_SENSOR_5)
		f.write('\t\t\tport="%s"\n' % _PORT_SENSOR_5)
		f.write('\t\t\thost="%s"\n' % _HOST_SENSOR_5)
		f.write('\t\t\tcommand="%s"\n' % repr(_CMD_SENSOR_5))
		f.write('\t\t</description>\n')
		f.write('\t\t<data>')
		if(buffer_sensor_5!="") : f.write(buffer_sensor_5)
		else :f.write("\t\t\tno data")
		f.write('\n\t\t</data>\n')
		f.write('\t</sensor>\n')
	if (_STATUS_SENSOR_6=="1" and _STATUS_RAW_SENSOR_6=="1"):
		f.write('\t<sensor>\n')
		f.write('\t\t<description>\n')
		f.write('\t\t\tremark="%s"\n' % _DESC_SENSOR_6)
		f.write('\t\t\ttype="%s"\n' % _TYPE_SENSOR_6)
		f.write('\t\t\tnote="%s"\n' % _NOTE_SENSOR_6)
		f.write('\t\t\tprotocole="%s"\n' % _PROTOCOLE_SENSOR_6)
		f.write('\t\t\tbaudrate="%s"\n' % _BAUDRATE_SENSOR_6)
		f.write('\t\t\tport="%s"\n' % _PORT_SENSOR_6)
		f.write('\t\t\thost="%s"\n' % _HOST_SENSOR_6)
		f.write('\t\t\tcommand="%s"\n' % repr(_CMD_SENSOR_6))
		f.write('\t\t</description>\n')
		f.write('\t\t<data>')
		if(buffer_sensor_6!="") : f.write(buffer_sensor_6)
		else :f.write("\t\t\tno data")
		f.write('\n\t\t</data>\n')
		f.write('\t</sensor>\n')
	if (_STATUS_SENSOR_7=="1" and _STATUS_RAW_SENSOR_7=="1"):
		f.write('\t<sensor>\n')
		f.write('\t\t<description>\n')
		f.write('\t\t\tremark="%s"\n' % _DESC_SENSOR_7)
		f.write('\t\t\ttype="%s"\n' % _TYPE_SENSOR_7)
		f.write('\t\t\tnote="%s"\n' % _NOTE_SENSOR_7)
		f.write('\t\t\tprotocole="%s"\n' % _PROTOCOLE_SENSOR_7)
		f.write('\t\t\tbaudrate="%s"\n' % _BAUDRATE_SENSOR_7)
		f.write('\t\t\tport="%s"\n' % _PORT_SENSOR_7)
		f.write('\t\t\thost="%s"\n' % _HOST_SENSOR_7)
		f.write('\t\t\tcommand="%s"\n' % repr(_CMD_SENSOR_7))
		f.write('\t\t</description>\n')
		f.write('\t\t<data>')
		if(buffer_sensor_7!="") : f.write(buffer_sensor_7)
		else :f.write("\t\t\tno data")
		f.write('\n\t\t</data>\n')
		f.write('\t</sensor>\n')
	if (_STATUS_SENSOR_8=="1" and _STATUS_RAW_SENSOR_8=="1"):
		f.write('\t<sensor>\n')
		f.write('\t\t<description>\n')
		f.write('\t\t\tremark="%s"\n' % _DESC_SENSOR_8)
		f.write('\t\t\ttype="%s"\n' % _TYPE_SENSOR_8)
		f.write('\t\t\tnote="%s"\n' % _NOTE_SENSOR_8)
		f.write('\t\t\tprotocole="%s"\n' % _PROTOCOLE_SENSOR_8)
		f.write('\t\t\tbaudrate="%s"\n' % _BAUDRATE_SENSOR_8)
		f.write('\t\t\tport="%s"\n' % _PORT_SENSOR_8)
		f.write('\t\t\thost="%s"\n' % _HOST_SENSOR_8)
		f.write('\t\t\tcommand="%s"\n' % repr(_CMD_SENSOR_8))
		f.write('\t\t</description>\n')
		f.write('\t\t<data>')
		if(buffer_sensor_8!="") : f.write(buffer_sensor_8)
		else: f.write("\t\t\tno data")
		f.write('\n\t\t</data>\n')
		f.write('\t</sensor>\n')
	if (_STATUS_SENSOR_9=="1" and _STATUS_RAW_SENSOR_9=="1"):
		f.write('\t<sensor>\n')
		f.write('\t\t<description>\n')
		f.write('\t\t\tremark="%s"\n' % _DESC_SENSOR_9)
		f.write('\t\t\ttype="%s"\n' % _TYPE_SENSOR_9)
		f.write('\t\t\tnote="%s"\n' % _NOTE_SENSOR_9)
		f.write('\t\t\tprotocole="%s"\n' % _PROTOCOLE_SENSOR_9)
		f.write('\t\t\tbaudrate="%s"\n' % _BAUDRATE_SENSOR_9)
		f.write('\t\t\tport="%s"\n' % _PORT_SENSOR_9)
		f.write('\t\t\thost="%s"\n' % _HOST_SENSOR_9)
		f.write('\t\t\tcommand="%s"\n' % repr(_CMD_SENSOR_9))
		f.write('\t\t</description>\n')
		f.write('\t\t<data>')
		if(buffer_sensor_9!="") : f.write(buffer_sensor_9)
		else :f.write("\t\t\tno data")
		f.write('\n\t\t</data>\n')
		f.write('\t</sensor>\n')
	if (_STATUS_SENSOR_10=="1" and _STATUS_RAW_SENSOR_10=="1"):
		f.write('\t<sensor>\n')
		f.write('\t\t<description>\n')
		f.write('\t\t\tremark="%s"\n' % _DESC_SENSOR_10)
		f.write('\t\t\ttype="%s"\n' % _TYPE_SENSOR_10)
		f.write('\t\t\tnote="%s"\n' % _NOTE_SENSOR_10)
		f.write('\t\t\tprotocole="%s"\n' % _PROTOCOLE_SENSOR_10)
		f.write('\t\t\tbaudrate="%s"\n' % _BAUDRATE_SENSOR_10)
		f.write('\t\t\tport="%s"\n' % _PORT_SENSOR_10)
		f.write('\t\t\thost="%s"\n' % _HOST_SENSOR_10)
		f.write('\t\t\tcommand="%s"\n' % repr(_CMD_SENSOR_10))
		f.write('\t\t</description>\n')
		f.write('\t\t<data>')
		if(buffer_sensor_10!="") : f.write(buffer_sensor_10)
		else : f.write("\t\t\tno data")
		f.write('\n\t\t</data>\n')
		f.write('\t</sensor>\n')
	f.write('</bulletin>\n')
	f.close()
	os.chmod(_ADDR_bulletin+'.'+namefile,0777)
	move(_ADDR_bulletin+'.'+namefile, _ADDR_bulletin+namefile)
	copyfile(_ADDR_bulletin+namefile, _ADDR_data+namefile)
#	copyfile(_ADDR_bulletin+namefile, _ADDR_data_info)
	screen_lock.acquire()
	print "Bulletin XML created"
	screen_lock.release()
	return
	
def GetValueSensor1():
	global buffer_sensor_1
	global DWH_value_sensor_1
	Timer(int(_REPEAT_TIME_SENSOR_1),GetValueSensor1).start()
	try :
		value=get_value(_NOTE_SENSOR_1,_CMD_SENSOR_1, _PORT_SENSOR_1, _PROTOCOLE_SENSOR_1,_HOST_SENSOR_1, _DESC_SENSOR_1, int(_BAUDRATE_SENSOR_1),_TYPE_SENSOR_1, serial_nbit[_NBITS_SENSOR_1](), serial_parity[_PARITY_SENSOR_1](), serial_stopbit[_STOPBIT_SENSOR_1](), int(_TIMEOUT_SENSOR_1), int(_SIZE_SENSOR_1))					
	except :
		screen_lock.acquire()
		print 'Communication issue with %s on %s' %(_TYPE_SENSOR_1, _PORT_SENSOR_1)		
		screen_lock.release()
		return
	try :
		buffer_sensor_1, DWH_value_sensor_1  = ProcessSensorData[_TYPE_SENSOR_1+"_"+_NOTE_SENSOR_1](value, buffer_sensor_1)
	except :
		screen_lock.acquire()
		print 'no or invalid data (value="%s") on %s' %(value, _TYPE_SENSOR_1)
		screen_lock.release()

	return
		

def GetValueSensor2():
	global buffer_sensor_2
	global DWH_value_sensor_2
	Timer(int(_REPEAT_TIME_SENSOR_2),GetValueSensor2).start()
	try :
		value=get_value(_NOTE_SENSOR_2,_CMD_SENSOR_2, _PORT_SENSOR_2, _PROTOCOLE_SENSOR_2,_HOST_SENSOR_2, _DESC_SENSOR_2, int(_BAUDRATE_SENSOR_2),_TYPE_SENSOR_2, serial_nbit[_NBITS_SENSOR_2](), serial_parity[_PARITY_SENSOR_2](), serial_stopbit[_STOPBIT_SENSOR_2](), int(_TIMEOUT_SENSOR_2), int(_SIZE_SENSOR_2))			
	except :
		screen_lock.acquire()
		print 'Communication issue with %s on %s' %(_TYPE_SENSOR_2, _PORT_SENSOR_2)		
		screen_lock.release()
		return
	try :
		buffer_sensor_2,DWH_value_sensor_2 = ProcessSensorData[_TYPE_SENSOR_2+"_"+_NOTE_SENSOR_2](value, buffer_sensor_2)
	except :
		print 'no or invalid data (value="%s") on %s' %(value, _TYPE_SENSOR_2)
	return

def GetValueSensor3():
	global buffer_sensor_3
	global DWH_value_sensor_3
	Timer(int(_REPEAT_TIME_SENSOR_3),GetValueSensor3).start()
	try :
		value=get_value(_NOTE_SENSOR_3,_CMD_SENSOR_3, _PORT_SENSOR_3, _PROTOCOLE_SENSOR_3,_HOST_SENSOR_3, _DESC_SENSOR_3, int(_BAUDRATE_SENSOR_3),_TYPE_SENSOR_3, serial_nbit[_NBITS_SENSOR_3](), serial_parity[_PARITY_SENSOR_3](), serial_stopbit[_STOPBIT_SENSOR_3](), int(_TIMEOUT_SENSOR_3), int(_SIZE_SENSOR_3))			
	except :
		print 'Communication issue with %s on %s' %(_TYPE_SENSOR_3, _PORT_SENSOR_3)	
		return
	try :
		buffer_sensor_3,DWH_value_sensor_3 = ProcessSensorData[_TYPE_SENSOR_3+"_"+_NOTE_SENSOR_3](value, buffer_sensor_3)
	except :
		print 'no or invalid data (value="%s") on %s' %(value, _TYPE_SENSOR_3)
	return

def GetValueSensor4():
	global buffer_sensor_4
	global DWH_value_sensor_4
	Timer(int(_REPEAT_TIME_SENSOR_4),GetValueSensor4).start()
	try :
		value=get_value(_NOTE_SENSOR_4,_CMD_SENSOR_4, _PORT_SENSOR_4, _PROTOCOLE_SENSOR_4,_HOST_SENSOR_4, _DESC_SENSOR_4, int(_BAUDRATE_SENSOR_4),_TYPE_SENSOR_4, serial_nbit[_NBITS_SENSOR_4](), serial_parity[_PARITY_SENSOR_4](), serial_stopbit[_STOPBIT_SENSOR_4](), int(_TIMEOUT_SENSOR_4), int(_SIZE_SENSOR_4))			
	except :
		print 'Communication issue with %s on %s' %(_TYPE_SENSOR_4, _PORT_SENSOR_4)	
		return
	try :
		buffer_sensor_4,DWH_value_sensor_4 = ProcessSensorData[_TYPE_SENSOR_4+"_"+_NOTE_SENSOR_4](value, buffer_sensor_4)
	except :
		print 'no or invalid data (value="%s") on %s' %(value, _TYPE_SENSOR_4)
	return

def GetValueSensor5():
	global buffer_sensor_5
	global DWH_value_sensor_5
	Timer(int(_REPEAT_TIME_SENSOR_5),GetValueSensor5).start()
	try :
		value=get_value(_NOTE_SENSOR_5,_CMD_SENSOR_5, _PORT_SENSOR_5, _PROTOCOLE_SENSOR_5,_HOST_SENSOR_5, _DESC_SENSOR_5, int(_BAUDRATE_SENSOR_5),_TYPE_SENSOR_5, serial_nbit[_NBITS_SENSOR_5](), serial_parity[_PARITY_SENSOR_5](), serial_stopbit[_STOPBIT_SENSOR_5](), int(_TIMEOUT_SENSOR_5), int(_SIZE_SENSOR_5))			
	except :
		print 'Communication issue with %s on %s' %(_TYPE_SENSOR_5, _PORT_SENSOR_5)	
		return
	try :
		buffer_sensor_5,DWH_value_sensor_5 = ProcessSensorData[_TYPE_SENSOR_5+"_"+_NOTE_SENSOR_5](value, buffer_sensor_5)
	except :
		print 'no or invalid data (value="%s") on %s' %(value, _TYPE_SENSOR_5)
	return

def GetValueSensor6():
	global buffer_sensor_6
	global DWH_value_sensor_6
	Timer(int(_REPEAT_TIME_SENSOR_6),GetValueSensor6).start()
	try :
		value=get_value(_NOTE_SENSOR_6,_CMD_SENSOR_6, _PORT_SENSOR_6, _PROTOCOLE_SENSOR_6,_HOST_SENSOR_6, _DESC_SENSOR_6, int(_BAUDRATE_SENSOR_6),_TYPE_SENSOR_6, serial_nbit[_NBITS_SENSOR_6](), serial_parity[_PARITY_SENSOR_6](), serial_stopbit[_STOPBIT_SENSOR_6](), int(_TIMEOUT_SENSOR_6), int(_SIZE_SENSOR_6))			
	except :
		print 'Communication issue with %s on %s' %(_TYPE_SENSOR_6, _PORT_SENSOR_6)	
		return
	try :
		buffer_sensor_6,DWH_value_sensor_6 = ProcessSensorData[_TYPE_SENSOR_6+"_"+_NOTE_SENSOR_6](value, buffer_sensor_6)
	except :
		print 'no or invalid data (value="%s") on %s' %(value, _TYPE_SENSOR_6)
	return

def GetValueSensor7():
	global buffer_sensor_7
	global DWH_value_sensor_7
	Timer(int(_REPEAT_TIME_SENSOR_7),GetValueSensor7).start()
	try :
		value=get_value(_NOTE_SENSOR_7,_CMD_SENSOR_7, _PORT_SENSOR_7, _PROTOCOLE_SENSOR_7,_HOST_SENSOR_7, _DESC_SENSOR_7, int(_BAUDRATE_SENSOR_7),_TYPE_SENSOR_7, serial_nbit[_NBITS_SENSOR_7](), serial_parity[_PARITY_SENSOR_7](), serial_stopbit[_STOPBIT_SENSOR_7](), int(_TIMEOUT_SENSOR_7), int(_SIZE_SENSOR_7))			
	except :
		print 'Communication issue with %s on %s' %(_TYPE_SENSOR_7, _PORT_SENSOR_7)	
		return
	try :
		buffer_sensor_7,DWH_value_sensor_7 = ProcessSensorData[_TYPE_SENSOR_7+"_"+_NOTE_SENSOR_7](value, buffer_sensor_7)
	except :
		print 'no or invalid data (value="%s") on %s' %(value, _TYPE_SENSOR_7)
	return

def GetValueSensor8():
	global buffer_sensor_8
	global DWH_value_sensor_8
	Timer(int(_REPEAT_TIME_SENSOR_8),GetValueSensor8).start()
	try :
		value=get_value(_NOTE_SENSOR_8,_CMD_SENSOR_8, _PORT_SENSOR_8, _PROTOCOLE_SENSOR_8,_HOST_SENSOR_8, _DESC_SENSOR_8, int(_BAUDRATE_SENSOR_8),_TYPE_SENSOR_8, serial_nbit[_NBITS_SENSOR_8](), serial_parity[_PARITY_SENSOR_8](), serial_stopbit[_STOPBIT_SENSOR_8](), int(_TIMEOUT_SENSOR_8), int(_SIZE_SENSOR_8))			
	except :
		print 'Communication issue with %s on %s' %(_TYPE_SENSOR_8, _PORT_SENSOR_8)	
		return
	try :
		buffer_sensor_8,DWH_value_sensor_8 = ProcessSensorData[_TYPE_SENSOR_8+"_"+_NOTE_SENSOR_8](value, buffer_sensor_8)
	except :
		print 'no or invalid data (value="%s") on %s' %(value, _TYPE_SENSOR_8)
	return
	
def GetValueSensor9():
	global buffer_sensor_9
	global DWH_value_sensor_9
	Timer(int(_REPEAT_TIME_SENSOR_9),GetValueSensor9).start()
	try :
		value=get_value(_NOTE_SENSOR_9,_CMD_SENSOR_9, _PORT_SENSOR_9, _PROTOCOLE_SENSOR_9,_HOST_SENSOR_9, _DESC_SENSOR_9, int(_BAUDRATE_SENSOR_9),_TYPE_SENSOR_9, serial_nbit[_NBITS_SENSOR_9](), serial_parity[_PARITY_SENSOR_9](), serial_stopbit[_STOPBIT_SENSOR_9](), int(_TIMEOUT_SENSOR_9), int(_SIZE_SENSOR_9))			
	except :
		print 'Communication issue with %s on %s' %(_TYPE_SENSOR_9, _PORT_SENSOR_9)	
		return
	try :
		buffer_sensor_9,DWH_value_sensor_9 = ProcessSensorData[_TYPE_SENSOR_9+"_"+_NOTE_SENSOR_9](value, buffer_sensor_9)
	except :
		print 'no or invalid data (value="%s") on %s' %(value, _TYPE_SENSOR_9)
	return
	
def GetValueSensor10():
	global buffer_sensor_10
	global DWH_value_sensor_10
	Timer(int(_REPEAT_TIME_SENSOR_10),GetValueSensor10).start()
	try :
		value=get_value(_NOTE_SENSOR_10,_CMD_SENSOR_10, _PORT_SENSOR_10, _PROTOCOLE_SENSOR_10,_HOST_SENSOR_10, _DESC_SENSOR_10, int(_BAUDRATE_SENSOR_10),_TYPE_SENSOR_10, serial_nbit[_NBITS_SENSOR_10](), serial_parity[_PARITY_SENSOR_10](), serial_stopbit[_STOPBIT_SENSOR_10](), int(_TIMEOUT_SENSOR_10), int(_SIZE_SENSOR_10))			
	except :
		print 'Communication issue with %s on %s' %(_TYPE_SENSOR_10, _PORT_SENSOR_10)	
		return
	try :
		buffer_sensor_10,DWH_value_sensor_10 = ProcessSensorData[_TYPE_SENSOR_10+"_"+_NOTE_SENSOR_10](value, buffer_sensor_10)
	except :
		print 'no or invalid data (value="%s") on %s' %(value, _TYPE_SENSOR_10)
	return

	
def CreateBulletinRAW():	
	global buffer_sensor_1
	global buffer_sensor_2
	global buffer_sensor_3
	global buffer_sensor_4
	global buffer_sensor_5
	global buffer_sensor_6
	global buffer_sensor_7
	global buffer_sensor_8
	global buffer_sensor_9
	global buffer_sensor_10
	Timer(int(_REPEAT_TIME_BULLETIN_RAW),CreateBulletinRAW).start()
	bulletinRAW(buffer_sensor_1,buffer_sensor_2,buffer_sensor_3,buffer_sensor_4,buffer_sensor_5,buffer_sensor_6,buffer_sensor_7,buffer_sensor_8,buffer_sensor_9,buffer_sensor_10)
	buffer_sensor_1=""
	buffer_sensor_2=""
	buffer_sensor_3=""
	buffer_sensor_4=""
	buffer_sensor_5=""
	buffer_sensor_6=""
	buffer_sensor_7=""
	buffer_sensor_8=""
	buffer_sensor_9=""
	buffer_sensor_10=""
	return
def CreateBulletinDWH():	
	global DWH_value_sensor_1
	global DWH_value_sensor_2
	global DWH_value_sensor_3
	global DWH_value_sensor_4
	global DWH_value_sensor_5
	global DWH_value_sensor_6
	global DWH_value_sensor_7
	global DWH_value_sensor_8
	global DWH_value_sensor_9
	global DWH_value_sensor_10
	time_in_sec = int(time.strftime("%M")[1:])*60+int(time.strftime("%S"))
	print time_in_sec
	X=time_in_sec%int(_REPEAT_TIME_BULLETIN_DWH)
	Y=int(_INIT_TIME_BULLETIN_DWH)%int(_REPEAT_TIME_BULLETIN_DWH)
	print X
	print Y
	Timer(int(_REPEAT_TIME_BULLETIN_DWH)-(X-Y),CreateBulletinDWH).start()
	

	if (DWH_value_sensor_1 == "") : DWH_value_sensor_1 = _INIT_VAR_SENSOR_1
	if (DWH_value_sensor_2 == "") : DWH_value_sensor_2 = _INIT_VAR_SENSOR_2
	if (DWH_value_sensor_3 == "") : DWH_value_sensor_3 = _INIT_VAR_SENSOR_3
	if (DWH_value_sensor_4 == "") : DWH_value_sensor_4 = _INIT_VAR_SENSOR_4
	if (DWH_value_sensor_5 == "") : DWH_value_sensor_5 = _INIT_VAR_SENSOR_5
	if (DWH_value_sensor_6 == "") : DWH_value_sensor_6 = _INIT_VAR_SENSOR_6
	if (DWH_value_sensor_7 == "") : DWH_value_sensor_7 = _INIT_VAR_SENSOR_7
	if (DWH_value_sensor_8 == "") : DWH_value_sensor_8 = _INIT_VAR_SENSOR_8
	if (DWH_value_sensor_9 == "") : DWH_value_sensor_9 = _INIT_VAR_SENSOR_9
	if (DWH_value_sensor_10 == "") : DWH_value_sensor_10 = _INIT_VAR_SENSOR_10
	bulletinDWH(DWH_value_sensor_1,DWH_value_sensor_2,DWH_value_sensor_3,DWH_value_sensor_4,DWH_value_sensor_5,DWH_value_sensor_6,DWH_value_sensor_7,DWH_value_sensor_8,DWH_value_sensor_9,DWH_value_sensor_10)
	DWH_value_sensor_1=_INIT_VAR_SENSOR_1
	DWH_value_sensor_2=_INIT_VAR_SENSOR_2
	DWH_value_sensor_3=_INIT_VAR_SENSOR_3
	DWH_value_sensor_4=_INIT_VAR_SENSOR_4
	DWH_value_sensor_5=_INIT_VAR_SENSOR_5
	DWH_value_sensor_6=_INIT_VAR_SENSOR_6
	DWH_value_sensor_7=_INIT_VAR_SENSOR_7
	DWH_value_sensor_8=_INIT_VAR_SENSOR_8
	DWH_value_sensor_9=_INIT_VAR_SENSOR_9
	DWH_value_sensor_10=_INIT_VAR_SENSOR_10
	return
	
def Display():
	global TemperatureDisplay
	global DewPointDisplay
	global HumidityDisplay
	global PressureDisplay
	global WindSpeedDisplay
	global WindDirDisplay
	global PrecipitationDisplay
	global OzoneDisplay
	Timer(600,Display).start()
	current.read(_ADDR_data_info)  
	current.set('temp','temp'+time.strftime("%H%M"),TemperatureDisplay)
	current.set('dew','dew'+time.strftime("%H%M"),DewPointDisplay)
	current.set('hum','hum'+time.strftime("%H%M"),HumidityDisplay)
	current.set('pres','pres'+time.strftime("%H%M"),PressureDisplay)
	current.set('wind','wind'+time.strftime("%H%M"),WindSpeedDisplay)
	current.set('dir','dir'+time.strftime("%H%M"),WindDirDisplay)
	current.set('prec','prec'+time.strftime("%H%M"),PrecipitationDisplay)
	current.set('ozo','ozo' + time.strftime("%H%M"),OzoneDisplay)
	f=open(_ADDR_data_info,'w')
	current.write(f)
	f.close()
	return

thread1 = Thread(target=GetValueSensor1)
thread2 = Thread(target=GetValueSensor2)
thread3 = Thread(target=GetValueSensor3)
thread4 = Thread(target=GetValueSensor4)
thread5 = Thread(target=GetValueSensor5)
thread6 = Thread(target=GetValueSensor6)
thread7 = Thread(target=GetValueSensor7)
thread8 = Thread(target=GetValueSensor8)
thread9 = Thread(target=GetValueSensor9)
thread10 = Thread(target=GetValueSensor10)

threadBulRaw = Thread(target=CreateBulletinRAW)
threadBulDWH = Thread(target=CreateBulletinDWH)
threadDisplay = Thread(target=Display)




screen_lock.acquire()
print "Start"
screen_lock.release()

init_sensor1=False
init_sensor2=False
init_sensor3=False
init_sensor4=False
init_sensor5=False
init_sensor6=False
init_sensor7=False
init_sensor8=False
init_sensor9=False
init_sensor10=False
init_bulletin_raw=False
init_bulletin_DWH=False
init_display=False

# cron minute
while (init_sensor1 and init_sensor2 and init_sensor3 and init_sensor4 and init_bulletin_raw and init_bulletin_DWH and init_display) is False :
	time_in_sec = int(time.strftime("%M")[1:])*60+int(time.strftime("%S"))
	if (time_in_sec==0 and init_display==0) :
		init_display=True
		threadDisplay.start()	
	if (time_in_sec==int(_INIT_TIME_SENSOR_1) and init_sensor1==0) :
		init_sensor1=True
		if (_STATUS_SENSOR_1=="1") :
			thread1.start()
	if (time_in_sec==int(_INIT_TIME_SENSOR_2) and init_sensor2==0) :
		init_sensor2=True
		if (_STATUS_SENSOR_2=="1") :
			thread2.start()
	if (time_in_sec==int(_INIT_TIME_SENSOR_3) and init_sensor3==0) :
		init_sensor3=True
		if (_STATUS_SENSOR_3=="1") :
			thread3.start()
	if (time_in_sec==int(_INIT_TIME_SENSOR_4) and init_sensor4==0) :
		init_sensor4=True
		if (_STATUS_SENSOR_4=="1") :
			thread4.start()
	if (time_in_sec==int(_INIT_TIME_SENSOR_5) and init_sensor5==0) :
		init_sensor5=True
		if (_STATUS_SENSOR_5=="1") :
			thread5.start()
	if (time_in_sec==int(_INIT_TIME_SENSOR_6) and init_sensor6==0) :
		init_sensor6=True
		if (_STATUS_SENSOR_6=="1") :
			thread6.start()
	if (time_in_sec==int(_INIT_TIME_SENSOR_7) and init_sensor7==0) :
		init_sensor7=True
		if (_STATUS_SENSOR_7=="1") :
			thread7.start()
	if (time_in_sec==int(_INIT_TIME_SENSOR_8) and init_sensor8==0) :
		init_sensor8=True
		if (_STATUS_SENSOR_8=="1") :
			thread8.start()
	if (time_in_sec==int(_INIT_TIME_SENSOR_9) and init_sensor9==0) :
		init_sensor9=True
		if (_STATUS_SENSOR_9=="1") :
			thread9.start()
	if (time_in_sec==int(_INIT_TIME_SENSOR_10) and init_sensor10==0) :
		init_sensor10=True
		if (_STATUS_SENSOR_10=="1") :
			thread10.start()
	if (time_in_sec==int(_INIT_TIME_BULLETIN_RAW) and init_bulletin_raw==0) :
		init_bulletin_raw=True
		if (_STATUS_BULLETIN_RAW=="1") :
			threadBulRaw.start()
	if (time_in_sec==int(_INIT_TIME_BULLETIN_DWH) and init_bulletin_DWH==0) :
		init_bulletin_DWH=True
		if (_STATUS_BULLETIN_DWH=="1") :
			threadBulDWH.start()
