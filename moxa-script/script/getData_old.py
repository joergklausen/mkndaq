#------------------------------------------------------------------------------
# name :	getDataLufft.py
# description : get wind data from Luft WS600 and Ozone for KENYA STATION
# author :	Meteoswiss / Grandjean Jacques, MDTD
# date :	10-03-2017
# version :	1.0.0
#------------------------------------------------------------------------------

import serial
import datetime
import time
import os
import sys
from shutil import move
from config import _ADDR_bulletin, _ADDR_data_info, _MHSDWH
from config import _LUFFT_10m_ENABLE, _LUFFT_2m_ENABLE, _OZONE_ENABLE, _var_lufft_10m, _var_lufft_2m, _var_ozone
from config import _ADDR_OZONE, _CMD_erec, _CMD_o3, _SLOPE_ANALYSER, _OFFSET_ANALYSER, _OFFSET_PRESSURE, _SLOPE_PRECIP
sys.path.insert(0,'/home/admin')
from version import _STATIONnumber, _STATIONname
import ConfigParser
current=ConfigParser.SafeConfigParser()
#-------------------------------------------------------------------------------------------
#configure serial connection
#OPTION 1 : LUFFT 2m on port ttyO1 and LUFFT 10m on port ttyO5
#OPTION 2 : LUFFT 2m on port ttyO1 and OZONE on port ttyO5
serO1 = serial.Serial(
	port='/dev/ttyO1',
	baudrate=19200,
	parity=serial.PARITY_NONE,
	stopbits=serial.STOPBITS_ONE,
	bytesize=serial.EIGHTBITS,
	timeout=5)
	
if _OZONE_ENABLE :	
	serO5 = serial.Serial(
		port='/dev/ttyO5',
		baudrate=9600,
		parity=serial.PARITY_NONE,
		stopbits=serial.STOPBITS_ONE,
		bytesize=serial.EIGHTBITS,
		timeout=5)
else : #LUFFT
	serO5 = serial.Serial(
		port='/dev/ttyO5',
		baudrate=19200,
		parity=serial.PARITY_NONE,
		stopbits=serial.STOPBITS_ONE,
		bytesize=serial.EIGHTBITS,
		timeout=5)
		
#--------------------------------------------------------------------------------------------	
#open port LUFFT 2m if exist and get data if time is multiple of 2min
dataM0_2m=' '
dataM2_2m=' '
if time.strftime("%M")[1:2] == "0" and _LUFFT_2m_ENABLE:
	if(serO1.isOpen() == False):
		serO1.open()
	else :
		serO1.close()
		serO1.open()
	print("\nconnected to" + serO1.portstr +", LUFT WS600 2m, serial 485, 19200 8N1")

	#try max i times to get M0 message
	print("\tget M0 message")
	i=0;
	while i<3:
		serO1.write('M0\x0D')
		dataM0_2m=serO1.read(77)
		if (dataM0_2m.__len__() == 77):
			print "\t\t" + dataM0_2m + "\n"
			break
		i=i+1

	#try max i times to get M2 message
	print("\tget M2 message")
	i=0
	while i<3:
		serO1.write('M2\x0D')
		dataM2_2m=serO1.read(68)
		if (dataM2_2m.__len__() == 68):
			print "\t\t" + dataM2_2m + "\n"
			break
		i=i+1
		
	serO1.write('R1\x0D')
	serO1.close()

#---------------------------------------------------------------------------------------------
dataOzo=' '
dataErec=' '
#open port Ozone measure and get data if _OZONE_ENABLE

if (time.strftime("%M")[1:2] in ("0", "5")) and _OZONE_ENABLE:
	if(serO5.isOpen() == False):
		serO5.open()
	else :
		serO5.close()
		serO5.open()

	print("\nconnected to" + serO5.portstr +", Ozone measure, serial 232, 9600 8N1")

	#try max i times to get M0 message
	print("\tget Ozone value")
	i=0
	while i<3:
		serO5.write(_ADDR_OZONE+_CMD_o3)
		dataOzo=serO5.read(17)
		if (dataOzo.__len__() == 17):
			print "\t\t" + dataOzo.split()[1] + "\n"
			break
		i=i+1
		
	if (time.strftime("%H%M") == "0000"):
		i=0
		while i<3:
			serO5.write(_ADDR_OZONE+_CMD_erec)
			dataErec=serO5.read(299)
			if (dataErec.__len__() > 10):
				dataErec=time.strftime("%Y-%m-%d %H:%M:%S ") + dataErec
				break
			else :
				dataErec = time.strftime("%Y-%m-%d %H:%M:%S ") + "No erec response"
			i=i+1
		dataErec=dataErec.replace("\r"," ")
		dataErec=dataErec.replace("\n"," ")
		dataErec="Time_Resolution 300 No_Photometer 2 Applied_Soft_Slope " +_SLOPE_ANALYSER + " Applied_Soft_Offset " + _OFFSET_ANALYSER + "\n" + dataErec
		fileozone="O3"+"_lb_"+time.strftime("%Y%m%d")+'.txt'
		output=open(_ADDR_bulletin+"."+fileozone,'w')
		output.write(dataErec)
		output.close()
	serO5.close()
#--------------------------------------------------------------------------------------------	
#else open port LUFFT 10m and get data if time is multiple of 10min
dataM0_10m=' '
dataM2_10m=' '
if time.strftime("%M")[1:2] == "0" and _LUFFT_10m_ENABLE:
	if(serO5.isOpen() == False):
		serO5.open()
	else :
		serO5.close()
		serO5.open()
	print("\nconnected to" + serO5.portstr +", LUFT WS600 10m, serial 485, 19200 8N1")

	#try max i times to get M0 message
	print("\tget M0 message")
	i=0;
	while i<3:
		serO5.write('M0\x0D')
		dataM0_10m=serO5.read(77)
		if (dataM0_10m.__len__() == 77):
			print "\t\t" + dataM0_10m + "\n"
			break
		i=i+1

	#try max i times to get M2 message
	print("\tget M2 message")
	i=0
	while i<3:
		serO5.write('M2\x0D')
		dataM2_10m=serO5.read(68)
		if (dataM2_10m.__len__() == 68):
			print "\t\t" + dataM2_10m + "\n"
			break
		i=i+1
		
	serO5.write('R1\x0D')
	serO5.close()

#-------------------------------------------------------------------------------------------------
#data parsing and save... value 9999 = no point for graphics
dataM0_10m=dataM0_10m.replace("\r","")
dataM0_10m=dataM0_10m.replace("\n","")
dataM0_10m=dataM0_10m.replace(";"," ")
dataM2_10m=dataM2_10m.replace("\r","")
dataM2_10m=dataM2_10m.replace("\n","")
dataM2_10m=dataM2_10m.replace(";"," ")

if (dataM0_10m.__len__() == 75):
	print "valid data M0 Lufft 10m"
	Temperature_10m=dataM0_10m.split()[1]
	DewPointTemp_10m=dataM0_10m.split()[2]
	WindChillTemp_10m=dataM0_10m.split()[3]
	RelHumidity_10m=dataM0_10m.split()[4]
	RelAirPress_10m=dataM0_10m.split()[5]
	RelAirPress_10m=str(round(float(RelAirPress_10m) + float(_OFFSET_PRESSURE),1))
	WindSpeed_10m=dataM0_10m.split()[6]
	WindDir_10m=dataM0_10m.split()[7]
	PrecipQ_10m=dataM0_10m.split()[8]
	PrecipQ_10m=str(round(float(PrecipQ_10m) * float(_SLOPE_PRECIP),1))
	PrecipT_10m=dataM0_10m.split()[9]
	PrecipI_10m=dataM0_10m.split()[10]	
else:
	Temperature_10m="9999"
	DewPointTemp_10m="9999"
	WindChillTemp_10m="9999"
	RelHumidity_10m="9999"
	RelAirPress_10m="9999"
	WindSpeed_10m="9999"
	WindDir_10m="9999"
	PrecipQ_10m="9999"
	PrecipT_10m="9999"
	PrecipI_10m="9999"
	if _LUFFT_10m_ENABLE and time.strftime("%M")[1:2] == "0" : 
		print "unvalid data M0 Lufft 10m"
	
if (dataM2_10m.__len__() == 66):
	print "valid data M2 Lufft 10m"
	MaxWindSpeed_10m=dataM2_10m.split()[3]
else:
	MaxWindSpeed_10m="9999"
	if _LUFFT_10m_ENABLE and time.strftime("%M")[1:2] == "0" : 
		print "unvalid data M2 Lufft 10m"
		
#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
dataM0_2m=dataM0_2m.replace("\r","")
dataM0_2m=dataM0_2m.replace("\n","")
dataM0_2m=dataM0_2m.replace(";"," ")
dataM2_2m=dataM2_2m.replace("\r","")
dataM2_2m=dataM2_2m.replace("\n","")
dataM2_2m=dataM2_2m.replace(";"," ")
if (dataM0_2m.__len__() == 75):
	print "valid data M0 Lufft 2m"
	Temperature_2m=dataM0_2m.split()[1]
	DewPointTemp_2m=dataM0_2m.split()[2]
	WindChillTemp_2m=dataM0_2m.split()[3]
	RelHumidity_2m=dataM0_2m.split()[4]
	RelAirPress_2m=dataM0_2m.split()[5]
	RelAirPress_2m=str(round(float(RelAirPress_2m) + float(_OFFSET_PRESSURE),1))
	WindSpeed_2m=dataM0_2m.split()[6]
	WindDir_2m=dataM0_2m.split()[7]
	PrecipQ_2m=dataM0_2m.split()[8]
	PrecipQ_2m=str(round(float(PrecipQ_2m) * float(_SLOPE_PRECIP),1))
	PrecipT_2m=dataM0_2m.split()[9]
	PrecipI_2m=dataM0_2m.split()[10]
else:
	Temperature_2m="9999"
	DewPointTemp_2m="9999"
	WindChillTemp_2m="9999"
	RelHumidity_2m="9999"
	RelAirPress_2m="9999"
	WindSpeed_2m="9999"
	WindDir_2m="9999"
	PrecipQ_2m="9999"
	PrecipT_2m="9999"
	PrecipI_2m="9999"
	if _LUFFT_2m_ENABLE and time.strftime("%M")[1:2] == "0" : 
		print "unvalid data M0 Lufft 2m"
	
if (dataM2_2m.__len__() == 66):
	print "valid data M0 Lufft 2m"
	MaxWindSpeed_2m=dataM2_2m.split()[3]
else:
	MaxWindSpeed_2m="9999"
	if _LUFFT_2m_ENABLE and time.strftime("%M")[1:2] == "0" : 
		print "unvalid data M2 Lufft 2m"
#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 	
if (dataOzo.__len__() == 17):
	print "valid data Ozone"
	Ozone="%.2f" %((float(dataOzo.split()[1])*float(_SLOPE_ANALYSER))+float(_OFFSET_ANALYSER))
	print Ozone
	OzoneDisplay=str(int(float(Ozone)))
	print OzoneDisplay
else:
	Ozone="/"
	OzoneDisplay="9999"
	if _OZONE_ENABLE and (time.strftime("%M")[1:2] in ("0", "5"))  : 
		print "unvalid data Ozone"
#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 	
if time.strftime("%M")[1:2] == "0":
	current.read(_ADDR_data_info)  
	current.set('temp','temp'+time.strftime("%H%M"),Temperature_2m)
	current.set('dew','dew'+time.strftime("%H%M"),DewPointTemp_2m)
	current.set('hum','hum'+time.strftime("%H%M"),RelHumidity_2m)
	current.set('pres','pres'+time.strftime("%H%M"),RelAirPress_2m)
	current.set('wind','wind'+time.strftime("%H%M"),WindSpeed_2m)
	current.set('dir','dir'+time.strftime("%H%M"),WindDir_2m)
	current.set('prec','prec'+time.strftime("%H%M"),PrecipQ_2m)
	current.set('ozo','ozo' + time.strftime("%H%M"),OzoneDisplay)
	f=open(_ADDR_data_info,'w')
	current.write(f)
	f.close()

if (Temperature_10m == "9999") : Temperature_10m = '/'
if (DewPointTemp_10m == "9999") : DewPointTemp_10m = '/'
if (RelHumidity_10m == "9999") : RelHumidity_10m = '/'
if (RelAirPress_10m == "9999") : RelAirPress_10m = '/'
if (WindSpeed_10m == "9999") : WindSpeed_10m = '/'
if (WindDir_10m == "9999") : WindDir_10m = '/'
if (MaxWindSpeed_10m == "9999") : MaxWindSpeed_10m = '/'
if (PrecipQ_10m == "9999") : PrecipQ_10m = '/'
if (Temperature_2m == "9999") : Temperature_2m = '/'
if (DewPointTemp_2m == "9999") : DewPointTemp_2m = '/'
if (RelHumidity_2m == "9999") : RelHumidity_2m = '/'
if (RelAirPress_2m == "9999") : RelAirPress_2m = '/'
if (WindSpeed_2m == "9999") : WindSpeed_2m = '/'
if (WindDir_2m == "9999") : WindDir_2m = '/'
if (MaxWindSpeed_2m == "9999") : MaxWindSpeed_2m = '/'
if (PrecipQ_2m == "9999") : PrecipQ_2m = '/'

#-----------------------------------------------------------------------------------------------------
# bulletin creation
# create bulletin Ozone desc if data exist
if time.strftime("%M")[1:2] == "0" or (time.strftime("%M")[1:2] == "5" and _OZONE_ENABLE) :	
	print("\ncreate bulletin")
	varlist=' '
	varresult=' '
	filename=_MHSDWH+"."+time.strftime("%Y%m%d%H%M")+'.001'
	header='001\n' + _MHSDWH +' LSSW ' + time.strftime("%d%H%M") + '\n\n'
	if _LUFFT_10m_ENABLE and _LUFFT_2m_ENABLE : #future KEMKN
		varlist='iii zzzztttt '+_var_lufft_10m+' '+_var_lufft_2m+'\n'
		varresult=_STATIONnumber + ' ' + time.strftime("%Y%m%d%H%M ") + Temperature_10m + ' ' + RelHumidity_10m + ' ' + RelAirPress_10m + ' ' + WindSpeed_10m + ' ' + MaxWindSpeed_10m + ' ' + WindDir_10m + ' ' + PrecipQ_10m + ' ' + Temperature_2m + ' ' + RelHumidity_2m + ' ' + RelAirPress_2m + ' ' + WindSpeed_2m + ' ' + MaxWindSpeed_2m + ' ' + WindDir_2m + ' ' + PrecipQ_2m + '\n'
	elif _LUFFT_2m_ENABLE and _OZONE_ENABLE : # actual KENAI
		varlist='iii zzzztttt '+_var_lufft_2m+' '+_var_ozone+'\n'
		varresult=_STATIONnumber+ ' ' + time.strftime("%Y%m%d%H%M ") + Temperature_2m + ' ' + RelHumidity_2m + ' ' + RelAirPress_2m + ' ' + WindSpeed_2m + ' ' + MaxWindSpeed_2m + ' ' + WindDir_2m + ' ' + PrecipQ_2m + ' ' + Ozone + '\n'		
		fileozone="O3"+"_lb_"+time.strftime("%Y%m%d")+'.txt'
		output=open(_ADDR_bulletin+"."+fileozone,'a')
		output.write(time.strftime("\n%Y-%m-%d %H:%M:%S ") + Ozone)
		output.close()
		
	elif _LUFFT_2m_ENABLE: # actual KEMKN , lufft 10m but on port of lufft 2m--> error
		varlist='iii zzzztttt '+_var_lufft_10m+'\n'
		varresult=_STATIONnumber+ ' ' + time.strftime("%Y%m%d%H%M ") + Temperature_2m + ' ' + RelHumidity_2m + ' ' + RelAirPress_2m + ' ' + WindSpeed_2m + ' ' + MaxWindSpeed_2m + ' ' + WindDir_2m + ' ' + PrecipQ_2m + '\n'
	elif _LUFFT_10m_ENABLE: # actual KEMKN if correction of port
		varlist='iii zzzztttt '+_var_lufft_10m+'\n'
		varresult=_STATIONnumber+ ' ' + time.strftime("%Y%m%d%H%M ") + Temperature_10m + ' ' + RelHumidity_10m + ' ' + RelAirPress_10m + ' ' + WindSpeed_10m + ' ' + MaxWindSpeed_10m + ' ' + WindDir_10m + ' ' + PrecipQ_10m + '\n'
		
	output=open(_ADDR_bulletin+filename,'a')
	output.write(header+varlist+varresult)
	output.close()
	print header+varlist+varresult + "\n"

	if time.strftime("%H%M") == "2355" and _OZONE_ENABLE :
		print("\ncreate bulletin desc Ozone")
		os.chmod(_ADDR_bulletin+"."+fileozone,0777)
		move(_ADDR_bulletin+"."+fileozone, _ADDR_bulletin+fileozone)
	
else :
	print "\ninvalid request time\n"
