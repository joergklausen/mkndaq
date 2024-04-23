#------------------------------------------------------------------------------
# name :	purge.py
# description :	delete basic files SARA  and bulletin older than day_limit
# author :	Meteoswiss / Grandjean Jacques, MDTD
# date :	09.07.2014
# version :	1.0.0
# comment :	execute every day / cron root
#------------------------------------------------------------------------------
import os
import sys
import time
sys.path.insert(0,'/SCRIPT')
import config

day_limit = 1

current_time=time.time()

for directory in (config._ADDR_data,config._ADDR_bulletin):
	for f in os.listdir(directory):
		creation_time = os.path.getctime(directory+f)
		if (current_time -creation_time) // (24*3600) >= day_limit:
			os.unlink(directory+f)
			print ('{} removed'.format(directory+f))
