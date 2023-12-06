import time
import ConfigParser 
import serial
import os
import re
from config import _ADDR_data_info, _ADDR_bulletin, _MHSDWH,\
    _DELAY_IRIDIUM, _TIME_IRIDIUM_TEST
from datetime import datetime, timedelta

config=ConfigParser.SafeConfigParser()
config.read(_ADDR_data_info)  


#configure serial conection
ser = serial.Serial(
    port='/dev/ttyO1',
    baudrate='19200',
    parity=serial.PARITY_NONE,
    stopbits=serial.STOPBITS_ONE,
    bytesize=serial.EIGHTBITS,
    timeout=7)

def SendToIRI(msg):
    if (ser.isOpen() == False):
        ser.open()
    else :
        ser.close()
        ser.open()
    ser.write(msg)
    ser.close
    config.set('IRI','IRI_last_send',time.strftime("%Y-%m-%d,%H:%M:%S  :  ")+msg)
    f=open(_ADDR_data_info,'w')
    config.write(f)
    f.close()
    return

old_time_file=datetime.now() - timedelta(minutes=int(_DELAY_IRIDIUM))
print old_time_file.strftime('%Y%m%d%H%M')

for filename in os.listdir("/bulletin"):
    if re.match(_MHSDWH+'.'+old_time_file.strftime('%Y%m%d%H%M')[:-1]+"0.*2",filename):
        print "old file exist, send by iridium"
        if _DELAY_IRIDIUM == "10" :
           SendToIRI('\x02' + config.get('IRI','IRI_msg_actual') + '\x03')
        elif _DELAY_IRIDIUM =="20" :
           SendToIRI('\x02' + config.get('IRI','IRI_msg_10min_old') + '\x03')
        elif _DELAY_IRIDIUM =="30" :
           SendToIRI('\x02' + config.get('IRI','IRI_msg_20min_old') + '\x03')
        break
if time.strftime("%H%M")[:-1] == _TIME_IRIDIUM_TEST[:-1] :
    print "time iridium test"
    SendToIRI('\x02' + time.strftime("%Y.%m.%d,%H:%M")[:-1]+'0,ALRM_Min=999999999999999,ALRM_Hour=999999999999999,ALRM_Day=999999999999999,\x03')
else :
    print "no iridium send"
