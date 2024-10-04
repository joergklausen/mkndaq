#!/usr/bin/env python
# -*- coding: utf-8 -*-

import schedule
import time

from mkndaq.inst.thermo import Thermo49i
from mkndaq.inst.g2401 import G2401

def main():
    global tei49i_o3

    cfg = {'file': 'mkndaq.cfg', 'version': '1.0.0-20210802', 'home': 'c:/users/mkn', 'reporting_interval': 10,
           'sftp': {'host': 'sftp.meteoswiss.ch', 'usr': 'gaw_mkn',
                    'key': 'C:\\Users\\jkl/.ssh/private-open-ssh-4096-mkn.ppk',
                    'proxy': {'socks5': None, 'port': 1080},
                    'logs': 'C:\\Users\\jkl/Documents/mkndaq/logs'}, 'logs': 'C:\\Users\\jkl/Documents/mkndaq/logs',
           'data': 'C:\\Users\\jkl/Documents/mkndaq/data',
           'staging': {'path': 'C:\\Users\\jkl/Documents/mkndaq/staging', 'zip': True},
           'COM4': {'protocol': 'RS232', 'baudrate': 9600, 'bytesize': 8, 'stopbits': 1, 'parity': 'N',
                    'timeout': 0.1},
           '_tei49c': {'type': 'TEI49C', 'id': 49, 'serial_number': 'unknown', 'port': 'COM4',
                       'get_config': ['mode', 'gas unit', 'range', 'avg time', 'temp comp', 'pres comp', 'format',
                                      'lrec format', 'o3 coef', 'o3 bkg'],
                       'set_config': ['set mode remote', 'set gas unit ppb', 'set range 1', 'set avg time 3',
                                      'set temp comp on', 'set pres comp on', 'set format 00',
                                      'set lrec format 01 02',
                                      'set save params'], 'get_data': 'lrec',
                       'data_header': 'time date  flags o3 cellai cellbi bncht lmpt o3lt flowa flowb pres',
                       'sampling_interval': 1, 'logs': 'C:/Users/mkn/Documents/mkndaq/logs'},
           'tei49i': {'type': 'TEI49I', 'id': 49, 'serial_number': 'unknown',
                      'socket': {'host': '192.168.0.20', 'port': 9880, 'timeout': 5, 'sleep': 0.5},
                      'get_config': ['mode', 'gas unit', 'range', 'avg time', 'temp comp', 'pres comp', 'format',
                                     'lrec format', 'o3 coef', 'o3 bkg'],
                      'set_config': ['set mode remote', 'set gas unit ppb', 'set range 1', 'set avg time 3',
                                     'set temp comp on', 'set pres comp on', 'set format 00', 'set lrec format 0',
                                     'set save params'], 'get_data': 'lr00',
                      'data_header': 'time date  flags o3 hio3 cellai cellbi bncht lmpt o3lt flowa flowb pres',
                      'sampling_interval': 1, 'logs': 'C:\\Users\\jkl/Documents/mkndaq/logs'},
           'g2401': {'type': 'G2401', 'serial_number': 'CFKADS2320',
                     'socket': {'host': '192.168.0.21', 'port': 51020, 'timeout': 5, 'sleep': 0.5},
                     'get_data': ['_Meas_GetBufferFirst', '_Instr_getStatus'],
                     'netshare': '\\PICARRO-MI970/DataLog_User_Sync',
                     'data_storage': 'hourly',
                     'sampling_interval': 5, 'aggregation_period': 600, 'reporting_interval': 600}
           }

    try:
        tei49i = Thermo49i(name='tei49i', config=cfg)
        g2401 = G2401('g2401', config=cfg)

        print("Called directly:", tei49i.get_o3())
        print("Called directly: CO2 %s ppm  CH4 %s ppm  CO %s ppm" % tuple(g2401.get_co2_ch4_co()))
        print("[%s] CO2 %s ppm  CH4 %s ppm  CO %s ppm" % \
              (g2401._name, *g2401.get_co2_ch4_co()))
        print("%s [%s] %s  [%s] CO2 %s ppm  CH4 %s ppm  CO %s ppm" % \
              (time.strftime("%Y-%m-%d %H:%M:%S"),
               tei49i._name, tei49i.get_o3(),
               g2401._name, *g2401.get_co2_ch4_co()))

        schedule.every(10).seconds.do(tei49i.print_o3)
        schedule.every(10).seconds.do(g2401.print_co2_ch4_co)
        # schedule.every(10).seconds.do(print, "%s [%s] %s  [%s] CO2 %s ppm  CH4 %s ppm  CO %s ppm" % \
        #                               (time.strftime("%Y-%m-%d %H:%M:%S"),
        #                                tei49i._name, tei49i.get_o3(),
        #                                g2401._name, *g2401.get_co2_ch4_co()))


        print("# Begin data acquisition and file transfer")
        while True:
            schedule.run_pending()
            time.sleep(1)

    except Exception as err:
        print(err)


if __name__ == '__main__':
    main()
