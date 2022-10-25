#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tests for TCP/IP communication with Picarro G2401

@author: joerg.klausen@meteoswiss.ch
"""

from mkndaq.inst.g2401 import G2401

if __name__ == '__main__':
    cfg = {'file': 'mkndaq.cfg', 'version': '1.0.0-20210802', 'home': 'c:/users/mkn', 'reporting_interval': 10,
#           'sftp': {'host': 'sftp.meteoswiss.ch', 'usr': 'gaw_mkn',
#                    'key': 'C:\\Users\\jkl/.ssh/private-open-ssh-4096-mkn.ppk', 'proxy': {'socks5': None, 'port': 1080},
#                    'logs': 'C:\\Users\\jkl/Documents/mkndaq/logs'},
           'logs': '~/Documents/devt/logs',
           'data': '~/Documents/devt/data',
           'staging': {'path': 'C:/Users/mkn/Documents/devt/staging', 'zip': True},
           'g2401': {'type': 'G2401', 'serial_number': 'CFKADS2320',
                     'staging_zip': True,
                    'socket': {'host': '192.168.4.102', 'port': 51020, 'timeout': 5, 'sleep': 0.5},
                    'get_data': ['_Meas_GetBufferFirst', '_Instr_getStatus'],
                    'netshare': 'DataLog_User_Sync',
                    'data_storage_interval': 'hourly',
                    'staging_interval': 60,
                    'staging_minute': 10,
                    'sampling_interval': 5, 'aggregation_period': 600, 'reporting_interval': 600}
           }

    g2401 = G2401('g2401', config=cfg)
    print("_Meas_GetConc:", g2401.tcpip_comm('_Meas_GetConc'))
#    print("_Instr_GetStatus:", g2401.tcpip_comm('_Instr_GetStatus'))
#    print("_Valves_Seq_Readstate:", g2401.tcpip_comm('_Valves_Seq_Readstate'))
#    print("_Meas_GetBuffer:", g2401.tcpip_comm('_Meas_GetBuffer'))
    tst = g2401.store_and_stage_new_files()
    print('done')
