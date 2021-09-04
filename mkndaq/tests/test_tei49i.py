# -*- coding: utf-8 -*-
"""
Tests for TCP/IP communication with TEI49i.
Uses examples from
- https://pymotw.com/2/socket/tcp.html#easy-client-connections
-
"""

from mkndaq.inst.tei49i import TEI49I

if __name__ == '__main__':
    cfg = {'file': 'mkndaq.cfg', 'version': '1.0.0-20210802', 'home': 'c:/users/jkl', 'reporting_interval': 10,
           'sftp': {'host': 'sftp.meteoswiss.ch', 'usr': 'gaw_mkn',
                    'key': 'C:\\Users\\jkl/.ssh/private-open-ssh-4096-mkn.ppk', 'proxy': {'socks5': None, 'port': 1080},
                    'logs': 'C:\\Users\\jkl/Documents/mkndaq/logs'}, 'logs': 'C:\\Users\\jkl/Documents/mkndaq/logs',
           'data': 'C:\\Users\\jkl/Documents/mkndaq/data',
           'staging': {'path': 'C:\\Users\\jkl/Documents/mkndaq/staging', 'zip': True},
           'tei49i': {'type': 'TEI49I', 'id': 49, 'serial_number': 'unknown',
                      'socket': {'host': '192.168.0.20', 'port': 9880, 'timeout': 5, 'sleep': 0.5},
                      'get_config': ['mode', 'gas unit', 'range', 'avg time', 'temp comp', 'pres comp', 'format',
                                     'lrec format', 'o3 coef', 'o3 bkg'],
                      'set_config': ['set mode remote', 'set gas unit ppb', 'set range 1', 'set avg time 3',
                                     'set temp comp on', 'set pres comp on', 'set format 00', 'set lrec format 0',
                                     'set save params'], 'get_data': 'lr00',
                      'data_header': 'time date  flags o3 hio3 cellai cellbi bncht lmpt o3lt flowa flowb pres',
                      'sampling_interval': 1, 'logs': 'C:\\Users\\jkl/Documents/mkndaq/logs'},
           }
    tei49i = TEI49I('tei49i', config=cfg, simulate=False)
    print(tei49i.get_config())
    print(tei49i.set_config())

    run = True
    while run:
        cmd = input("Enter command or Q to quit: ")
        if cmd != "Q":
            print(tei49i.get_data(cmd, save=False))
        else:
            run = False

    print('done')
