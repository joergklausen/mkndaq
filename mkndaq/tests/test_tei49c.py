# -*- coding: utf-8 -*-
"""
Tests for serial communication with TEI49C
"""

from mkndaq.inst.thermo import Thermo49C

if __name__ == '__main__':
    cfg = {'data': '~/Documents/mkndaq/data',
            'staging': {'path': '~/Documents/mkndaq/staging', 'zip': True},
            'reporting_interval': 1,
            'COM4': {'protocol': 'RS232', 'baudrate': 9600, 'bytesize': 8, 'stopbits': 1, 'parity': 'N', 'timeout': 0.1},
            'tei49c': {'type': 'TEI49C',
                        'id': 49,
                        'serial_number': 'unknown',
                        'port': 'COM4',
                        'get_config': ['mode', 'gas unit', 'range', 'avg time', 'temp comp', 'pres comp', 'format',
                                        'lrec format', 'o3 coef', 'o3 bkg'],
                        'set_config': ['set mode remote', 'set gas unit ppb', 'set range 1', 'set avg time 3',
                                        'set temp comp on', 'set pres comp on', 'set format 00', 'set lrec format 01 02',
                                        'set srec format 01 02', 'set save params'],
                        'sampling_interval': 1,
                        'staging_zip': True,
                        'logs': '~/Documents/mkndaq/logs'}}

    tei49c = Thermo49C(name='tei49c', config=cfg)

    print(tei49c.get_config())
    print(tei49c.set_config())

    run = True
    while run:
        cmd = input("Enter command or Q to quit: ")
        if cmd != "Q":
            print(tei49c.accumulate_lrec())
        else:
            run = False

    print('done')
