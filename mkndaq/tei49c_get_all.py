#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import os
# from mkndaq.utils.configparser import config
from mkndaq.inst.tei49c import TEI49C


def main():
    try:
        # config_file = os.path.expanduser("~/mkndaq/mkndaq.cfg")

        # # read config file
        # cfg = config(config_file)
        cfg = {'data': 'C:/Users/mkn/Documents/mkndaq/data',
               'staging': {'path': 'C:/Users/mkn/Documents/mkndaq/staging', 'zip': True},
               'COM4': {'protocol': 'RS232', 'baudrate': 9600, 'bytesize': 8, 'stopbits': 1, 'parity': 'N', 'timeout': 0.1},
               'tei49c': {'type': 'TEI49C', 
                          'id': 49, 
                          'serial_number': 'unknown', 
                          'port': 'COM4',
                          'get_config': ['mode', 'gas unit', 'range', 'avg time', 'temp comp', 'pres comp', 'format',
                                         'lrec format', 'o3 coef', 'o3 bkg'],
                          'set_config': ['set mode remote', 'set gas unit ppb', 'set range 1', 'set avg time 3',
                                         'set temp comp on', 'set pres comp on', 'set format 00', 'set lrec format 01 02',
                                         'set save params'], 
                          'get_data': 'lrec',
                          'data_header': 'time date flags o3 cellai cellbi bncht lmpt o3lt flowa flowb pres',
                          'sampling_interval': 1, 'logs': 'C:/Users/mkn/Documents/mkndaq/logs'}}

        if cfg.get('tei49c', None):
            tei49c = TEI49C(name='tei49c', config=cfg)
            tei49c.get_all_rec()
            print('done.')
    except Exception as err:
        print(err)
        