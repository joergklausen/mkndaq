# -*- coding: utf-8 -*-
import os

from mkndaq.inst.aerosol import AEROSOL

if __name__ == '__main__':
    cfg = {'logs': 'C:\\Users\\jkl/Documents/mkndaq/logs',
           'data': 'C:\\Users\\jkl/Documents/mkndaq/data',
           'staging': {'path': 'C:\\Users\\jkl/Documents/mkndaq/staging', 'zip': True},
           'aerosol': {'type': 'AEROSOL',
                     'netshare': '\\192.168.0.12\psi',
                     'staging_interval': 5}
           }

    aerosol = AEROSOL(name='aerosol', config=cfg)
    aerosol.print_aerosol()
    aerosol.store_and_stage_files()

    print('done')