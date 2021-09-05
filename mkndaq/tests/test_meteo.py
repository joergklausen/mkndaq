# -*- coding: utf-8 -*-
import os

from mkndaq.inst.meteo import METEO

if __name__ == '__main__':
    cfg = {'logs': 'C:\\Users\\jkl/Documents/mkndaq/logs',
           'data': 'C:\\Users\\jkl/Documents/mkndaq/data',
           'staging': {'path': 'C:\\Users\\jkl/Documents/mkndaq/staging', 'zip': True},
           'meteo': {'type': 'METEO',
                     'source': 'c:/ftproot/meteo',
                     'staging_interval': 5}
           }

    meteo = METEO(name='meteo', config=cfg)
    meteo.print_meteo()
    meteo.store_and_stage_files()

    # files = os.listdir("c:/ftproot/meteo")
    print('done')