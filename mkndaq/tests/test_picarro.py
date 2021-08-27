# -*- coding: utf-8 -*-
"""
bla

"""

from mkndaq.inst.picarro import G2401
# from mkndaq.utils.filetransfer import SFTPClient
# from mkndaq.utils.configparser import config
import os


# import time


def list_files(startpath):
    for root, dirs, files in os.walk(startpath):
        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * level
        print('{}{}/'.format(indent, os.path.basename(root)))


def main():
    try:
        cfg = {'file': 'mkndaq.cfg',
               'version': '1.0.0-20210802',
               'home': 'c:/users/mkn',
               'reporting_interval': 10,
               'sftp': {'host': 'sftp.meteoswiss.ch', 'usr': 'gaw_mkn', 'key': '~/.ssh/private-open-ssh-4096-mkn.ppk',
                        'proxy': {'socks5': None, 'port': 1080}, 'logs': '~/Documents/mkndaq/logs'},
               'logs': '~/Documents/mkndaq/logs',
               'data': '~/Documents/mkndaq/data',
               'staging': {'path': '~/Documents/mkndaq/staging', 'zip': True},
               'g2401': {'type': 'G2401',
                         'serial_number': 'CFKADS2329',
                         'netshare': '//PICARRO-MI910/DataLog_User_Sync',
                         'socket': {'host': '192.168.0.51', 'port': 51020, 'timeout': 1},
                         'get_data': ['_Meas_GetBufferFirst', '_Instr_getStatus'],
                         'data_storage': 'hourly',
                         'reporting_interval': 3600}}

        g2401 = G2401('g2401', config=cfg)
        g2401.stage_latest_file()

        # files = os.walk(cfg['netshare'])
        # for file in files:
        #     print(file)
        # list_files(cfg['g2401']['netshare'])

        # config = config('c:/users/administrator/documents/git/gaw-mkn-daq/mkndaq/mkndaq.cfg')

        # data = picarro.get_data()

    except Exception as err:
        print(err)


if __name__ == "__main__":
    main()
