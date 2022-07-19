#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from mkndaq.utils.configparser import config
# from mkndaq.utils.filetransfer import SFTPClient
from mkndaq.inst.tei49c import TEI49C


def main():
    try:
        config_file = os.path.expanduser("~/mkndaq/mkndaq.cfg")

        # read config file
        cfg = config(config_file)

        if cfg.get('tei49c', None):
            tei49c = TEI49C(name='tei49c', config=cfg)
            tei49c.get_all_lrec()
            print('done.')
    except Exception as err:
        print(err)
        