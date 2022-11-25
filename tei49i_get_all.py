#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from mkndaq.utils.configparser import config
# from mkndaq.utils.filetransfer import SFTPClient
from mkndaq.inst.tei49i import TEI49I


def main():
    try:
        config_file = os.path.expanduser("~/mkndaq/mkndaq.cfg")

        # read config file
        cfg = config(config_file)

        if cfg.get('tei49i', None):
            tei49i = TEI49I(name='tei49i', config=cfg)
            tei49i.get_all_rec()
            print('done.')

        if cfg.get('tei49i_2', None):
            tei49i = TEI49I(name='tei49i_2', config=cfg)
            tei49i.get_all_rec()
            print('done.')

    except Exception as err:
        print(err)


if __name__ == "__main__":
    main()        