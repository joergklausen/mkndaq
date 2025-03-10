#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os

from mkndaq.inst.thermo import Thermo49i
from mkndaq.utils.utils import (load_config, setup_logging)


def main():
    try:
        # collect and parse CLI arguments
        parser = argparse.ArgumentParser(
            description='Retrieve all lrec from instrument.',
            usage='python3 tei49c_get_all.py -c')
        parser.add_argument('-c', '--configuration', type=str,
                            help='full path to configuration file',
                            default='dist/mkndaq.yml', required=False)
        args = parser.parse_args()
       # fetch = args.fetch()
        config_file = args.configuration

        # load configuation
        cfg = load_config(config_file=config_file)

        # setup logging
        logfile = os.path.join(os.path.expanduser(str(cfg['root'])),
                            cfg['logging']['file'])
        logger = setup_logging(file=logfile)

        tei49i = Thermo49i(name='tei49i', config=cfg)

        file = tei49i.get_all_lrec()
        print(f'{file} written.')
    except Exception as err:
        print(err)


if __name__ == "__main__":
    main()