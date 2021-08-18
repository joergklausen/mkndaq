#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Main driver for Windows 10. Initializes instruments and communication, then activates scheduled jobs for the main loop.
This relies on https://schedule.readthedocs.io/en/stable/index.html.

@author: joerg.klausen@meteoswiss.ch
"""
import os
import logging
import time
import argparse
import schedule

from mkndaq.utils.configparser import config
from mkndaq.utils.filetransfer import SFTPClient
from mkndaq.inst.tei49c import TEI49C
from mkndaq.inst.tei49i import TEI49I


def main():
    logs = None
    logger = None
    try:
        print("###  MKNDAQ started on %s" % time.strftime("%Y-%m-%d %H:%M"))
        parser = argparse.ArgumentParser(
            description='Data acquisition and transfer for MKN Global GAW Station.',
            usage='mkndaq[.exe] [-s] -c')
        parser.add_argument('-s', '--simulate', action='store_true',
                            help='simulate communication with instruments', required=False)
        parser.add_argument('-c', '--configuration', type=str, help='path to configuration file', required=True)
        args = parser.parse_args()

        simulate = args.simulate

        # read config file
        config_file = args.configuration
        cfg = config(config_file)

        # setup logging
        logs = os.path.expanduser(cfg['logs'])
        os.makedirs(logs, exist_ok=True)
        logfile = os.path.join(logs,
                               '%s.log' % time.strftime('%Y%m%d'))
        logger = logging.getLogger(__name__)
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                            datefmt='%y-%m-%d %H:%M:%S',
                            filename=str(logfile),
                            filemode='a')
        logging.getLogger('schedule').setLevel(level=logging.ERROR)
        logging.getLogger('paramiko.transport').setLevel(level=logging.ERROR)

        logger.info("=== mkndaq started ===")

        # initialize data transfer
        sftp = SFTPClient(config=cfg)

        # transfer most recent log file
        print("%s Transfering most recent log file ..." % time.strftime('%Y-%m-%d %H:%M:%S'))
        sftp.transfer_most_recent_file(localpath=os.path.expanduser(cfg['logs']), remotepath='logs')

        # transfer any existing data files
        print("%s Transfering existing staged files ..." % time.strftime('%Y-%m-%d %H:%M:%S'))
        sftp.move_r()

        # initialize instruments, get and set configurations and define schedules
        # NB: In case, more instruments should be handled, the relevant calls need to be included here below.
        try:
            if cfg['tei49c']:
                tei49c = TEI49C(name='tei49c', config=cfg, simulate=simulate)
                tei49c.get_config()
                tei49c.set_config()
                schedule.every(cfg['tei49c']['sampling_interval']).minutes.at(':00').do(tei49c.get_data)
        except Exception as err:
            logger.error(err)

        try:
            if cfg['tei49i']:
                tei49i = TEI49I(name='tei49i', config=cfg, simulate=simulate)
                tei49i.get_config()
                tei49i.set_config()
                schedule.every(cfg['tei49i']['sampling_interval']).minutes.at(':00').do(tei49i.get_data)
        except Exception as err:
            logger.error(err)

        # schedule for data transfer
        rep_int = cfg['reporting_interval']
        schedule.every(rep_int).minutes.at(':20').do(sftp.move_r)

        print("# Begin data acquisition and file transfer")
        while True:
            schedule.run_pending()
            time.sleep(1)

    except Exception as err:
        if logs:
            logger.error(err)
        print(err)


if __name__ == '__main__':
    main()
