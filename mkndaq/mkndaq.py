#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Main driver. Initializes instruments and communication, then activates scheduled jobs for the main loop.

This relies on https://schedule.readthedocs.io/en/stable/index.html.

@author: joerg.klausen@meteoswiss.ch
"""
import argparse
import os
import threading
import time

import colorama
import schedule

from mkndaq.utils.sftp import SFTPClient
from mkndaq.utils.utils import load_config, setup_logging, copy_file, seconds_to_next_n_minutes


def run_threaded(job_func):
    """Set up threading and start job.

    Args:
        job_func ([type]): [description]
    """
    job_thread = threading.Thread(target=job_func)
    job_thread.start()


def main():
    """Read config file, set up instruments, and launch data acquisition."""
    try:
        # collect and parse CLI arguments
        parser = argparse.ArgumentParser(
            description='Data acquisition and transfer for MKN Global GAW Station.',
            usage='python3 mkndaq.py|mkndaq.exe -c [-f]')
        parser.add_argument('-c', '--configuration', type=str,
                            help='full path to configuration file',
                            default='dist/mkndaq.yml', required=False)
        parser.add_argument('-f', '--fetch', type=int, default=20,
                            help='interval in seconds to fetch and display current instrument data',
                            required=False)
        args = parser.parse_args()
        fetch = args.fetch
        config_file = args.configuration

        # load configuation
        cfg = load_config(config_file=config_file)

        # setup logging
        logfile = os.path.join(os.path.expanduser(str(cfg['root'])),
                            cfg['logging']['file'])
        logger = setup_logging(file=logfile)

        colorama.init(autoreset=True)

        # get version from setup.py
        version = 'vx.y.z'
        with open('setup.py', 'r') as fh:
            for line in fh:
                if 'version=' in line:
                    version = line.split('version=')[1].split(',')[0].strip().strip("'\"")
                    break

        # Inform user on what's going on
        logger.info(f"==  MKNDAQ ({version}) started =====================")

        # initialize data transfer, set up remote folders
        sftp = SFTPClient(config=cfg)
        # sftp.setup_remote_folders()

        # setup staging
        staging = os.path.join(os.path.expanduser(cfg['root']), cfg['staging'])

        # stage most recent config file
        logger.info(f"Staging current config file {config_file}")
        copy_file(source=config_file, target=staging, logger=logger)

        # initialize instruments, get and set configurations and define schedules
        # NB: In case more instruments should be handled, the relevant calls need to be included here below.
        try:
            if cfg.get('tei49c', None):
                from mkndaq.inst.thermo import Thermo49C
                tei49c = Thermo49C(name='tei49c', config=cfg)
                tei49c.setup_schedules()
                remote_path = os.path.join(sftp.remote_path, tei49c.remote_path)
                sftp.setup_transfer_schedules(local_path=tei49c.staging_path,
                                            remote_path=remote_path,
                                            interval=tei49c.reporting_interval)  
                schedule.every(6).hours.at(':00').do(run_threaded, tei49c.set_datetime)
                schedule.every(fetch).seconds.do(run_threaded, tei49c.print_o3)
            if cfg.get('tei49i', None):
                from mkndaq.inst.thermo import Thermo49i
                tei49i = Thermo49i(name='tei49i', config=cfg)
                tei49i.setup_schedules()
                remote_path = os.path.join(sftp.remote_path, tei49i.remote_path)
                sftp.setup_transfer_schedules(local_path=tei49i.staging_path,
                                            remote_path=remote_path,
                                            interval=tei49i.reporting_interval)  
                schedule.every().day.at('00:00').do(run_threaded, tei49i.set_datetime)
                schedule.every(fetch).seconds.do(run_threaded, tei49i.print_o3)
            if cfg.get('tei49i_2', None):
                from mkndaq.inst.thermo import Thermo49i
                tei49i_2 = Thermo49i(name='tei49i_2', config=cfg)
                tei49i_2.setup_schedules()
                remote_path = os.path.join(sftp.remote_path, tei49i_2.remote_path)
                sftp.setup_transfer_schedules(local_path=tei49i_2.staging_path,
                                            remote_path=remote_path,
                                            interval=tei49i_2.reporting_interval)  
                schedule.every().day.at('00:00').do(run_threaded, tei49i_2.set_datetime)
                schedule.every(fetch+5).seconds.do(run_threaded, tei49i_2.print_o3)
            if cfg.get('g2401', None):
                from mkndaq.inst.g2401 import G2401
                g2401 = G2401('g2401', config=cfg)
                g2401.store_and_stage_files()
                schedule.every(cfg['g2401']['reporting_interval']).minutes.do(run_threaded, g2401.store_and_stage_files)
                remote_path = os.path.join(sftp.remote_path, g2401.remote_path)
                sftp.setup_transfer_schedules(local_path=g2401.staging_path,
                                            remote_path=remote_path,
                                            interval=g2401.reporting_interval)  
                schedule.every(fetch).seconds.do(run_threaded, g2401.print_co2_ch4_co)
            if cfg.get('meteo', None):
                from mkndaq.inst.meteo import METEO
                meteo = METEO('meteo', config=cfg)
                meteo.store_and_stage_files()
                remote_path = os.path.join(sftp.remote_path, meteo.remote_path)
                sftp.setup_transfer_schedules(local_path=meteo.staging_path,
                                            remote_path=remote_path,
                                            interval=meteo.reporting_interval)  
                schedule.every(cfg['meteo']['reporting_interval']).minutes.do(run_threaded, meteo.store_and_stage_files)
                schedule.every(cfg['meteo']['reporting_interval']).minutes.do(run_threaded, meteo.print_meteo)
            if cfg.get('ae33', None):
                from mkndaq.inst.ae33 import AE33
                ae33 = AE33(name='ae33', config=cfg)
                ae33.setup_schedules()
                remote_path = os.path.join(sftp.remote_path, ae33.remote_path)
                sftp.setup_transfer_schedules(local_path=ae33._staging_path_data,
                                            remote_path=os.path.join(remote_path, 'data'),
                                            interval=ae33.reporting_interval)  
                sftp.setup_transfer_schedules(local_path=ae33._staging_path_logs,
                                            remote_path=os.path.join(remote_path, 'logs'),
                                            interval=ae33.reporting_interval)  
                # schedule.every(cfg['ae33']['sampling_interval']).minutes.at(':00').do(ae33.get_new_data)
                # schedule.every(cfg['ae33']['sampling_interval']).minutes.at(':00').do(ae33.get_new_log_entries)
                schedule.every(fetch).seconds.do(run_threaded, ae33.print_ae33)
            if cfg.get('ne300', None):
                from mkndaq.inst.neph import NEPH
                ne300 = NEPH(name='ne300', config=cfg)
                ne300 = NEPH('ne300', cfg, verbosity=0)
                ne300.setup_schedules()
                remote_path = os.path.join(sftp.remote_path, ne300.remote_path)
                sftp.setup_transfer_schedules(local_path=ne300.staging_path,
                                            remote_path=remote_path,
                                            interval=ne300.reporting_interval)  
                schedule.every(fetch).seconds.do(run_threaded, ne300.print_ssp_bssp)
            #     schedule.every(cfg['ne300']['get_data_interval']).minutes.at(':10').do(run_threaded, ne300.get_new_data)
            #     schedule.every(cfg['ne300']['zero_span_check_interval']).minutes.at(':00').do(run_threaded, ne300.do_zero_span_check)

        except Exception as err:
            logger.error(err)

        # # transfer any existing staged files and define schedule for data transfer
        # logger.info("mkndaq, Transfering existing staged files ...")
        # sftp.xfer_r()
        # # schedule.every(cfg['reporting_interval']).minutes.at(':20').do(run_threaded, sftp.xfer_r)

        # list all jobs
        logger.info(schedule.get_jobs())

        # transfer most recent log file and define schedule
        logger.info(f"Staging current log file {logfile}")
        copy_file(source=logfile, target=staging, logger=logger)
        schedule.every(1).day.at('00:00').do(copy_file, source=logfile, target=staging, logger=logger)

        # align start with a multiple-of-minute timestamp
        n = 1

        # Countdown to the next full 10 minutes
        seconds_left = seconds_to_next_n_minutes(n)
        while seconds_left > 0:
            print(f"Time remaining: {seconds_left} seconds", end="\r")
            time.sleep(1)
            seconds_left -= 1
        logger.info("Beginning data acquisition and file transfer ...")

        while True:
            schedule.run_pending()
            time.sleep(1)

    except Exception as err:
        logger.error(err)


if __name__ == '__main__':
    main()
