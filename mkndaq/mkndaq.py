#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Main driver for Windows 10. Initializes instruments and communication, then activates scheduled jobs for the main loop.

This relies on https://schedule.readthedocs.io/en/stable/index.html.

@author: joerg.klausen@meteoswiss.ch
"""
import os
import argparse
import logging
import time
import threading
import schedule
import colorama

from mkndaq.utils.configparser import config
from mkndaq.utils.filetransfer import SFTPClient

def run_threaded(job_func):
    """Set up threading and start job.

    Args:
        job_func ([type]): [description]
    """
    job_thread = threading.Thread(target=job_func)
    job_thread.start()

def main():
    """Read config file, set up instruments, and launch data acquisition."""
    colorama.init(autoreset=True)
    version = 'v0.7.0'
    print(f"###  MKNDAQ ({version}) started on {time.strftime('%Y-%m-%d %H:%M')}")
    print(f"Supports following instruments (depending on configuration):")
    print(f" - TEI49C, Thermo 49i")
    print(f" - aerosol (file transfer only)")
    print(f" - Picarro G2401 (file transfer only)")
    print(f" - Magee AE33")
    print(f" - Acoem NE-300")

    # collect and interprete CLI arguments
    parser = argparse.ArgumentParser(
        description='Data acquisition and transfer for MKN Global GAW Station.',
        usage='mkndaq[.exe] [-s] -c')
    parser.add_argument('-c', '--configuration', type=str, help='path to configuration file', 
                        default='dist/mkndaq.cfg', required=False)
#                        default=os.path.expanduser('~dist\mkndaq.cfg'), required=False)
    parser.add_argument('-f', '--fetch', type=int, default=20,
                        help='interval in seconds to fetch and display current instrument data',
                        required=False)
    parser.add_argument('-s', '--simulate', action='store_true', default=False,
                        help='simulate communication with instruments', required=False)
    args = parser.parse_args()
    simulate = args.simulate
    fetch = args.fetch
    config_file = args.configuration

    # read config file
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

    logger.info("=== mkndaq (%s) started ===" % version)

    try:
        # initialize data transfer, set up remote folders
        if cfg.get('sftp', None):
            sftp = SFTPClient(config=cfg)
            sftp.setup_remote_folders()

            # stage most recent config file
            print("%s Staging current config file ..." % time.strftime('%Y-%m-%d %H:%M:%S'))
            sftp.stage_current_config_file(config_file)

        # initialize instruments, get and set configurations and define schedules
        # NB: In case more instruments should be handled, the relevant calls need to be included here below.
        try:
            if cfg.get('ne300', None):
                from mkndaq.inst.neph import NEPH
                ne300 = NEPH(name='ne300', config=cfg)
                schedule.every(fetch).seconds.do(run_threaded, ne300.print_ssp_bssp)
                schedule.every(cfg['ne300']['get_data_interval']).minutes.at(':10').do(run_threaded, ne300.get_new_data)                
                schedule.every(cfg['ne300']['zero_span_check_interval']).minutes.at(':00').do(run_threaded, ne300.do_zero_span_check)
            if cfg.get('tei49c', None):
                from mkndaq.inst.tei49c import TEI49C
                tei49c = TEI49C(name='tei49c', config=cfg, simulate=simulate)
                schedule.every(cfg['tei49c']['sampling_interval']).minutes.at(':00').do(run_threaded, tei49c.get_data)
                schedule.every(6).hours.at(':00').do(run_threaded, tei49c.set_datetime)
                schedule.every(fetch).seconds.do(run_threaded, tei49c.print_o3)
            if cfg.get('tei49i', None):
                from mkndaq.inst.tei49i import TEI49I
                tei49i = TEI49I(name='tei49i', config=cfg, simulate=simulate)
                schedule.every(cfg['tei49i']['sampling_interval']).minutes.at(':00').do(run_threaded, tei49i.get_data)
                schedule.every().day.at('00:00').do(run_threaded, tei49i.set_datetime)
                schedule.every(fetch).seconds.do(run_threaded, tei49i.print_o3)
            if cfg.get('tei49i_2', None):
                from mkndaq.inst.tei49i import TEI49I
                tei49i_2 = TEI49I(name='tei49i_2', config=cfg, simulate=simulate)
                schedule.every(cfg['tei49i_2']['sampling_interval']).minutes.at(':00').do(run_threaded, tei49i_2.get_data)
                schedule.every().day.at('00:00').do(run_threaded, tei49i_2.set_datetime)
                schedule.every(fetch+5).seconds.do(run_threaded, tei49i_2.print_o3)
            if cfg.get('g2401', None):
                from mkndaq.inst.g2401 import G2401
                g2401 = G2401('g2401', config=cfg)
                g2401.store_and_stage_files()
                schedule.every(cfg['g2401']['staging_interval']).minutes.do(run_threaded, g2401.store_and_stage_files)
                schedule.every(fetch).seconds.do(run_threaded, g2401.print_co2_ch4_co)
            if cfg.get('meteo', None):
                from mkndaq.inst.meteo import METEO
                meteo = METEO('meteo', config=cfg)
                meteo.store_and_stage_files()
                schedule.every(cfg['meteo']['staging_interval']).minutes.do(run_threaded, meteo.store_and_stage_files)
                schedule.every(cfg['meteo']['staging_interval']).minutes.do(run_threaded, meteo.print_meteo)
            if cfg.get('aerosol', None):
                from mkndaq.inst.aerosol import AEROSOL
                aerosol = AEROSOL('aerosol', config=cfg)
                aerosol.store_and_stage_files()
                schedule.every(cfg['aerosol']['staging_interval']).minutes.do(run_threaded, aerosol.store_and_stage_files)
                schedule.every(cfg['aerosol']['staging_interval']).minutes.do(run_threaded, aerosol.print_aerosol)
            if cfg.get('ae33', None):
                from mkndaq.inst.ae33 import AE33
                ae33 = AE33(name='ae33', config=cfg)
                schedule.every(cfg['ae33']['sampling_interval']).minutes.at(':00').do(ae33.get_new_data)
                schedule.every(cfg['ae33']['sampling_interval']).minutes.at(':00').do(ae33.get_new_log_entries)
                schedule.every(fetch).seconds.do(run_threaded, ae33.print_ae33)

        except Exception as err:
            if logs:
                logger.error(err)
            print(err)

        # stage most recent log file and define schedule
        print("%s Staging current log file ..." % time.strftime('%Y-%m-%d %H:%M:%S'))
        sftp.stage_current_log_file()
        schedule.every().day.at('00:00').do(sftp.stage_current_log_file)

        # transfer any existing staged files and define schedule for data transfer
        print("%s Transfering existing staged files ..." % time.strftime('%Y-%m-%d %H:%M:%S'))
        sftp.xfer_r()
        schedule.every(cfg['reporting_interval']).minutes.at(':20').do(run_threaded, sftp.xfer_r)

        print("# Begin data acquisition and file transfer")

        # align start with a 10' timestamp
        while int(time.time()) % 10 > 0:
            time.sleep(0.1)

        while True:
            schedule.run_pending()
            time.sleep(1)

    except Exception as err:
        if logs:
            logger.error(err)
        print(err)


if __name__ == '__main__':
    main()
