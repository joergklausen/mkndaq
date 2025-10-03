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
from pathlib import Path, PurePosixPath

import colorama
import schedule
from mkndaq.utils.s3fsc import S3FSC
from mkndaq.utils.sftp import SFTPClient

from mkndaq.utils.utils import (copy_file, load_config,
                                seconds_to_next_n_minutes, setup_logging)


def run_threaded(job_func):
    """Set up threading and start job.

    Args:
        job_func ([type]): [description]
    """
    job_thread = threading.Thread(target=job_func)
    job_thread.start()


def main():
    """Read config file, set up instruments, and launch data acquisition."""
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

    try:
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

        # decide on file transfer mechanism
        s3fsc = None
        sftp = None

        # Prefer S3 when config contains an 's3' section
        if cfg.get("s3"):
            # You can control these via mkndaq.yml's s3.* or override here if needed
            s3fsc = S3FSC(
                cfg,
                use_proxies=bool(cfg["s3"].get("use_proxies", True)),
                addressing_style=cfg["s3"].get("addressing_style", "path"),
                verify=cfg["s3"].get("verify", True),
                default_prefix=cfg["s3"].get("default_prefix", ""),
            )
        if SFTPClient and cfg.get("sftp"):
            # Optional fallback if S3 is not configured
            sftp = SFTPClient(config=cfg)
        else:
            raise RuntimeError("Neither S3 nor SFTP is configured in mkndaq.yml")

        # setup staging
        staging = os.path.join(os.path.expanduser(cfg['root']), cfg['staging'])

        # stage most recent config file
        logger.info(f"Staging current config file {config_file}")
        copy_file(source=config_file, target=staging, logger=logger)

        # initialize instruments, get and set configurations and define schedules
        try:
            if cfg.get('tei49c', None):
                from mkndaq.inst.thermo import Thermo49C
                tei49c = Thermo49C(name='tei49c', config=cfg)
                tei49c.get_config()
                tei49c.set_config()
                tei49c.setup_schedules()

                # === S3 transfer scheduling ===
                if s3fsc:
                    s3fsc.setup_transfer_schedules(
                        local_path=tei49c.staging_path,
                        key_prefix=tei49c.remote_path,
                        interval=tei49c.reporting_interval,
                        remove_on_success=False,
                    )
                if sftp:
                    remote_path = (PurePosixPath(sftp.remote_path) / tei49c.remote_path).as_posix()
                    sftp.setup_transfer_schedules(local_path=tei49c.staging_path,
                                                  remote_path=remote_path,
                                                  interval=tei49c.reporting_interval)

                schedule.every(6).hours.at(':00').do(run_threaded, tei49c.set_datetime)
                schedule.every(fetch).seconds.do(run_threaded, tei49c.print_o3)

            if cfg.get('tei49i', None):
                from mkndaq.inst.thermo import Thermo49i
                tei49i = Thermo49i(name='tei49i', config=cfg)
                tei49i.setup_schedules()

                if s3fsc:
                    s3fsc.setup_transfer_schedules(
                        local_path=tei49i.staging_path,
                        key_prefix=tei49i.remote_path,
                        interval=tei49i.reporting_interval,
                        remove_on_success=False,
                    )
                if sftp:
                    remote_path = (PurePosixPath(sftp.remote_path) / tei49i.remote_path).as_posix()
                    sftp.setup_transfer_schedules(local_path=tei49i.staging_path,
                                                  remote_path=remote_path,
                                                  interval=tei49i.reporting_interval)

                schedule.every().day.at('00:00').do(run_threaded, tei49i.set_datetime)
                schedule.every(fetch).seconds.do(run_threaded, tei49i.print_o3)

            if cfg.get('tei49i_2', None):
                from mkndaq.inst.thermo import Thermo49i
                tei49i_2 = Thermo49i(name='tei49i_2', config=cfg)
                tei49i_2.setup_schedules()

                if s3fsc:
                    s3fsc.setup_transfer_schedules(
                        local_path=tei49i_2.staging_path,
                        key_prefix=tei49i_2.remote_path,
                        interval=tei49i_2.reporting_interval,
                        remove_on_success=False,
                    )
                if sftp:
                    remote_path = (PurePosixPath(sftp.remote_path) / tei49i_2.remote_path).as_posix()
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

                if s3fsc:
                    s3fsc.setup_transfer_schedules(
                        local_path=g2401.staging_path,
                        key_prefix=g2401.remote_path,
                        interval=g2401.reporting_interval,
                        remove_on_success=False,
                    )
                if sftp:
                    remote_path = (PurePosixPath(sftp.remote_path) / g2401.remote_path).as_posix()
                    sftp.setup_transfer_schedules(local_path=g2401.staging_path,
                                                  remote_path=remote_path,
                                                  interval=g2401.reporting_interval)

                schedule.every(fetch).seconds.do(run_threaded, g2401.print_co2_ch4_co)

            if cfg.get('meteo', None):
                from mkndaq.inst.meteo import METEO
                meteo = METEO('meteo', config=cfg)
                meteo.store_and_stage_files()

                if s3fsc:
                    s3fsc.setup_transfer_schedules(
                        local_path=meteo.staging_path,
                        key_prefix=meteo.remote_path,
                        interval=meteo.reporting_interval,
                        remove_on_success=False,
                    )
                if sftp:
                    remote_path = (PurePosixPath(sftp.remote_path) / meteo.remote_path).as_posix()
                    sftp.setup_transfer_schedules(local_path=meteo.staging_path,
                                                  remote_path=remote_path,
                                                  interval=meteo.reporting_interval)

                schedule.every(cfg['meteo']['reporting_interval']).minutes.do(run_threaded, meteo.store_and_stage_files)
                schedule.every(cfg['meteo']['reporting_interval']).minutes.do(run_threaded, meteo.print_meteo)

            if cfg.get('ae33', None):
                from mkndaq.inst.ae33 import AE33
                ae33 = AE33(name='ae33', config=cfg)
                ae33.setup_schedules()

                if s3fsc:
                    # data
                    s3fsc.setup_transfer_schedules(
                        local_path=ae33.staging_path_data,
                        key_prefix=ae33.remote_path_data,
                        interval=ae33.reporting_interval,
                        remove_on_success=False,
                    )
                    # logs
                    s3fsc.setup_transfer_schedules(
                        local_path=ae33.staging_path_logs,
                        key_prefix=ae33.remote_path_logs,
                        interval=ae33.reporting_interval,
                        remove_on_success=False,
                    )
                if sftp:
                    remote_path_data = (PurePosixPath(sftp.remote_path) / ae33.remote_path_data).as_posix()
                    remote_path_logs = (PurePosixPath(sftp.remote_path) / ae33.remote_path_logs).as_posix()
                    sftp.setup_transfer_schedules(local_path=ae33.staging_path_data,
                                                  remote_path=remote_path_data,
                                                  interval=ae33.reporting_interval)
                    sftp.setup_transfer_schedules(local_path=ae33.staging_path_logs,
                                                  remote_path=remote_path_logs,
                                                  interval=ae33.reporting_interval)

                schedule.every(fetch).seconds.do(run_threaded, ae33.print_ae33)

            if cfg.get('ne300', None):
                from mkndaq.inst.neph import NEPH
                ne300 = NEPH('ne300', cfg, verbosity=0)
                ne300.setup_schedules()

                if s3fsc:
                    s3fsc.setup_transfer_schedules(
                        local_path=ne300.staging_path,
                        key_prefix=ne300.remote_path,
                        interval=ne300.reporting_interval,
                        remove_on_success=False,
                    )
                if sftp:
                    remote_path = (PurePosixPath(sftp.remote_path) / ne300.remote_path).as_posix()
                    sftp.setup_transfer_schedules(local_path=ne300.staging_path,
                                                  remote_path=remote_path,
                                                  interval=ne300.reporting_interval)

                schedule.every(fetch).seconds.do(run_threaded, ne300.print_ssp_bssp)

            # if cfg.get('fidas', None):
            #     from mkndaq.inst.fidas import FIDAS
            #     fidas = FIDAS(config=cfg)
            #     fidas.setup_schedules()

            #     if s3fsc:
            #         s3fsc.setup_transfer_schedules(
            #             local_path=fidas.staging_path,
            #             key_prefix=fidas.remote_path,
            #             interval=fidas.reporting_interval,
            #             remove_on_success=False,
            #         )
            #     if sftp:
            #         remote_path = (PurePosixPath(sftp.remote_path) / fidas.remote_path).as_posix()
            #         sftp.setup_transfer_schedules(local_path=fidas.staging_path,
            #                                       remote_path=remote_path,
            #                                       interval=fidas.reporting_interval)

            #     schedule.every(fetch).seconds.do(run_threaded, fidas.print_parsed_record)

        except Exception as err:
            logger.error(err)

        # list all jobs
        logger.info(schedule.get_jobs())

        # transfer most recent log file and define schedule
        logger.info(f"Staging current log file {logfile}")
        copy_file(source=logfile, target=staging, logger=logger)
        schedule.every(1).day.at('00:00').do(copy_file, source=logfile, target=staging, logger=logger)

        # # transfer any existing staged files
        # logger.info("Transfering existing staged files ... this could take a while")
        # sftp.transfer_files(local_path=staging, remote_path=sftp.remote_path)

        # align start with a multiple-of-minute timestamp
        seconds_left = seconds_to_next_n_minutes(1)
        while seconds_left > 0:
            print(f"Time remaining (s): {seconds_left:0.0f}", end="\r")
            dt = 0.1
            time.sleep(dt)
            seconds_left -= dt
        logger.info("Beginning data acquisition and file transfer ...")

        while True:
            schedule.run_pending()
            time.sleep(1)

    except Exception as err:
        logger.error(err)


if __name__ == '__main__':
    main()
