from __future__ import annotations

import argparse
import logging
import os
import threading
import time
from pathlib import Path, PurePosixPath

import colorama
import schedule
import yaml


def configure_logger(filename: str, loglevel_console: str = "INFO", loglevel_file: str = "INFO") -> logging.Logger:
    """Configure and return a logger instance."""
    log = logging.getLogger()
    log.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s, %(levelname)s, %(name)s, %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # file
    fh = logging.FileHandler(filename)
    fh.setLevel(getattr(logging, loglevel_file))
    fh.setFormatter(formatter)

    # console
    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, loglevel_console))
    ch.setFormatter(formatter)

    # Avoid duplicate handlers if main() called twice
    if not log.handlers:
        log.addHandler(fh)
        log.addHandler(ch)

    return log


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
        usage='python3 mkndaq.py|mkndaq.exe -c mkndaq.yml',
    )
    parser.add_argument('-c', '--config', default='./dist/mkndaq.yml', help='path to yml config file')
    args = parser.parse_args()

    # read config file and set up logging
    cfgfile = args.config
    with open(cfgfile, 'r') as fh:
        cfg = yaml.safe_load(fh)

    # configure logger
    logger = configure_logger(filename=cfg['logging']['file'],
                           loglevel_console=cfg['logging']['level_console'],
                           loglevel_file=cfg['logging']['level_file'])

    try:
        colorama.init(autoreset=True)

        # get version from setup.py
        version = 'vx.y.z'
        with open('setup.py', 'r') as fh:
            for line in fh:
                if 'version=' in line:
                    version = line.split('version=')[1].split(',')[0].strip().strip("'\"")
                    break

        logger.info(f"[mkndaq] started, version={version}, config={cfgfile}")

        # global settings
        fetch = cfg.get('print_interval_seconds', 20)

        # configure transfer clients (S3, SFTP)
        s3fsc = None
        sftp = None

        if cfg.get('s3', None):
            from mkndaq.utils.s3fsc import S3FSC
            s3fsc = S3FSC(config=cfg)
            logger.info("[s3] enabled")

        if cfg.get('sftp', None):
            from mkndaq.utils.sftp import SFTPClient
            sftp = SFTPClient(config=cfg)
            logger.info("[sftp] enabled")

        # initialize instruments, get and set configurations and define schedules
        try:
            if cfg.get('tei49c', None):
                from mkndaq.inst.thermo import Thermo49C
                tei49c = Thermo49C(name='tei49c', config=cfg)
                tei49c.setup_schedules()

                if s3fsc:
                    s3fsc.setup_transfer_schedules(
                        local_path=str(tei49c.staging_path),
                        key_prefix=tei49c.remote_path,
                        interval=tei49c.reporting_interval,
                        delay_transfer=2,
                        remove_on_success=False,
                    )
                if sftp:
                    remote_path_data = (PurePosixPath(sftp.remote_path) / tei49c.remote_path).as_posix()
                    sftp.setup_transfer_schedules(
                        local_path=str(tei49c.staging_path),
                        remote_path=remote_path_data,
                        interval=tei49c.reporting_interval,
                        delay_transfer=15,
                    )

                schedule.every(fetch).seconds.do(run_threaded, tei49c.print_o3)
                logger.info(f"[tei49c] setup complete")

            if cfg.get('tei49i', None):
                from mkndaq.inst.thermo import Thermo49i
                tei49i = Thermo49i(name='tei49i', config=cfg)
                tei49i.setup_schedules()

                schedule.every(fetch).seconds.do(run_threaded, tei49i.print_o3)
                schedule.every().day.at('00:05').do(run_threaded, tei49i.set_datetime)

                if s3fsc:
                    s3fsc.setup_transfer_schedules(
                        local_path=str(tei49i.staging_path),
                        key_prefix=tei49i.remote_path,
                        interval=tei49i.reporting_interval,
                        delay_transfer=2,
                        remove_on_success=False,
                    )
                if sftp:
                    remote_path = (PurePosixPath(sftp.remote_path) / tei49i.remote_path).as_posix()
                    sftp.setup_transfer_schedules(local_path=str(tei49i.staging_path),
                                                  remote_path=remote_path,
                                                  interval=tei49i.reporting_interval,
                                                  delay_transfer=15,
                                                  remove_on_success=True,
                                                  )
                logger.info(f"[tei49i] setup complete")

            if cfg.get('tei49i_2', None):
                from mkndaq.inst.thermo import Thermo49i
                tei49i_2 = Thermo49i(name='tei49i_2', config=cfg)
                tei49i_2.setup_schedules()

                if s3fsc:
                    s3fsc.setup_transfer_schedules(
                        local_path=str(tei49i_2.staging_path),
                        key_prefix=tei49i_2.remote_path,
                        interval=tei49i_2.reporting_interval,
                        remove_on_success=False,
                    )
                if sftp:
                    remote_path = (PurePosixPath(sftp.remote_path) / tei49i_2.remote_path).as_posix()
                    sftp.setup_transfer_schedules(local_path=str(tei49i_2.staging_path),
                                                  remote_path=remote_path,
                                                  interval=tei49i_2.reporting_interval)

                schedule.every().day.at('00:00').do(run_threaded, tei49i_2.set_datetime)
                schedule.every(fetch+5).seconds.do(run_threaded, tei49i_2.print_o3)

            if cfg.get('tei49c-ps', None):
                from mkndaq.inst.thermo import Thermo49CPS
                tei49cps = Thermo49CPS(name='tei49c-ps', config=cfg)
                tei49cps.get_config()
                tei49cps.set_config()
                tei49cps.setup_schedules()

                schedule.every(fetch).seconds.do(run_threaded, tei49cps.print_o3)
                schedule.every().day.at('00:03').do(run_threaded, tei49c.set_datetime)

                if s3fsc:
                    s3fsc.setup_transfer_schedules(
                        local_path=str(tei49cps.staging_path),
                        key_prefix=tei49cps.remote_path,
                        interval=tei49cps.reporting_interval,
                        delay_transfer=3,
                        remove_on_success=False,
                    )
                if sftp:
                    remote_path = (PurePosixPath(sftp.remote_path) / tei49cps.remote_path).as_posix()
                    sftp.setup_transfer_schedules(local_path=str(tei49cps.staging_path),
                                                  remote_path=remote_path,
                                                  interval=tei49cps.reporting_interval, 
                                                  delay_transfer=15,
                                                  remove_on_success=True,
                                                  )

            if cfg.get('g2401', None):
                from mkndaq.inst.g2401 import G2401
                g2401 = G2401('g2401', config=cfg)
                g2401.store_and_stage_files()

                schedule.every(fetch).seconds.do(run_threaded, g2401.print_co2_ch4_co)
                schedule.every(cfg['g2401']['reporting_interval']).minutes.do(run_threaded, g2401.store_and_stage_files)

                if s3fsc:
                    s3fsc.setup_transfer_schedules(
                        local_path=str(g2401.staging_path),
                        key_prefix=g2401.remote_path,
                        interval=g2401.reporting_interval,
                        delay_transfer=2,
                        remove_on_success=False,
                    )
                if sftp:
                    remote_path = (PurePosixPath(sftp.remote_path) / g2401.remote_path).as_posix()
                    sftp.setup_transfer_schedules(local_path=str(g2401.staging_path),
                                                  remote_path=remote_path,
                                                  interval=g2401.reporting_interval,
                                                  delay_transfer=15,
                                                  remove_on_success=True,
                                                  )
                logger.info(f"[g2401] setup complete")

            if cfg.get('meteo', None):
                from mkndaq.inst.meteo import METEO
                meteo = METEO('meteo', config=cfg)
                meteo.store_and_stage_files()

                schedule.every(cfg['meteo']['reporting_interval']).minutes.do(run_threaded, meteo.print_meteo)
                schedule.every(cfg['meteo']['reporting_interval']).minutes.do(run_threaded, meteo.store_and_stage_files)

                if s3fsc:
                    s3fsc.setup_transfer_schedules(
                        local_path=str(meteo.staging_path),
                        key_prefix=meteo.remote_path,
                        interval=meteo.reporting_interval,
                        delay_transfer=2,
                        remove_on_success=False,
                    )
                if sftp:
                    remote_path = (PurePosixPath(sftp.remote_path) / meteo.remote_path).as_posix()
                    sftp.setup_transfer_schedules(local_path=str(meteo.staging_path),
                                                  remote_path=remote_path,
                                                  interval=meteo.reporting_interval,
                                                  delay_transfer=15,
                                                  remove_on_success=True,
                                                  )
                logger.info(f"[meteo] setup complete")

            if cfg.get('ne300', None):
                from mkndaq.inst.neph import NEPH
                ne300 = NEPH(name='ne300', config=cfg)
                ne300.setup_schedules()

                if s3fsc:
                    s3fsc.setup_transfer_schedules(
                        local_path=str(ne300.staging_path),
                        key_prefix=ne300.remote_path,
                        interval=ne300.reporting_interval,
                        delay_transfer=2,
                        remove_on_success=False,
                    )
                if sftp:
                    remote_path_data = (PurePosixPath(sftp.remote_path) / ne300.remote_path).as_posix()
                    sftp.setup_transfer_schedules(
                        local_path=str(ne300.staging_path),
                        remote_path=remote_path_data,
                        interval=ne300.reporting_interval,
                        delay_transfer=15,
                    )

                schedule.every(fetch).seconds.do(run_threaded, ne300.print_ssp_bssp)
                logger.info(f"[ne300] setup complete")

            if cfg.get('ae33', None):
                from mkndaq.inst.ae33 import AE33
                ae33 = AE33(name='ae33', config=cfg)
                ae33.setup_schedules()

                if s3fsc:
                    s3fsc.setup_transfer_schedules(
                        local_path=str(ae33.staging_path_data),
                        key_prefix=ae33.remote_path_data,
                        interval=ae33.reporting_interval,
                        delay_transfer=2,
                        remove_on_success=False,
                    )
                    s3fsc.setup_transfer_schedules(
                        local_path=str(ae33.staging_path_logs),
                        key_prefix=ae33.remote_path_logs,
                        interval=ae33.reporting_interval,
                        delay_transfer=2,
                        remove_on_success=False,
                    )

                if sftp:
                    remote_path_data = (PurePosixPath(sftp.remote_path) / ae33.remote_path_data).as_posix()
                    remote_path_logs = (PurePosixPath(sftp.remote_path) / ae33.remote_path_logs).as_posix()
                    sftp.setup_transfer_schedules(
                        local_path=str(ae33.staging_path_data),
                        remote_path=remote_path_data,
                        interval=ae33.reporting_interval,
                        delay_transfer=15,
                    )
                    sftp.setup_transfer_schedules(
                        local_path=str(ae33.staging_path_logs),
                        remote_path=remote_path_logs,
                        interval=ae33.reporting_interval,
                        delay_transfer=15,
                    )

                schedule.every(fetch).seconds.do(run_threaded, ae33.print_ae33)
                logger.info(f"[ae33] setup complete")

            # --- HMP110 (RS-485 daisy chain on shared COM port) ---
            # The two probes share the same COM port and are addressed by their individual IDs.
            # We (a) share the underlying serial port in HMP110ASCII, (b) serialize I/O with a per-port lock,
            # and (c) stagger acquisition/save schedules by 1s/2s so both sensors get polled reliably.
            hmp110_sensors = []

            if cfg.get('hmp110-inlet', None):
                from mkndaq.inst.vaisala import HMP110ASCII
                hmp110_inlet = HMP110ASCII(name='hmp110-inlet', config=cfg)
                hmp110_inlet.setup_schedules(delay_job=1)
                hmp110_sensors.append(hmp110_inlet)

                if s3fsc:
                    s3fsc.setup_transfer_schedules(
                        local_path=str(hmp110_inlet.staging_path),
                        key_prefix=hmp110_inlet.remote_path,
                        interval=hmp110_inlet.reporting_interval,
                        delay_transfer=2,
                        remove_on_success=False,
                    )
                if sftp:
                    remote_path_data = (PurePosixPath(sftp.remote_path) / hmp110_inlet.remote_path).as_posix()
                    sftp.setup_transfer_schedules(
                        local_path=str(hmp110_inlet.staging_path),
                        remote_path=remote_path_data,
                        interval=hmp110_inlet.reporting_interval,
                        delay_transfer=15,
                    )
                logger.info(f"[hmp110-inlet] setup complete")

            if cfg.get('hmp110-ae33', None):
                from mkndaq.inst.vaisala import HMP110ASCII
                hmp110_ae33 = HMP110ASCII(name='hmp110-ae33', config=cfg)
                hmp110_ae33.setup_schedules(delay_job=2)
                hmp110_sensors.append(hmp110_ae33)

                if s3fsc:
                    s3fsc.setup_transfer_schedules(
                        local_path=str(hmp110_ae33.staging_path),
                        key_prefix=hmp110_ae33.remote_path,
                        interval=hmp110_ae33.reporting_interval,
                        delay_transfer=2,
                        remove_on_success=False,
                    )
                if sftp:
                    remote_path_data = (PurePosixPath(sftp.remote_path) / hmp110_ae33.remote_path).as_posix()
                    sftp.setup_transfer_schedules(
                        local_path=str(hmp110_ae33.staging_path),
                        remote_path=remote_path_data,
                        interval=hmp110_ae33.reporting_interval,
                        delay_transfer=15,
                    )
                logger.info(f"[hmp110-ae33] setup complete")

            # Regular console readout (single threaded job; serial access stays sequential)
            if hmp110_sensors:
                def print_all_hmp110():
                    for inst in hmp110_sensors:
                        inst.print_readings()

                schedule.every(fetch).seconds.do(run_threaded, print_all_hmp110)

            if cfg.get('tapo', None):
                from mkndaq.inst.tapo import Tapo

                tapo = Tapo(name='tapo', config=cfg)

                # Take one snapshot every snapshot_interval_seconds (from mkndaq.yml)
                schedule.every(tapo.snapshot_interval_seconds).seconds.do(
                    run_threaded, tapo.capture_snapshot
                )

                # File transfer of staged JPEGs (uses reporting_interval_minutes)
                if s3fsc:
                    s3fsc.setup_transfer_schedules(
                        local_path=str(tapo.staging_path),
                        key_prefix=tapo.remote_path,
                        interval=tapo.reporting_interval,
                        delay_transfer=2,
                        remove_on_success=False,
                    )
                if sftp:
                    remote_path_data = (PurePosixPath(sftp.remote_path) / tapo.remote_path).as_posix()
                    sftp.setup_transfer_schedules(
                        local_path=str(tapo.staging_path),
                        remote_path=remote_path_data,
                        interval=tapo.reporting_interval,
                        delay_transfer=15,
                    )

                logger.info(f"[tapo] setup complete")

        except Exception as err:
            logger.exception(err)

        # infinite schedule loop
        while True:
            schedule.run_pending()
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("[mkndaq] stopped by user")
    except Exception as err:
        logger.exception(err)


if __name__ == '__main__':
    main()