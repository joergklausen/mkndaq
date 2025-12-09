# -*- coding: utf-8 -*-
"""
Define a class TEI49I facilitating communication with a Thermo 49C and 49i instruments.

@author: joerg.klausen@meteoswiss.ch
"""
import functools
import logging
import os
import socket
import threading
import time
import zipfile
from datetime import datetime

import colorama
import schedule
import serial


# def with_serial(func):
#     @functools.wraps(func)
#     def wrapper(self, *args, retries: int = 3, **kwargs) -> str:
#         # cooldown gate
#         now = time.time()
#         if getattr(self, "_cooldown_until", 0.0) > now:
#             return ""

#         # non-overlapping I/O
#         if not self._io_lock.acquire(blocking=False):
#             return ""

#         try:
#             last_err = None
#             for i in range(retries):
#                 try:
#                     if not self._serial.is_open:
#                         self._serial.open()
#                     # one attempt: protocol-specific code
#                     result = func(self, *args, **kwargs)
#                     # success
#                     self._fail_count = 0
#                     self._cooldown_until = 0.0
#                     return result
#                 except (serial.SerialTimeoutException, serial.SerialException, OSError) as err:
#                     last_err = err
#                     self.logger.error(
#                         f"[{self.name}] serial_comm attempt {i+1}/{retries} failed: {err}"
#                     )
#                     try:
#                         if self._serial.is_open:
#                             self._serial.close()
#                     except Exception:
#                         pass

#                     self._fail_count = getattr(self, "_fail_count", 0) + 1
#                     max_fail = getattr(self, "_max_fail_before_cooldown", 5)
#                     cooldown = getattr(self, "_cooldown_seconds", 120)
#                     if self._fail_count >= max_fail:
#                         self._cooldown_until = time.time() + cooldown
#                         self.logger.error(
#                             f"[{self.name}] communication failing repeatedly; "
#                             f"backing off for {cooldown}s."
#                         )
#                         break

#                     time.sleep(min(0.5 * (2 ** i), 3.0))

#             # all retries failed
#             return ""
#         finally:
#             self._io_lock.release()
#     return wrapper
def with_serial(func):
    @functools.wraps(func)
    def wrapper(self, cmd: str, *args, retries: int = 3, **kwargs) -> str:
        # cooldown gate
        now = time.time()
        if getattr(self, "_cooldown_until", 0.0) > now:
            return ""

        # non-overlapping I/O
        if not self._io_lock.acquire(blocking=False):
            return ""

        try:
            last_err: Exception | None = None
            for i in range(retries):
                try:
                    if not self._serial.is_open:
                        self._serial.open()

                    resp = func(self, cmd, *args, **kwargs)

                    if resp:
                        # success
                        self._fail_count = 0
                        self._cooldown_until = 0.0
                        return resp

                    # Empty / incomplete response -> treat like a soft timeout
                    if i < retries - 1:
                        self.logger.debug(
                            f"[{self.name}] serial_comm attempt {i+1}/{retries} "
                            f"returned empty for {cmd!r}; retrying..."
                        )
                    else:
                        self.logger.error(
                            f"[{self.name}] serial_comm empty response after "
                            f"{retries} attempts for {cmd!r}"
                        )

                except (serial.SerialTimeoutException, serial.SerialException, OSError) as err:
                    last_err = err
                    # Only escalate to ERROR on the last attempt; warn before that
                    level = logging.WARNING if i < retries - 1 else logging.ERROR
                    self.logger.log(
                        level,
                        f"[{self.name}] serial_comm attempt {i+1}/{retries} failed for {cmd!r}: {err}",
                    )
                    try:
                        if self._serial.is_open:
                            self._serial.close()
                    except Exception:
                        pass

                # Backoff before next try
                time.sleep(min(0.5 * (2 ** i), 3.0))

            # All attempts failed or empty
            self._fail_count = getattr(self, "_fail_count", 0) + 1
            max_fail = getattr(self, "_max_fail_before_cooldown", 5)
            cooldown = getattr(self, "_cooldown_seconds", 120)
            if self._fail_count >= max_fail:
                self._cooldown_until = time.time() + cooldown
                self.logger.error(
                    f"[{self.name}] communication failing repeatedly; "
                    f"backing off for {cooldown}s."
                )
            return ""
        finally:
            self._io_lock.release()

    return wrapper


class Thermo49C:
    """
    Instrument of type Thermo TEI 49C with methods, attributes for interaction.
    """
    def __init__(self, name: str, config: dict) -> None:
        """
        Initialize instrument class.

        :param name: name of instrument
        :param config: dictionary of attributes defining the instrument, serial port and other information
            - config[name]['type']
            - config[name]['id']
            - config[name]['serial_number']
            - config[name]['get_config']
            - config[name]['set_config']
            - config[name]['data_header']
            - config[name]['port']
            - config[port]['baudrate']
            - config[port]['bytesize']
            - config[port]['parity']
            - config[port]['stopbits']
            - config[port]['timeout']
            - config[name]['sampling_interval']
            - config['reporting_interval']
            - config['data']
            - config['staging']['path']
            - config['staging']['zip']
        """
        colorama.init(autoreset=True)

        try:
            self.name = name
            self.serial_number = config[name]['serial_number']

            # configure logging
            _logger = f"{os.path.basename(config['logging']['file'])}".split('.')[0]
            self.logger = logging.getLogger(f"{_logger}.{__name__}")
            self.logger.info(f"[{self.name}] Initializing TEI49C (S/N: {self.serial_number})")

            # read instrument control properties for later use
            self._id = config[name]['id'] + 128
            self._get_config = config[name]['get_config']
            self._set_config = config[name]['set_config']
            self._data_header = config[name]['data_header']

            # configure serial port and open it
            port = config[name]['port']
            try:
                self._io_lock = threading.Lock()
                self._serial = serial.Serial(
                    port=port,
                    baudrate=config[port]['baudrate'],
                    bytesize=config[port]['bytesize'],
                    parity=config[port]['parity'],
                    stopbits=config[port]['stopbits'],
                    timeout=config[port]['timeout'],
                    write_timeout=config[port].get('write_timeout', 2.0),
                )
                # track repeated communication failures and back off if necessary
                self._fail_count = 0
                self._max_fail_before_cooldown = 5   # consecutive failing commands
                self._cooldown_seconds = 120         # pause 2 minutes after repeated failures
                self._cooldown_until = 0.0           # unix timestamp until which we stay quiet

            except serial.SerialException as err:
                self.logger.error(f"__init__ produced SerialException {err}")
                pass
            # sampling, aggregation, reporting/storage
            self.sampling_interval = config[name]['sampling_interval']
            self.reporting_interval = config[name]['reporting_interval']
            if not (self.reporting_interval==10 or (self.reporting_interval % 60)==0) and self.reporting_interval<=1440:
                raise ValueError(f"[{self.name}] reporting_interval must be 10 or a multiple of 60 and less or equal to 1440 minutes.")

            # configure saving, staging and archiving
            root = os.path.expanduser(config['root'])
            self.data_path = os.path.join(root, config['data'], config[name]['data_path'])
            self.staging_path = os.path.join(root, config['staging'], config[name]['staging_path'])
            # self.archive_path = os.path.join(root, config[name]['archive'])
            self._file_to_stage = ""
            self._zip = config[name]['staging_zip']

            # configure remote transfer
            self.remote_path = config[name]['remote_path']

            # initialize data response
            self._data = ""

            # self.get_config()
            # self.set_config()

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")


    def setup_schedules(self, delay_job: int=1):
        try:
            # configure folders needed
            os.makedirs(self.data_path, exist_ok=True)
            os.makedirs(self.staging_path, exist_ok=True)

            # configure data acquisition schedule
            schedule.every(self.sampling_interval).minutes.at(':00').do(self.accumulate_lrec)

            # configure saving and staging schedules
            if self.reporting_interval == 10:
                self._file_timestamp_format = '%Y%m%d%H'
                for minute in (0, 10, 20, 30, 40, 50):
                    schedule.every(1).hours.at(f"{minute:02d}:{delay_job:02d}").do(self._save_and_stage_data)
            elif (self.reporting_interval % 60) == 0 and self.reporting_interval < 1440:
                self._file_timestamp_format = '%Y%m%d'
                hours = self.reporting_interval // 60
                schedule.every(hours).hours.at(f"00:{delay_job:02d}").do(self._save_and_stage_data)
            elif self.reporting_interval == 1440:
                schedule.every().day.at(f"00:00:{delay_job:02d}").do(self._save_and_stage_data)
            else:
                raise ValueError("'reporting_interval' must be 10 minutes, a multiple of 60 minutes (<1440), or 1440.")

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")


    @with_serial
    def serial_comm(self, cmd: str) -> str:
        _id = bytes([self._id])

        # Clear stale buffers once per call
        self._serial.reset_input_buffer()
        self._serial.reset_output_buffer()

        # Send command
        self._serial.write(_id + (f"{cmd}\r").encode())
        self._serial.flush()
        # Optional: tiny think-time for the 49C; safe to keep small
        # time.sleep(0.05)

        rcvd = b""
        timeout = self._serial.timeout or 1.0
        # Allow a bit more than the per-byte timeout in total
        deadline = time.monotonic() + max(2 * timeout, 2.0)

        while time.monotonic() < deadline:
            # Always try to read; let pyserial's timeout handle blocking
            chunk = self._serial.read(1024)
            if chunk:
                rcvd += chunk
                # Most TEI replies end with CR and may optionally have a '*checksum'
                if b"*" in rcvd or rcvd.endswith(b"\r"):
                    break
            else:
                # No bytes this iteration -> loop again until deadline
                # (pyserial has already waited up to `timeout` for us)
                pass

        text = (
            rcvd.decode(errors="ignore")
            .split("*")[0]  # drop checksum if present
            .replace(cmd, "")
            .strip()
        )
        # Important: don't raise here – let the wrapper decide how to treat ""
        return text


    def get_config(self) -> list:
        """
        Read current configuration of instrument and optionally write to log.

        :return current configuration of instrument

        """
        cfg = []
        try:
            for cmd in self._get_config:
                cfg.append(self.serial_comm(cmd))
            self.logger.info(f"[{self.name}] Configuration is: {cfg}")

            return cfg

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")
            return list()


    def set_datetime(self) -> None:
        """
        Synchronize date and time of instrument with computer time. Assumes an open connection.

        :return:
        """
        try:
            dte = self.serial_comm(f"set date {time.strftime('%m-%d-%y')}")
            dte = self.serial_comm("date")
            tme = self.serial_comm(f"set time {time.strftime('%H:%M')}")
            tme = self.serial_comm("time")
            self.logger.info(f"[{self.name}] Date and time set and reported as: {dte} {tme}")
        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")


    def set_config(self) -> list:
        """
        Set configuration of instrument and optionally write to log.

        :return new configuration as returned from instrument
        """
        self.logger.info(f"[{self.name}] .set_config")
        cfg = []
        try:
            self.set_datetime()
            for cmd in self._set_config:
                cfg.append(f"{cmd}: {self.serial_comm(cmd)}")
            time.sleep(1)

            self.logger.info(f"[{self.name}] Configuration set to: {cfg}")

            return cfg

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")
            return list()


    def accumulate_lrec(self) -> None:
        """Send lrec, append response to buffer, respecting cooldown.

        Locking, retries and cooldown are handled by @with_serial on serial_comm().
        """
        # If we recently saw repeated failures, stay quiet for a while.
        if getattr(self, "_cooldown_until", 0.0) and time.time() < self._cooldown_until:
            return

        try:
            dtm = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            lrec = self.serial_comm("lrec")  # @with_serial handles locking
            if not lrec:
                return  # no data, nothing to append

            self._data += f"{dtm} {lrec}\n"
            self.logger.debug(f"[{self.name}] {lrec[:60]}[...]")
        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")


    def _save_data(self) -> None:
        try:
            # data_file = ""
            # self.data_file = ""
            if self._data:
                # create appropriate file name and write mode
                now = datetime.now()
                timestamp = now.strftime(self._file_timestamp_format)
                yyyy = now.strftime('%Y')
                mm = now.strftime('%m')
                dd = now.strftime('%d')
                self.data_file = os.path.join(self.data_path, yyyy, mm, dd, f"{self.name}-{timestamp}.dat")
                os.makedirs(os.path.dirname(self.data_file), exist_ok=True)

                # configure file mode, open file and write to it
                if os.path.exists(self.data_file):
                    mode = 'a'
                    header = ""
                else:
                    mode = 'w'
                    header = f"{self._data_header}\n"

                with open(file=self.data_file, mode=mode) as fh:
                    fh.write(header)
                    fh.write(self._data)
                    self.logger.info(f"[{self.name}] file saved: {self.data_file}")

                # reset self._data
                self._data = ""

            return

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")


    def _stage_file(self):
        """ Create zip file from self.data_file and stage archive.
        """
        try:
            if self.data_file:
                archive = os.path.join(self.staging_path, os.path.basename(self.data_file).replace('.dat', '.zip'))
                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.write(self.data_file, os.path.basename(self.data_file))
                    self.logger.info(f"file staged: {archive}")

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")


    def _save_and_stage_data(self):
        self._save_data()
        self._stage_file()


    def get_o3(self) -> str:
        try:
            return self.serial_comm('O3')

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")
            return ""


    def print_o3(self) -> None:
        """Log a one-shot O3 readout.

        Locking and retries are handled by @with_serial on serial_comm().
        """
        # don't hammer the port while in cooldown
        if getattr(self, "_cooldown_until", 0.0) and time.time() < self._cooldown_until:
            return

        try:
            o3 = self.get_o3().split()
            if len(o3) == 2:
                self.logger.info(
                    colorama.Fore.GREEN + f"[{self.name}] O3 {float(o3[0]):0.1f} {o3[1]}"
                )
            elif len(o3) == 3:
                self.logger.info(
                    colorama.Fore.GREEN + f"[{self.name}] {o3[0].upper()} {float(o3[1]):0.1f} {o3[2]}"
                )
        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"[{self.name}] print_o3: {err}")


    def get_all_rec(self, capacity=[1790, 4096], save=True) -> str:
        """
        Retrieve all long and short records from instrument and optionally write to file.

        :param bln save: Should data be saved to file? default=True
        :return str response as decoded string
        """
        try:
            data = ""
            data_file = ""

            # lrec and srec capacity of logger
            CMD = ["lrec", "srec"]
            CAPACITY = capacity

            self.logger.info(f"[{self.name}] .get_all_rec (save={save})")

            # retrieve data from instrument
            for i in [0, 1]:
                index = CAPACITY[i]
                retrieve = 50 #10
                if save:
                    # generate the datafile name
                    dtm = time.strftime('%Y%m%d%H%M%S')
                    data_file = os.path.join(self.data_path,
                                            f"{self.name}-all-{CMD[i]}-{dtm}.dat")

                while index > 0:
                    if index < retrieve:
                        retrieve = index
                    cmd = f"{CMD[i]} {str(index)} {str(retrieve)}"
                    self.logger.info(cmd)
                    data += f"{self.serial_comm(cmd)}\n"

                    index = index - retrieve

                if save:
                    if not os.path.exists(data_file):
                        # if file doesn't exist, create and write header
                        with open(data_file, "at") as fh:
                            fh.write(f"{self._data_header}\n")
                            fh.close()
                    with open(data_file, "at") as fh:
                        # add data to file
                        fh.write(f"{data}\n")
                        fh.close()

                    # create zip file
                    archive = os.path.join(data_file.replace(".dat", ".zip"))
                    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                        fh.write(data_file, os.path.basename(data_file))

            return data_file

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")
            return ""


class Thermo49i:
    def __init__(self, config: dict, name: str='49i'):
        """
        Initialize the Thermo 49i instrument class with parameters from a configuration file.

        Args:
            config (dict): general configuration
        """
        colorama.init(autoreset=True)

        try:
            self.name = name
            self.serial_number = config[name]['serial_number']

            # configure logging
            _logger = f"{os.path.basename(config['logging']['file'])}".split('.')[0]
            self.logger = logging.getLogger(f"{_logger}.{__name__}")
            self.logger.info(f"[{self.name}] Initializing Thermo 49i (S/N: {self.serial_number})")

            # read instrument control properties for later use
            self._id = config[name]['id'] + 128
            self._get_config = config[name]['get_config']
            self._set_config = config[name]['set_config']

            self._serial_com = config.get(name, {}).get('serial', None)
            self._io_lock = threading.Lock()
            if self._serial_com:
                # configure serial port
                port = config[name]['port']
                self._serial = serial.Serial(
                    port=port,
                    baudrate=config[port]['baudrate'],
                    bytesize=config[port]['bytesize'],
                    parity=config[port]['parity'],
                    stopbits=config[port]['stopbits'],
                    timeout=config[port]['timeout'],
                    write_timeout=config[port].get('write_timeout', 2.0),
                )
                self.logger.info(f"Serial port {port} successfully opened.")
            else:
                # configure tcp/ip
                self._sockaddr = (config[name]['socket']['host'],
                                config[name]['socket']['port'])
                self._socktout = config[name]['socket']['timeout']
                self._socksleep = config[name]['socket']['sleep']

            # configure data collection and reporting
            self.sampling_interval = config[name]['sampling_interval']
            self.reporting_interval = config[name]['reporting_interval']
            if not (self.reporting_interval==10 or (self.reporting_interval % 60)==0) and self.reporting_interval<=1440:
                raise ValueError(f"[{self.name}] reporting_interval must be 10 or a multiple of 60 and less or equal to 1440 minutes.")

            # configure saving, staging and archiving
            root = os.path.expanduser(config['root'])
            self.data_path = os.path.join(root, config['data'], config[name]['data_path'])
            self.staging_path = os.path.join(root, config['staging'], config[name]['staging_path'])
            # self.archive_path = os.path.join(root, config[name]['archive'])

            # configure remote transfer
            self.remote_path = config[name]['remote_path']

            # initialize data response
            self._data = ""

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")


    def setup_schedules(self):
        try:
            # configure folders needed
            os.makedirs(self.data_path, exist_ok=True)
            os.makedirs(self.staging_path, exist_ok=True)
            # os.makedirs(self.archive_path, exist_ok=True)

            # configure data acquisition schedule
            schedule.every(int(self.sampling_interval)).minutes.at(':00').do(self.accumulate_lr00)

            # configure saving and staging schedules
            if self.reporting_interval==10:
                self._file_timestamp_format = '%Y%m%d%H%M'
                minutes = [f"{self.reporting_interval*n:02}" for n in range(6) if self.reporting_interval*n < 60]
                for minute in minutes:
                    schedule.every().hour.at(f"{minute}:01").do(self._save_and_stage_data)
            elif self.reporting_interval==60:
                self._file_timestamp_format = '%Y%m%d%H'
                schedule.every().hour.at('00:01').do(self._save_and_stage_data)
            elif self.reporting_interval==1440:
                self._file_timestamp_format = '%Y%m%d'
                schedule.every().day.at('00:00:01').do(self._save_and_stage_data)
            else:
                raise ValueError(f"[{self.name}] unsupported reporting_interval.")

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")


    def tcpip_comm(self, cmd: str) -> str:
        """
        Send a command and retrieve the response. Assumes an open connection.

        :param cmd: command sent to instrument
        :return: response of instrument, decoded
        """
        _id = bytes([self._id])
        rcvd = b''
        try:
            # open socket connection as a client
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM, ) as s:
                # connect to the server
                s.settimeout(self._socktout)
                s.connect(self._sockaddr)

                # send data
                s.sendall(_id + (f"{cmd}\x0D").encode())
                time.sleep(self._socksleep)

                # receive response
                while True:
                    data = s.recv(1024)
                    rcvd = rcvd + data
                    if b'\x0D' in data:
                        break

            rcvd = rcvd.decode()
            # remove checksum after and including the '*'
            rcvd = rcvd.split("*")[0]
            # remove echo before and including '\n'
            # rcvd = rcvd.replace(f"{cmd}\n", "")
            rcvd = rcvd.replace(cmd, "").strip()

            return rcvd

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")
            return ""


    # def serial_comm(self, cmd: str, tidy=True) -> str:
    #     """
    #     Send a command and retrieve the response. Assumes an open connection.

    #     :param cmd: command sent to instrument
    #     :param tidy: remove echo and checksum after '*'
    #     :return: response of instrument, decoded
    #     """
    #     __id = bytes([self._id])
    #     rcvd = b''
    #     try:
    #         self._serial.write(__id + (f"{cmd}\x0D").encode())
    #         time.sleep(0.5)

    #         # test if this improves stability
    #         self._serial.flush()
    #         # end test

    #         deadline = time.monotonic() + max(self._serial.timeout or 1.0, 1.0)

    #         # while (self._serial.in_waiting > 0):
    #         while (self._serial.in_waiting > 0) and (time.monotonic() < deadline):
    #             rcvd = rcvd + self._serial.read(1024)
    #             time.sleep(0.1)

    #         rcvd = rcvd.decode()
    #         # remove checksum after and including the '*'
    #         rcvd = rcvd.split("*")[0]
    #         # remove echo before and including '\n'
    #         rcvd = rcvd.replace(cmd, "").strip()

    #         return rcvd

    #     except Exception as err:
    #         self.logger.error(f"[{self.name}] {err}")
    #         return ""
    def serial_comm(self, cmd: str, retries: int = 3) -> str:
        _id = bytes([self._id])
        for i in range(retries):
            try:
                if not self._serial.is_open:
                    self._serial.open()
                # clear stale buffers instead of flushing after write
                self._serial.reset_input_buffer()
                self._serial.reset_output_buffer()

                self._serial.write(_id + (f"{cmd}\r").encode())  # uses write_timeout
                rcvd = b""
                deadline = time.monotonic() + max(self._serial.timeout or 1.0, 1.0)
                while time.monotonic() < deadline:
                    if self._serial.in_waiting:
                        rcvd += self._serial.read(self._serial.in_waiting)
                        if b"*" in rcvd or rcvd.endswith(b"\r"):
                            break
                    time.sleep(0.05)

                text = rcvd.decode(errors="ignore").split("*")[0].replace(cmd, "").strip()
                if text:
                    return text
                raise serial.SerialTimeoutException("empty response")
            except (serial.SerialTimeoutException, serial.SerialException) as err:
                self.logger.error(f"[{self.name}] serial_comm attempt {i+1}/{retries} failed: {err}")
                try: self._serial.close()
                except Exception: pass
                time.sleep(min(0.5 * (2 ** i), 3.0))
        return ""


    def send_command(self, cmd: str) -> str:
        try:
            if self._serial_com:
                if not self._serial.is_open:
                    self._serial.open()
                response = self.serial_comm(cmd)
                self._serial.close()
            else:
                response = self.tcpip_comm(cmd)
            return response
        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"{err}")
            return ""


    def get_config(self) -> list:
        """
        Read current configuration of instrument and optionally write to log.

        :return (err, cfg) configuration or errors, if any.

        """
        cfg = []
        try:
            for cmd in self._get_config:
                if self._serial_com:
                    if not self._serial.is_open:
                        self._serial.open()
                    cfg.append(self.serial_comm(cmd))
                    self._serial.close()
                else:
                    cfg.append(self.tcpip_comm(cmd))

            self.logger.info(f"[{self.name}] Configuration read as: {cfg}")

            return cfg

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")
            return list()


    def set_datetime(self) -> None:
        """
        Synchronize date and time of instrument with computer time.

        :return:
        """
        try:
            cmd = f"set date {time.strftime('%m-%d-%y')}"
            if self._serial_com:
                if not self._serial.is_open:
                    self._serial.open()
                dte = self.serial_comm(cmd)
                self._serial.close()
            else:
                dte = self.tcpip_comm(cmd)
            self.logger.info(f"[{self.name}] Date set to: {dte}")

            cmd = f"set time {time.strftime('%H:%M:%S')}"
            if self._serial_com:
                if not self._serial.is_open:
                    self._serial.open()
                tme = self.serial_comm(cmd)
                self._serial.close()
            else:
                tme = self.tcpip_comm(cmd)
            self.logger.info(f"[{self.name}] Time set to: {tme}")

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")


    def set_config(self) -> list:
        """
        Set configuration of instrument and optionally write to log.

        :return (err, cfg) configuration set or errors, if any.
        """
        print("%s .set_config (name=%s)" % (time.strftime('%Y-%m-%d %H:%M:%S'), self.name))
        cfg = []
        try:
            for cmd in self._set_config:
                if self._serial_com:
                    cfg.append(f"{cmd}: {self.serial_comm(cmd)}")
                else:
                    cfg.append(f"{cmd}: {self.tcpip_comm(cmd)}")
                time.sleep(1)

            self.logger.info(f"[{self.name}] Configuration set to: {cfg}")

            return cfg

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")
            return list()


    # def accumulate_lr00(self):
    #     """
    #     Send command, retrieve response from instrument and append to self._data.
    #     """
    #     try:
    #         dtm = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #         if self._serial_com:
    #             _ = self.serial_comm('lr00')
    #         else:
    #             _ = self.tcpip_comm('lr00')
    #         self._data += f"{dtm} {_}\n"
    #         self.logger.debug(f"[{self.name}] {_[:60]}[...]")

    #         return

    #     except Exception as err:
    #         self.logger.error(f"[{self.name}] {err}")
    def accumulate_lr00(self) -> None:
        """Send lr00 command, append response to buffer; non-blocking lock."""
        # Try to grab lock; return immediately if another job is using the IO channel.
        if not self._io_lock.acquire(blocking=False):
            return

        try:
            dtm = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            _ = self.serial_comm('lr00') if self._serial_com else self.tcpip_comm('lr00')
            self._data += f"{dtm} {_}\n"
            self.logger.debug(f"[{self.name}] {_[:60]}[...]")
        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")
        finally:
            self._io_lock.release()

    def get_all_lrec(self, save: bool=True) -> str:
        """download entire buffer from instrument and save to file

        :param bln save: Should data be saved to file? default=True
        :return str response as decoded string
        """
        try:
            data = ""
            file = ""

            # retrieve numbers of lrec stored in buffer
            cmd = "no of lrec"
            if self._serial_com:
                no_of_lrec = int(self.serial_comm(cmd).split()[0])
            else:
                no_of_lrec = int(self.tcpip_comm(cmd).split()[0])
            # no_of_lrec = int(re.findall(r"(\d+)", no_of_lrec)[0])

            if save:
                # generate the datafile name
                dtm = datetime.now().strftime('%Y%m%d%H%M%S')
                file = os.path.join(self.data_path,
                                    f"{self.name}_all_lrec-{dtm}.dat")

            # get current lrec format, then set lrec format
            if self._serial_com:
                lrec_format = self.serial_comm('lrec format')
                _ = self.serial_comm('set lrec format 0')
            else:
                lrec_format = self.tcpip_comm('lrec format')
                _ = self.tcpip_comm('set lrec format 0')
            if not 'ok' in _:
                self.logger.warning(f"{cmd} returned '{_}' instead of 'ok'.")

            # retrieve all lrec records stored in buffer
            index = no_of_lrec
            retrieve = 100

            while index > 0:
                if index < retrieve:
                    retrieve = index
                cmd = f"lrec {str(index)} {str(retrieve)}"
                self.logger.info(cmd)
                if self._serial_com:
                    data += f"{self.serial_comm(cmd)}\n"
                else:
                    data += f"{self.tcpip_comm(cmd)}\n"

                # remove all the extra info in the string returned
                # 05:26 07-19-22 flags 0C100400 o3 30.781 hio3 0.000 cellai 50927 cellbi 51732 bncht 29.9 lmpt 53.1 o3lt 0.0 flowa 0.435 flowb 0.000 pres 493.7
                # data = data.replace("flags ", "")
                # data = data.replace("hio3 ", "")
                # data = data.replace("cellai ", "")
                # data = data.replace("cellbi ", "")
                # data = data.replace("bncht ", "")
                # data = data.replace("lmpt ", "")
                # data = data.replace("o3lt ", "")
                # data = data.replace("flowa ", "")
                # data = data.replace("flowb ", "")
                # data = data.replace("pres ", "")
                # data = data.replace("o3 ", "")

                index = index - retrieve

            if save:
                # write .dat file
                with open(file, "at", encoding='utf8') as fh:
                    fh.write(f"{data}\n")
                    fh.close()

                # create zip file
                archive = os.path.join(file.replace(".dat", ".zip"))
                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                    fh.write(file, os.path.basename(file))

            # restore lrec format
            if self._serial_com:
                _ = self.serial_comm(f'set {lrec_format}')
            else:
                _ = self.tcpip_comm(f'set {lrec_format}')

            return data

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")
            return ""


    def get_o3(self) -> str:
        try:
            if self._serial_com:
                return self.serial_comm('o3')
            else:
                return self.tcpip_comm('o3')

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")
            return ""


    # def print_o3(self) -> None:
    #     try:
    #         o3 = self.get_o3().split()
    #         if len(o3)==2:
    #             self.logger.info(colorama.Fore.GREEN + f"[{self.name}] O3 {float(o3[0]):0.1f} {o3[1]}")
    #         if len(o3)==3:
    #             self.logger.info(colorama.Fore.GREEN + f"[{self.name}] {o3[0].upper()}  {float(o3[1]):0.1f} {o3[2]}")

    #     except Exception as err:
    #         self.logger.error(colorama.Fore.RED + f"[{self.name}] print_o3: {err}")
    def print_o3(self) -> None:
        acquired = self._io_lock.acquire(blocking=False)
        if not acquired:
            return
        try:
            o3 = self.get_o3().split()
            if len(o3) == 2:
                self.logger.info(colorama.Fore.GREEN + f"[{self.name}] O3 {float(o3[0]):0.1f} {o3[1]}")
            elif len(o3) == 3:
                self.logger.info(colorama.Fore.GREEN + f"[{self.name}] {o3[0].upper()} {float(o3[1]):0.1f} {o3[2]}")
        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"[{self.name}] print_o3: {err}")
        finally:
            if acquired:
                self._io_lock.release()

    # def _save_data(self) -> None:
    #     try:
    #         # data_file = ""
    #         self.data_file = ""
    #         if self._data:
    #             # create appropriate file name and write mode
    #             now = datetime.now()
    #             timestamp = now.strftime(self._file_timestamp_format)
    #             yyyy, mm, dd = now.strftime('%Y'), now.strftime('%m'), now.strftime('%d')
    #             data_file = os.path.join(self.data_path, yyyy, mm, dd, f"{self.name}-{timestamp}.dat")
    #             os.makedirs(os.path.dirname(self.data_file), exist_ok=True)

    #             # configure file mode, open file and write to it
    #             if os.path.exists(self.data_file):
    #                 mode = 'a'
    #                 header = ""
    #             else:
    #                 mode = 'w'
    #                 header = 'pcdate pctime time date flags o3 hio3 cellai cellbi bncht lmpt o3lt flowa flowb pres\n'

    #             with open(file=data_file, mode=mode) as fh:
    #                 fh.write(header)
    #                 fh.write(self._data)
    #                 self.logger.info(f"[{self.name}] file saved: {self.data_file}")

    #             # reset self._data
    #             self._data = ""

    #         # self.data_file = data_file
    #         return

    #     except Exception as err:
    #         self.logger.error(f"[{self.name}] {err}")
    def _save_data(self) -> None:
        """Write accumulated data to a .dat file and clear the buffer."""
        try:
            if self._data:
                # create appropriate file name and write mode
                now = datetime.now()
                timestamp = now.strftime(self._file_timestamp_format)
                yyyy = now.strftime('%Y')
                mm = now.strftime('%m')
                dd = now.strftime('%d')

                # store on the instance so _stage_file can use it
                self.data_file = os.path.join(
                    self.data_path, yyyy, mm, dd, f"{self.name}-{timestamp}.dat"
                )
                os.makedirs(os.path.dirname(self.data_file), exist_ok=True)

                # configure file mode and header
                if os.path.exists(self.data_file):
                    mode = 'a'
                    header = ""
                else:
                    mode = 'w'
                    header = (
                        "pcdate pctime time date flags o3 hio3 cellai cellbi "
                        "bncht lmpt o3lt flowa flowb pres\n"
                    )

                with open(self.data_file, mode=mode) as fh:
                    fh.write(header)
                    fh.write(self._data)
                    self.logger.info(f"[{self.name}] file saved: {self.data_file}")

                # reset buffer
                self._data = ""

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")


    def _stage_file(self):
        """ Create zip file from self.data_file and stage archive.
        """
        try:
            if self.data_file:
                archive = os.path.join(self.staging_path, os.path.basename(self.data_file).replace('.dat', '.zip'))
                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.write(self.data_file, os.path.basename(self.data_file))
                    self.logger.info(f"[{self.name}] file staged: {archive}")

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")


    def _save_and_stage_data(self):
        self._save_data()
        self._stage_file()


class Thermo49CPS(Thermo49C):
    """Thermo 49C-PS ozone calibrator helper.

    Extends Thermo49C with:
    - an O3 setpoint (in ppb)
    - an extra column "setpoint_ppb" appended to each lrec line.
    - a level program driven by mkndaq.yml (levels + duration_minutes).
    """

    def __init__(self, name: str, config: dict) -> None:
        super().__init__(name, config)
        cal_cfg = config.get(name, {})

        # Sequence of levels (ppb) and dwell time per level (minutes).
        self.levels: list[int] = [int(x) for x in cal_cfg.get("levels", [])]
        self.level_duration_minutes: int = int(cal_cfg.get("duration_minutes", 15))

        # Template for the remote command that sets the level.
        # The value (in ppb) is passed in as {value}.
        # -> adapt this to the real TEI49C-PS syntax if needed.
        self.level_cmd_template: str = cal_cfg.get(
            "level_cmd_template", "set o3 conc {value}"
        )

        # Track the last requested level (ppb) and index into the program.
        self.current_level: float | None = None
        self._level_index: int = 0

        # Make sure the header has a column for the setpoint.
        if "setpoint" not in self._data_header:
            self._data_header = f"{self._data_header} setpoint_ppb"

    # ------------------------------------------------------------------
    # High-level API
    # ------------------------------------------------------------------
    def set_o3_level(self, level_ppb: int) -> str:
        """Set the calibrator ozone concentration (in ppb).

        The actual command string is controlled by ``level_cmd_template``.
        By default this sends e.g. "set o3 conc 80" for 80 ppb.
        """
        cmd = self.level_cmd_template.format(value=int(level_ppb))
        reply = self.serial_comm(cmd)  # same helper as in Thermo49C
        self.current_level = float(level_ppb)
        # Use the normal logger
        self.logger.info(
            "[%s] set_o3_level -> %d ppb (cmd=%r, reply=%r)",
            self.name,
            level_ppb,
            cmd,
            reply,
        )
        return reply

    def accumulate_lrec(self) -> None:
        """Collect one lrec and append it to the internal buffer.

        Compared to Thermo49C.accumulate_lrec(), this version appends the
        current O3 setpoint (in ppb) as the last column on each line.
        """
        # Respect any cool-down set by failing I/O (same pattern as base class).
        if getattr(self, "_cooldown_until", 0.0) and time.time() < self._cooldown_until:
            return

        try:
            dtm = time.strftime("%Y-%m-%d %H:%M:%S")
            lrec = self.serial_comm("lrec")
            if not lrec:
                return

            # Append the current setpoint; empty if none has been set yet.
            setpoint_str = "" if self.current_level is None else f" {self.current_level}"
            self._data += f"{dtm} {lrec}{setpoint_str}\n"
            self.logger.debug(
                "[%s] lrec: %s ... (setpoint=%s)",
                self.name,
                lrec[:60],
                self.current_level,
            )
        except Exception as err:  # defensive
            self.logger.error("[%s] accumulate_lrec: %s", self.name, err)

    # ------------------------------------------------------------------
    # Level program scheduling
    # ------------------------------------------------------------------
    def _advance_level(self) -> None:
        """Advance to the next level in the configured sequence and send it."""
        try:
            if not self.levels:
                self.logger.warning("[%s] No levels configured for 49C-PS.", self.name)
                return

            level = self.levels[self._level_index]
            self._level_index = (self._level_index + 1) % len(self.levels)

            self.logger.info("[%s] Advancing to calibration level %d ppb", self.name, level)
            self.set_o3_level(level)
        except Exception as err:
            self.logger.error("[%s] _advance_level: %s", self.name, err)

    def setup_schedules(self, delay_job: int = 1) -> None:
        """Set up schedules for the 49C-PS.

        Behaviour:
        - For data acquisition and file handling, behave exactly like Thermo49C
          (same sampling/reporting schedule, same save/stage logic).
          Because accumulate_lrec() is overridden in this subclass, the
          scheduled data acquisition will automatically use the CPS version
          that appends the setpoint column.
        - Additionally, run an automatic level program based on mkndaq.yml
          (tei49c-ps.levels and tei49c-ps.duration_minutes).
        """
        try:
            # 1) Base schedules: sampling + save/stage -> uses self.accumulate_lrec()
            #    thanks to normal Python method overriding.
            super().setup_schedules(delay_job=delay_job)

            # 2) Level program
            if not self.levels:
                self.logger.warning(
                    "[%s] setup_schedules: no levels configured; "
                    "49C-PS will behave like a plain 49C.", self.name
                )
                return

            if self.level_duration_minutes <= 0:
                self.logger.warning(
                    "[%s] setup_schedules: invalid duration_minutes=%s; "
                    "skipping level program.",
                    self.name,
                    self.level_duration_minutes,
                )
                return

            # Immediately set the first level once at startup
            self._level_index = 0
            self._advance_level()

            # Then step through the configured levels every duration_minutes.
            # This repeats the sequence cyclically.
            schedule.every(self.level_duration_minutes).minutes.at(":00").do(
                self._advance_level
            )

            self.logger.info(
                "[%s] Level program scheduled: %d levels, %d min per level.",
                self.name,
                len(self.levels),
                self.level_duration_minutes,
            )

        except Exception as err:
            self.logger.error("[%s] setup_schedules: %s", self.name, err)

if __name__ == "__main__":
    pass
