# -*- coding: utf-8 -*-
"""
Define a class TEI49I facilitating communication with a Thermo 49C and 49i instruments.

@author: joerg.klausen@meteoswiss.ch
"""
import logging
import os
import socket
import time
import zipfile
from datetime import datetime

import colorama
import schedule
import serial


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

            # configure serial port
            port = config[name]['port']
            self._serial = serial.Serial(port=port,
                                        baudrate=config[port]['baudrate'],
                                        bytesize=config[port]['bytesize'],
                                        parity=config[port]['parity'],
                                        stopbits=config[port]['stopbits'],
                                        timeout=config[port]['timeout'])
            if self._serial.is_open:
                self._serial.close()

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
            self._file_to_stage = str()
            self._zip = config[name]['staging_zip']

            # configure remote transfer
            self.remote_path = config[name]['remote_path']

            # initialize data response
            self._data = str()

            self.get_config()
            self.set_config()

        except Exception as err:
            self.logger.error(err)


    def setup_schedules(self):
        try:
            # configure folders needed
            os.makedirs(self.data_path, exist_ok=True)
            os.makedirs(self.staging_path, exist_ok=True)
            # os.makedirs(self.archive_path, exist_ok=True)

            # configure data acquisition schedule
            schedule.every(self.sampling_interval).minutes.at(':00').do(self.accumulate_lrec)

            # configure saving and staging schedules
            if self.reporting_interval==10:
                self._file_timestamp_format = '%Y%m%d%H%M'
                minutes = [f"{self.reporting_interval*n:02}" for n in range(6) if self.reporting_interval*n < 6]
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
            self.logger.error(err)


    def serial_comm(self, cmd: str) -> str:
        """
        Send a command and retrieve the response. Assumes an open connection.

        :param cmd: command sent to instrument
        :return: response of instrument, decoded
        """
        id = bytes([self._id])
        try:
            rcvd = b''
            self._serial.write(id + (f"{cmd}\x0D").encode())
            time.sleep(0.5)
            while self._serial.in_waiting > 0:
                rcvd = rcvd + self._serial.read(1024)
                time.sleep(0.1)

            rcvd = rcvd.decode()
            # remove checksum after and including the '*'
            rcvd = rcvd.split("*")[0]
            # remove cmd echo
            rcvd = rcvd.replace(cmd, "").strip()
            return rcvd

        except Exception as err:
            self.logger.error(f"serial_comm: {err}")
            return str()


    def get_config(self) -> list:
        """
        Read current configuration of instrument and optionally write to log.

        :return current configuration of instrument

        """
        cfg = []
        try:
            self._serial.open()
            for cmd in self._get_config:
                cfg.append(self.serial_comm(cmd))
            self._serial.close()
            self.logger.info(f"[{self.name}] Configuration is: {cfg}")

            return cfg

        except Exception as err:
            self.logger.error(err)
            return list()


    def set_datetime(self) -> None:
        """
        Synchronize date and time of instrument with computer time. Assumes an open connection.

        :return:
        """
        try:
            if self._serial.closed:
                self._serial.open()
            dte = self.serial_comm(f"set date {time.strftime('%m-%d-%y')}")
            dte = self.serial_comm("date")
            tme = self.serial_comm(f"set time {time.strftime('%H:%M')}")
            tme = self.serial_comm("time")
            self._serial.close()
            self.logger.info(f"[{self.name}] Date and time set and reported as: {dte} {tme}")
        except Exception as err:
            self.logger.error(err)


    def set_config(self) -> list:
        """
        Set configuration of instrument and optionally write to log.

        :return new configuration as returned from instrument
        """
        self.logger.info(f"[{self.name}] .set_config")
        cfg = []
        try:
            if self._serial.closed:
                self._serial.open()
            self.set_datetime()
            for cmd in self._set_config:
                cfg.append(f"{cmd}: {self.serial_comm(cmd)}")
            self._serial.close()
            time.sleep(1)

            self.logger.info(f"[{self.name}] Configuration set to: {cfg}")

            return cfg

        except Exception as err:
            self.logger.error(err)
            return list()


    def accumulate_lrec(self):
        """
        Send command, retrieve response from instrument and append to self._data.
        """
        try:
            dtm = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if self._serial.closed:
                self._serial.open()
            _ = self.serial_comm('lrec')
            self._serial.close()

            self._data += f"{dtm} {_}\n"
            self.logger.debug(f"[{self.name}] {_[:60]}[...]")

            return

        except Exception as err:
            self.logger.error(err)


    def _save_data(self) -> None:
        try:
            data_file = str()
            self.data_file = str()
            if self._data:
                # create appropriate file name and write mode
                now = datetime.now()
                timestamp = now.strftime(self._file_timestamp_format)
                yyyy = now.strftime('%Y')
                mm = now.strftime('%m')
                dd = now.strftime('%d')
                data_file = os.path.join(self.data_path, yyyy, mm, dd, f"{self.name}-{timestamp}.dat")
                os.makedirs(os.path.dirname(data_file), exist_ok=True)

                # configure file mode, open file and write to it
                if os.path.exists(self.data_file):
                    mode = 'a'
                    header = str()
                else:
                    mode = 'w'
                    header = f"{self._data_header}\n"

                with open(file=data_file, mode=mode) as fh:
                    fh.write(header)
                    fh.write(self._data)
                    self.logger.info(f"[{self.name}] file saved: {data_file}")

                # reset self._data
                self._data = str()

            self.data_file = data_file
            return

        except Exception as err:
            self.logger.error(err)


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
            self.logger.error(err)


    def _save_and_stage_data(self):
        self._save_data()
        self._stage_file()


    def get_o3(self) -> str:
        try:
            if self._serial.closed:
                self._serial.open()
            o3 = self.serial_comm('O3')
            self._serial.close()
            return o3

        except Exception as err:
            self.logger.error(err)
            return str()


    def print_o3(self) -> None:
        try:
            o3 = self.get_o3().split()
            if len(o3)==2:
                self.logger.info(colorama.Fore.GREEN + f"[{self.name}] O3 {float(o3[0]):0.1f} {o3[1]}")
            if len(o3)==3:
                self.logger.info(colorama.Fore.GREEN + f"[{self.name}] {o3[0].upper()} {float(o3[1]):0.1f} {o3[2]}")

        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"[{self.name}] print_o3: {err}")


    def get_all_rec(self, capacity=[1790, 4096], save=True) -> str:
        """
        Retrieve all long and short records from instrument and optionally write to file.

        :param bln save: Should data be saved to file? default=True
        :return str response as decoded string
        """
        try:
            data = str()
            data_file = str()

            # lrec and srec capacity of logger
            CMD = ["lrec", "srec"]
            CAPACITY = capacity

            self.logger.info(f"[{self.name}] .get_all_rec (save={save})")

            # retrieve data from instrument
            for i in [0, 1]:
                index = CAPACITY[i]
                retrieve = 10
                if save:
                    # generate the datafile name
                    dtm = time.strftime('%Y%m%d%H%M%S')
                    data_file = os.path.join(self.data_path,
                                            f"{self.name}_all_{CMD[i]}-{dtm}.dat")

                while index > 0:
                    if index < 10:
                        retrieve = index
                    cmd = f"{CMD[i]} {str(index)} {str(retrieve)}"
                    self.logger.info(cmd)
                    if self._serial.closed:
                        self._serial.open()
                    data += f"{self.serial_comm(cmd)}\n"
                    self._serial.close()

                    index = index - 10

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

            return data

        except Exception as err:
            self.logger.error(err)
            return str()


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
            if self._serial_com:
                # configure serial port
                port = config[name]['port']
                self._serial = serial.Serial(port=port,
                                            baudrate=config[port]['baudrate'],
                                            bytesize=config[port]['bytesize'],
                                            parity=config[port]['parity'],
                                            stopbits=config[port]['stopbits'],
                                            timeout=config[port]['timeout'])
                if self._serial.is_open:
                    self._serial.close()
                self.logger.info(f"Serial port {port} successfully opened and closed.")
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
            self._data = str()

        except Exception as err:
            self.logger.error(err)


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
                minutes = [f"{self.reporting_interval*n:02}" for n in range(6) if self.reporting_interval*n < 6]
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
            self.logger.error(err)


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
            self.logger.error(err)
            return str()


    def serial_comm(self, cmd: str, tidy=True) -> str:
        """
        Send a command and retrieve the response. Assumes an open connection.

        :param cmd: command sent to instrument
        :param tidy: remove echo and checksum after '*'
        :return: response of instrument, decoded
        """
        __id = bytes([self._id])
        rcvd = b''
        try:
            self._serial.write(__id + (f"{cmd}\x0D").encode())
            time.sleep(0.5)
            while self._serial.in_waiting > 0:
                rcvd = rcvd + self._serial.read(1024)
                time.sleep(0.1)

            rcvd = rcvd.decode()
            # remove checksum after and including the '*'
            rcvd = rcvd.split("*")[0]
            # remove echo before and including '\n'
            rcvd = rcvd.replace(cmd, "").strip()

            return rcvd

        except Exception as err:
            self.logger.error(err)
            return str()


    def send_command(self, cmd: str) -> str:
        try:
            if self._serial_com:
                if self._serial.closed:
                    self._serial.open()
                response = self.serial_comm(cmd)
                self._serial.close()
            else:
                response = self.tcpip_comm(cmd)
            return response
        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"{err}")
            return str()


    def get_config(self) -> list:
        """
        Read current configuration of instrument and optionally write to log.

        :return (err, cfg) configuration or errors, if any.

        """
        cfg = []
        try:
            for cmd in self._get_config:
                if self._serial_com:
                    if self._serial.closed:
                        self._serial.open()
                    cfg.append(self.serial_comm(cmd))
                    self._serial.close()
                else:
                    cfg.append(self.tcpip_comm(cmd))

            self.logger.info(f"[{self.name}] Configuration read as: {cfg}")

            return cfg

        except Exception as err:
            self.logger.error(err)
            return list()


    def set_datetime(self) -> None:
        """
        Synchronize date and time of instrument with computer time.

        :return:
        """
        try:
            cmd = f"set date {time.strftime('%m-%d-%y')}"
            if self._serial_com:
                if self._serial.closed:
                    self._serial.open()
                dte = self.serial_comm(cmd)
                self._serial.close()
            else:
                dte = self.tcpip_comm(cmd)
            self.logger.info(f"[{self.name}] Date set to: {dte}")

            cmd = f"set time {time.strftime('%H:%M:%S')}"
            if self._serial_com:
                if self._serial.closed:
                    self._serial.open()
                tme = self.serial_comm(cmd)
                self._serial.close()
            else:
                tme = self.tcpip_comm(cmd)
            self.logger.info(f"[{self.name}] Time set to: {tme}")

        except Exception as err:
            self.logger.error(err)


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
            self.logger.error(err)
            return list()


    def accumulate_lr00(self):
        """
        Send command, retrieve response from instrument and append to self._data.
        """
        try:
            dtm = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if self._serial_com:
                _ = self.serial_comm('lr00')
            else:
                _ = self.tcpip_comm('lr00')
            self._data += f"{dtm} {_}\n"
            self.logger.debug(f"[{self.name}] {_[:60]}[...]")

            return

        except Exception as err:
            self.logger.error(err)


    def get_all_lrec(self, save: bool=True) -> str:
        """download entire buffer from instrument and save to file

        :param bln save: Should data be saved to file? default=True
        :return str response as decoded string
        """
        try:
            data = str()
            file = str()

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
            retrieve = 10

            while index > 0:
                if index < 10:
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

                index = index - 10

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
            self.logger.error(err)
            return str()


    def get_o3(self) -> str:
        try:
            if self._serial_com:
                return self.serial_comm('o3')
            else:
                return self.tcpip_comm('o3')

        except Exception as err:
            self.logger.error(err)
            return str()


    def print_o3(self) -> None:
        try:
            o3 = self.get_o3().split()
            if len(o3)==2:
                self.logger.info(colorama.Fore.GREEN + f"[{self.name}] O3 {float(o3[0]):0.1f} {o3[1]}")
            if len(o3)==3:
                self.logger.info(colorama.Fore.GREEN + f"[{self.name}] {o3[0].upper()}  {float(o3[1]):0.1f} {o3[2]}")

        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"[{self.name}] print_o3: {err}")


    def _save_data(self) -> None:
        try:
            data_file = str()
            self.data_file = str()
            if self._data:
                # create appropriate file name and write mode
                now = datetime.now()
                timestamp = now.strftime(self._file_timestamp_format)
                yyyy = now.strftime('%Y')
                mm = now.strftime('%m')
                dd = now.strftime('%d')
                data_file = os.path.join(self.data_path, yyyy, mm, dd, f"{self.name}-{timestamp}.dat")
                os.makedirs(os.path.dirname(data_file), exist_ok=True)

                # configure file mode, open file and write to it
                if os.path.exists(self.data_file):
                    mode = 'a'
                    header = str()
                else:
                    mode = 'w'
                    header = 'pcdate pctime time date flags o3 hio3 cellai cellbi bncht lmpt o3lt flowa flowb pres\n'

                with open(file=data_file, mode=mode) as fh:
                    fh.write(header)
                    fh.write(self._data)
                    self.logger.info(f"[{self.name}] file saved: {data_file}")

                # reset self._data
                self._data = str()

            self.data_file = data_file
            return

        except Exception as err:
            self.logger.error(err)


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
            self.logger.error(err)


    def _save_and_stage_data(self):
        self._save_data()
        self._stage_file()


if __name__ == "__main__":
    pass
