# -*- coding: utf-8 -*-
"""
Define a class TEI49I facilitating communication with a Thermo TEI49i instrument.

@author: joerg.klausen@meteoswiss.ch
"""
import os
from datetime import datetime
import logging
# import shutil
import socket
# import re
import serial
import schedule
import time
import zipfile
import colorama


class Thermo49i:
    def __init__(self, config: dict, name: str='49i'):
        """
        Initialize the Thermo 49i instrument class with parameters from a configuration file.

        Args:
            config (dict): general configuration
        """
        colorama.init(autoreset=True)

        try:
            # configure logging
            _logger = f"{os.path.basename(config['logging']['file'])}".split('.')[0]
            self.logger = logging.getLogger(f"{_logger}.{__name__}")

            # read instrument control properties for later use
            self._name = name
            self._id = config[name]['id'] + 128
            self._serial_number = config[name]['serial_number']
            self._get_config = config[name]['get_config']
            self._set_config = config[name]['set_config']
            self._get_data = config[name]['get_data']

            self.logger.info(f"Initialize Thermo 49i (name: {self._name}  S/N: {self._serial_number})")

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

            root = os.path.expanduser(config['root'])

            # configure data collection and reporting
            self._sampling_interval = config[name]['sampling_interval']
            self.reporting_interval = config[name]['reporting_interval']
            if not (self.reporting_interval % 60)==0 and self.reporting_interval<=1440:
                raise ValueError('reporting_interval must be a multiple of 60 and less or equal to 1440 minutes.')
                   
            # configure saving, staging and archiving
            self.data_path = os.path.join(root, config[name]['data'])
            self.staging_path = os.path.join(root, config[name]['staging'])
            self.archive_path = os.path.join(root, config[name]['archive'])

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
            os.makedirs(self.archive_path, exist_ok=True)

            # configure data acquisition schedule
            schedule.every(int(self._sampling_interval)).minutes.at(':00').do(self.accumulate_lr00)
            
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
                response = self.serial_comm(cmd)
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
                    cfg.append(self.serial_comm(cmd))
                else:
                    cfg.append(self.tcpip_comm(cmd))

            self.logger.info(f"{self._name}: Configuration read as: {cfg}")

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
                dte = self.serial_comm(cmd)
            else:
                dte = self.tcpip_comm(cmd)
            self.logger.info(f"{self._name}: Date set to: {dte}")

            cmd = f"set time {time.strftime('%H:%M:%S')}"
            if self._serial_com:
                tme = self.serial_comm(cmd)
            else:
                tme = self.tcpip_comm(cmd)
            self.logger.info(f"{self._name}: Time set to: {tme}")

        except Exception as err:
            self.logger.error(err)


    def set_config(self) -> list:
        """
        Set configuration of instrument and optionally write to log.

        :return (err, cfg) configuration set or errors, if any.
        """
        print("%s .set_config (name=%s)" % (time.strftime('%Y-%m-%d %H:%M:%S'), self._name))
        cfg = []
        try:
            for cmd in self._set_config:
                if self._serial_com:
                    cfg.append(self.serial_comm(cmd))
                else:
                    cfg.append(self.tcpip_comm(cmd))
                time.sleep(1)

            self.logger.info(f"{self._name}: Configuration set to: {cfg}")

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
            self.logger.info(f"{self._name}: {_[:60]}[...]")

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
                                    f"{self._name}_all_lrec-{dtm}.dat")

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
            if self._serial_com:
                o3 = self.serial_comm('o3').split()
            else:
                o3 = self.tcpip_comm('o3').split()
            self.logger.info(colorama.Fore.GREEN + f"{self._name}: {o3[0].upper()} {str(float(o3[1]))} {o3[2]}")

        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"{err}")


    def _save_data(self) -> None:
        try:
            data_file = str()
            self.data_file = str()
            if self._data:
                # create appropriate file name and write mode
                timestamp = datetime.now().strftime(self._file_timestamp_format)               
                data_file = os.path.join(self.data_path, f"49i-{timestamp}.dat")

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
                    self.logger.info(f"file saved: {data_file}")
            
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
