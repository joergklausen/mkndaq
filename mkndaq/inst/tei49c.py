# -*- coding: utf-8 -*-
"""
Define a class TEI49C facilitating communication with a Thermo TEI49c instrument.

@author: joerg.klausen@meteoswiss.ch
"""

# from datetime import datetime
import logging
import os
import shutil
import time
import zipfile
import colorama
import serial

from mkndaq.utils import datetimebin


class TEI49C:
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
            - config[name]['get_data']
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
            - config['logs']: default=True, write information to logfile
            - config['staging']['path']
            - config['staging']['zip']
        """
        colorama.init(autoreset=True)

        try:
            # configure logging
            _logger = f"{os.path.basename(config['logging']['file'])}".split('.')[0]
            self.logger = logging.getLogger(f"{_logger}.{__name__}")

            # read instrument control properties for later use
            self._name = name
            self._id = config[name]['id'] + 128
            self._type = config[name]['type']
            self.__serial_number = config[name]['serial_number']
            self.__get_config = config[name]['get_config']
            self.__set_config = config[name]['set_config']
            self.__get_data = config[name]['get_data']
            self.__data_header = config[name]['data_header']

            # configure serial port
            port = config[name]['port']
            self.__serial = serial.Serial(port=port,
                                        baudrate=config[port]['baudrate'],
                                        bytesize=config[port]['bytesize'],
                                        parity=config[port]['parity'],
                                        stopbits=config[port]['stopbits'],
                                        timeout=config[port]['timeout'])
            if self.__serial.is_open:
                self.__serial.close()

            # sampling, aggregation, reporting/storage
            # self._sampling_interval = config[name]['sampling_interval']
            self.__reporting_interval = config['reporting_interval']

            # setup data directory
            data_path = os.path.expanduser(config['data'])
            self._data_path = os.path.join(data_path, self._name)
            os.makedirs(self._data_path, exist_ok=True)

            # staging area for files to be transfered
            self.__staging = os.path.expanduser(config['staging']['path'])
            self.__zip = config[name]['staging_zip']

            self.logger.info(f"Initialize TEI49C (name: {self._name}  S/N: {self.__serial_number})")
            self.get_config()
            self.set_config()

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
            self.__serial.write(id + (f"{cmd}\x0D").encode())
            time.sleep(0.5)
            while self.__serial.in_waiting > 0:
                rcvd = rcvd + self.__serial.read(1024)
                time.sleep(0.1)

            rcvd = rcvd.decode()
            # remove checksum after and including the '*'
            rcvd = rcvd.split("*")[0]
            # remove cmd echo
            rcvd = rcvd.replace(cmd, "").strip()
            return rcvd

        except Exception as err:
            self.logger.error(err)


    def get_config(self) -> list:
        """
        Read current configuration of instrument and optionally write to log.

        :return current configuration of instrument

        """
        self.logger.info(f" .get_config (name={self._name})")
        cfg = []
        try:
            self.__serial.open()
            for cmd in self.__get_config:
                cfg.append(self.serial_comm(cmd))
            self.__serial.close()

            self.logger.info(f"Current configuration of '{self._name}': {cfg}")

            return cfg

        except Exception as err:
            self.logger.error(err)


    def set_datetime(self) -> None:
        """
        Synchronize date and time of instrument with computer time. Assumes an open connection.

        :return:
        """
        try:
            dte = self.serial_comm(f"set date {time.strftime('%m-%d-%y')}")
            dte = self.serial_comm("date")
            self.logger.info(f"Date of instrument {self._name} set and reported as: {dte}")

            tme = self.serial_comm(f"set time {time.strftime('%H:%M')}")
            tme = self.serial_comm("time")
            self.logger.info(f"Time of instrument {self._name} set and reported as: {tme}")
        except Exception as err:
            self.logger.error(err)


    def set_config(self) -> list:
        """
        Set configuration of instrument and optionally write to log.

        :return new configuration as returned from instrument
        """
        self.logger.info(f"{time.strftime('%Y-%m-%d %H:%M:%S')} .set_config (name={self._name})")
        cfg = []
        try:
            self.__serial.open()
            self.set_datetime()
            for cmd in self.__set_config:
                cfg.append(self.serial_comm(cmd))
            self.__serial.close()
            time.sleep(1)

            self.logger.info(f"Configuration of '{self._name}' set to: {cfg}")

            return cfg

        except Exception as err:
            self.logger.error(err)


    def get_data(self, cmd=None, save=True):
        """
        Retrieve long record from instrument and optionally write to log.

        :param str cmd: command sent to instrument
        :param bln save: Should data be saved to file? default=True
        :return str response as decoded string
        """
        try:
            dtm = time.strftime('%Y-%m-%d %H:%M:%S')
            self.logger.info(f"{dtm} .get_data (name={self._name}, save={save})")

            if cmd is None:
                cmd = self.__get_data

            if self.__serial.is_open:
                self.__serial.close()

            self.__serial.open()
            data = self.serial_comm(cmd)
            self.__serial.close()

            if save:
                # generate the datafile name
                # self._data_file = os.path.join(self._data_path,
                #                              "".join([self._name, "-",
                #                                       datetimebin.dtbin(self.__reporting_interval), ".dat"]))
                self._data_file = os.path.join(self._data_path, time.strftime("%Y"), time.strftime("%m"), time.strftime("%d"),
                                               f"{self._name}-{datetimebin.dtbin(self.__reporting_interval)}.dat")

                os.makedirs(os.path.dirname(self._data_file), exist_ok=True)
                if not os.path.exists(self._data_file):
                    # if file doesn't exist, create and write header
                    with open(self._data_file, "at", encoding='utf8') as fh:
                        fh.write(f"{self.__data_header}\n")
                        fh.close()
                with open(self._data_file, "at", encoding='utf8') as fh:
                    # add data to file
                    fh.write(f"{dtm} {data}\n")
                    fh.close()

                # stage data for transfer
                # root = os.path.join(self.__staging, os.path.basename(self._data_path))
                # os.makedirs(root, exist_ok=True)
                # if self.__zip:
                #     # create zip file
                #     archive = os.path.join(root, "".join([os.path.basename(self._data_file)[:-4], ".zip"]))
                #     with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                #         fh.write(self._data_file, os.path.basename(self._data_file))
                # else:
                #     shutil.copyfile(self._data_file, os.path.join(root, os.path.basename(self._data_file)))
                if self.__file_to_stage is None:
                    self.__file_to_stage = self._data_file
                elif self.__file_to_stage != self._data_file:
                    root = os.path.join(self.__staging, os.path.basename(self._data_path))
                    os.makedirs(root, exist_ok=True)
                    if self.__zip:
                        # create zip file
                        archive = os.path.join(root, "".join([os.path.basename(self.__file_to_stage)[:-4], ".zip"]))
                        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                            zf.write(self.__file_to_stage, os.path.basename(self.__file_to_stage))
                    else:
                        shutil.copyfile(self.__file_to_stage, os.path.join(root, os.path.basename(self.__file_to_stage)))
                    self.__file_to_stage = self._data_file

            return data

        except Exception as err:
            self.logger.error(err)


    def get_o3(self) -> str:
        try:
            self.__serial.open()
            o3 = self.serial_comm('O3')
            self.__serial.close()
            return o3

        except Exception as err:
            self.logger.error(err)


    def print_o3(self) -> None:
        try:
            self.__serial.open()
            o3 = self.serial_comm('O3').split()
            self.__serial.close()

            print(colorama.Fore.GREEN + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self._name}] {o3[0].upper()} {str(float(o3[1]))} {o3[2]}")

        except Exception as err:
            self.logger.error(err)


    def get_all_rec(self, capacity=[1790, 4096], save=True) -> str:
        """
        Retrieve all long and short records from instrument and optionally write to file.

        :param bln save: Should data be saved to file? default=True
        :return str response as decoded string
        """
        try:
            data_file = str()
            dtm = time.strftime('%Y-%m-%d %H:%M:%S')

            # lrec and srec capacity of logger
            CMD = ["lrec", "srec"]
            CAPACITY = capacity

            self.logger.info("%s .get_all_rec (name=%s, save=%s)" % (dtm, self._name, save))

            # close potentially open port
            if self.__serial.is_open:
                self.__serial.close()

            # retrieve data from instrument
            for i in [0, 1]:
                index = CAPACITY[i]
                retrieve = 10
                if save:
                    # generate the datafile name
                    datafile = os.path.join(self._data_path,
                                            f"{self._name}_all_{CMD[i]}-{time.strftime('%Y%m%d%H%M00')}.dat")

                while index > 0:
                    if index < 10:
                        retrieve = index
                    cmd = f"{CMD[i]} {str(index)} {str(retrieve)}"
                    print(cmd)
                    self.__serial.open()
                    data = self.serial_comm(cmd)
                    self.__serial.close()

                    if save:
                        if not os.path.exists(data_file):
                            # if file doesn't exist, create and write header
                            with open(data_file, "at") as fh:
                                fh.write(f"{self.__data_header}\n")
                                fh.close()
                        with open(data_file, "at") as fh:
                            # add data to file
                            fh.write(f"{data}\n")
                            fh.close()

                    index = index - 10

                # stage data for transfer
                root = os.path.join(self.__staging, os.path.basename(self._data_path))
                os.makedirs(root, exist_ok=True)
                if self.__zip:
                    # create zip file
                    archive = os.path.join(root, "".join([os.path.basename(data_file[:-4]), ".zip"]))
                    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                        fh.write(data_file, os.path.basename(data_file))
                else:
                    shutil.copyfile(data_file, os.path.join(root, os.path.basename(data_file)))

            return

        except Exception as err:
            self.logger.error(err)


if __name__ == "__main__":
    pass
