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

    __datadir = None
    __datafile = None
    __file_to_stage = None
    __data_header = None
    __get_config = None
    __get_data = None
    __id = None
    _log = False
    _logger = None
    __name = None
    __reporting_interval = None
    __serial = None
    __set_config = None
    _simulate = None
    __staging = None
    __zip = False

    def __init__(self, name: str, config: dict, simulate=False) -> None:
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
        :param simulate: default=True, simulate instrument behavior. Assumes a serial loopback connector.
        """
        colorama.init(autoreset=True)

        try:
            self._simulate = simulate
            # setup logging
            if 'logs' in config.keys():
                self._log = True
                logs = os.path.expanduser(config['logs'])
                os.makedirs(logs, exist_ok=True)
                logfile = f"{time.strftime('%Y%m%d')}.log"
                self._logger = logging.getLogger(__name__)
                logging.basicConfig(level=logging.DEBUG,
                                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                                    datefmt='%y-%m-%d %H:%M:%S',
                                    filename=str(os.path.join(logs, logfile)),
                                    filemode='a')

            # read instrument control properties for later use
            self.__name = name
            self.__id = config[name]['id'] + 128
            self._type = config[name]['type']
            self.__serial_number = config[name]['serial_number']
            self.__get_config = config[name]['get_config']
            self.__set_config = config[name]['set_config']
            self.__get_data = config[name]['get_data']
            self.__data_header = config[name]['data_header']

            # configure serial port
            if not self._simulate:
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
            datadir = os.path.expanduser(config['data'])
            self.__datadir = os.path.join(datadir, self.__name)
            os.makedirs(self.__datadir, exist_ok=True)

            # staging area for files to be transfered
            self.__staging = os.path.expanduser(config['staging']['path'])
            self.__zip = config[name]['staging_zip']

            print(f"# Initialize TEI49C (name: {self.__name}  S/N: {self.__serial_number})")
            self.get_config()
            self.set_config()

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def serial_comm(self, cmd: str, tidy=True) -> str:
        """
        Send a command and retrieve the response. Assumes an open connection.

        :param cmd: command sent to instrument
        :param tidy: remove echo and checksum after '*'
        :return: response of instrument, decoded
        """
        __id = bytes([self.__id])
        rcvd = b''
        try:
            if self._simulate:
                __id = b''
            self.__serial.write(__id + (f"{cmd}\x0D").encode())
            time.sleep(0.5)
            while self.__serial.in_waiting > 0:
                rcvd = rcvd + self.__serial.read(1024)
                time.sleep(0.1)

            rcvd = rcvd.decode()
            if tidy:
                # - remove checksum after and including the '*'
                rcvd = rcvd.split("*")[0]
                # - remove echo before and including '\n'
                if cmd.join("\n") in rcvd:
                    # rcvd = rcvd.split("\n")[1]
                    rcvd = rcvd.replace(cmd, "")
                # remove trailing '\r\n'
                # rcvd = rcvd.rstrip()
                rcvd = rcvd.strip()
            return rcvd

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)

    # def test_serial_comm(self, cmd: str, tidy=True, sleep=0.1, debug=False) -> str:
    #     """
    #     Send a command and retrieve the response. Assumes an open connection.

    #     :param cmd: command sent to instrument
    #     :param tidy: remove echo and checksum after '*'
    #     :return: response of instrument, decoded
    #     """
    #     __id = bytes([self.__id])
    #     rcvd = b''
    #     try:
    #         self.__serial.open()

    #         self.__serial.write(__id + (f"{cmd}\x0D").encode())
    #         time.sleep(5 * sleep)
    #         while self.__serial.in_waiting > 0:
    #             rcvd = rcvd + self.__serial.read(1024)
    #             time.sleep(sleep)

    #         rcvd = rcvd.decode()
    #         if debug:
    #             print(f"Response before tidying (between ##): #{rcvd}#")
    #         if tidy:
    #             # - remove checksum after and including the '*'
    #             rcvd = rcvd.split("*")[0]
    #             # - remove echo before and including '\n'
    #             if cmd.join("\n") in rcvd:
    #                 rcvd = rcvd.split("\n")[1]
    #             # remove trailing '\r\n'
    #             rcvd = rcvd.rstrip()
    #             if debug:
    #                 print(f"Response before tidying (between ##): #{rcvd}#")

    #         self.__serial.close()
    #         return rcvd

    #     except Exception as err:
    #         if self._log:
    #             self._logger.error(err)
    #         print(err)

    def get_config(self) -> list:
        """
        Read current configuration of instrument and optionally write to log.

        :return current configuration of instrument

        """
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} .get_config (name={self.__name})")
        cfg = []
        try:
            self.__serial.open()
            for cmd in self.__get_config:
                cfg.append(self.serial_comm(cmd))
            self.__serial.close()

            if self._log:
                self._logger.info(f"Current configuration of '{self.__name}': {cfg}")

            return cfg

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)

    def set_datetime(self) -> None:
        """
        Synchronize date and time of instrument with computer time. Assumes an open connection.

        :return:
        """
        try:
            dte = self.serial_comm(f"set date {time.strftime('%m-%d-%y')}")
            dte = self.serial_comm("date")
            msg = f"Date of instrument {self.__name} set and reported as: {dte}"
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}")
            if self._log:
                self._logger.info(msg)

            tme = self.serial_comm(f"set time {time.strftime('%H:%M')}")
            tme = self.serial_comm("time")
            msg = f"Time of instrument {self.__name} set and reported as: {tme}"
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}")
            if self._log:
                self._logger.info(msg)

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)

    def set_config(self) -> list:
        """
        Set configuration of instrument and optionally write to log.

        :return new configuration as returned from instrument
        """
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} .set_config (name={self.__name})")
        cfg = []
        try:
            self.__serial.open()
            self.set_datetime()
            for cmd in self.__set_config:
                cfg.append(self.serial_comm(cmd))
            self.__serial.close()
            time.sleep(1)

            if self._log:
                self._logger.info(f"Configuration of '{self.__name}' set to: {cfg}")

            return cfg

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)

    def get_data(self, cmd=None, save=True) -> str:
        """
        Retrieve long record from instrument and optionally write to log.

        :param str cmd: command sent to instrument
        :param bln save: Should data be saved to file? default=True
        :return str response as decoded string
        """
        try:
            dtm = time.strftime('%Y-%m-%d %H:%M:%S')
            if self._simulate:
                print(f"{dtm} .get_data (name={self.__name}, save={save}, simulate={self._simulate})")
            else:
                print(f"{dtm} .get_data (name={self.__name}, save={save})")

            if cmd is None:
                cmd = self.__get_data

            if self._simulate:
                data = self.simulate__get_data(cmd)
            else:
                if self.__serial.is_open:
                    self.__serial.close()

                self.__serial.open()
                data = self.serial_comm(cmd)
                self.__serial.close()

            if save:
                # generate the datafile name
                # self.__datafile = os.path.join(self.__datadir,
                #                              "".join([self.__name, "-",
                #                                       datetimebin.dtbin(self.__reporting_interval), ".dat"]))
                self.__datafile = os.path.join(self.__datadir, time.strftime("%Y"), time.strftime("%m"), time.strftime("%d"),
                                             "".join([self.__name, "-",
                                                      datetimebin.dtbin(self.__reporting_interval), ".dat"]))

                os.makedirs(os.path.dirname(self.__datafile), exist_ok=True)
                if not os.path.exists(self.__datafile):
                    # if file doesn't exist, create and write header
                    with open(self.__datafile, "at", encoding='utf8') as fh:
                        fh.write(f"{self.__data_header}\n")
                        fh.close()
                with open(self.__datafile, "at", encoding='utf8') as fh:
                    # add data to file
                    fh.write(f"{dtm} {data}\n")
                    fh.close()

                # stage data for transfer
                # root = os.path.join(self.__staging, os.path.basename(self.__datadir))
                # os.makedirs(root, exist_ok=True)
                # if self.__zip:
                #     # create zip file
                #     archive = os.path.join(root, "".join([os.path.basename(self.__datafile)[:-4], ".zip"]))
                #     with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                #         fh.write(self.__datafile, os.path.basename(self.__datafile))
                # else:
                #     shutil.copyfile(self.__datafile, os.path.join(root, os.path.basename(self.__datafile)))
                if self.__file_to_stage is None:
                    self.__file_to_stage = self.__datafile
                elif self.__file_to_stage != self.__datafile:
                    root = os.path.join(self.__staging, os.path.basename(self.__datadir))
                    os.makedirs(root, exist_ok=True)
                    if self.__zip:
                        # create zip file
                        archive = os.path.join(root, "".join([os.path.basename(self.__file_to_stage)[:-4], ".zip"]))
                        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                            zf.write(self.__file_to_stage, os.path.basename(self.__file_to_stage))
                    else:
                        shutil.copyfile(self.__file_to_stage, os.path.join(root, os.path.basename(self.__file_to_stage)))
                    self.__file_to_stage = self.__datafile

            return data

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)

    def get_o3(self) -> str:
        try:
            self.__serial.open()
            o3 = self.serial_comm('O3')
            self.__serial.close()
            return o3

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)

    def print_o3(self) -> None:
        try:
            self.__serial.open()
            o3 = self.serial_comm('O3').split()
            self.__serial.close()

            print(colorama.Fore.GREEN + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self.__name}] {o3[0]} {str(float(o3[1]))} {o3[2]}")

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(colorama.Fore.RED + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self.__name}] produced error {err}.")

    def simulate_get_data(self, cmd=None) -> str:
        """

        :param cmd:
        :return:
        """
        if cmd is None:
            cmd = 'lrec'

        dtm = time.strftime("%H:%M %m-%d-%y", time.gmtime())

        if cmd == 'lrec':
            data = f"(simulated) {dtm}  flags D800500 o3 0.394 cellai 123853.000 cellbi 94558.000 bncht 31.220 lmpt " \
                   "53.754 o3lt 68.363 flowa 0.000 flowb 0.000 pres 724.798"
        else:
            data = f"(simulated) {dtm} Sorry, I can only simulate lrec. "

        return data

    def get_all_rec(self, capacity=[1790, 4096], save=True) -> str:
        """
        Retrieve all long and short records from instrument and optionally write to file.

        :param bln save: Should data be saved to file? default=True
        :return str response as decoded string
        """
        try:
            dtm = time.strftime('%Y-%m-%d %H:%M:%S')

            # lrec and srec capacity of logger
            CMD = ["lrec", "srec"]
            CAPACITY = capacity

            print("%s .get_all_rec (name=%s, save=%s)" % (dtm, self.__name, save))

            # close potentially open port
            if self.__serial.is_open:
                self.__serial.close()

            # retrieve data from instrument
            for i in [0, 1]:
                index = CAPACITY[i]
                retrieve = 10
                if save:
                    # generate the datafile name
                    datafile = os.path.join(self.__datadir,
                                            "".join([self.__name, f"_all_{CMD[i]}-",
                                                    time.strftime("%Y%m%d%H%M00"), ".dat"]))

                while index > 0:
                    if index < 10:
                        retrieve = index
                    cmd = f"{CMD[i]} {str(index)} {str(retrieve)}"
                    print(cmd)
                    self.__serial.open()
                    data = self.serial_comm(cmd)
                    self.__serial.close()

                    if save:
                        if not os.path.exists(datafile):
                            # if file doesn't exist, create and write header
                            with open(datafile, "at") as fh:
                                fh.write(f"{self.__data_header}\n")
                                fh.close()
                        with open(datafile, "at") as fh:
                            # add data to file
                            fh.write(f"{data}\n")
                            fh.close()

                    index = index - 10

                # stage data for transfer
                root = os.path.join(self.__staging, os.path.basename(self.__datadir))
                os.makedirs(root, exist_ok=True)
                if self.__zip:
                    # create zip file
                    archive = os.path.join(root, "".join([os.path.basename(datafile[:-4]), ".zip"]))
                    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                        fh.write(datafile, os.path.basename(datafile))
                else:
                    shutil.copyfile(datafile, os.path.join(root, os.path.basename(datafile)))

            return 0

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


if __name__ == "__main__":
    pass
