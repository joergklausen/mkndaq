# -*- coding: utf-8 -*-
"""
Define a class G2401 facilitating communication with a Picarro G2401 instrument.

@author: joerg.klausen@meteoswiss.ch
"""

import os
import socket
import time
import logging
import shutil
from mkndaq.utils.filesync import rsync
import zipfile

import colorama


class G2401:
    """
    Instrument of type Picarro G2401.

    Instrument of type Picarro G2410 with methods, attributes for interaction.
    """

    _datadir = None
    _buckets = None
    _days_to_sync = None
    _log = None
    _logger = None
    _name = None
    _netshare = None
    _source = None
    _socksleep = None
    _sockaddr = None
    _socktout = None
    _staging = None
    _zip = None

    def __init__(self, name: str, config: dict) -> None:
        """
        Constructor

        Parameters
        ----------
        name : str
            name of instrument as defined in config file
        config : dict
            dictionary of attributes defining the instrument and port
        """
        colorama.init(autoreset=True)
        print(f"# Initialize G2401 (name: {name})")

        try:
            # setup logging
            logdir = os.path.expanduser(config['logs'])
            os.makedirs(logdir, exist_ok=True)
            logfile = '%s.log' % time.strftime('%Y%m%d')
            logfile = os.path.join(logdir, logfile)
            self._logger = logging.getLogger(__name__)
            logging.basicConfig(level=logging.DEBUG,
                                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                                datefmt='%y-%m-%d %H:%M:%S',
                                filename=str(logfile),
                                filemode='a')

            # configure tcp/ip
            self._sockaddr = (config[name]['socket']['host'],
                             config[name]['socket']['port'])
            self._socktout = config[name]['socket']['timeout']
            self._socksleep = config[name]['socket']['sleep']

            # read instrument control properties for later use
            self._name = name
            self._type = config[name]['type']
            self._serial_number = config[name]['serial_number']
            # self._get_data = config[name]['get_data']

            # setup data directory
            datadir = os.path.expanduser(config['data'])
            self._datadir = os.path.join(datadir, name)
            os.makedirs(self._datadir, exist_ok=True)

            # reporting/storage
            self._buckets = config[name]['buckets']

            # netshare of user data files
            dbs = r"\\"
            self._netshare = os.path.join(f"{dbs}{config[name]['socket']['host']}", config[name]['netshare'])

            # days up to present for which files should be synched to data directory
            self._days_to_sync = config[name]['days_to_sync']

            # staging area for files to be transfered
            self._staging = os.path.expanduser(config['staging']['path'])
            self._zip = config[name]['staging_zip']

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def tcpip_comm(self, cmd: str, tidy=True) -> str:
        """
        Send a command and retrieve the response. Assumes an open connection.

        :param cmd: command sent to instrument
        :param tidy: remove cmd echo, \n and *\r\x00 from result string, terminate with \n
        :return: response of instrument, decoded
        """
        rcvd = b''
        try:
            # open socket connection as a client
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM, ) as s:
                # connect to the server
                s.settimeout(self._socktout)
                s.connect(self._sockaddr)

                # send data
                s.sendall((cmd + chr(13) + chr(10)).encode())
                time.sleep(self._socksleep)

                # receive response
                while True:
                    try:
                        data = s.recv(1024)
                        rcvd = rcvd + data
                    except:
                        break

            # decode response, tidy
            rcvd = rcvd.decode()
            if tidy:
                if "\n" in rcvd:
                    rcvd = rcvd.split("\n")[0]

            return rcvd

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def print_co2_ch4_co(self) -> None:
        try:
            conc = self.tcpip_comm("_Meas_GetConc").split(';')[0:3]
            print(colorama.Fore.GREEN + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self._name}] CO2 {conc[0]} ppm  CH4 {conc[1]} ppm  CO {conc[1]} ppm")

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(colorama.Fore.RED + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self._name}] produced error {err}.")


    def store_and_stage_files(self):
        """Copy files from source (netshare folder) to target (datadir) and stage them in the staging area for transfer.

        Raises:
            ValueError: raised if buckets is not correctly specified. Based on this, the subfolder structure is assumed.
        """
        sep = os.path.sep
        try:            
            if os.path.exists(self._netshare):
                # copy 'new' files from source to target
                files_received = rsync(source=self._netshare, 
                                        target=self._datadir, 
                                        buckets=self._buckets, 
                                        days=self._days_to_sync)
                
                # stage data for transfer
                for file in files_received:
                    stage = os.path.join(self._staging, self._name)
                    os.makedirs(stage, exist_ok=True)

                    if self._zip:
                        # create zip file
                        archive = os.path.join(stage, "".join([os.path.basename(file)[:-4], ".zip"]))
                        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                            fh.write(file, os.path.basename(file))
                    else:
                        shutil.copyfile(os.path.join(self._datadir, file), os.path.join(stage, os.path.basename(file)))

                    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} .store_and_stage_files (name={self._name}, file={os.path.basename(file)})")

            else:
                msg = f"{time.strftime('%Y-%m-%d %H:%M:%S')} (name={self._name}) Warning: {self._netshare} is not accessible!)"
                if self._log:
                    self._logger.error(msg)
                print(colorama.Fore.RED + msg)

            return

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def get_meas_getconc(self) -> str:
        """
        Retrieve instantaneous data from instrument

        :return:
        """
        try:
            return self.tcpip_comm('_Meas_GetConc')

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def get_co2_ch4_co(self) -> list:
        """
        Get instantaneous cleaned response to '_Meas_GetConc' from instrument.

        :return: list: concentration values from instrument
        """
        try:
            return self.tcpip_comm("_Meas_GetConc").split(';')[0:3]

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)
