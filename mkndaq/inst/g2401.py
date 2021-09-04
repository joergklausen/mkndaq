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
import zipfile

import colorama


class G2401:
    """
    Instrument of type Picarro G2401.

    Instrument of type Picarro G2410 with methods, attributes for interaction.
    """

    _socksleep = None
    _sockaddr = None
    _socktout = None
    _data_storage = None
    _log = None
    _zip = None
    _staging = None
    _netshare = None
    _datadir = None
    _name = None
    _logger = None
    _get_data = None
    _socket_port = None
    _socket_host = None

    @classmethod
    def __init__(cls, name: str, config: dict) -> None:
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
        print("# Initialize G2401")

        try:
            # setup logging
            logdir = os.path.expanduser(config['logs'])
            os.makedirs(logdir, exist_ok=True)
            logfile = '%s.log' % time.strftime('%Y%m%d')
            logfile = os.path.join(logdir, logfile)
            cls._logger = logging.getLogger(__name__)
            logging.basicConfig(level=logging.DEBUG,
                                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                                datefmt='%y-%m-%d %H:%M:%S',
                                filename=str(logfile),
                                filemode='a')

            # configure tcp/ip
            cls._sockaddr = (config[name]['socket']['host'],
                             config[name]['socket']['port'])
            cls._socktout = config[name]['socket']['timeout']
            cls._socksleep = config[name]['socket']['sleep']

            # # configure ftp access
            # self._ftp_host = config[name]['ftp']['host']
            # self._ftp_port = config[name]['ftp']['port']
            # self._ftp_usr = config[name]['ftp']['usr']
            # self._ftp_pwd = config[name]['ftp']['pwd']
            # self._ftp_path = config[name]['ftp']['path']
            # self._ftp_archive_on_server = config[name]['ftp']['archive_on_server']

            # read instrument control properties for later use
            cls._name = name
            cls._type = config[name]['type']
            cls._serial_number = config[name]['serial_number']
            cls._get_data = config[name]['get_data']

            # setup data directory
            datadir = os.path.expanduser(config['data'])
            cls._datadir = os.path.join(datadir, name)
            os.makedirs(cls._datadir, exist_ok=True)

            # reporting/storage
            cls._reporting_interval = config[name]['reporting_interval']
            cls._data_storage = config[name]['data_storage']

            # netshare of user data files
            cls._netshare = os.path.expanduser(config[name]['netshare'])

            # staging area for files to be transfered
            cls._staging = os.path.expanduser(config['staging']['path'])
            cls._zip = config['staging']['zip']

        except Exception as err:
            if cls._log:
                cls._logger.error(err)
            print(err)

    @classmethod
    def tcpip_comm(cls, cmd: str, tidy=True) -> str:
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
                s.settimeout(cls._socktout)
                s.connect(cls._sockaddr)

                # send data
                s.sendall((cmd + chr(13) + chr(10)).encode())
                time.sleep(cls._socksleep)

                # receive response
                while True:
                    data = s.recv(1024)
                    rcvd = rcvd + data
                    if chr(13).encode() in data:
                        break

            # decode response, tidy
            rcvd = rcvd.decode()
            if tidy:
                if "\n" in rcvd:
                    rcvd = rcvd.split("\n")[0]

            return rcvd

        except Exception as err:
            if cls._log:
                cls._logger.error(err)
            print(err)

    @classmethod
    def store_and_stage_latest_file(cls):
        try:
            # get data file from netshare
            if cls._data_storage == 'hourly':
                path = os.path.join(cls._netshare, time.strftime("/%Y/%m/%d"))
            elif cls._data_storage == 'daily':
                path = os.path.join(cls._netshare, time.strftime("/%Y/%m"))
            else:
                raise ValueError("Configuration 'data_storage' of %s must be <hourly|daily>." % cls._name)
            file = max(os.listdir(path))

            # store data file
            shutil.copyfile(os.path.join(path, file), os.path.join(cls._datadir, file))

            # stage data for transfer
            stage = os.path.join(cls._staging, cls._name)
            os.makedirs(stage, exist_ok=True)

            if cls._zip:
                # create zip file
                archive = os.path.join(stage, "".join([file[:-4], ".zip"]))
                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                    fh.write(os.path.join(path, file), file)
            else:
                shutil.copyfile(os.path.join(path, file), os.path.join(stage, file))

            print("%s .store_and_stage_latest_file (name=%s)" % (time.strftime('%Y-%m-%d %H:%M:%S'), cls._name))

        except Exception as err:
            if cls._log:
                cls._logger.error(err)
            print(err)

    @classmethod
    def get_meas_getconc(cls) -> str:
        """
        Retrieve instantaneous data from instrument

        :return:
        """
        try:
            return cls.tcpip_comm('_Meas_GetConc')

        except Exception as err:
            if cls._log:
                cls._logger.error(err)
            print(err)

    @classmethod
    def get_co2_ch4_co(cls) -> list:
        """
        Get instantaneous cleaned response to '_Meas_GetConc' from instrument.

        :return: list: concentration values from instrument
        """
        try:
            return cls.tcpip_comm("_Meas_GetConc").split(';')[0:3]

        except Exception as err:
            if cls._log:
                cls._logger.error(err)
            print(err)

    @classmethod
    def print_co2_ch4_co(cls) -> None:
        try:
            conc = cls.tcpip_comm("_Meas_GetConc").split(';')[0:3]
            print(colorama.Fore.GREEN + "%s [%s] CO2 %s ppm  CH4 %s ppm  CO %s ppm" % \
                  (time.strftime("%Y-%m-%d %H:%M:%S"), cls._name, *conc))

        except Exception as err:
            if cls._log:
                cls._logger.error(err)
            print(err)

    def read_user_file(self, file, log=False):
        """
        Read user file to Pandas data.frame

        Parameters
        ----------
        file : str
            Full path to file
        log : str, optional
            DESCRIPTION. The default is False.

        Returns
        -------
        Pandas data.frame
        
        """
