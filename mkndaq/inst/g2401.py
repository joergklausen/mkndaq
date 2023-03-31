# -*- coding: utf-8 -*-
"""
Define a class G2401 facilitating communication with a Picarro G2401 instrument.

@author: joerg.klausen@meteoswiss.ch
"""

import os
import socket
import datetime
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

    _source = None
    _socksleep = None
    _sockaddr = None
    _socktout = None
    _data_storage_interval = None
    _log = None
    _zip = None
    _staging = None
    _netshare = None
    _datadir = None
    _name = None
    _logger = None
    # _get_data = None
    _socket_port = None
    _socket_host = None


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

            # source of data files
#            self._source = config[name]['source']

            # interval to fetch and stage data files
            self._staging_interval = config[name]['staging_interval']

            # reporting/storage
            # self._reporting_interval = config[name]['reporting_interval']
            self._data_storage_interval = config[name]['data_storage_interval']

            # netshare of user data files
            #self._netshare = os.path.expanduser(config[name]['netshare'])
            dbs = r"\\"
            self._netshare = os.path.join(f"{dbs}{config[name]['socket']['host']}", config[name]['netshare'])
            # print("netshare:", self._netshare)

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
                    # data = s.recv(1024)
                    # rcvd = rcvd + data
                    # if chr(13).encode() in data:
                    #     break
                    # data = s.recv(1024)

                    # if not data:
                    #     break
                    # else:
                    #     rcvd = rcvd + data
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
            # print(colorama.Fore.GREEN + "%s [%s] CO2 %s ppm  CH4 %s ppm  CO %s ppm" % \
            #       (time.strftime("%Y-%m-%d %H:%M:%S"), self._name, *conc))
            print(colorama.Fore.GREEN + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self._name}] CO2 {conc[0]} ppm  CH4 {conc[1]} ppm  CO {conc[1]} ppm")

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(colorama.Fore.RED + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self._name}] produced error {err}.")


    def store_and_stage_new_files(self):
        try:
            # list data files available on netshare
            # retrieve a list of all files on netshare for sync_period, except the latest file (which is presumably still written too)
            # retrieve a list of all files on local disk for sync_period
            # copy and stage files available on netshare but not locally
            
            if self._data_storage_interval == 'hourly':
                ftime = "%Y/%m/%d"
            elif self._data_storage_interval == 'daily':
                ftime = "%Y/%m"
            else:
                raise ValueError(f"Configuration 'data_storage_interval' of {self._name} must be <hourly|daily>.")

            try:
                if os.path.exists(self._netshare):
                    for delta in (0, 1):
                        relative_path = (datetime.datetime.today() - datetime.timedelta(days=delta)).strftime(ftime)
                        netshare_path = os.path.join(self._netshare, relative_path)
                        # local_path = os.path.join(self._datadir, relative_path)
                        local_path = os.path.join(self._datadir, time.strftime("%Y"), time.strftime("%m"), time.strftime("%d"), relative_path)
                        os.makedirs(local_path, exist_ok=True)

                        # files on netshare except the most recent one
                        if delta==0:
                            netshare_files = os.listdir(netshare_path)[:-1]
                        else:
                            netshare_files = os.listdir(netshare_path)

                        # local files
                        local_files = os.listdir(local_path)

                        files_to_copy = set(netshare_files) - set(local_files)

                        for file in files_to_copy:
                            # store data file on local disk
                            shutil.copyfile(os.path.join(netshare_path, file), os.path.join(local_path, file))            

                            # stage data for transfer
                            stage = os.path.join(self._staging, self._name)
                            os.makedirs(stage, exist_ok=True)

                            if self._zip:
                                # create zip file
                                archive = os.path.join(stage, "".join([file[:-4], ".zip"]))
                                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                                    fh.write(os.path.join(local_path, file), file)
                            else:
                                shutil.copyfile(os.path.join(local_path, file), os.path.join(stage, file))

                            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} .store_and_stage_new_files (name={self._name}, file={file})")
                else:
                    msg = f"{time.strftime('%Y-%m-%d %H:%M:%S')} (name={self._name}) Warning: {self._netshare} is not accessible!)"
                    if self._log:
                        self._logger.error(msg)
                    print(colorama.Fore.RED + msg)

            except:
                print(colorama.Fore.RED + f"{time.strftime('%Y-%m-%d %H:%M:%S')} (name={self._name}) Warning: {self._netshare} is not accessible!)")

                return
                
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)

    # Methods below not currently in use

    def store_and_stage_latest_file(self):
        try:
            # get data file from netshare
            if self._data_storage_interval == 'hourly':
                path = os.path.join(self._netshare, time.strftime("/%Y/%m/%d"))
            elif self._data_storage_interval == 'daily':
                path = os.path.join(self._netshare, time.strftime("/%Y/%m"))
            else:
                raise ValueError(f"Configuration 'data_storage_interval' of {self._name} must be <hourly|daily>.")
            file = max(os.listdir(path))

            # store data file on local disk
            shutil.copyfile(os.path.join(path, file), os.path.join(self._datadir, file))

            # stage data for transfer
            stage = os.path.join(self._staging, self._name)
            os.makedirs(stage, exist_ok=True)

            if self._zip:
                # create zip file
                archive = os.path.join(stage, "".join([file[:-4], ".zip"]))
                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                    fh.write(os.path.join(path, file), file)
            else:
                shutil.copyfile(os.path.join(path, file), os.path.join(stage, file))

            print("%s .store_and_stage_latest_file (name=%s)" % (time.strftime('%Y-%m-%d %H:%M:%S'), self._name))

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def store_and_stage_files(self):
        """
        Fetch data files from local source and move to datadir. Zip files and place in staging area.

        :return: None
        """
        try:
            print("%s .store_and_stage_files (name=%s)" % (time.strftime('%Y-%m-%d %H:%M:%S'), self._name))

            # get data file from local source
            files = os.listdir(self._source)

            if files:
                # staging location for transfer
                stage = os.path.join(self._staging, self._name)
                os.makedirs(stage, exist_ok=True)

                # store and stage data files
                for file in files:
                    # stage file
                    if self._zip:
                        # create zip file
                        archive = os.path.join(stage, "".join([file[:-4], ".zip"]))
                        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                            fh.write(os.path.join(self._source, file), file)
                    else:
                        shutil.copyfile(os.path.join(self._source, file), os.path.join(stage, file))

                    # move to data storage location
                    shutil.move(os.path.join(self._source, file), os.path.join(self._datadir, file))

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


    # def read_user_file(self, file, log=False):
    #     """
    #     Read user file to Pandas data.frame

    #     Parameters
    #     ----------
    #     file : str
    #         Full path to file
    #     log : str, optional
    #         DESCRIPTION. The default is False.

    #     Returns
    #     -------
    #     Pandas data.frame
        
    #     """
