# -*- coding: utf-8 -*-

import os
import socket
import time
import logging
from ftplib import FTP

class G2401:
    """
    Instrument of type Picarro G2401.

    Instrument of type Picarro G2410 with methods, attributes for interaction.
    """

    def __init__(self, name, config):
        """
        Constructor

        Parameters
        ----------
        name : str
            name of instrument as defined in config file
        config : dict
            dictionary of attributes defining the instrument and port
        """
        try:                        
            # setup logging
            logdir = os.path.expanduser(config['logging'])
            os.makedirs(logdir, exist_ok=True)
            logfile = '%s.log' % time.strftime('%Y%m%d')
            self.logfile = os.path.join(logdir, logfile)
            self.logger = logging.getLogger(__name__)
            logging.basicConfig(level=logging.DEBUG,
                                format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                                datefmt='%y-%m-%d %H:%M:%S',
                                filename=str(self.logfile),
                                filemode='a')

            # configure socket access
            self._socket_host = config[name]['socket']['host']
            self._socket_port = config[name]['socket']['port']
            self._socket_timeout = config[name]['socket']['timeout']
            
            # configure ftp access
            self._ftp_host = config[name]['ftp']['host']
            self._ftp_port = config[name]['ftp']['port']
            self._ftp_usr = config[name]['ftp']['usr']
            self._ftp_pwd = config[name]['ftp']['pwd']
            self._ftp_path = config[name]['ftp']['path']
            self._ftp_archive_on_server = config[name]['ftp']['archive_on_server']

            # read instrument control properties for later use
            self._name = name
            self._type = config[name]['type']
            self._serial_number = config[name]['serial_number']
            self._get_data = config[name]['get_data']
            
            # setup data directory
            datadir = os.path.expanduser(config['data'])
            self._datadir = os.path.join(datadir, name)
            os.makedirs(self._datadir, exist_ok=True)

            # sampling, aggregation, reporting/storage
            self._sampling_interval = config[name]['sampling_interval']
            self._aggregation_period = config[name]['aggregation_period']
            self._reporting_interval = config[name]['reporting_interval']

        except Exception as err:
            self.logger.error(err)                    
            

    @classmethod
    def get_data(self, log=False):
        """
        Retrieve data from instrument
        
        Retrieve data from instrument and optionally write to log

        Parameters
        ----------
        log : bln, optional
            Should output be written to logfile? The default is False.

        Returns
        -------
        raw response from instrument.

        """
        try:                        
            res = []
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self._host, self._port))

                for cmd in self._get_data:
                    s.sendall((cmd + chr(13) + chr(10)).encode())
      
                    res.append(s.recv(2048).decode())

            if log:
                self.logger.info("Data retrieved from '%s': %s" % (self._name, res))
                            
            return(res)
        except Exception as err:
            self.logger.error(err)

    @classmethod
    def download_from_ftp_server(self, filename, log=True):
        """
        Download user file from ftp server

        Files are downloaded and - depending on configuration - archived on
        server as well as on local disk.

        Parameters
        ----------
        file : str
            Filename only of file that ought to be downloaded
        log : bln
            Should information be logged? Defaults to True

        Returns
        -------
        path of file if download was successful

        """
        try:
            self.logger.info('Begin downloading files from Picarro ftp server.')

            # create ftp context manager
            with FTP(self._ftp_host) as ftp:
                ftp.login(user=self._ftp_usr, passwd=self._ftp_pwd)

                # harvest sub-directories and save files to local disk
                downloaded = {}
                for path_ in ftp.mlsd():
                    files_ = []
                    # if re.search('[A-D]', path_[0]):
                    #     # change ftp directory
                    #     ftp.cwd(path_[0])

                        # create local directory if it doesn't exists
                        localdir = os.path.join(os.path.expanduser(self.raw),
                                                path_[0])
                        os.makedirs(localdir, exist_ok=True)

                        # download files and save to disk, archive remote files
                        for file_ in ftp.mlsd():
                            if re.search('^min[0-9]+.csv$', file_[0]):
#                                remote_file = os.path.join(self.host,
#                                                           path_[0], file_[0])
                                files_.append(file_[0])
                                local_file = os.path.join(localdir, file_[0])
                                if log:
                                    msg = "Saving file to '" + local_file + "'"
                                    self.logger.info(msg)

                                with open(local_file, 'wb') as f:
                                    ftp.retrbinary('RETR ' + file_[0],
                                                   f.write, 1024)
                                    f.close()

                                # move remote file to archive on server
                                if self.archive_on_server:
                                    try:
                                        ftp.rename(file_[0], 'archive/' + file_[0])
                                    except Exception as err:
                                        msg = "Error: could not archive file '"
                                        msg += path_[0] + '/' + file_[0] + "'."
                                        self.logger.error(msg, err)
                                
                        downloaded[path_[0]] = files_

                        ftp.cwd('..')          

                ftp.quit()

            if log:
                self.logger.info("Finished ftp download %s." % downloaded)
            return(downloaded)

        except Exception as err:
            msg = "'.download_from_ftp_server' error: "
            self.logger.error(msg, err)


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
        
        

    # def save_data(self, reading, log=False):
    #     """
    #     Save data from instrument
        
    #     Save data from instrument and optionally write to log

    #     Parameters
    #     ----------
    #     reading : str
    #         Result of a single call to .get_data()
    #     log : bln, optional
    #         Should output be written to logfile? The default is False.

    #     Returns
    #     -------
    #     file name

    #     """
    #     try:                        
    #         # determine filename
            
            
            
    #         return(res)
    #     except Exception as err:
    #         self.logger.error(err)
    #         self.serial.close()






    #         # save data



    #         self._sampling_interval = config[name]['sampling_interval']
    #         self._aggregation_period = config[name]['aggregation_period']
    #         self._reporting_interval = config[name]['reporting_interval']


    #         if self._file = 
            
    #         with open(_file, 'ab+') as f:
    #             f.write(res)
    #             f.close()


