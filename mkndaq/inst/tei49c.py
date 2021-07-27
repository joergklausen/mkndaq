# -*- coding: utf-8 -*-

import os
import serial
import time
import logging

class TEI49C:
    """
    Instrument of type Thermo TEI 49C.

    Instrument of type Thermo TEI 49C with methods, attributes for interaction.
    """

    def __init__(self, name, port, config, log=True, test=True) -> None:
        """
        Initialize instrument class.
        
        Parameters
        ----------
        name : str
            name of instrument
        port : str
            string specifying the (serial) port to use for communication
        config : dict
            dictionary of attributes defining the instrument and port
        """
        try:                        
            self._test = test
            # setup logging
            self._log = log
            if log:
                logs = os.path.expanduser(config['logs'])
                os.makedirs(logs, exist_ok=True)
                logfile = '%s.log' % time.strftime('%Y%m%d')
                self.logfile = os.path.join(logs, logfile)
                self.logger = logging.getLogger(__name__)
                logging.basicConfig(level=logging.DEBUG,
                                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                                    datefmt='%y-%m-%d %H:%M:%S',
                                    filename=str(self.logfile),
                                    filemode='a')

            # configure serial port
            ser = serial.Serial()
            ser.port = port
            ser.baudrate = config[port]['baudrate']
            ser.bytesize = config[port]['bytesize']
            ser.parity = config[port]['parity']
            ser.stopbits = config[port]['stopbits']
            ser.timeout = config[port]['timeout']
            ser.open()
            if ser.is_open == True:
                ser.close()
            else:
                raise
            self._serial = ser
            
            # read instrument control properties for later use
            self._name = name
            self._id = config[name]['id'] + 128
            self._type = config[name]['type']
            self._serial_number = config[name]['serial_number']
            self._get_config = ["mode",
                                "gas unit", 
                                "range", 
                                "avg time", 
                                "temp comp", 
                                "pres comp", 
                                "format", 
                                "lrec format"]
            self._set_config = ["set mode remote", 
                                "set gas unit ppb", 
                                "set range 1", 
                                "set avg time 3", 
                                "set temp comp on", 
                                "set pres comp on", 
                                "set format 00", 
                                "set lrec format 01 02"]
            self._get_data = "lrec"
            
            # setup data directory
            datadir = os.path.expanduser(config['data'])
            self._datadir = os.path.join(datadir, name)
            os.makedirs(self._datadir, exist_ok=True)

            # sampling, aggregation, reporting/storage
            self._sampling_interval = config[name]['sampling_interval']
            self._aggregation_period = config[name]['aggregation_period']
            self._reporting_interval = config[name]['reporting_interval']

        except Exception as err:
            ser.close()
            self.logger.error(err)
            print(err)
                    
            
    def get_config(self) -> dict:
        """
        Read current configuration of instrument.
        
        Read current configuration of instrument and optionally write to log
        

        Returns
        -------
        dict
            configuration or errors, if any.

        """
        try:                        
            err = None
            cfg = []
            self._serial.open()
            for cmd in self._get_config:
                if self._test:
                    print("Echo: %s" % cmd)
                    cfg.append(cmd)
                else:
                    self._serial.write(bytes([self._id]) + ('%s\x0D' % cmd).encode())
                    cfg.append(self._serial.read(256).decode())
            self._serial.close()

            if self._log:
                self.logger.info("Current configuration of '%s': %s" % (self._name, cfg))        
            
            return(err, cfg)

        except Exception as err:
            self._serial.close()
            return(err, cfg)            
    
    def set_config(self):
        """
        Set configuration of instrument
        
        Set configuration of instrument and optionally write to log

        Parameters
        ----------
        log : bln, optional
            Should output be written to logfile? The default is True.

        Returns
        -------
        dictionary with configuration.

        """
        try:                        
            cfg = []
            self._serial.open()
            for cmd in self._set_config:
                if self._test:
                    print("Echo: %s" % cmd)
                    cfg.append(cmd)
                else:
                    self._serial.write(bytes([self._id]) + ('%s\x0D' % cmd).encode())
                    cfg.append(self._serial.read(256).decode())
            self._serial.close()
            if self._log:
                self.logger.info("Configuration of '%s' set to: %s" % (self._name, cfg))        
            return(cfg)
        except Exception as err:
            self.logger.error(err)
            self._serial.close()


    def get_data(self):
        """
        Retrieve data from instrument.
        
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
            # retrieve data
            self._serial.open()
            for cmd in self._get_data:
                if self._test:
                    print("Echo: %s" % cmd)
                    res.append(cmd)
                else:
                    self._serial.write(bytes([self._id]) + ('%s\x0D' % cmd).encode())
                    res.append(self._serial.read(256).decode())
            self._serial.close()
            if self._log:
                self.logger.info("Data retrieved from '%s': %s" % (self._name, res))
                            
            return(res)
        except Exception as err:
            self.logger.error(err)
            self._serial.close()


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


