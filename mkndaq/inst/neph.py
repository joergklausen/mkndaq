"""
Define a class NE300 facilitating communication with a Acoem NE-300 nephelometer.

@author: joerg.klausen@meteoswiss.ch
"""

import os
import datetime
import logging
import shutil
import socket
import re
import time
import zipfile

import colorama

from mkndaq.utils import datetimebin

class NEPH:
    """
    Instrument of type Acoem NE-300 or Ecotech Aurora 3000 nephelometer with methods, attributes for interaction.
    """
    # __datadir = ""
    # __datafile = ""
    # __datafile_to_stage = None
    # __logdir = None
    # __serial_id = None
    # __logfile = None
    # __logfile_to_stage = None
    # _log = None
    # _logger = None
    # __name = None
    # __reporting_interval = None
    # __set_datetime = None
    # __sockaddr = ""
    # __socksleep = None
    # __socktout = None
    # __staging = None
    # __zip = False
    # __protocol = None

    def __init__(self, name: str, config: dict, simulate=False) -> None:
        """
        Initialize instrument class.

        :param name: name of instrument
        :param config: dictionary of attributes defining instrument, serial port and more
            - config[name]['type']
            - config[name]['serial_number']
            - config[name]['serial_id']
            - config[name]['socket']['host']
            - config[name]['socket']['port']
            - config[name]['socket']['timeout']
            - config[name]['socket']['sleep']
            - config['logs']
            - config[name]['sampling_interval']
            - config['staging']['path'])
            - config[name]['staging_zip']
            - config['protocol']
        """
        colorama.init(autoreset=True)

        try:
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
            self._type = config[name]['type']
            self.__serial_number = config[name]['serial_number']
            self.__serial_id = config[name]['serial_id']

            # configure tcp/ip
            self.__sockaddr = (config[name]['socket']['host'],
                             config[name]['socket']['port'])
            self.__socktout = config[name]['socket']['timeout']
            # self.__socksleep = config[name]['socket']['sleep']

            # configure comms protocol
            if config[name]['protocol'] in ["acoem", "legacy"]:
                self.__protocol = config[name]['protocol']
            else:
                raise ValueError("Communication protocol not recognized.")

            # sampling, aggregation, reporting/storage
            # self._sampling_interval = config[name]['sampling_interval']
            self.__reporting_interval = config['reporting_interval']

            # setup data and log directory
            datadir = os.path.expanduser(config['data'])
            self.__datadir = os.path.join(datadir, name, "data")
            os.makedirs(self.__datadir, exist_ok=True)
            self.__logdir = os.path.join(datadir, name, "logs")
            os.makedirs(self.__logdir, exist_ok=True)

            # staging area for files to be transfered
            self.__staging = os.path.expanduser(config['staging']['path'])
            self.__zip = config[name]['staging_zip']

            self.__verbosity = config[name]['verbosity']

            if self.__verbosity>0:
                print(f"# Initialize NEPH (name: {self.__name}  S/N: {self.__serial_number})")

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def __checksum(self, x: bytes) -> bytes:
        """
        Compute the checksum of all bytes except checksum and EOT by XORing bytes from left 
        to right. (Reference: Aurora NE Series User Manual v1.2 Appendix A.1)

        Args:
            x (bytes): Bytes 1..(N-2)

        Returns:
            bytes: Checksum (1 Byte)
        """
        try:
            cs = bytes([0])
            for i in range(len(x)):
                cs = bytes([a^b for a,b in zip(cs, bytes([x[i]]))])
            return cs

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def __acoem_timestamp_to_datetime(self, token: bytes) -> datetime.datetime:
        try:
            dtm = token[0]
            SS = dtm % 64
            dtm = dtm // 64
            MM = dtm % 64
            dtm = dtm // 64
            HH = dtm % 32
            dtm = dtm // 32
            dd = dtm % 32
            dtm = dtm // 32
            mm = dtm % 16
            yyyy = dtm // 16 + 2000

            return datetime.datetime(yyyy, mm, dd, HH, MM, SS)

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def __acoem_datetime_to_timestamp(self, dtm: datetime.datetime=datetime.datetime.now()) -> bytes:
        try:
            SS = bin(dtm.time().second)[2:].zfill(6)
            MM = bin(dtm.time().minute)[2:].zfill(6)
            HH = bin(dtm.time().hour)[2:].zfill(5)
            dd = bin(dtm.date().day)[2:].zfill(5)
            mm = bin(dtm.date().month)[2:].zfill(4)
            yyyy = bin(dtm.date().year - 2000).zfill(6)

            return (int(yyyy + mm + dd + HH + MM + SS, base=2)).to_bytes(4)

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def __legacy_timestamp_to_datetime(self, fmt: str, dte: str, tme: str) -> datetime.datetime:
        """Convert a legacy timestamp to datetime

        Args:
            fmt (str): date reporting format as string: D/M/Y, M/D/Y or Y-M-D (where D=Day, M=Month, Y=Year)
            dte (str): instrument date
            tme (str): instrument time as %H:%M:%S

        Returns:
            datetime.datetime: instrument date and time
        """
        try:
            if fmt=="D/M/Y":
                fmt = "%d/%m%y"
            elif fmt=="M/D/Y":
                fmt = "%m/%d/%y"
            elif fmt=="Y-M-D":
                fmt = "%y-%m-%d"
            else:
                raise ValueError("'fmt' not recognized.")
            return datetime.datetime.strptime(f"{dte} {tme}", f"{fmt} %H:%M:%S")

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def __acoem_construct_parameter_id(self, base_id: int, wavelength: int, angle: int) -> int:
        """
        Construct ACOEM measurement parameter id. Cf. ACOEM manual for details.
        Parameter ID = Base ID * 1,000,000 + Wavelength * 1,000 + Angle

        Args:
            base_id (int): cf. ACOEM manual Table 45 - Aurora Base IDs for Constructed Parameters
            wavelength (int): One of <450|525|635>. Defaults to None.
            angle (int): 0 for Fullscatter, 90 for Backscatter

        Returns:
            int: _description_
        """
        return base_id * 1000000 + wavelength * 1000 + angle

    def __acoem_construct_message(self, command: int, parameter_id: int=None, payload: bytes=None) -> bytes:
        """
        Construct ACOEM packet to be sent to instrument. This is fairly involved and we refer to the ACOEM manual for explanations.
        
        Byte  |1  |2  |3  |4  |5..6     |7..10    |11       |12
              |STX|SID|CMD|ETX|msg_len  |msg_data |checksum |EOT
        STX = chr(2)
        SID = serial_id
        CMD = command
        ETX = chr(3)
        msg_len = message length
        msg_data = message data
        EOT = chr(4)

        Args:
            command (int): cf. ACOEM manual Table 19 - List of Commands
            parameter (int, optional): cf. ACOEM manual Table 46 - Aurora Parameters. Defaults to None.
            payload (int, optional): _description_. Defaults to None.

        Returns:
            bytes: _description_
        """
        msg_data = bytes()
        if parameter_id:
            msg_data = (parameter_id).to_bytes(4)
        if payload:
            msg_data += payload
        msg_len = len(msg_data)
        # if msg_len==0:
        #     msg_data = bytes([0])
        msg = bytes([2, self.__serial_id, command, 3]) + (msg_len).to_bytes(2) + msg_data
        return msg + self.__checksum(msg) + bytes([4])


    def tcpip_comm2(self, message: bytes, verbosity: int=0) -> (list[int], bytes):
        """
        Send and receive data using ACOEM protocol
        
        Args:
            command (str): Command to be sent (valid commands depend on protocol used)
            message_data (bytes, optional): Message data as required by the ACOEM protocol. Defaults to None.
            verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

        Raises:
            ValueError: if protocol is unknown.

        Returns:
            list[int]: list of items returned.
        """
        # if self.__protocol=="acoem":
        #     command = int(command)
        #     if message_data:
        #         msgl = len(message_data)
        #         msg = bytes([2, self.__serial_id, command, 3, 0, msgl]) + message_data
        #     else:
        #         msg = bytes([2, self.__serial_id, command, 3, 0, 0])
        #     checksum = self.__checksum(msg)
        #     msg += checksum + EOT
        # elif self.__protocol=="legacy":
        #     print("whatever needs to be done to use the legacy protocol ...")
        # else:
        #     raise ValueError("Communication protocol unknown")

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM, ) as s:
            # connect to the server
            s.settimeout(self.__socktout)
            s.connect(self.__sockaddr)

            # clear buffer (at least the first 1024 bytes, should be sufficient)
            s.recv(1024)

            # send message
            if verbosity>0:
                print(f"message sent    : {message}")
            s.sendall(message)

            # receive response
            rcvd = b''
            while True:
                try:
                    data = s.recv(256)
                    rcvd += data
                    if EOT in rcvd:
                        break
                except:
                    break
            if verbosity>1:
            #     print(f"response (raw)  : {rcvd}")
            # # rcvd = rcvd.replace(b'\xff\xfb\x01\xff\xfe\x01\xff\xfb\x03', b'')

            # if verbosity>1:
                print(f"response (bytes): {rcvd}")

            response_length = int(int.from_bytes(rcvd[4:6]) / 4)
            if verbosity>1:
                print(f"response length : {response_length}")
            
            response = []
            for i in range(6, (response_length + 1) * 4 + 2, 4):
                item = int.from_bytes(rcvd[i:(i+4)])
                itm2 = float.fr
                if verbosity>1:
                    print(f"response item{(i-2)/4:3.0f}: {item}")
                response.append(item)
        return response, rcvd
    
    
    def tcpip_comm(self, command: str, message_data: bytes=None, verbosity: int=0) -> list[int]:
        """
        Send and receive data using ACOEM protocol
        
        Byte  |1  |2  |3  |4  |5..6     |7..10    |11       |12
              |STX|SID|CMD|ETX|msg_len  |msg_data |checksum |EOT
        STX = chr(2)
        SID = serial_id
        CMD = command
        ETX = chr(3)
        msg_len = message length
        msg_data = message data
        EOT = chr(4)

        Args:
            command (str): Command to be sent (valid commands depend on protocol used)
            message_data (bytes, optional): Message data as required by the ACOEM protocol. Defaults to None.
            verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

        Raises:
            ValueError: if protocol is unknown.

        Returns:
            list[int]: list of items returned.
        """
        EOT = bytes([4])

        if self.__protocol=="acoem":
            command = int(command)
            if message_data:
                msgl = len(message_data)
                msg = bytes([2, self.__serial_id, command, 3, 0, msgl]) + message_data
            else:
                msg = bytes([2, self.__serial_id, command, 3, 0, 0])
            checksum = self.__checksum(msg)
            msg += checksum + EOT
        elif self.__protocol=="legacy":
            print("whatever needs to be done to use the legacy protocol ...")
        else:
            raise ValueError("Communication protocol unknown")

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM, ) as s:
            # connect to the server
            s.settimeout(self.__socktout)
            s.connect(self.__sockaddr)

            # clear buffer (at least the first 1024 bytes, should be sufficient)
            s.recv(1024)

            # send message
            if verbosity>0:
                print(f"message sent    : {msg}")
            s.sendall(msg)

            # receive response
            rcvd = b''
            while True:
                try:
                    data = s.recv(1024)
                    rcvd = rcvd + data
                    if EOT in rcvd:
                        break
                except:
                    break
            if verbosity>1:
                print(f"response (raw)  : {rcvd}")
            rcvd = rcvd.replace(b'\xff\xfb\x01\xff\xfe\x01\xff\xfb\x03', b'')

            if verbosity>1:
                print(f"response (clean): {rcvd}")

            response_length = int(int.from_bytes(rcvd[4:6]) / 4)
            if verbosity>1:
                print(f"response length : {response_length}")
            
            response = []
            for i in range(6, (response_length + 1) * 4 + 2, 4):
                item = int.from_bytes(rcvd[i:(i+4)])
                if verbosity>1:
                    print(f"response item{(i-2)/4:3.0f}: {item}")
                response.append(item)
        return response
    
    
    # commands = {
    #     # 'get_error': bytes([0]),
    #     'get_instr_type': 1,
    #     'get_version': 2,
    #     'reset': 3,
    #     'get_values': 4,
    #     }
    def get_logging_config(self, verbosity: int=0) -> dict:
        message = self.__acoem_construct_message(6)
        return self.tcpip_comm2(message, verbosity=verbosity)


    def get_instr_type(self, verbosity: int=0) -> dict:
        message = self.__acoem_construct_message(1)
        return self.tcpip_comm2(message, verbosity=verbosity)


    def get_logged_data(self, start: datetime.datetime, end: datetime.datetime, verbosity: int=0) -> dict:
        # initial request
        if start:
            payload = self.__acoem_datetime_to_timestamp(start)
            if end:
                payload += self.__acoem_datetime_to_timestamp(end)
        else:
            raise ValueError("start and/or end date not valid.")
        message = self.__acoem_construct_message(command=7, payload=payload)
        return self.tcpip_comm2(message, verbosity=verbosity)


    def get_current_operation(self, verbosity: int=0) -> None:
        """_summary_

        Args:
            verbosity (int, optional): _description_. Defaults to 0.
        """
        parameter_id = 4035
        message = self.__acoem_construct_message(command=4, parameter_id=parameter_id)
        return self.tcpip_comm2(message, verbosity=verbosity)

        
    def set_current_operation(self, state: int=0, verbosity: int=0) -> None:
        """_summary_

        Args:
            state (int, optional): 0: ambient, 1: zero, 2: span. Defaults to 0.
            verbosity (int, optional): _description_. Defaults to 0.
        """
        # set operating state
        payload = bytes([0,0,0,state])
        parameter_id = 4035
        message = self.__acoem_construct_message(command=5, parameter_id=parameter_id, payload=payload)
        self.tcpip_comm2(message, verbosity=verbosity)

        # wait for valve action to be completed by polling operating state
        message = self.__acoem_construct_message(command=4, parameter_id=parameter_id)
        while True:
            if res := self.tcpip_comm2(message, verbosity=verbosity)[0]:
                return res


    def get_id(self, verbosity: int=0) -> str:
        """Get instrument type, s/w, firmware versions

        Parameters:
            verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

        Returns:
            str: response depends on protocol
        """
        try:
            if self.__protocol=="acoem":
                instr_type = self.tcpip_comm(command='1', verbosity=verbosity)
                version = self.tcpip_comm(command='2', verbosity=verbosity)
                resp = dict(zip(['Model', 'Variant', 'Sub-Type', 'Range', 'Build', 'Branch'], instr_type + version))
            elif self.__protocol=="legacy":
                resp = self.tcpip_comm(command=f"ID{self.__serial_id}", verbosity=verbosity)
            else:
                raise ValueError("Communication protocol unknown")

            self._logger.info(f"get_id: {resp}")
            return resp

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def get_datetime(self, verbosity: int=0) -> datetime.datetime:
        """Get date and time of instrument

        Parameters:
            verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

        Returns:
            datetime.datetime: Date and time of instrument
        """
        try:
            if self.__protocol=="acoem":
                resp = self.tcpip_comm(command='4', message_data=bytes([0,0,0,1]), verbosity=verbosity)
                resp = self.__acoem_timestamp_to_datetime(resp)
            else:
                fmt = self.tcpip_comm(f"VI{self.__serial_id}64", verbosity=verbosity)
                dte = self.tcpip_comm(f"VI{self.__serial_id}80", verbosity=verbosity)
                tme = self.tcpip_comm(f"VI{self.__serial_id}81", verbosity=verbosity)
                resp = self.__legacy_timestamp_to_datetime(fmt, dte, tme)

            self._logger.info(f"get_datetime: {resp}")
            return resp

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def set_datetime(self, dtm: datetime.datetime=time.gmtime(), verbosity: int=0) -> None:
        """Set date and time of instrument

        Parameters:
            dtm (datetime.datetime, optional): Date and time to be set. Defaults to time.gmtime().
            verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

        Returns:
            None
        """
        try:
            if self.__protocol=="acoem":
                message_data = bytes([0,0,0,1]) + self.__acoem_datetime_to_timestamp(dtm=dtm)
                resp = self.tcpip_comm(command='5', message_data=message_data)
            else:
                resp = self.tcpip_comm(f"**{self.__serial_id}S{dtm.strftime('%H%M%S%d%m%y')}")
                msg = f"DateTime of instrument {self.__name} set to {dtm} ... {resp}"
                print(f"{dtm} {msg}")
                self._logger.info(msg)
            return resp
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def do_span_check(self, verbosity: int=0) -> (int, str):
        """
        Override digital IO control and DOSPAN.

        Parameters:
            serial_id (str, optional): Defaults to '0'.

        Returns:
            int: 0 if no error
            str: OK
        """
        try:
            resp = self.tcpip_comm(f"DO{serial_id}001")
            if resp=="OK":
                msg = f"Force instrument {self.__name} into SPAN mode"
                print(msg)
                self._logger.info(msg)
                return 0, resp
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def do_zero_check(self, serial_id: str="0", verbosity: int=0) -> (int, str):
        """
        Override digital IO control and DOZERO.

        Parameters:
            serial_id (str, optional): Defaults to '0'.

        Returns:
            int: 0 if no error
            str: OK
        """
        try:
            resp = self.tcpip_comm(f"DO{serial_id}011")
            if resp=="OK":
                msg = f"Force instrument {self.__name} into ZERO mode"
                print(msg)
                self._logger.info(msg)
                return 0, resp
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def do_ambient(self, serial_id: str="0", verbosity: int=0) -> (int, str):
        """
        Override digital IO control and return to ambient measurement.

        Parameters:
            serial_id (str, optional): Defaults to '0'.

        Returns:
            int: 0 if no error
            str: OK
        """
        try:
            resp = self.tcpip_comm(f"DO{serial_id}000")
            if resp=="OK":
                msg = f"Force instrument {self.__name} into AMBIENT mode"
                print(msg)
                self._logger.info(msg)
                return 0, resp
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def get_status_word(self, serial_id: str="0", verbosity: int=0) -> (int, str):
        """
        Read the System status of the Aurora 3000 microprocessor board. The status word 
        is the status of the nephelometer in hexadecimal converted to decimal.

        Parameters:
            serial_id (str, optional): Defaults to '0'.

        Returns:
            int: 0 if no error
            str: {<STATUS WORD>}
        """
        try:
            resp = self.tcpip_comm(f"VI{serial_id}88")
            if resp:
                msg = f"Instrument {self.__name} status: {resp}."
                print(msg)
                self._logger.info(msg)
                return 0, resp
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def get_all_data(self, serial_id: str="0", verbosity: int=0) -> (int, str):
        """
        Read the System status of the Aurora 3000 microprocessor board. The status word 
        is the status of the nephelometer in hexadecimal converted to decimal.

        Parameters:
            serial_id (str, optional): Defaults to '0'.

        Returns:
            int: 0 if no error
            str: {<STATUS WORD>}
        """
        try:
            resp = self.tcpip_comm(f"***R")
            resp = self.tcpip_comm(f"***D")
            if resp:
                msg = f"Instrument {self.__name} status: {resp}."
                print(msg)
                self._logger.info(msg)
                return 0, resp
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def get_data(self, serial_id: str="0", get_status_word=True, sep: str=",", save: bool=True, verbosity: int=0) -> (int, str):
        """
        Retrieve latest reading on one line

        Parameters:
            serial_id (str, optional): Defaults to '0'.

        Returns:
            int: 0 if no error
            str: {<date>},{<time>},{< σsp 1>}, {<σsp 2>}, {<σsp 3>}, {<σbsp 1>}, {<σbsp 2>}, {<σbsp 3>},{<sampletemp>},{<enclosure temp>},{<RH>},{<pressure>},{<major state>},{<DIO state>}<CR><LF>        
        """
        try:
            resp = self.tcpip_comm(f"VI{serial_id}99")
            resp = resp.replace(", ", ",")
            if get_status_word:
                resp += f",{self.get_status_word()}"
            resp = resp.replace(",", sep)

            dtm = time.strftime('%Y-%m-%d %H:%M:%S')
            print(f"{dtm} .get_data (name={self.__name}, save={save})")

            # read the latest record from the instrument
            if resp:
                if save:
                    # generate the datafile name
                    self.__datafile = os.path.join(self.__datadir, time.strftime("%Y"), time.strftime("%m"), time.strftime("%d"),
                                                "".join([self.__name, "-",
                                                        datetimebin.dtbin(self.__reporting_interval), ".dat"]))
                    os.makedirs(os.path.dirname(self.__datafile), exist_ok=True)
                    with open(self.__datafile, "at", encoding='utf8') as fh:
                        fh.write(resp)
                        fh.close()

                    # stage data for transfer
                    self.stage_data_file()
                return 0, resp

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def get_all_new_data(self, serial_id: str="0", get_status_word=True, sep: str=",", save: bool=True, verbosity: int=0) -> (int, str):
        """
        Retrieve all readings from current cursor

        Parameters:
            serial_id (str, optional): Defaults to '0'.

        Returns:
            int: 0 if no error
            str: {<date>},{<time>},{< σsp 1>}, {<σsp 2>}, {<σsp 3>}, {<σbsp 1>}, {<σbsp 2>}, {<σbsp 3>},{<sampletemp>},{<enclosure temp>},{<RH>},{<pressure>},{<major state>},{<DIO state>}<CR><LF>        
        """
        try:
            resp = self.tcpip_comm(f"***D")
            resp = resp.replace(", ", ",")
            # if get_status_word:
            #     resp += f",{self.get_status_word()}"
            resp = resp.replace(",", sep)

            dtm = time.strftime('%Y-%m-%d %H:%M:%S')
            print(f"{dtm} .get_data (name={self.__name}, save={save})")

            # read the latest record from the instrument
            if resp:
                if save:
                    # generate the datafile name
                    self.__datafile = os.path.join(self.__datadir, time.strftime("%Y"), time.strftime("%m"), time.strftime("%d"),
                                                "".join([self.__name, "-",
                                                        datetimebin.dtbin(self.__reporting_interval), ".dat"]))
                    os.makedirs(os.path.dirname(self.__datafile), exist_ok=True)
                    with open(self.__datafile, "at", encoding='utf8') as fh:
                        fh.write(resp)
                        fh.close()

                    # stage data for transfer
                    self.stage_data_file()
                return 0, resp

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def stage_data_file(self) -> None:
        """Stage a file if it is no longer written to. This is determined by checking if the path 
           of the file to be staged is different from the path of the current (data)file.

        Raises:
            ValueError: _description_
            ValueError: _description_
            ValueError: _description_
        """
        try:
            if self.__datafile is None:
                raise ValueError("__datafile cannot be None.")
            if self.__staging is None:
                raise ValueError("__staging cannot be None.")
            if self.__datadir is None:
                raise ValueError("__datadir cannot be None.")
            if self.__datafile_to_stage is None:
                self.__datafile_to_stage = self.__datafile
            elif self.__datafile_to_stage != self.__datafile:
                root = os.path.join(self.__staging, self.__name, os.path.basename(self.__datadir))
                os.makedirs(root, exist_ok=True)
                if self.__zip:
                    # create zip file
                    archive = os.path.join(root, "".join([os.path.basename(self.__datafile_to_stage)[:-4], ".zip"]))
                    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                        zf.write(self.__datafile_to_stage, os.path.basename(self.__datafile_to_stage))
                else:
                    shutil.copyfile(self.__datafile_to_stage, os.path.join(root, os.path.basename(self.__datafile_to_stage)))
                self.__datafile_to_stage = self.__datafile

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def print_data(self, serial_id: str="0") -> None:
        """Retrieve current record and print."""
        try:
            # read the last record from the Data table
            data = self.tcpip_comm(f"VI{serial_id}99", tidy=True)
            print(colorama.Fore.GREEN + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self.__name}] {data}")

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(colorama.Fore.RED + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self.__name}] produced error {err}.")


    def stage_log_file(self) -> None:
        """Stage a file if it is no longer written to. This is determined by checking if the path 
           of the file to be staged is different the path of the current (data)file.

        Raises:
            ValueError: _description_
            ValueError: _description_
            ValueError: _description_
        """
        try:
            if self.__logfile is None:
                raise ValueError("__logfile cannot be None.")
            if self.__staging is None:
                raise ValueError("__staging cannot be None.")
            if self.__logdir is None:
                raise ValueError("__logdir cannot be None.")

            if self.__logfile_to_stage is None:
                self.__logfile_to_stage = self.__logfile
            elif self.__logfile_to_stage != self.__logfile:
                root = os.path.join(self.__staging, self.__name, os.path.basename(self.__logdir))
                os.makedirs(root, exist_ok=True)
                if self.__zip:
                    # create zip file
                    archive = os.path.join(root, "".join([os.path.basename(self.__logfile_to_stage)[:-4], ".zip"]))
                    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                        zf.write(self.__logfile_to_stage, os.path.basename(self.__logfile_to_stage))
                else:
                    shutil.copyfile(self.__logfile_to_stage, os.path.join(root, os.path.basename(self.__logfile_to_stage)))
                self.__logfile_to_stage = self.__logfile

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)



# %%
if __name__ == "__main__":
    pass