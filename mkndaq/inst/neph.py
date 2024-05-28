"""
Define a class NE300 facilitating communication with a Acoem NE-300 nephelometer.

@author: joerg.klausen@meteoswiss.ch
"""

import os
import datetime
import logging
import shutil
import socket
import struct
import time
import zipfile
import timeit
import warnings

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
    # __sockaddr = ""
    # __socksleep = None
    # __socktout = None
    # __staging = None
    # __zip = False
    # __protocol = None

    def __init__(self, name: str, config: dict, verbosity: int=0) -> None:
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
            - config[name]['get_data_interval']
            - config[name]['reporting_interval']
            - config[name]['zero_check_duration']
            - config[name]['span_check_duration']
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
            
            # flag to maintain state to prevent concurrent tcpip communication in a threaded setup
            self.__tcpip_line_is_busy = False

            # configure comms protocol
            if config[name]['protocol'] in ["acoem", "legacy"]:
                self.__protocol = config[name]['protocol']
            else:
                raise ValueError("Communication protocol not recognized.")

            # sampling, aggregation, reporting/storage
            self.__get_data_interval = config[name]['get_data_interval']
            self.__reporting_interval = config['reporting_interval']

            # zero and span check durations
            self.__zero_check_duration = config[name]['zero_check_duration']
            self.__span_check_duration = config[name]['span_check_duration']

            # setup data and log directory
            datadir = os.path.expanduser(config['data'])
            self.__datadir = os.path.join(datadir, name, "data")
            os.makedirs(self.__datadir, exist_ok=True)
            self.__logdir = os.path.join(datadir, name, "logs")
            os.makedirs(self.__logdir, exist_ok=True)

            # staging area for files to be transfered
            self.__staging = os.path.expanduser(config['staging']['path'])
            self.__datafile_to_stage = None
            self.__zip = config[name]['staging_zip']

            self.__verbosity = config[name]['verbosity']

            if self.__verbosity>0:
                print(f"# Initialize NEPH (name: {self.__name}  S/N: {self.__serial_number})")

                id = self.get_id(verbosity=verbosity)
                if id=={}:
                    warnings.warn(f"Could not communicate with instrument. Protocol set to '{self.__protocol}'. Please verify instrument settings.")
                else:
                    print(f"  - Instrument identified itself as '{id}'.")

            # put instrument in ambient mode
            state = self.do_ambient(verbosity=verbosity)
            if state==0:
                if self.__verbosity>0:
                    print(f"  - Instrument current operation: ambient.")
                self._logger.info("Instrument current operation: ambient.")
            else:
                warnings.warn(f"Could not verify {self.__name} measurement mode as 'ambient'.")

            # get dtm from instrument, then set date and time
            self.get_set_datetime(dtm=datetime.datetime.now())

            # get logging config and save to file
            logging_config = self.get_logging_config()
            if self.__verbosity>0:
                print(f"  - Currently logged parametzers: {logging_config}.")
            self._logger.info(f"logging_config: {logging_config}")

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def _acoem_checksum(self, x: bytes) -> bytes:
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
            return b''


    def _acoem_timestamp_to_datetime(self, timestamp: int) -> datetime.datetime:
        try:
            dtm = timestamp
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
            return datetime.datetime(1111, 1, 1)


    def _acoem_datetime_to_timestamp(self, dtm: datetime.datetime=datetime.datetime.now()) -> bytes:
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
            return b''


    def _acoem_construct_parameter_id(self, base_id: int, wavelength: int, angle: int) -> int:
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
    

    def _acoem_construct_message(self, command: int, parameter_id: int=0, payload: bytes=b'') -> bytes:
        """
        Construct ACOEM packet to be sent to instrument. See the ACOEM manual for explanations.
        
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
            parameter (int, optional): cf. ACOEM manual Table 46 - Aurora Parameters. Defaults to 0 (Not a valid parameter).
            payload (int, optional): _description_. Defaults to None.

        Returns:
            bytes: _description_
        """
        msg_data = bytes()
        if parameter_id>0:
            msg_data = (parameter_id).to_bytes(4)
        if len(payload)>0:
            msg_data += payload
        msg_len = len(msg_data)
        msg = bytes([2, self.__serial_id, command, 3]) + (msg_len).to_bytes(2) + msg_data
        return msg + self._acoem_checksum(msg) + bytes([4])
    

    def _acoem_bytes2int(self, response: bytes, verbosity: int=0) -> list[int]:
        """Convert byte response obtained from instrument into integers. 
    

        Args:
            response (bytes): Raw response obtained from instrument
            verbosity (int, optional): _description_. Defaults to 0.

        Returns:
            list[int]: integers corresponding to the bytes returned. NB: The resulting integers may represent IEEE encoded floats, i.e., this conversion is only meaningful for certian responses.
        """
        response_length = int(int.from_bytes(response[4:6]) / 4)
        if verbosity>1:
            print(f"response length : {response_length}")
        
        items = []
        for i in range(6, (response_length + 1) * 4 + 2, 4):
            item = int.from_bytes(response[i:(i+4)])

            if verbosity>1:
                print(f"response item{(i-2)/4:3.0f}: {item}")
            items.append(item)

        return items


    def _acoem_response2values(self, parameters: list[int], response: bytes, verbosity: int=0) -> dict:
        """Convert byte response obtained from instrument into integers, floats or datetime, depending on parameter.     

        Args:
            parameters (list[int]): Parameters requested from instrument
            response (bytes): Raw response obtained from instrument
            verbosity (int, optional): _description_. Defaults to 0.

        Returns:
            dict: dictionary with parameters and corresponding values, decoded. Parameter 1 is decoded to datetime, the others to either int or float.
        """
        data = dict()
        response_length = int(int.from_bytes(response[4:6]) / 4)
        if verbosity>1:
            print(f"response length : {response_length}")

        items_bytes = [response[i:(i+4)] for i in range(6, (response_length + 1) * 4 + 2, 4)]
        if verbosity>1:
            print(f"items: {len(items_bytes)}\nitems (bytes): {items_bytes}")

        if len(parameters)==len(items_bytes):
            data_bytes = dict(zip(parameters, items_bytes))
        else:
            raise ValueError("Number of parameters does not match number of items retrieved from response.")    

        # decode values
        for parameter, item in data_bytes.items():
            if parameter in [1, 2201]:
                data[parameter] = self._acoem_timestamp_to_datetime(int.from_bytes(item))
            elif ((parameter>1000 and parameter<5000) \
                  or (parameter>12000000 and parameter<13000000) \
                  or (parameter>14000000 and parameter<15000000) \
                  or (parameter>14000000 and parameter<15000000) \
                  or (parameter>16000000 and parameter<17000000) \
                  or (parameter>27000000 and parameter<2027000000)):
                data[parameter] = struct.unpack('>i', item)[0]
            else:
                data[parameter] = struct.unpack('>f', item)[0]

        if verbosity==1:
            print(f"response items:\n{data}")
        if verbosity>1:
            print(f"response items (bytes):\n{data_bytes}")
            print(f"response items:\n{data}")

        return data


    def _acoem_decode_logged_data(self, response: bytes, verbosity: int=0) -> list[dict]:
        """Decode the binary response received from the instrument after sending command 7.

        Args:
            response (bytes): See A.3.8 in the manual for more information
            verbosity (int, optional): _description_. Defaults to 0.

        Returns:
            list[dict]: List of dictionaries, where the keys are the parameter ids, and the values are the measured values.
        """
        # data = dict()
        all = []
        if response[2] == 7:
            # command 7 (byte 3)
            message_length = int(int.from_bytes(response[4:6]) / 4)
            response_body = response[6:-2]
            fields_per_record = int.from_bytes(response_body[12:16])
            items_per_record = fields_per_record + 4
            number_of_records = message_length // items_per_record
            if verbosity>1:
                print(f"message length (items): {message_length}")
                print(f"response body length  : {len(response_body)}")
                print(f"response body (bytes) : {response_body}")
                print(f"number of records     : {number_of_records}")

            # parse bytearray into records and records into dict of header record(s) and data records
            records = [response_body[(i*items_per_record*4):((i+1)*(items_per_record*4)-1)] for i in range(number_of_records)]
            keys = []
            values = []
            for i in range(number_of_records):
                if records[i][0]==1:
                    # header record
                    number_of_fields = int.from_bytes(records[i][12:16])
                    keys = [int.from_bytes(records[i][(16 +j*4):(16 + (j+1)*4)]) for j in range(number_of_fields)]
                elif records[i][0]==0:
                    # data record
                    number_of_fields = int.from_bytes(records[i][12:16])
                    values = [records[i][(16 +j*4):(16 + (j+1)*4)] for j in range(number_of_fields)]

                    data = dict(zip(keys, values))
                    for k, v in data.items():
                        data[k] = struct.unpack('>f', v)[0] if (k>1000 and len(v)>0) else v#b''
                    data['logging_interval'] = int.from_bytes(records[i][8:12])
                    data['dtm'] = self._acoem_timestamp_to_datetime(int.from_bytes(records[i][4:8])).strftime('%Y-%m-%d %H:%M:%S')
                    if verbosity==1:
                        print(data)
                    if verbosity>1:
                        print(f"record  {i:2.0f}: {records[i]}")
                        print(f"type    : {records[i][0]}")
                        print(f"inst op : {records[i][0]}")
                        print(f"{i}: keys: {keys}")
                        print(f"{i}: values: {values}")
                        print(data)
                    all.append(data)
            return all
        elif response[2]==0:
            # response error
            return [{'communication_error': self._acoem_decode_error(error_code=response[7])}]
        else:
            return [dict()]


    def _acoem_decode_error(self, error_code: int) -> str:
        """A.3.1 Return description of error code

        Args:
            error_code (bytes): A.3.1. Table 21 error codes

        Returns:
            str: description of error code
        """
        error_map = {0: 'checksum failed',
                     1: 'invalid command byte',
                     2: 'invalid parameter',
                     3: 'invalid message length',
                     4: 'reserved',
                     5: 'reserved',
                     6: 'reserved',
                     7: 'reserved',
                     8: 'media not connected',
                     9: 'media busy',}
        return error_map[error_code]
    

    def _legacy_timestamp_to_date_time(self, fmt: str, dte: str, tme: str) -> datetime.datetime:
        """Convert a legacy timestamp to datetime

        Args:
            fmt (str): date reporting format as string: D/M/Y, M/D/Y or Y-M-D (where D=Day, M=Month, Y=Year)
            dte (str): instrument date
            tme (str): instrument time as %H:%M:%S

        Returns:
            datetime.datetime: instrument date and time
        """
        try:
            fmt = fmt.replace(' ', '').replace('\r\n', '')
            dte = dte.replace('\r\n', '')
            tme = tme.replace('\r\n', '')
            if fmt=="D/M/Y":
                fmt = "%d/%m/%Y"
            elif fmt=="M/D/Y":
                fmt = "%m/%d/%Y"
            elif fmt=="Y-M-D":
                fmt = "%Y-%m-%d"
            else:
                raise ValueError("'fmt' not recognized.")
            return datetime.datetime.strptime(f"{dte} {tme}", f"{fmt} %H:%M:%S")

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)
            return datetime.datetime(1111, 1, 1, 1, 1, 1)
        

    def _acoem_logged_data_to_string(self, data: list[dict], sep: str=',') -> str:
        """Convert data retrieved using the get_logged_data to a string format ready for saving.

        Args:
            data (list[dict]): data retrieved from get_logged_data
            sep (str, optional): Separator to use for export. Defaults to ','.

        Returns:
            str: string consisting of rows of <sep>-separated items
        """
        result = []
        for d in data:
            dtm_value = d.pop('dtm')
            values = [str(dtm_value)] + [str(value) for key, value in d.items()]
            result.append(sep.join(values))

        return '\n'.join(result)


    def tcpip_comm_wait_for_line(self) -> None:                        
        t0 = time.perf_counter()
        while self.__tcpip_line_is_busy:
            time.sleep(0.1)
            if time.perf_counter() > (t0 + 3 * self.__socktout):
                msg = "'tcpip_comm_wait_for_line' timed out!"
                warnings.warn(msg)
                self._logger.warning(msg)
                break
        return


    def tcpip_comm(self, message: bytes, expect_response: bool=True, verbosity: int=0) -> bytes:
        """
        Send and receive data using ACOEM protocol
        
        Args:
            message (bytes): message data as required by the ACOEM protocol.
            expect_repsonse (bool, optional): If True, read response after sending message. Defaults to True.
            verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

        Raises:
            ValueError: if protocol is unknown.

        Returns:
            bytes: bytes returned.
        """
        try:
            # prevent other callers from proceeding if they require to send and retrieve
            self.__tcpip_line_is_busy = True

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM, ) as s:
                # connect to the server
                s.settimeout(self.__socktout)
                s.connect(self.__sockaddr)

                # clear buffer (at least the first 1024 bytes, should be sufficient)
                # tmp = s.recv(128)
                start = time.perf_counter()

                # send message
                if verbosity>0:
                    print(f"message sent: {message}")
                s.sendall(message)

                # receive response
                rcvd = b''
                if expect_response:
                    if self.__protocol=='acoem':
                        while not b'\x04' in rcvd:
                        # while not rcvd.endswith(b'\x04'):
                            data = s.recv(1024)
                            if not data:
                                break
                            rcvd += data
                    elif self.__protocol=='legacy':
                        while not (rcvd.endswith(b'\r\n') or rcvd.endswith(b'\r\n\n')):
                            data = s.recv(1024)
                            rcvd += data
                    else:
                        raise ValueError('Protocol not recognized.')
                    rcvd = rcvd.strip()
                    # remove pre-ambel
                    rcvd = rcvd.replace(b'\xff\xfb\x01\xff\xfe\x01\xff\xfb\x03', b'')

                    end = time.perf_counter()    
                    if verbosity>1:
                        print(f"response (bytes): {rcvd}")
                        print(f"time elapsed (s): {end - start:0.4f}")

                # inform other callers tha line is free
                self.__tcpip_line_is_busy = False
                return rcvd

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)
            # inform other callers tha line is free
            self.__tcpip_line_is_busy = False
            return b''

    
    def get_instr_type(self, verbosity: int=0) -> list[int]:
        """A.3.2 Requests details on the type of analyser being communicated with.

        Args:
            verbosity (int, optional): Verbosity for debugging purposes. Defaults to 0.

        Returns:
            list: 4 integers, namely, Model, Variant, Sub-Type, and Range.
        """
        try:
            if self.__protocol=='acoem':
                message = self._acoem_construct_message(1)
                self.tcpip_comm_wait_for_line()       
                response = self.tcpip_comm(message, verbosity=verbosity)
                return self._acoem_bytes2int(response=response, verbosity=verbosity)
            else:
                warnings.warn("Not implemented.")
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)
            return []


    def get_version(self, verbosity: int=0) -> list[int]:
        """A.3.3 Requests the current firmware version running on the analyser.

        Args:
            verbosity (int, optional): Verbosity for debugging purposes. Defaults to 0.

        Returns:
            list: 2 integers, namely, Build, and Branch.
        """
        try:
            if self.__protocol=='acoem':
                message = self._acoem_construct_message(1)
                self.tcpip_comm_wait_for_line()
                response = self.tcpip_comm(message, verbosity=verbosity)        
                return self._acoem_bytes2int(response=response, verbosity=verbosity)
            else:
                warnings.warn("Not implemented.")
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)
            return []


    def reset(self, verbosity: int=0) -> None:
        """
        A.3.4 Forces the analyser to do a full restart.
        The payload must contain the letters REALLY exactly, as 6 4 byte words.

        Args:
            verbosity (int, optional): Verbosity for debugging purposes. Defaults to 0.

        Returns:
            None.
        """
        try:
            if self.__protocol=='acoem':
                payload = bytes([82, 69, 65, 76, 76, 89])
                message = self._acoem_construct_message(command=1, payload=payload)
            elif self.__protocol=='legacy':
                message = f"**B\r".encode()
            else:
                raise ValueError('Protocol not recognized.')
            self.tcpip_comm(message, expect_response=False, verbosity=verbosity)
            return
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)
    

    def get_values(self, parameters: list[int], verbosity: int=0) -> dict:
        """
        Requests the value of one or more instrument parameters.
        If the ACOEM protocol is used, cf. A.3.5 Get Values, with A.4 List of Aurora parameters.
        If the legacy protocol s used, cf. B.7 Command VI, with Table 47 VI voltage input numbers.

        Args:
            parameters (list[int]): list of parameters to query
            verbosity (int, optional): _description_. Defaults to 0.

        Returns:
            dict: requested indexes are the keys, values are the responses from the instrument, decoded.
        """
        try:
            if self.__protocol=='acoem':
                msg_data = b''
                for p in parameters:
                    msg_data += (p).to_bytes(4)
                msg_len = len(msg_data)
                msg = bytes([2, self.__serial_id, 4, 3]) + (msg_len).to_bytes(2) + msg_data
                msg += self._acoem_checksum(msg) + bytes([4])
                self.tcpip_comm_wait_for_line()                
                response = self.tcpip_comm(message=msg, verbosity=verbosity)
                data = self._acoem_response2values(parameters=parameters, response=response, verbosity=verbosity)
                # items = self._acoem_bytes2int(response=response, verbosity=verbosity)
            elif self.__protocol=='legacy':
                items = []
                for p in parameters:
                    if p in range(100):
                        items.append(self.tcpip_comm(message=f"VI{self.__serial_id}{p:02.0f}\r".encode(), verbosity=verbosity).decode())
                    else:
                        items.append('')
                data = dict(zip(parameters, items))
            else:
                warnings.warn("Not implemented.")
            return data
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)
            return dict()


    def set_value(self, parameter_id: int, value: int, verify: bool=True, verbosity: int=0) -> int:
        """A.3.6 Sets the value of an instrument parameter.

        Args:
            parameter_id (int): Parameter to set.
            value (int): Value to be set.
            verify (bool, optional): Should the new value be queried and echoed after setting? Defaults to True.
            verbosity (int, optional): _description_. Defaults to 0.
        """
        try:
            response = -1
            if self.__protocol=='acoem':
                payload = bytes([0,0,0,value])
                message = self._acoem_construct_message(command=5, parameter_id=parameter_id, payload=payload)
                self.tcpip_comm_wait_for_line()                
                self.tcpip_comm(message=message, expect_response=False, verbosity=verbosity)
                if verify:
                    time.sleep(0.1)
                    while response!=value:
                        response = self.get_values(parameters=[parameter_id], verbosity=verbosity)[parameter_id]
                        # print(f"{time.perf_counter()} {response}")
                        time.sleep(0.1)
                    return response
                else:
                    return response
            else:
                warnings.warn("Not implemented.")
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)
            return -1


    def get_logging_config(self, verbosity: int=0) -> list[int]:
        """
        A.3.7 Return the list of parameter IDs currently being logged. 
        It is sent with zero message data length.
        The first 4 byte word of the response data is the number of fields being logged (0..500).
        Each following 4 byte word is the ID of the parameter.

        Args:
            verbosity (int, optional): Verbosity. Defaults to 0.

        Returns:
            list: Parameter IDs (integers) currently being logged
        """
        try:
            if self.__protocol=='acoem':
                message = self._acoem_construct_message(6)
                response = self.tcpip_comm(message, verbosity=verbosity)
                return self._acoem_bytes2int(response=response, verbosity=verbosity)
            else:
                warnings.warn("Not implemented.")
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)
            return []


    def get_logged_data(self, start: datetime.datetime, end: datetime.datetime, verbosity: int=0) -> list[dict]:
        """
        A.3.8 Requests all logged data over a specific date range.

        Args:
            start (datetime.datetime): Beginning of period.
            end (datetime.datetime): End of period.
            verbosity (int, optional): _description_. Defaults to 0.

        Raises:
            ValueError: _description_
            Warning: _description_

        Returns:
            list[dict]: _description_
        """
        try:
            if self.__protocol=='acoem':
                if start:
                    payload = self._acoem_datetime_to_timestamp(start)
                    if end:
                        payload += self._acoem_datetime_to_timestamp(end)
                else:
                    raise ValueError("start and/or end date not valid.")
                message = self._acoem_construct_message(command=7, payload=payload)
                response = self.tcpip_comm(message, verbosity=verbosity)
                data =  self._acoem_decode_logged_data(response=response, verbosity=verbosity)
                return data
                # # get next record
                # message = self._acoem_construct_message(command=7, payload=bytes([4]))
                # while response:=self.tcpip_comm(message, verbosity=verbosity):
                #     data.append(self._acoem_decode_logged_data(response=response, verbosity=verbosity))
            else:
                warnings.warn("Not implemented. For the legacy protocol, try 'get_all_data' or 'get_new_data'.")
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)
            return []


    def get_current_operation(self, verbosity: int=0) -> int:
        """
        Retrieves the operating state of the instrument. 
        If the ACOEM protocol is used, this requests Aurora parameter 4035 (cf.A.4 List of Aurora parameters).
        If the legacy protocol is used, this requests Voltage input number 71 (cf. Appendix B.7 Command: VI).

        Args:
            verbosity (int, optional): _description_. Defaults to 0.

        Returns:
            int: 0 (Normal monitoring), 1 (Zero calibration/check), 2 (Span calibration/check), 9 (Error)
        """
        try:
            if self.__protocol=='acoem':
                parameter_id = 4035
                # message = self._acoem_construct_message(command=4, parameter_id=parameter_id)
                return self.get_values(parameters=[parameter_id], verbosity=verbosity)[parameter_id]
                # return self._acoem_bytes2int(response=response, verbosity=verbosity)[0]
            elif self.__protocol=='legacy':
                response = self.tcpip_comm(message=f"VI{self.__serial_id}71\r".encode(), verbosity=verbosity).decode()
                mapping = {'000': 0, '016': 2, '032': 1}
                return mapping[response]
                # warnings.warn("Not implemented.")
            else:
                raise ValueError("Protocol not recognized.")
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)
            return 9


    def set_current_operation(self, state: int=0, verify: bool=True, verbosity: int=0) -> int:
        """Sets the instrument operating state by actuating the internal valve.

        Args:
            state (int, optional): 0: ambient, 1: zero, 2: span. Defaults to 0.
            verify (bool, optional): Should the value be queried and echoed after setting? Defaults to True.
            verbosity (int, optional): _description_. Defaults to 0.
        """
        try:
            if self.__protocol=='acoem':
                return self.set_value(parameter_id=4035, value=state, verify=verify, verbosity=verbosity)
            elif self.__protocol=='legacy':
                # if self.get_instr_type()[0]==158:
                warnings.warn("Not implemented for NE-300.")
                # else:
                #     map_state = {
                #         0: "0000", 
                #         1: "0011", 
                #         2: "0001", 
                #         }
                #     message = f"DO{self.__serial_number}{map_state[state]}\r".encode()
                #     response = self.tcpip_comm(message=message, verbosity=verbosity).decode()
                #     inverse_map_state = {0: 0, 1: 2, 11: 1}
                #     # while response!=[state]:
                #     #     response = self.get_values(parameters=[71], verbosity=verbosity)    # Could be 68 (major state) rather than 71 (DO span/zero measure mode)
                #     #     # [k for k, v in inverse_map_state.items()]
                #     #     time.sleep(1)
                #     #     warnings.warn(f"Incomplete implementation. Please expand and test. Call returned {response}.")
                #     warnings.warn(f"Incomplete implementation. Please expand and test. Call returned '{response}'.")
                #     return state
            else:
                raise ValueError("Protocol not recognized.")
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)
            return 9


    def get_id(self, verbosity: int=0) -> dict[str, str]:
        """Get instrument type, s/w, firmware versions

        Parameters:
            verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

        Returns:
            dict: response depends on protocol
        """
        try:
            if self.__protocol=="acoem":
                instr_type = self.get_instr_type(verbosity=verbosity)
                version = self.get_version(verbosity=verbosity)
                map_instr_type = {
                        'Model': {158: 'ACOEM Aurora',
                                  },
                        'Variant': {300: 'NE-300',
                                    },
                        }
                # resp = dict(zip(['Model', 'Variant', 'Sub-Type', 'Range', 'Build', 'Branch'], instr_type + version))
                id = f"{map_instr_type['Model'][instr_type[0]]} {map_instr_type['Variant'][instr_type[1]]} Sub-Type: {instr_type[2]} Range: {instr_type[3]} "
                id += f"Build: {version[0]} Branch: {version[1]}"
                resp = {'id': id}

            elif self.__protocol=="legacy":
                resp = {'id': self.tcpip_comm(message=f"ID{self.__serial_id}\r".encode(), verbosity=verbosity).decode()}
            else:
                raise ValueError("Communication protocol unknown")

            self._logger.info(f"get_id: {resp}")
            return resp

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)
            return dict()


    def get_datetime(self, verbosity: int=0) -> datetime.datetime:
        """Get date and time of instrument

        Parameters:
            verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

        Returns:
            datetime.datetime: Date and time of instrument
        """
        response = datetime.datetime(1111,1,1)
        try:
            if self.__protocol=="acoem":
                msg = self._acoem_construct_message(4, 1)
                response_bytes = self.tcpip_comm(message=msg, verbosity=verbosity)
                response_int = self._acoem_bytes2int(response=response_bytes, verbosity=verbosity)[0]
                response = self._acoem_timestamp_to_datetime(response_int)
            elif self.__protocol=='legacy':
                fmt = self.tcpip_comm(message=f"VI{self.__serial_id}64\r".encode(), verbosity=verbosity).decode()
                dte = self.tcpip_comm(message=f"VI{self.__serial_id}80\r".encode(), verbosity=verbosity).decode()
                tme = self.tcpip_comm(message=f"VI{self.__serial_id}81\r".encode(), verbosity=verbosity).decode()
                response = self._legacy_timestamp_to_date_time(fmt, dte, tme)
            else:
                raise ValueError("Protocol not recognized.")
            self._logger.info(f"get_datetime: {response}")
            return response
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)
            return response


    def get_set_datetime(self, dtm: datetime.datetime=datetime.datetime.now(), verbosity: int=1) -> None:
        """Set date and time of instrument

        Parameters:
            dtm (datetime.datetime, optional): Date and time to be set. Defaults to time.gmtime().
            verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

        Returns:
            None
        """
        try:
            if self.__protocol=="acoem":
                # get dtm from instrument
                dtm_found = self.get_values(parameters=[1])[1].strftime('%Y-%m-%d %H:%M:%S')
                
                # set dtm of instrument
                payload = self._acoem_datetime_to_timestamp(dtm=dtm)
                msg = self._acoem_construct_message(command=5, parameter_id=1, payload=payload)
                self.tcpip_comm(message=msg, expect_response=False, verbosity=0)
                
                # get new dtm from instrument
                dtm_set = self.get_values(parameters=[1])[1].strftime('%Y-%m-%d %H:%M:%S')
            else:
                warnings.warn("Not implemented.")
                # resp = self.tcpip_comm1(f"**{self.__serial_id}S{dtm.strftime('%H%M%S%d%m%y')}")
                # msg = f"DateTime of instrument {self.__name} set to {dtm} ... {resp}"
                # print(f"{dtm} {msg}")
                # self._logger.info(msg)
            
            if verbosity>0:
                msg = f"dtm found: {dtm_found} > dtm after set: {dtm_set}."
                print(colorama.Fore.GREEN + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self.__name}] {msg}")            
                self._logger.info(msg)
            return
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def do_zero_span_check(self, verbosity: int=0) -> None:
        """
        Launch a zero check, followed by a span check.

        Parameters:
            verbosity (int, optional): ...

        Returns:
            None
        """
        dtm = now = datetime.datetime.now()

        # change operating state to ZERO
        self.tcpip_comm_wait_for_line()
        resp = self.set_current_operation(state=1, verbosity=verbosity)
        if resp==1:
            data = f"Instrument switched to ZERO CHECK mode."
            print(colorama.Fore.GREEN + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self.__name}] {data}")
        while dtm < now + datetime.timedelta(minutes=self.__zero_check_duration):
            now = datetime.datetime.now()
            time.sleep(1)
        
        # change operating state to SPAN
        self.tcpip_comm_wait_for_line()
        resp = self.set_current_operation(state=2, verbosity=verbosity)
        if resp==2:
            data = f"Instrument switched to SPAN CHECK mode."
            print(colorama.Fore.GREEN + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self.__name}] {data}")
        while dtm < now + datetime.timedelta(minutes=self.__span_check_duration):
            now = datetime.datetime.now()
            time.sleep(1)
        
        # change operating state to AMBIENT
        self.tcpip_comm_wait_for_line()        
        resp = self.set_current_operation(state=0, verbosity=verbosity)
        if resp==0:
            data = f"Instrument switched to AMBIENT mode."
            print(colorama.Fore.GREEN + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self.__name}] {data}")
        return


    def do_span(self, verify: bool=True, verbosity: int=0) -> int:
        """
        Override digital IO control and DOSPAN. Wrapper for set_current_operation.

        Parameters:
            verbosity (int, optional): ...

        Returns:
            int: 2: span
        """
        return self.set_current_operation(state=2, verify=verify, verbosity=verbosity)


    def do_zero(self, verify: bool=True, verbosity: int=0) -> int:
        """
        Override digital IO control and DOZERO. Wrapper for set_current_operation.

        Parameters:
            verbosity (int, optional): ...
        
        Returns:
            int: 1: zero
        """
        return self.set_current_operation(state=1, verify=verify, verbosity=verbosity)
    

    def do_ambient(self, verify: bool=True, verbosity: int=0) -> int:
        """
        Override digital IO control and return to ambient measurement. Wrapper for set_current_operation.

        Parameters:
            verbosity (int, optional): ...

        Returns:
            int: 0: ambient
        """
        self.tcpip_comm_wait_for_line()
        return self.set_current_operation(state=0, verify=verify, verbosity=verbosity)

    

    def get_status_word(self, verbosity: int=0) -> int:
        """
        Read the System status of the Aurora 3000 microprocessor board. The status word 
        is the status of the nephelometer in hexadecimal converted to decimal.

        Parameters:
    
        Returns:
            int: {<STATUS WORD>}
        """
        return int(self.tcpip_comm(f"VI{self.__serial_id}88\r".encode()).decode())


    def get_all_data(self, verbosity: int=0) -> str:
        """
        Rewind the pointer of the data logger to the first entry, then retrieve all data (cf. B.4 ***R, B.3 ***D). 
        This only works with the legacy protocol (and doesn't work very well with the NE-300).

        Parameters:
            verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

        Returns:
            str: response
        """
        try:
            if self.__protocol=="acoem":
                warnings.warn("Not implemented. Use 'get_logged_data' with specified period instead.")
            elif self.__protocol=='legacy':
                response = self.tcpip_comm(message=f"***R\r".encode(), verbosity=verbosity).decode()
                response = self.get_new_data(verbosity=verbosity)
                # response = self.tcpip_comm(message=f"***D\r".encode(), verbosity=verbosity).decode()
                return response
            else:
                raise ValueError("Protocol not implemented.")
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)
            return ''


    def get_current_data(self, add_params: list=[], strict: bool=False, sep: str=' ', verbosity: int=0) -> dict:
        """
        Retrieve latest near-real-time reading on one line.
        With the legacy protocol, this uses the command 99 (cf. B.7 VI: 99), returning parameters [80,81,30,2,31,3,32,17,18,16,19,00,90].
        These are mapped to the corresponding Acoem parameters (cf. A.4 List of Aurora parametes) [1,1635000,1525000,1450000,1635090,1525090,1450090,5001,5004,5003,5002,4036,4035].
        Optionally, several more parameters can be retrieved, depending on the protocol.

        Parameters:
            add_params (list, optional): read more values and append to dictionary. Defaults to [].
            strict (bool, optional): If True, the dictionary returned is {99: response}, where response is the <sep>-separated response of the VI<serial_id>99 legacy command. Defaults to False.
            sep (str, optional): Separator applied if strict=True. Defaults to ' '.
            verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

        Returns:
            dict: Dictionary of parameters and values obtained.
        """
        parameters = [1,1635000,1525000,1450000,1635090,1525090,1450090,5001,5004,5003,5002,4036,4035]
        if add_params:
            parameters += add_params
        try:
            if self.__protocol=='acoem':
                # warnings.warn("Not implemented.")
                data = self.get_values(parameters=parameters, verbosity=verbosity)
                if strict:
                    if 1 in parameters:
                        data[1] = data[1].strftime(format=f"%d/%m/%Y{sep}%H:%M:%S")
                    response = sep.join([str(data[k]) for k, v in data.items()])
                    data = {99: response}
                # dtm = self._acoem_timestamp_to_datetime(response[1])
                # response[80] = dtm.strftime("%Y-%m-%d")
                # response[81] = dtm.strftime("%H:%M:%S")
                # response[1] = dtm
            elif self.__protocol=='legacy':
                # response = self.get_values(parameters=parameters)
                response = self.tcpip_comm(f"VI{self.__serial_id}99\r".encode(), verbosity=verbosity).decode()
                response = response.replace(", ", ",")
                if strict:
                    response = response.replace(',', sep)
                    data = {99: response}
                else:
                    response = response.split(',')
                    response = [response[0]] + [float(v) for v in response[1:]]
                    data = dict(zip(parameters, response))
                # response.insert(2, datetime.datetime.strptime(f"{response[0]} {response[1]}", "%d/%m/%Y %H:%M:%S"))
                # response = dict(zip(parameters, response))
            else:
                raise ValueError("Protocol not recognized.")
            return data
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)
            return dict()


    def get_new_data(self, sep: str=",", save: bool=True, verbosity: int=0) -> str:
        """
        For the acoem format: Retrieve all readings from (now - get_data_interval) until now.
        For the legacy format: Retrieve all readings from current cursor.
        
        Args:
            sep (str, optional): Separator to use for output and file, respectively. Defaults to ",".
            save (bool, optional): Should data be saved to file? Defaults to True.
            verbosity (int, optional): _description_. Defaults to 0.

        Raises:
            Warning: _description_
            ValueError: _description_

        Returns:
            str: data retrieved from logger as decoded string, including line breaks.
        """
        try:
            dtm = time.strftime('%Y-%m-%d %H:%M:%S')
            print(f"{dtm} .get_new_data (name={self.__name}, save={save})")


            if self.__protocol=='acoem':
                if self.__get_data_interval is None:
                    raise ValueError("'get_data_interval' cannot be None.")
                result = []
                end = datetime.datetime.now(datetime.timezone.utc)
                start = end - datetime.timedelta(minutes=self.__get_data_interval)
                self.tcpip_comm_wait_for_line()            
                data = self.get_logged_data(start=start, end=end, verbosity=verbosity)
                if verbosity>0:
                    print(data)

                for d in data:
                    values = [str(d.pop('dtm'))] + [str(value) for key, value in d.items()]
                    result.append(sep.join(values))
                data = '\n'.join(result)
            elif self.__protocol=='legacy':
                data = self.tcpip_comm(f"***D\r".encode()).decode()
                data = data.replace('\r\n\n', '\r\n').replace(", ", ",").replace(",", sep)
            else:
                raise ValueError("Protocol not recognized.")
            
            if verbosity>0:
                print(data)

            if save:
                if self.__reporting_interval is None:
                    raise ValueError("'reporting_interval' cannot be None.")
                # generate the datafile name
                self.__datafile = os.path.join(self.__datadir, time.strftime("%Y"), time.strftime("%m"), time.strftime("%d"),
                                            "".join([self.__name, "-",
                                                    datetimebin.dtbin(self.__reporting_interval), ".dat"]))

                os.makedirs(os.path.dirname(self.__datafile), exist_ok=True)
                with open(self.__datafile, "at", encoding='utf8') as fh:
                    fh.write(data)
                    fh.close()

                if self.__staging:
                    # stage data for transfer
                    self.stage_data_file()
            return data
        
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)
            return ''


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


    def print_ssp_bssp(self) -> None:
        """Retrieve current readings and print."""
        try:
            data = self.get_values(parameters=[2635000, 2635090, 2525000, 2525090, 2450000, 2450090])
            data = f"ssp|bssp (Mm-1) r: {data[2635000]:0.4f}|{data[2635090]:0.4f} g: {data[2525000]:0.4f}|{data[2525090]:0.4f} b: {data[2450000]:0.4f}|{data[2450090]:0.4f}"
            print(colorama.Fore.GREEN + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self.__name}] {data}")

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(colorama.Fore.RED + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self.__name}] produced error {err}.")


    # def stage_log_file(self) -> None:
    #     """Stage a file if it is no longer written to. This is determined by checking if the path 
    #        of the file to be staged is different the path of the current (data)file.

    #     Raises:
    #         ValueError: _description_
    #         ValueError: _description_
    #         ValueError: _description_
    #     """
    #     try:
    #         if self.__logfile is None:
    #             raise ValueError("__logfile cannot be None.")
    #         if self.__staging is None:
    #             raise ValueError("__staging cannot be None.")
    #         if self.__logdir is None:
    #             raise ValueError("__logdir cannot be None.")

    #         if self.__logfile_to_stage is None:
    #             self.__logfile_to_stage = self.__logfile
    #         elif self.__logfile_to_stage != self.__logfile:
    #             root = os.path.join(self.__staging, self.__name, os.path.basename(self.__logdir))
    #             os.makedirs(root, exist_ok=True)
    #             if self.__zip:
    #                 # create zip file
    #                 archive = os.path.join(root, "".join([os.path.basename(self.__logfile_to_stage)[:-4], ".zip"]))
    #                 with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    #                     zf.write(self.__logfile_to_stage, os.path.basename(self.__logfile_to_stage))
    #             else:
    #                 shutil.copyfile(self.__logfile_to_stage, os.path.join(root, os.path.basename(self.__logfile_to_stage)))
    #             self.__logfile_to_stage = self.__logfile

    #     except Exception as err:
    #         if self._log:
    #             self._logger.error(err)
    #         print(err)
    

# %%
if __name__ == "__main__":
    pass