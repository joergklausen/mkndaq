"""
Define a class NE300 facilitating communication with a Acoem NE-300 nephelometer.

@author: joerg.klausen@meteoswiss.ch
"""

import logging
import os
import shutil
import socket
import re
import time
import zipfile

import colorama

from mkndaq.utils import datetimebin

class NE300:
    """
    Instrument of type Acoem NE-300 nephelometer with methods, attributes for interaction.
    """

    __datadir = None
    __datafile = None
    __datafile_to_stage = None
    __logdir = None
    __serial_id = None
    __logfile = None
    __logfile_to_stage = None
    _log = None
    _logger = None
    __name = None
    __reporting_interval = None
    __set_datetime = None
    __sockaddr = None
    __socksleep = None
    __socktout = None
    __staging = None
    __zip = False

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
            self._type = config[name]['type']
            self.__serial_number = config[name]['serial_number']
            self.__serial_id = config[name]['serial_id']

            # configure tcp/ip
            self.__sockaddr = (config[name]['socket']['host'],
                             config[name]['socket']['port'])
            self.__socktout = config[name]['socket']['timeout']
            self.__socksleep = config[name]['socket']['sleep']

            # sampling, aggregation, reporting/storage
            self._sampling_interval = config[name]['sampling_interval']
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

            print(f"# Initialize NE300 (name: {self.__name}  S/N: {self.__serial_number})")

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def tcpip_comm(self, cmd: str, tidy=True) -> str:
        """
        Send a command and retrieve the response. Assumes an open connection.

        :param cmd: command sent to instrument
        :param tidy: clean-up response, currently not in use.
        :return: response of instrument, decoded (i.e. UTF-8)
        """
        rcvd = b''
        try:
            # send data using ACOEM protocol
            # chr(2) = STX
            # chr(3) = ETX

            # stx = chr(2).encode()
            # cmd = r'1'.encode()
            # etx = chr(3).encode()
            # msb = r'0'.encode()
            # lsb = r'0'.encode()
            # chksum = stx^self.__serial_id^cmd^etx^msb^lsb 
            # eot = chr(4).encode()
            
            # msg = (stx + self.__serial_id + cmd + etx + msb + lsb + chksum + eot)

            msg = f"{cmd}\r"

            # open socket connection as a client
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM, ) as s:
                # connect to the server
                s.settimeout(self.__socktout)
                s.connect(self.__sockaddr)

                s.sendall(msg.encode())
                time.sleep(self.__socksleep)

                # receive response
                while True:
                    try:
                        data = s.recv(1024)
                        rcvd = rcvd + data
                        if b"\r\n" in rcvd:
                            break
                    except:
                        break

            # strip pre-amble, decode response, tidy
            rcvd = rcvd.replace(b"\xff\xfb\x01\xff\xfe\x01\xff\xfb\x03", b"")
            rcvd = rcvd.decode()
            # if tidy:
                # rcvd = rcvd.replace("\r\n", "\n")
                # rcvd = rcvd.replace("\n\n", "\n")
            return rcvd

        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)

    def get_id(self, serial_id: str="0") -> (int, str):
        """Get instrument id, s/w, firmware versions

        Parameters:
            serial_id (str, optional): Defaults to '0'.

        Returns:
            int: 0 (if no error)
            str: <...> (if no error)
        """
        try:
            if self.__serial_id:
                serial_id = self.__serial_id
            resp = self.tcpip_comm(cmd=f"ID{serial_id}")
            if resp:
                print(resp)
                self._logger.info(resp)
                return 0, resp
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def set_datetime(self, serial_id: str="0") -> (int, str):
        """
        Synchronize date and time of instrument with computer time.

        Parameters:
            serial_id (str, optional): Defaults to '0'.

        Returns:
            int: 0 (if no error)
            str: OK (if no error)
        """
        try:
            dtm = time.strftime('%Y-%m-%d %H:%M:%S')
            resp = self.tcpip_comm(f"**{serial_id}S{time.strftime('%H%M%S%d%m%y')}")
            if resp=="OK":
                msg = f"Setting DateTime of instrument {self.__name} to {dtm} ... {resp}"
                print(f"{dtm} {msg}")
                self._logger.info(msg)
                return 0, resp
        except Exception as err:
            if self._log:
                self._logger.error(err)
            print(err)


    def do_span_check(self, serial_id: str="0") -> (int, str):
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


    def do_zero_check(self, serial_id: str="0") -> (int, str):
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


    def do_ambient(self, serial_id: str="0") -> (int, str):
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


    def get_status_word(self, serial_id: str="0") -> (int, str):
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


    def get_all_data(self, serial_id: str="0") -> (int, str):
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


    def get_data(self, serial_id: str="0", get_status_word=True, sep: str=",", save: bool=True) -> (int, str):
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


    def get_all_new_data(self, serial_id: str="0", get_status_word=True, sep: str=",", save: bool=True) -> (int, str):
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


    # def get_latest_ATN_info(self, save=True) -> str:
    #     """Get latest ATN info from log file, based on tape advance info"""
    #     try:
    #         # minid = int(self.tcpip_comm(cmd="MINID Log", tidy=True))
    #         maxid = int(self.tcpip_comm(cmd="MAXID Log", tidy=True))
    #         if int(maxid) > 49:
    #             minid = str(int(maxid) - 50)
    #         else:
    #             minid = "1"
    #         log = self.tcpip_comm(cmd=f"FETCH Log {minid} {maxid}", tidy=False)
    #         log = log.replace("AE33>", "")

    #         log = log.splitlines()
    #         i = [i for i in range(len(log)) if "Tape Advance number" in log[i]][-1]
    #         j = [i for i in range(len(log)) if "ATN1zero" in log[i]][-1]
    #         k = log[i].find("Tape Advance number:")
    #         tape_advance_number = int(log[i][k+20:k+25])

    #         # pattern_1 = r"(.*Tape Advance number:.*)" # to capture line containing words
    #         # atn_pattern = r"(ATN\dzero\(\d\):\s+\d+.\d+)"

    #         if save:
    #             # generate the datafile name
    #             self.__datafile = os.path.join(self.__datadir,
    #                                            "".join([self.__name, "-",
    #                                                    datetimebin.dtbin(self.__reporting_interval), ".log"]))

    #             with open(self.__datafile, "at", encoding='utf8') as fh:
    #                 fh.write(f"{dtm}{sep}{data}\n")
    #                 fh.close()

    #             # stage data for transfer
    #             self.stage_file()
    #         return tape_advance_number, log[i:j]

    #     except Exception as err:
    #         if self._log:
    #             self._logger.error(err)
    #         print(colorama.Fore.RED + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self.__name}] produced error {err}.")

# Header:
# Date(yyyy/MM/dd); Time(hh:mm:ss); Timebase; RefCh1; Sen1Ch1; Sen2Ch1; RefCh2; 
# Sen1Ch2; Sen2Ch2; RefCh3; Sen1Ch3; Sen2Ch3; RefCh4; Sen1Ch4; Sen2Ch4; RefCh5; 
# Sen1Ch5; Sen2Ch5; RefCh6; Sen1Ch6; Sen2Ch6; RefCh7; Sen1Ch7; Sen2Ch7; Flow1; Flow2; 
# FlowC; Pressure (Pa); Temperature (°C); BB (%); ContTemp; SupplyTemp; Status; ContStatus; 
# DetectStatus; LedStatus; ValveStatus; LedTemp; BC11; BC12; BC1; BC21; BC22; BC2; BC31; 
# BC32; BC3; BC41; BC42; BC4; BC51; BC52; BC5; BC61; BC62; BC6; BC71; BC72; BC7; 
# K1; K2; K3; K4; K5; K6; K7; TapeAdvCount; ID_com1; ID_com2; ID_com3; fields_i
# Data line:
# 2012/09/21 00:34:00 60 890416 524323 709193 823296 573862 756304 884844 619592 789142 
# 822391 673266 816066 792706 686925 828401 738101 718325 841075 789053 722690 833686 
# 3325 1674 4999 101325 21.11 -1 30 40 0 0 10 10 00000 0 1150 1290 1242 1166 1248 1215 
# 1150 1231 1190 1146 1196 1175 1214 1195 1234 1144 1114 1139 1180 1225 1174 0.00133 
# 0.00095 0.00092 0.00080 0.00057 -0.00024 -0.00025 12 0 2 0 21.1    

# Data field description:
# - Date (yyyy/MM/dd): date.
# - Time (hh:mm:ss): time. 
# - Timebase: timebase – units in seconds.
# - Ref1, Sen1Ch1, Sen2Ch1…: are the raw signal Reference (Ref), Sensing Spot 1 (Sen1) and 
# Sensing Spot 2 (Sen2) values from which the BC concentrations are calculated for channel 1 
# (Ch1), wavelength 370 nm. BC11 is the uncompensated BC calculated from the spot 1 for 
# channel 1. BC1 is the final result for the BC calculated from measurements for channel 1 
# (370 nm). This is repeated for channels 2 through 7 (wavelengths 470 ~ 950 nm).
# - Flow1; Flow2; FlowC: Measured flow in ml/min. Flow 1 is flow through the spot 1, FlowC 
# is common (total flow) through the optical chamber, Flow2 is the difference between these 
# two.
# - Pressure (Pa); Temperature (°C): These are the pressure and temperature which the 
# instrument uses to report flow. By choosing to report mass flow, the values of (101325 Pa, 
# 21.11 C) are used to convert the measured mass flow to reported volumetric flow at these 
# conditions, since these are values used by the flow sensors in the AE33. These values can 
# be exchanged for any other (Pressure, Temperature) values, if data reporting to other 
# standards is required. Optionally, you can choose the data to be reported as volumetric 
# flow at ambient conditions. In such a case, accessory measurements of the ambient 
# (Pressure, Temperature) are required. We offer different types of weather sensors to 
# provide this data automatically when connected to a COM port.
# - BB (%): the percentage of BC created by biomass burning, determined by the Sandradewi 
# model.
# - ContTemp; SupplyTemp: control and power supply board temperatures
# - Status; ContStatus; DetectStatus; LedStatus; ValveStatus; LedTemp: internal status codes 
# for the instrument.
# - BC11; BC12; BC1; BC21; BC22; BC2; BC31; BC32; BC3; BC41; BC42; BC4; BC51; BC52; 
# BC5; C61; BC62; BC6; BC71; BC72; BC7: BCn1 is the uncompensated BC calculated from 
# the spot 1 for channel n; BCn2 is the uncompensated BC calculated from the spot 2 for 
# channel n. BCn is the final result for the compensated BC calculated from measurements for 
# channel n.
# - K1; K2; K3; K4; K5; K6; K7: the K_n are the compensation parameters for wavelength
# channels n = 1 ~ 7
# - TapeAdvCount: TapeAdvCount – tape advances since start
# - ID_com1; ID_com2; ID_com3; The Fields_i: ID_com_i are the identifiers indicating which 
# auxiliary device is connected to which serial port (necessary because of the different data 
# structure). This is a 3 byte field: thus, 
# 0 2 0 21.1
# means that the “Comet temperature probe” (instrument code 2) is connected to COM2 and 
# nothing is connected to COM1 and COM3. The temperature is 21.1 C.

# %%
if __name__ == "__main__":
    pass