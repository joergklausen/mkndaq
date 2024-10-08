"""
Define a class AE33 facilitating communication with a Magee Scientific AE33 instrument.

@author: joerg.klausen@meteoswiss.ch
"""

import logging
import os
import shutil
import socket
# import re
import time
import zipfile
from datetime import datetime

import colorama
import schedule

# from mkndaq.utils import datetimebin


class AE33:
    """
    Instrument of type Magee Scientific AE33 with methods, attributes for interaction.
    """

    # _datadir = None
    # _data_begin_read_id = None
    # _datafile = None
    # _datafile_to_stage = None
    # _get_config = None
    # _logdir = None
    # _log_begin_read_id = None
    # _logfile = None
    # _logfile_to_stage = None
    # _log = None
    # _logger = None
    # _name = None
    # _reporting_interval = None
    # # __set_config = None
    # _set_datetime = None
    # _sockaddr = None
    # _socksleep = None
    # _socktout = None
    # _staging = None
    # _zip = False

    def __init__(self, name: str, config: dict) -> None:
        """
        Initialize instrument class.

        :param name: name of instrument
        :param config: dictionary of attributes defining instrument, serial port and more
            - config[name]['type']
            - config[name]['serial_number']
            - config[name]['socket']['host']
            - config[name]['socket']['port']
            - config[name]['socket']['timeout']
            - config[name]['socket']['sleep']
            - config[name]['get_config']
            - config[name]['set_config']
            - config[name]['get_data']
            - config[name]['set_datetime']
            - config['logs']
            - config[name]['sampling_interval']
            - config['staging']['path'])
            - config[name]['staging_zip']
        :param simulate: default=True, simulate instrument behavior. Assumes a serial loopback connector.
        """
        colorama.init(autoreset=True)

        try:
            self._name = name
            self._serial_number = config[name]['serial_number']

            # configure logging
            _logger = f"{os.path.basename(config['logging']['file'])}".split('.')[0]
            self.logger = logging.getLogger(f"{_logger}.{__name__}")
            self.logger.info(f"[{self._name}] Initializing AE33 (S/N: {self._serial_number})")

            # read instrument control properties for later use
            self._type = config[name]['type']
            self._get_config = config[name]['get_config']
            # self.__set_config = config[name]['set_config']
            self._set_datetime = config[name]['set_datetime']

            # configure tcp/ip
            self._sockaddr = (config[name]['socket']['host'],
                             config[name]['socket']['port'])
            self._socktout = config[name]['socket']['timeout']
            self._socksleep = config[name]['socket']['sleep']

            # sampling, aggregation, reporting/storage
            self.sampling_interval = config[name]['sampling_interval']
            # self.reporting_interval = config['reporting_interval']

            # configure saving, staging and archiving
            # _data_path: path for measurement data
            # _logs-path: path for instrument logs
            root = os.path.expanduser(config['root'])
            self.data_path = os.path.join(root, config[name]['data_path'], 'data')
            os.makedirs(self.data_path, exist_ok=True)
            self.logs_path = os.path.join(root, config[name]['data_path'], 'logs')
            os.makedirs(self.logs_path, exist_ok=True)
            self._data_staging_path = os.path.join(root, config[name]['staging_path'], 'data')
            os.makedirs(self._data_staging_path, exist_ok=True)
            self._logs_staging_path = os.path.join(root, config[name]['staging_path'], 'logs')
            os.makedirs(self._logs_staging_path, exist_ok=True)
            self._zip = config[name]['staging_zip']

            # initialize data response
            self._data = str()

            self.get_config()
            if self._set_datetime:
                self.set_datetime()

        except Exception as err:
            self.logger.error(err)


    def setup_schedules(self):
        try:
            # configure folders needed
            os.makedirs(self.data_path, exist_ok=True)
            os.makedirs(self.staging_path, exist_ok=True)
            # os.makedirs(self.archive_path, exist_ok=True)

            # configure data acquisition schedule
            schedule.every(int(self.sampling_interval)).minutes.at(':00').do(self.accumulate_new_data)

            # configure saving and staging schedules
            if self.reporting_interval==10:
                self._file_timestamp_format = '%Y%m%d%H%M'
                minutes = [f"{self.reporting_interval*n:02}" for n in range(6) if self.reporting_interval*n < 6]
                for minute in minutes:
                    schedule.every().hour.at(f"{minute}:01").do(self._save_and_stage_data)
            elif self.reporting_interval==60:
                self._file_timestamp_format = '%Y%m%d%H'
                schedule.every().hour.at('00:01').do(self._save_and_stage_data)
            elif self.reporting_interval==1440:
                self._file_timestamp_format = '%Y%m%d'
                schedule.every().day.at('00:00:01').do(self._save_and_stage_data)

        except Exception as err:
            self.logger.error(err)


    def tcpip_comm(self, cmd: str, tidy=True) -> str:
        """
        Send a command and retrieve the response. Assumes an open connection.

        :param cmd: command sent to instrument
        :param tidy:
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
                # rcvd = rcvd.replace("\n", "").replace("\r", "").replace("AE33>", "")
                rcvd = rcvd.replace("AE33>", "")
                rcvd = rcvd.replace("\r\n", "\n")
                rcvd = rcvd.replace("\n\n", "\n")
            return rcvd

        except Exception as err:
            self.logger.error(err)
            return str()


    def fetch_from_table(self, name: str, rows=None, first=None, last=None) -> str:
        try:
            if name is None:
                raise("Table 'name' must be provided.")
            if first is None:
                if last is None:
                    if rows is None:
                        # fetch all data from table
                        cmd = f"FETCH {name} 1"
                    else:
                        # fetch number of rows from end of table
                        maxid = int(self.tcpip_comm(cmd=f"MAXID {name}", tidy=True))
                        cmd=f"FETCH {name} {maxid-rows}"
                elif rows is None:
                    raise ValueError("Number of 'rows' to read must be provided together with 'last'.")
                else:
                    # fetch number of rows up until last
                    cmd = f"FETCH {name} {last-rows} {last}"
            elif last is None:
                if rows is None:
                    # fetch all data starting at first
                    cmd = f"FETCH {name} {first}"
                else:
                    # fetch number of rows starting at first
                    cmd = f"FETCH {name} {first} {first+rows}"
            else:
                if rows is None:
                    cmd = f"FETCH {name} {first} {last}"
                else:
                    raise ValueError("Ambiguous request, cannot use all of 'first', 'last' and 'rows' at once.")

            resp = self.tcpip_comm(cmd=cmd, tidy=False)

            return resp
        except Exception as err:
            self.logger.error(err)
            return str()


    def set_datetime(self) -> None:
        """
        Synchronize date and time of instrument with computer time.

        :return:
        """
        try:
            cmd = f"$AE33:{time.strftime('T%Y%m%d%H%M%S')}"
            dtm = self.tcpip_comm(cmd)
            msg = f"[{self._name}] DateTime of instrument set to: {cmd}"
            self.logger.info(msg)

        except Exception as err:
            self.logger.error(err)


    def get_config(self) -> list:
        """
        Read current configuration of instrument and optionally write to log.

        :return (err, cfg) configuration or errors, if any.

        """
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} .get_config (name={self._name})")
        cfg = []
        try:
            for cmd in self._get_config:
                cfg.append(self.tcpip_comm(cmd))

            self.logger.info(f"[{self._name}] Current configuration: {cfg}")

            return cfg

        except Exception as err:
            self.logger.error(err)
            return list()


    def accumulate_new_data(self, sep="|") -> None:
        """
        Retrieve all records from table data that have not been read.

        :param str sep: item separator. Defaults to True.
        :param bln save: Should data be saved to file? Default=True
        :return str response as decoded string
        """
        try:
            dtm = time.strftime('%Y-%m-%d %H:%M:%S')
            self.logger.info(f"[{self._name}]: .accumulate_new_data")

            # read the latest records from the Data table
            data = ""
            maxid = int(self.tcpip_comm(cmd="MAXID Data", tidy=True))

            # get data_begin_read_id
            if self._data_begin_read_id:
                data_begin_read_id = self._data_begin_read_id
            else:
                # if we don't know where to start, we start at the beginning
                minid = int(self.tcpip_comm(cmd="MINID Data", tidy=True))

                # limit the number of records to download to 1440 (1 day)
                if maxid - minid > 1440:
                    minid = maxid - 1440
                data_begin_read_id = minid

            if data_begin_read_id < maxid:
                chunk_size = 1000
                while data_begin_read_id < maxid:
                    if (maxid - data_begin_read_id) > chunk_size:
                        cmd=f"FETCH Data {data_begin_read_id} {data_begin_read_id + chunk_size}"
                    else:
                        cmd=f"FETCH Data {data_begin_read_id} {maxid}"
                    data = self.tcpip_comm(cmd, tidy=True)

                    self._data += data
                    self.logger.info(f"{self._name}: {data[:60]}[...]")

                    data_begin_read_id += chunk_size + 1

                # set data_begin_read_id
                self._data_begin_read_id = maxid + 1

            #     if save:
            #         # generate the datafile name
            #         # self.__datafile = os.path.join(self.__datadir,
            #         #                             "".join([self._name, "-",
            #         #                                     datetimebin.dtbin(self._reporting_interval), ".dat"]))
            #         self.__datafile = os.path.join(self._data_path, time.strftime("%Y"), time.strftime("%m"), time.strftime("%d"),
            #                                     f"{self._name}-{datetimebin.dtbin(self.reporting_interval)}.dat")

            #         os.makedirs(os.path.dirname(self.__datafile), exist_ok=True)
            #         with open(self.__datafile, "at", encoding='utf8') as fh:
            #             # fh.write(f"{dtm}{sep}{data}\n")
            #             fh.write(data)
            #             fh.close()

            #         # stage data for transfer
            #         self.stage_data_file()

            # return data
                return

        except Exception as err:
            self.logger.error(err)
            return str()


    def _save_data(self) -> None:
        try:
            data_file = str()
            self.data_file = str()
            if self._data:
                # create appropriate file name and write mode
                timestamp = datetime.now().strftime(self._file_timestamp_format)
                data_file = os.path.join(self.data_path, f"ae33-{timestamp}.dat")

                # configure file mode, open file and write to it
                if os.path.exists(self.data_file):
                    mode = 'a'
                    header = str()
                else:
                    mode = 'w'
                    header = 'pcdate pctime time date flags o3 hio3 cellai cellbi bncht lmpt o3lt flowa flowb pres\n'

                with open(file=data_file, mode=mode) as fh:
                    fh.write(header)
                    fh.write(self._data)
                    self.logger.info(f"file saved: {data_file}")

                # reset self._data
                self._data = str()

            self.data_file = data_file
            return

        except Exception as err:
            self.logger.error(err)


    def _stage_file(self):
        """ Create zip file from self.data_file and stage archive.
        """
        try:
            if self.data_file:
                archive = os.path.join(self.staging_path, os.path.basename(self.data_file).replace('.dat', '.zip'))
                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.write(self.data_file, os.path.basename(self.data_file))
                    self.logger.info(f"file staged: {archive}")

        except Exception as err:
            self.logger.error(err)


    def stage_data_file(self) -> None:
        """Stage a file if it is no longer written to. This is determined by checking if the path
           of the file to be staged is different from the path of the current (data)file.

        Raises:
            ValueError: _description_
            ValueError: _description_
            ValueError: _description_
        """
        try:
            if self._datafile is None:
                raise ValueError("__datafile cannot be None.")
            if self._staging is None:
                raise ValueError("__staging cannot be None.")
            if self._data_path is None:
                raise ValueError("__datadir cannot be None.")
            if self._datafile_to_stage is None:
                self._datafile_to_stage = self.__datafile
            elif self._datafile_to_stage != self.__datafile:
                root = os.path.join(self._staging, self._name, os.path.basename(self._data_path))
                os.makedirs(root, exist_ok=True)
                if self._zip:
                    # create zip file
                    archive = os.path.join(root, "".join([os.path.basename(self._datafile_to_stage)[:-4], ".zip"]))
                    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                        zf.write(self._datafile_to_stage, os.path.basename(self._datafile_to_stage))
                else:
                    shutil.copyfile(self._datafile_to_stage, os.path.join(root, os.path.basename(self._datafile_to_stage)))
                self._datafile_to_stage = self._datafile

        except Exception as err:
            self.logger.error(err)


    def print_ae33(self) -> None:
        """Retrieve current record from Data table and print."""
        try:
            # read the last record from the Data table
            maxid = int(self.tcpip_comm(cmd="MAXID Data", tidy=True))
            cmd=f"FETCH Data {maxid}"
            data = self.tcpip_comm(cmd, tidy=True)
            data = data.split(sep="|")

            tape_adv_remaining = self.tape_advances_remaining()
            msg = f"Tape advances remaining: {tape_adv_remaining}"
            if int(tape_adv_remaining) < 10:
                msg += " ATTENTION: Get ready to change change!"
            print(colorama.Fore.GREEN + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self._name}] BC: {data[44]} ng/m3 UVPM: {data[29]} ng/m3 ({msg})")

        except Exception as err:
            self.logger.error(err)


    def tape_advances_remaining(self) -> str:
        try:
            cmd = "$AE33:A"
            res = self.tcpip_comm(cmd, tidy=True)
            res = res.replace("\n", "")
            return res

        except Exception as err:
            self.logger.error(err)
            return str()


    def get_new_log_entries(self, sep="|", save=True) -> str:
        """
        Retrieve all records from table data that have not been read and optionally write to log.

        :param str sep: item separator. Defaults to True.
        :param bln save: Should data be saved to file? Default=True
        :return str response as decoded string
        """
        try:
            dtm = time.strftime('%Y-%m-%d %H:%M:%S')
            self.logger.info(f"[{self._name}] .get_new_log_entries, save={save})")

            # get data_begin_read_id
            if self.__log_begin_read_id:
                log_begin_read_id = self.__log_begin_read_id
            else:
                # if we don't know where to start, we start at the beginning
                minid = int(self.tcpip_comm(cmd="MINID Log", tidy=True))
                log_begin_read_id = minid
            # read the last record from the Log table
            # get the maximum id in the Log table
            log = str()
            maxid = int(self.tcpip_comm(cmd="MAXID Log", tidy=True))
            if log_begin_read_id < maxid:
                cmd=f"FETCH Log {log_begin_read_id} {maxid}"
                log = self.tcpip_comm(cmd, tidy=True)

                # set log_begin_read_id for the next call
                self.__log_begin_read_id = maxid + 1

                if save:
                    # generate the datafile name
                    # self.__logfile = os.path.join(self._logdir,
                    #                             "".join([self._name, "-",
                    #                                     datetimebin.dtbin(self._reporting_interval), ".log"]))
                    self.__logfile = os.path.join(self._logdir, time.strftime("%Y"), time.strftime("%m"), time.strftime("%d"),
                                                "".join([self._name, "-",
                                                        datetimebin.dtbin(self.reporting_interval), ".log"]))

                    os.makedirs(os.path.dirname(self.__logfile), exist_ok=True)
                    with open(self.__logfile, "at", encoding='utf8') as fh:
                        fh.write(f"{dtm}{sep}{log}\n")
                        fh.close()

                    # stage data for transfer
                    self.stage_log_file()

            return log

        except Exception as err:
            self.logger.error(err)
            return str()


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
                raise ValueError("_logfile cannot be None.")
            if self._staging is None:
                raise ValueError("_staging cannot be None.")
            if self._logdir is None:
                raise ValueError("_logdir cannot be None.")

            if self.__logfile_to_stage is None:
                self.__logfile_to_stage = self.__logfile
            elif self.__logfile_to_stage != self.__logfile:
                root = os.path.join(self._staging, self._name, os.path.basename(self._logdir))
                os.makedirs(root, exist_ok=True)
                if self._zip:
                    # create zip file
                    archive = os.path.join(root, "".join([os.path.basename(self.__logfile_to_stage)[:-4], ".zip"]))
                    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                        zf.write(self.__logfile_to_stage, os.path.basename(self.__logfile_to_stage))
                else:
                    shutil.copyfile(self.__logfile_to_stage, os.path.join(root, os.path.basename(self.__logfile_to_stage)))
                self.__logfile_to_stage = self.__logfile

        except Exception as err:
            self.logger.error(err)


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
    #                                            "".join([self._name, "-",
    #                                                    datetimebin.dtbin(self._reporting_interval), ".log"]))

    #             with open(self.__datafile, "at", encoding='utf8') as fh:
    #                 fh.write(f"{dtm}{sep}{data}\n")
    #                 fh.close()

    #             # stage data for transfer
    #             self.stage_file()
    #         return tape_advance_number, log[i:j]

    #     except Exception as err:
    #         if self._log:
    #             self._logger.error(err)
    #         print(colorama.Fore.RED + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self._name}] produced error {err}.")

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
