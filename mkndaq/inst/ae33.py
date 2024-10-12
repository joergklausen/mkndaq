"""
Define a class AE33 facilitating communication with a Magee Scientific AE33 instrument.

@author: joerg.klausen@meteoswiss.ch
"""

import logging
import os
import shutil
import socket
import time
import zipfile
from datetime import datetime

import colorama
import schedule

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
class AE33:
    """
    Instrument of type Magee Scientific AE33 with methods, attributes for interaction.
    """

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
            self.name = name
            self.type = config[name]['type']
            self.serial_number = config[name]['serial_number']
            self._tables = ('Data', 'Log')

            # configure logging
            _logger = f"{os.path.basename(config['logging']['file'])}".split('.')[0]
            self.logger = logging.getLogger(f"{_logger}.{__name__}")
            self.logger.info(f"[{self.name}] Initializing {self.type} (S/N: {self.serial_number})")

            # read instrument control properties for later use
            # self.type = config[name]['type']
            self._get_config = config[name]['get_config']
            # self.__set_config = config[name]['set_config']
            self._set_datetime = config[name]['set_datetime']

            # configure tcp/ip
            self.sockaddr = (config[name]['socket']['host'],
                             config[name]['socket']['port'])
            self.socktout = config[name]['socket']['timeout']
            self.socksleep = config[name]['socket']['sleep']

            # sampling, aggregation, reporting/storage
            self.sampling_interval = config[name]['sampling_interval']
            self.reporting_interval = config[name]['reporting_interval']

            # configure saving, staging and archiving
            # _data_path: path for measurement data
            # _log_path: path for instrument logs
            root = os.path.expanduser(config['root'])
            self.data_path = os.path.join(root, config[name]['data_path'], 'data')
            os.makedirs(self.data_path, exist_ok=True)
            self.log_path = os.path.join(root, config[name]['data_path'], 'logs')
            os.makedirs(self.log_path, exist_ok=True)
            self._data_staging_path = os.path.join(root, config[name]['staging_path'], 'data')
            os.makedirs(self._data_staging_path, exist_ok=True)
            self._log_staging_path = os.path.join(root, config[name]['staging_path'], 'logs')
            os.makedirs(self._log_staging_path, exist_ok=True)
            self.zip = config[name]['staging_zip']

            # initialize data, logs response
            self._data = str()
            self._log = str()

            self.get_config()

            if self._set_datetime:
                self.set_datetime()

        except Exception as err:
            self.logger.error(err)


    def setup_schedules(self):
        try:
            # configure data acquisition schedule
            schedule.every(int(self.sampling_interval)).minutes.at(':00').do(self._accumulate_new_data, table='Data')
            schedule.every(int(self.sampling_interval)).minutes.at(':00').do(self._accumulate_new_data, table='Log')

            # configure saving and staging schedules
            if self.reporting_interval==10:
                self._file_timestamp_format = '%Y%m%d%H%M'
                # minutes = [f"{self.reporting_interval*n:02}" for n in range(6) if self.reporting_interval*n < 6]
                # minutes = (0, 10, 20, 30, 40, 50)
                for minute in range(6):
                    schedule.every().hour.at(f"{minute}0:01").do(self._save_and_stage_data)
            elif self.reporting_interval==60:
                self._file_timestamp_format = '%Y%m%d%H'
                # schedule.every().hour.at('00:01').do(self._save_and_stage_data)
                schedule.every().hour.at('00:01').do(self._save_and_stage_data)
            elif self.reporting_interval==1440:
                self._file_timestamp_format = '%Y%m%d'
                # schedule.every().day.at('00:00:01').do(self._save_and_stage_data)
                schedule.every().day.at('00:00:01').do(self._save_and_stage_data)

        except Exception as err:
            self.logger.error(err)


    def _tcpip_comm(self, cmd: str, tidy=True) -> str:
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
                s.settimeout(self.socktout)
                s.connect(self.sockaddr)

                # send data
                s.sendall((cmd + chr(13) + chr(10)).encode())
                time.sleep(self.socksleep)

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


    def _fetch_from_table(self, name: str, rows=None, first=None, last=None) -> str:
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
                        maxid = int(self._tcpip_comm(cmd=f"MAXID {name}", tidy=True))
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

            resp = self._tcpip_comm(cmd=cmd, tidy=False)

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
            dtm = self._tcpip_comm(cmd)
            msg = f"[{self.name}] DateTime of instrument set to: {cmd}"
            self.logger.info(msg)

        except Exception as err:
            self.logger.error(err)


    def get_config(self) -> list:
        """
        Read current configuration of instrument and optionally write to log.

        :return (err, cfg) configuration or errors, if any.

        """
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} .get_config (name={self.name})")
        cfg = []
        try:
            for cmd in self._get_config:
                cfg.append(self._tcpip_comm(cmd))

            self.logger.info(f"[{self.name}] Current configuration: {cfg}")

            return cfg

        except Exception as err:
            self.logger.error(err)
            return list()


    def _accumulate_new_data(self, table: str, sep="|") -> None:
        """
        Retrieve all records from table data that have not been read.

        :param str table: One od 'Data' or 'Log'. Determines the kind of query.
        :param str sep: item separator. Defaults to True.
        :return str response as decoded string
        """
        try:
            if not table in self._tables:
                raise ValueError(f"'table' must be one of {self._tables}")
            self.logger.info(f"[{self.name}]: ._accumulate_new_data {table}")

            # read the latest records from the table
            _ = ""
            maxid = int(self._tcpip_comm(cmd=f"MAXID {table}", tidy=True))

            # get {table}_begin_read_id
            if self._data_begin_read_id:
                begin_read_id = self._data_begin_read_id
            elif self._log_begin_read_id:
                begin_read_id = self._log_begin_read_id
            else:
                # if we don't know where to start, we start at the beginning
                minid = int(self._tcpip_comm(cmd=f"MINID {table}", tidy=True))

                # limit the number of records to download to 1440 (1 day)
                if maxid - minid > 1440:
                    minid = maxid - 1440
                begin_read_id = minid

            if begin_read_id < maxid:
                chunk_size = 1000
                while begin_read_id < maxid:
                    if (maxid - begin_read_id) > chunk_size:
                        cmd=f"FETCH {table} {begin_read_id} {begin_read_id + chunk_size}"
                    else:
                        cmd=f"FETCH {table} {begin_read_id} {maxid}"
                    _ = self._tcpip_comm(cmd, tidy=True)

                    if table=='Data':
                        self._data += _
                    elif table=='Log':
                            self._log += _
                    else:
                        raise ValueError(f"not implemented")                    
                    self.logger.info(f"{self.name}: {_[:60]}[...]")

                    begin_read_id += chunk_size + 1

                # set {table}_begin_read_id
                if table=='Data':
                    self._data_begin_read_id = maxid + 1
                elif table=='Log':
                    self._log_begin_read_id = maxid + 1
                else:
                    raise ValueError(f"not implemented")
                return

        except Exception as err:
            self.logger.error(err)


    # def _save_data(self) -> None:
    #     try:
    #         now = datetime.now()
    #         timestamp = now.strftime(self._file_timestamp_format)
    #         yyyy = now.strftime('%Y')
    #         mm = now.strftime('%m')
    #         dd = now.strftime('%d')

    #         data_file = str()
    #         self.data_file = str()
            
    #         if self._data:
    #             # create appropriate file name and write mode
    #             data_file = os.path.join(self.data_path, yyyy, mm, dd, f"{self.name}-{timestamp}.dat")

    #             # configure file mode, open file and write to it
    #             if os.path.exists(self.data_file):
    #                 mode = 'a'
    #                 header = str()
    #             else:
    #                 mode = 'w'
    #                 header = str()

    #             with open(file=data_file, mode=mode) as fh:
    #                 fh.write(header)
    #                 fh.write(self._data)
    #                 self.logger.info(f"[{self.name}] file saved: {data_file}")

    #             # reset self._data
    #             self._data = str()

    #         self.data_file = data_file

    #         log_file = str()
    #         self.log_file = str()
    #         if self._log:
    #             # create appropriate file name and write mode
    #             log_file = os.path.join(self.log_path, yyyy, mm, dd, f"{self.name}-{timestamp}.log")

    #             # configure file mode, open file and write to it
    #             if os.path.exists(self.log_file):
    #                 mode = 'a'
    #                 header = str()
    #             else:
    #                 mode = 'w'
    #                 header = str()

    #             with open(file=log_file, mode=mode) as fh:
    #                 fh.write(header)
    #                 fh.write(self._log)
    #                 self.logger.info(f"[{self.name}] file saved: {log_file}")

    #             # reset self._data
    #             self._log = str()

    #         self.log_file = log_file

    #         return

    #     except Exception as err:
    #         self.logger.error(err)


    def _save_data(self, table: str) -> None:
        try:
            if not table in self._tables:
                raise ValueError(f"'table' must be one of {self._tables}")
            self.logger.debug(f"[{self.name}]: ._save_data {table}")

            now = datetime.now()
            timestamp = now.strftime(self._file_timestamp_format)
            yyyy = now.strftime('%Y')
            mm = now.strftime('%m')
            dd = now.strftime('%d')

            if table=='Data':
                path = self.data_path
                data = self._data
                ext = '.dat'

                # reset attributes
                self.data_file = str()
                self._data = str()
            
            elif table=='Log':
                path = self.log_path
                data = self._log
                ext = '.log'

                # reset attributes
                self.log_file = str()
                self._log = str()
            else:
                raise ValueError(f"not implemented")

            file = str()
            # self.data_file = str()
            
            if data:
                # create appropriate file name and write mode
                file = os.path.join(path, yyyy, mm, dd, f"{self.name}-{timestamp}.{ext}")

                # configure file mode, open file and write to it
                if os.path.exists(file):
                    mode = 'a'
                    header = str()
                else:
                    mode = 'w'
                    header = str()

                with open(file=file, mode=mode) as fh:
                    fh.write(header)
                    fh.write(data)
                    self.logger.info(f"[{self.name}] file saved: {file}")

            
            #     # reset self._data
            #     self._data = str()


            # self.data_file = data_file

            # log_file = str()
            # self.log_file = str()
            # if self._log:
            #     # create appropriate file name and write mode
            #     log_file = os.path.join(self.log_path, yyyy, mm, dd, f"{self.name}-{timestamp}.log")

            #     # configure file mode, open file and write to it
            #     if os.path.exists(self.log_file):
            #         mode = 'a'
            #         header = str()
            #     else:
            #         mode = 'w'
            #         header = str()

            #     with open(file=log_file, mode=mode) as fh:
            #         fh.write(header)
            #         fh.write(self._log)
            #         self.logger.info(f"[{self.name}] file saved: {log_file}")

            #     # reset self._data
            #     self._log = str()

            # self.log_file = log_file

            return

        except Exception as err:
            self.logger.error(err)


    def _stage_file(self, table: str):
        """ Stage file, optionally as .zip archive.
        """
        try:
            if not table in self._tables:
                raise ValueError(f"'table' must be one of {self._tables}")
            self.logger.info(f"[{self.name}]: .stage_file {table}")

            if table=='Data':
                file = self.data_file
                ext = '.dat'
                path = self._data_staging_path
            elif table=='Log':
                file = self.log_file
                ext = '.log'
                path = self._log_staging_path
            else:
                raise ValueError(f"not implemented")

            if file:
                if self.zip:
                    file_staged = os.path.join(path, os.path.basename(file).replace(ext, '.zip'))
                    with zipfile.ZipFile(file_staged, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                        zf.write(file, os.path.basename(file))
                else:
                    file_staged = os.path.join(path, os.path.basename(file))
                    shutil.copy(src=file, dst=file_staged)
                self.logger.info(f"[{self.name}] file staged: {file_staged}")

        except Exception as err:
            self.logger.error(err)


    def _save_and_stage_data(self):
        try:
            self.logger.info(f"[{self.name}]: ._save_and_stage_data")
        
            self._save_data('Data')
            self._stage_file('Data')
            self._save_data('Log')
            self._stage_file('Log')
            return

        except Exception as err:
            self.logger.error(err)


    def print_ae33(self) -> None:
        """Retrieve current record from Data table and print."""
        try:
            # read the last record from the Data table
            maxid = int(self._tcpip_comm(cmd="MAXID Data", tidy=True))
            cmd=f"FETCH Data {maxid}"
            data = self._tcpip_comm(cmd, tidy=True)
            data = data.split(sep="|")

            tape_adv_remaining = self.tape_advances_remaining()
            msg = f"Tape advances remaining: {tape_adv_remaining}"
            if int(tape_adv_remaining) < 10:
                msg += " ATTENTION: Get ready to change change!"
            print(colorama.Fore.GREEN + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self.name}] BC: {data[44]} ng/m3 UVPM: {data[29]} ng/m3 ({msg})")

        except Exception as err:
            self.logger.error(err)


    def tape_advances_remaining(self) -> str:
        try:
            cmd = "$AE33:A"
            res = self._tcpip_comm(cmd, tidy=True)
            res = res.replace("\n", "")
            return res

        except Exception as err:
            self.logger.error(err)
            return str()


    # def _accumulate_new_data(self, sep="|") -> None:
    #     """
    #     Retrieve all records from table data that have not been read.

    #     :param str sep: item separator. Defaults to True.
    #     :param bln save: Should data be saved to file? Default=True
    #     :return str response as decoded string
    #     """
    #     try:
    #         # dtm = time.strftime('%Y-%m-%d %H:%M:%S')
    #         self.logger.info(f"[{self.name}]: ._accumulate_new_data_data")

    #         # read the latest records from the Data table
    #         data = ""
    #         maxid = int(self._tcpip_comm(cmd="MAXID Data", tidy=True))

    #         # get data_begin_read_id
    #         if self._data_begin_read_id:
    #             data_begin_read_id = self._data_begin_read_id
    #         else:
    #             # if we don't know where to start, we start at the beginning
    #             minid = int(self._tcpip_comm(cmd="MINID Data", tidy=True))

    #             # limit the number of records to download to 1440 (1 day)
    #             if maxid - minid > 1440:
    #                 minid = maxid - 1440
    #             data_begin_read_id = minid

    #         if data_begin_read_id < maxid:
    #             chunk_size = 1000
    #             while data_begin_read_id < maxid:
    #                 if (maxid - data_begin_read_id) > chunk_size:
    #                     cmd=f"FETCH Data {data_begin_read_id} {data_begin_read_id + chunk_size}"
    #                 else:
    #                     cmd=f"FETCH Data {data_begin_read_id} {maxid}"
    #                 data = self._tcpip_comm(cmd, tidy=True)

    #                 self._data += data
    #                 self.logger.info(f"{self.name}: {data[:60]}[...]")

    #                 data_begin_read_id += chunk_size + 1

    #             # set data_begin_read_id
    #             self._data_begin_read_id = maxid + 1
    #             return

    #     except Exception as err:
    #         self.logger.error(err)


    # def _save_data(self) -> None:
    #     try:
    #         data_file = str()
    #         self.data_file = str()
    #         if self._data:
    #             # create appropriate file name and write mode
    #             now = datetime.now()
    #             timestamp = now.strftime(self._file_timestamp_format)
    #             yyyy = now.strftime('%Y')
    #             mm = now.strftime('%m')
    #             dd = now.strftime('%d')
    #             data_file = os.path.join(self.data_path, yyyy, mm, dd, f"{self.name}-{timestamp}.dat")

    #             # configure file mode, open file and write to it
    #             if os.path.exists(self.data_file):
    #                 mode = 'a'
    #                 header = str()
    #             else:
    #                 mode = 'w'
    #                 header = str()

    #             with open(file=data_file, mode=mode) as fh:
    #                 fh.write(header)
    #                 fh.write(self._data)
    #                 self.logger.info(f"[{self.name}] file saved: {data_file}")

    #             # reset self._data
    #             self._data = str()

    #         self.data_file = data_file
    #         return

    #     except Exception as err:
    #         self.logger.error(err)


    # def _stage_data_file(self):
    #     """ Create zip file from self.data_file and stage archive.
    #     """
    #     try:
    #         if self.data_file:
    #             archive = os.path.join(self._data_staging_path, os.path.basename(self.data_file).replace('.dat', '.zip'))
    #             with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    #                 zf.write(self.data_file, os.path.basename(self.data_file))
    #                 self.logger.info(f"[{self.name}] file staged: {archive}")

    #     except Exception as err:
    #         self.logger.error(err)


    # def _save_and_stage_data(self):
    #     self._save_data()
    #     self._stage_data_file()


    # def get_new_log_entries(self, sep="|", save=True) -> str:
    #     """
    #     Retrieve all records from table data that have not been read and optionally write to log.

    #     :param str sep: item separator. Defaults to True.
    #     :param bln save: Should data be saved to file? Default=True
    #     :return str response as decoded string
    #     """
    #     try:
    #         dtm = time.strftime('%Y-%m-%d %H:%M:%S')
    #         self.logger.info(f"[{self.name}] .get_new_log_entries, save={save})")

    #         # get data_begin_read_id
    #         if self.__log_begin_read_id:
    #             log_begin_read_id = self.__log_begin_read_id
    #         else:
    #             # if we don't know where to start, we start at the beginning
    #             minid = int(self._tcpip_comm(cmd="MINID Log", tidy=True))
    #             log_begin_read_id = minid
    #         # read the last record from the Log table
    #         # get the maximum id in the Log table
    #         log = str()
    #         maxid = int(self._tcpip_comm(cmd="MAXID Log", tidy=True))
    #         if log_begin_read_id < maxid:
    #             cmd=f"FETCH Log {log_begin_read_id} {maxid}"
    #             log = self._tcpip_comm(cmd, tidy=True)

    #             # set log_begin_read_id for the next call
    #             self.__log_begin_read_id = maxid + 1

    #             if save:
    #                 # generate the datafile name
    #                 # self.__logfile = os.path.join(self._logdir,
    #                 #                             "".join([self.name, "-",
    #                 #                                     datetimebin.dtbin(self._reporting_interval), ".log"]))
    #                 self.__logfile = os.path.join(self._logdir, time.strftime("%Y"), time.strftime("%m"), time.strftime("%d"),
    #                                             "".join([self.name, "-",
    #                                                     datetimebin.dtbin(self.reporting_interval), ".log"]))

    #                 os.makedirs(os.path.dirname(self.__logfile), exist_ok=True)
    #                 with open(self.__logfile, "at", encoding='utf8') as fh:
    #                     fh.write(f"{dtm}{sep}{log}\n")
    #                     fh.close()

    #                 # stage data for transfer
    #                 self.stage_log_file()

    #         return log

    #     except Exception as err:
    #         self.logger.error(err)
    #         return str()


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
    #             raise ValueError("_logfile cannot be None.")
    #         if self._staging is None:
    #             raise ValueError("_staging cannot be None.")
    #         if self._logdir is None:
    #             raise ValueError("_logdir cannot be None.")

    #         if self.__logfile_to_stage is None:
    #             self.__logfile_to_stage = self.__logfile
    #         elif self.__logfile_to_stage != self.__logfile:
    #             root = os.path.join(self._staging, self.name, os.path.basename(self._logdir))
    #             os.makedirs(root, exist_ok=True)
    #             if self.zip:
    #                 # create zip file
    #                 archive = os.path.join(root, "".join([os.path.basename(self.__logfile_to_stage)[:-4], ".zip"]))
    #                 with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    #                     zf.write(self.__logfile_to_stage, os.path.basename(self.__logfile_to_stage))
    #             else:
    #                 shutil.copyfile(self.__logfile_to_stage, os.path.join(root, os.path.basename(self.__logfile_to_stage)))
    #             self.__logfile_to_stage = self.__logfile

    #     except Exception as err:
    #         self.logger.error(err)


    # def get_latest_ATN_info(self, save=True) -> str:
    #     """Get latest ATN info from log file, based on tape advance info"""
    #     try:
    #         # minid = int(self._tcpip_comm(cmd="MINID Log", tidy=True))
    #         maxid = int(self._tcpip_comm(cmd="MAXID Log", tidy=True))
    #         if int(maxid) > 49:
    #             minid = str(int(maxid) - 50)
    #         else:
    #             minid = "1"
    #         log = self._tcpip_comm(cmd=f"FETCH Log {minid} {maxid}", tidy=False)
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
    #                                            "".join([self.name, "-",
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
    #         print(colorama.Fore.RED + f"{time.strftime('%Y-%m-%d %H:%M:%S')} [{self.name}] produced error {err}.")


# %%
if __name__ == "__main__":
    pass
