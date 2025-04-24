import datetime as dt
import logging
import logging.handlers
import os
import shutil
import socket
import struct
import time
import warnings
import zipfile

import colorama
from pymodbus.client.tcp import ModbusTcpClient
from pymodbus.exceptions import ModbusException

# Instrument setup
# > 'accessories' > IADS > change from 'remove volatile/moisture compensation' to OFF
# > Control Panel >


# Text file format Fidas:
header = ['Date',
'Time',
'Comment',
'PM1',
'PM2.5',
'PM4',
'PM10',
'PMtotal',
'Number Concentration',
'Humidity',
'Temperature',
'Pressure',
'Flow',
'Coincidence',
'Pumps',
'Weather station',
'IADS',
'Calibration',
'LED',
'Operating mode',
'Device status',
'PM1',
'PM2.5',
'PM4',
'PM10',
'PMtotal',
'PM1_classic',
'PM2.5_classic',
'PM4_classic',
'PM10_classic',
'PMtotal_classic',
'PMthoraic',
'PMalveo',
'PMrespirable',
'Flowrate',
'Velocity',
'Coincidence',
'Pump_output',
'IADS_temperature',
'Raw channel deviation',
'LED temperature',
'Temperature*',
'Humidity*',
'Pressure*',]

device_status = {'Scope':0,
                 'Auto':1,
                 'Manual':2,
                 'Idle':3,
                 'Calib':4,
                 'Offset':5,
                 'PDControl':6,
                 }



class FIDAS:
    def __init__(self, config: dict, name: str='fidas'):
        """
        Initialize the FIDAS 200 instrument class with parameters from a configuration file.

        Args:
            config (dict): general configuration
        """
        colorama.init(autoreset=True)

        try:
            # configure logging
            _logger = f"{os.path.basename(config['logging']['file'])}".split('.')[0]
            self.logger = logging.getLogger(f"{_logger}.{__name__}")

            # read instrument control properties for later use
            self._name = name
            self._serial_number = config[name]['serial_number']
            self._get_data = config[name]['get_data']

            self.logger.info(f"Initialize FIDAS 200 (name: {self._name}  S/N: {self._serial_number})")

            # configure tcp/ip
            self._sockaddr = (config[name]['socket']['host'],
                            config[name]['socket']['port'])
            self._socktout = config[name]['socket']['timeout']
            self._socksleep = config[name]['socket']['sleep']

            root = os.path.expanduser(config['root'])

            # configure data collection and reporting
            self._sampling_interval = config[name]['sampling_interval']
            self.reporting_interval = config[name]['reporting_interval']
            if not (self.reporting_interval % 60)==0 and self.reporting_interval<=1440:
                raise ValueError('reporting_interval must be a multiple of 60 and less or equal to 1440 minutes.')

            self.header = 'Fidas header\n'

            # configure saving, staging and remote transfer
            self.data_path = os.path.join(root, config['data'], config[name]['data_path'])
            self.staging_path = os.path.join(root, config['staging'], config[name]['staging_path'])
            self.remote_path = config[name]['remote_path']

            # initialize data response
            self._data = str()

            # initialize data_file (path)
            self.data_file = str()

        except Exception as err:
            self.logger.error(err)


    def setup_schedules(self):
        try:
            # configure folders needed
            os.makedirs(self.data_path, exist_ok=True)
            os.makedirs(self.staging_path, exist_ok=True)

            # configure data acquisition schedule
            schedule.every(int(self._sampling_interval)).minutes.at(':00').do(self.accumulate_data)

            # configure saving and staging schedules
            if self.reporting_interval==10:
                self._file_timestamp_format = '%Y%m%d%H%M'
                minutes = [f"{self.reporting_interval*n:02}" for n in range(6) if self.reporting_interval*n < 6]
                for minute in minutes:
                    schedule.every(1).hour.at(f"{minute}:01").do(self._save_and_stage_data)
            elif self.reporting_interval==60:
                self._file_timestamp_format = '%Y%m%d%H'
                schedule.every(1).hour.at('00:01').do(self._save_and_stage_data)
            elif self.reporting_interval==1440:
                self._file_timestamp_format = '%Y%m%d'
                schedule.every(1).day.at('00:00:01').do(self._save_and_stage_data)

        except Exception as err:
            self.logger.error(err)


    def udp_ascii_retrieve(self) -> str:
        """
        Establish a connection and retrieve a record.

        :return: response of instrument, decoded
        """
        rcvd = b''
        try:
            # open socket connection as a client
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, ) as s:
                # connect to the server
                s.settimeout(self._socktout)
                s.bind(self._sockaddr)

                while True:
                    data, addr = s.recv(1024)
                    rcvd = rcvd + data
                    if b'>' in data:
                        break

            return rcvd.decode()

        except Exception as err:
            self.logger.error(err)
            return str()


     def accumulate_data(self):
        """
        Retrieve data from instrument during self.sampling_interval, compute median, add time stamp and append to self._data.
        """
        try:
            dtm = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            instant_data = self.udp_ascii_retrieve()
            self._data += f"{dtm} {instant_data}\n"
            self.logger.info(f"{self._name}, {_[:60]}[...]")

            return

        except Exception as err:
            self.logger.error(err)


    # def print_o3(self) -> None:
    #     try:
    #         if self._serial_com:
    #             o3 = self.serial_comm('o3').split()
    #         else:
    #             o3 = self.tcpip_comm('o3').split()
    #         self.logger.info(colorama.Fore.GREEN + f"{self._name}, {o3[0].upper()} {str(float(o3[1]))} {o3[2]}")

    #     except Exception as err:
    #         self.logger.error(colorama.Fore.RED + f"{err}")


    def _save_data(self) -> None:
        try:
            data_file = str()
            if self._data:
                # create appropriate file name and write mode
                timestamp = datetime.now().strftime(self._file_timestamp_format)
                data_file = os.path.join(self.data_path, f"49i-{timestamp}.dat")

                # configure file mode, open file and write to it
                if os.path.exists(data_file):
                    with open(file=data_file, mode='a') as fh:
                        fh.write(self._data)
                else:
                    with open(file=data_file, mode='w') as fh:
                        fh.write(self.header)
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


    def _save_and_stage_data(self):
        self._save_data()
        self._stage_file()


if __name__ == "__main__":
    pass






class ModbusTCPDriver:
    def __init__(self, ip: str, port: int = 11231, unit_id: int = 1):
        """
        Initialize a Modbus TCP connection.

        Args:
            ip (str): IP address of the Modbus instrument.
            port (int): TCP port number (default 502).
            unit_id (int): Modbus slave/unit ID.
        """
        self.ip = ip
        self.port = port
        self.unit_id = unit_id
        self.client = ModbusTcpClient(ip, port=port)
        self.connected = False

    def connect(self):
        """Establish the TCP connection."""
        self.connected = self.client.connect()
        if not self.connected:
            raise ConnectionError(f"Failed to connect to {self.ip}:{self.port}")

    def close(self):
        """Close the TCP connection."""
        self.client.close()
        self.connected = False

    def read_holding_registers(self, address: int, count: int):
        """Read holding registers starting at address."""
        try:
            response = self.client.read_holding_registers(address=address, count=count, slave=self.unit_id)
            if response.isError():
                raise ModbusException(f"Error reading registers at {address}: {response}")
            return response.registers
        except ModbusException as e:
            print(f"Modbus error: {e}")
            return None

    def write_single_register(self, address: int, value: int):
        """Write a single value to one holding register."""
        try:
            response = self.client.write_register(address=address, value=value, slave=self.unit_id)
            if response.isError():
                raise ModbusException(f"Error writing to register {address}: {response}")
            return True
        except ModbusException as e:
            print(f"Modbus error: {e}")
            return False

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()






import argparse
import logging
import os
import re
import time
from datetime import datetime
from typing import Callable

import polars as pl
import schedule


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def read_from_instrument() -> str:
    # Replace this with your actual instrument I/O
    return '6082<sendVal 0=0.0;1=1.0;2=2.0;8=4.8;14=42.4;74=0.0>3E'


def collect_and_aggregate_polars(
    read_func: Callable[[], str],
    interval_seconds: int,
    output_dir: str
) -> None:
    """
    Collects instrument data for 1 minute, parses into a Polars DataFrame,
    computes medians, and saves results to a timestamped CSV file.
    """
    logging.info("Collecting data...")
    rows = []
    end_time = time.time() + 60

    while time.time() < end_time:
        line = read_func()
        match = re.search(r"<sendVal (.+?)>", line)
        if match:
            payload = match.group(1)
            parsed = {}
            for item in payload.split(";"):
                if "=" not in item:
                    continue
                key_str, value_str = item.split("=")
                try:
                    key = f"v{int(key_str)}"
                    value = float(value_str)
                    if not value_str.lower() == "nan":
                        parsed[key] = value
                except ValueError:
                    continue
            if parsed:
                rows.append(parsed)
        time.sleep(interval_seconds)

    if not rows:
        logging.warning("No valid data collected in this interval.")
        return

    df = pl.DataFrame(rows).fill_nan(None)
    median_row = df.select(pl.all().median()).to_dict(as_series=False)

    now = datetime.utcnow()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    filename = os.path.join(output_dir, f"fidas-{now.strftime('%Y%m%d%H')}.csv")

    sorted_keys = sorted(median_row.keys())
    file_exists = os.path.exists(filename)

    os.makedirs(output_dir, exist_ok=True)
    with open(filename, "a") as f:
        if not file_exists:
            f.write("timestamp," + ",".join(sorted_keys) + "\n")
        line = timestamp + "," + ",".join(
            f"{median_row[k]:.4f}" if median_row[k] is not None else "NaN"
            for k in sorted_keys
        )
        f.write(line + "\n")

    logging.info("Wrote 1-minute aggregate to %s", filename)


def main():
    parser = argparse.ArgumentParser(description="Fidas Data Collector")
    parser.add_argument("--interval", type=int, default=5,
                        help="Sampling interval in seconds (default: 5)")
    parser.add_argument("--output", type=str, default=".",
                        help="Output directory for CSV files")
    args = parser.parse_args()

    setup_logging()
    logging.info("Starting Fidas data collector...")
    schedule.every(1).minutes.do(
        collect_and_aggregate_polars,
        read_func=read_from_instrument,
        interval_seconds=args.interval,
        output_dir=args.output
    )

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
















if __name__ == "__main__":
    ip = "192.168.0.216"  # your instrument's IP
    port = 502            # default Modbus TCP port
    unit_id = 1           # check your instrument docs

    with ModbusTCPDriver(ip, port, unit_id) as driver:
        registers = driver.read_holding_registers(address=0, count=10)
        if registers is not None:
            print("Register values:", registers)
        else:
            print("Failed to read registers")
