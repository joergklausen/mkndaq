import datetime
import logging
import socket
import time
from pathlib import Path
from typing import Any

import colorama
import polars as pl
import schedule

from mkndaq.utils.utils import setup_logging


class FIDAS:
    def __init__(self, config: dict, name: str='fidas'):
        """FIDAS data acquisition class.

        | Channel | Meaning (short)                   | Unit                             |
        | ------: | --------------------------------- | -------------------------------- |
        |  **0**  |                                   |                                  |
        |  **14** | ? (values in the range 65-66)     |                                  |
        |  **15** | ? (steps between 0 and 75)        |                                  |
        |  **16** | ? (values ca 3300-3550)           |                                  |
        |  **18** | ? (values between 12.5-13)        |                                  |
        |  **19** | LED status                        |                                  |               
        |  **20** | General operating status          |                                  |
        |  **21** | ? (strange pattern, ca 0.0-1.0)   |                                  |
        |  **22** | ? (always 1, with small gaps)     |                                  |
        |  **23** | Aerosol pump power                | %                                |
        |  **23** | Aerosol pump power                | %                                |
        |  **23** | Aerosol pump power                | %                                |
        |  **23** | Aerosol pump power                | %                                |
        |  **24** | IADS temperature                  | °C                               |
        |  **26** | ?? LED temperature ??             | °C                               |
        |  **27** | Volume flow                       | L/min                            |
        |  **29** | ? 0.3-0.6 up to 20, large gap     |                                  |

        |  **35** | Air Quality Index (AQI)           | % (AQI index, 0–101 range in UI) |
        |  **36** | Infection Risk Index              | %                                |
        |  **40** | Air temperature                   | °C                               |
        |  **41** | Relative humidity                 | %                                |
        |  **47** | Air pressure                      | hPa                              |
        |  **50** | CO₂ concentration                 | ppm                              |
        |  **51** | VOC mass concentration            | mg/m³                            |
        |  **60** | Particle number concentration Cn  | particles/cm³                    |
        |  **61** | PM₁ mass concentration            | µg/m³                            |
        |  **62** | PM₂.₅ mass concentration          | µg/m³                            |
        |  **63** | PM₄ mass concentration            | µg/m³                            |
        |  **64** | PM₁₀ mass concentration           | µg/m³                            |
        |  **65** | PM_total (TSP) mass concentration | µg/m³                            |

        The data channels from channel 110 onward each give the number concentration in particles/cm³ for the specified interval. 
        The following list shows the lower and upper limits of the intervals (X_uk, X_ok).
        Examples:
        |     Channel | Lower bound Xᵤₖ [µm] | Upper bound Xₒₖ [µm] |
        | ----------: | -------------------- | -------------------- |
        |         110 | 0.1778               | 0.1911               |
        |         111 | 0.1911               | 0.2054               |
        |         112 | 0.2054               | 0.2207               |
        |         113 | 0.2207               | 0.2371               |
        |         114 | 0.2371               | 0.2548               |
        |           … | …                    | …                    |
        | up to ~141+ | …                    | …                    |


        Args:
            config (dict): Configuration dictionary.
            name (str, optional): Name of the instrument. Defaults to 'fidas'.
        """
        colorama.init(autoreset=True)

        self.name = name

        # configure logging
        # _logger = config['logging']['file'].split('.')[0]
        self.logger = setup_logging(file=str(Path(config['root']).expanduser() / f"{name}.log"))
        # self.logger = logging.getLogger(f"{_logger}.{__name__}")
        self.logger.info(f"[{self.name}] Initializing")

        self.host = config[name]['socket']['host']
        self.port = config[name]['socket']['port']
        self.buffer_size = config[name]['socket']['buffer_size']

        self.data_path = Path(config['root']).expanduser() / config['data'] / config[name]['data_path']
        self.staging_path = Path(config['root']).expanduser() / config['staging'] / config[name]['staging_path']

        self.remote_path = config[name]['remote_path']

        self.raw_record_interval = config[name]['raw_record_interval']
        self.aggregation_period = config[name]['aggregation_period']
        self.reporting_interval = config[name]['reporting_interval']

        self.sock = None
        self.buffer = ""
        self.parsed: dict[str, Any] = {}
        self.raw_records: list[dict[str, Any]] = []
        self.df_raw_data_median = pl.DataFrame()
        self.current_hour = datetime.datetime.now(datetime.timezone.utc).replace(minute=0, second=0, microsecond=0)

    def __enter__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.host, self.port))
        self.logger.info(f"Listening on {self.host}:{self.port}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.sock:
            self.sock.close()

    def receive_udp_record(self) -> str:
        if self.sock is None:
            return str()
        try:
            self.sock.settimeout(self.raw_record_interval)
            while True:
                data, _ = self.sock.recvfrom(self.buffer_size)
                self.buffer += data.decode('ascii', errors='ignore')
                if '>' in self.buffer:
                    raw_record = self.buffer
                    self.buffer = str()
                    return raw_record
        except socket.timeout:
            pass
        return str()


    def parse_record(self, record: str) -> "dict[str, Any]":
        self.logger.debug(f"[{time.time()}] parse_record")

        try:
            id_part, rest = record.split('<', 1)
            data_part, checksum = rest.split('>', 1)
            self.parsed = {"id": int(id_part.strip()), "checksum": checksum.strip()}

            if data_part.startswith("sendVal"):
                data_part = data_part[len("sendVal"):].strip()

            for pair in data_part.split(';'):
                if '=' in pair:
                    k, v = pair.split('=', 1)
                    key = f"{int(k.strip())}"
                    try:
                        val = float(v.strip())
                    except ValueError:
                        val = float('nan')
                    self.parsed[key] = val

            return self.parsed
        except Exception as err:
            self.logger.error(f"Failed to parse record: {err}")
            return {}

    def print_parsed_record(self, keys=['60', '61', '62', '63', '64']):
        """Print latest parsed record"""
        if self.parsed:
            result = "; ".join(f"{k}: {int(self.parsed[k])}" for k in keys if k in self.parsed and self.parsed[k] is not None)
            self.logger.debug(colorama.Fore.GREEN + f"[{self.name}] {result}")
        else:
            self.logger.warning(colorama.Fore.YELLOW + f"[{self.name}] no valid data retrieved." + colorama.Fore.GREEN)

    def collect_raw_record(self):
        # self.logger.debug("[collect_raw_record] called")
        self.raw_record = self.receive_udp_record()
        self.logger.debug(self.raw_record[:90])
        if self.raw_record:
            parsed = self.parse_record(self.raw_record)
            if parsed:
                self.raw_records.append(parsed)
                # self.logger.debug("[collect_raw_record] raw_record appended")
        else:
            self.logger.warning("[collect_raw_record] raw_record is empty")

    def compute_raw_data_median(self, cols: list=['60','61','62','63','64','65']) -> dict:
        self.logger.debug(f"[compute_raw_data_median] called")
        if not self.raw_records:
            self.logger.debug("[compute_raw_data_median] self.raw_records is empty.")
            return dict()

        df = pl.DataFrame(self.raw_records)
        value_cols = [col for col in df.columns if col not in {"id", "checksum"} and df.schema[col] in {pl.Float64, pl.Float32}]

        median_row = df.select([pl.median(col).alias(col) for col in value_cols])
        now = datetime.datetime.now(datetime.timezone.utc)

        median_row = median_row.with_columns([
            pl.lit("median").alias("id"),
            pl.lit("").alias("checksum"),
            pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("dtm")
        ])

        for col in df.columns:
            if col not in median_row.columns:
                median_row = median_row.with_columns(pl.lit(None).alias(col))

        median_row = median_row.select(sorted(median_row.columns))
        median_dict = {col: median_row[0, col] for col in cols}
        self.df_raw_data_median = pl.concat([self.df_raw_data_median, median_row], how="diagonal")
        self.raw_records.clear()

        # self.logger.info(f"[compute_raw_data_median] df_median contains {len(self.df_raw_data_median)} rows.")
        self.logger.info(f"[{self.name}] median  {median_dict}.")

        return median_dict

    def save_hourly(self, stage: bool=True):
        self.logger.debug(f"[save_hourly] called")
        now = datetime.datetime.now(datetime.timezone.utc)
        if now.hour != self.current_hour.hour:
            if not self.df_raw_data_median.is_empty():
                data_path = self.ensure_data_path(self.current_hour)
                if data_path.exists():
                    existing = pl.read_parquet(data_path)
                    self.df_raw_data_median = pl.concat([existing, self.df_raw_data_median], how="diagonal").unique()
                self.df_raw_data_median.write_parquet(data_path)
                self.logger.info(f"Saved hourly file: {data_path}")
                if stage:
                    staging_path = self.ensure_staging_path(self.current_hour)
                    self.df_raw_data_median.write_parquet(staging_path)
                    self.logger.info(f"Staged hourly file: {staging_path}")

            self.df_raw_data_median = pl.DataFrame()
            self.current_hour = now.replace(minute=0, second=0, microsecond=0)

    def ensure_data_path(self, dt: datetime.datetime) -> Path:
        folder = self.data_path / f"{dt.year:04d}" / f"{dt.month:02d}" / f"{dt.day:02d}"
        folder.mkdir(parents=True, exist_ok=True)
        filename = f"fidas-{dt.year:04d}{dt.month:02d}{dt.day:02d}{dt.hour:02d}.parquet"
        return folder / filename

    def ensure_staging_path(self, dt: datetime.datetime) -> Path:
        folder = self.staging_path
        folder.mkdir(parents=True, exist_ok=True)
        filename = f"fidas-{dt.year:04d}{dt.month:02d}{dt.day:02d}{dt.hour:02d}.parquet"
        return folder / filename

    def setup_schedules(self):
        try:
            schedule.every(self.raw_record_interval).seconds.do(self.collect_raw_record)
            schedule.every(self.aggregation_period).minutes.do(self.compute_raw_data_median)
            schedule.every(1).hours.do(self.save_hourly, stage=True)
            self.logger.info(schedule.get_jobs())
        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)


    def run(self):
        self.logger.info("=== Starting FIDAS DAQ =======")
        # print("=== Starting FIDAS DAQ =======")
        schedule.every(self.raw_record_interval).seconds.do(self.collect_raw_record)
        schedule.every(self.aggregation_period).minutes.do(self.compute_raw_data_median)
        schedule.every(1).hours.do(self.save_hourly, stage=True)
        self.logger.info(schedule.get_jobs())
        # print(schedule.get_jobs())

        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Stopping FIDAS...")
            self.save_hourly()  # Save any remaining data on exit


if __name__ == "__main__":
    pass


# import datetime as dt
# import logging
# import logging.handlers
# import os
# import shutil
# import socket
# import struct
# import time
# import warnings
# import zipfile

# import colorama
# # from pymodbus.client.tcp import ModbusTcpClient
# # from pymodbus.exceptions import ModbusException

# # Instrument setup
# # > 'accessories' > IADS > change from 'remove volatile/moisture compensation' to OFF
# # > Control Panel >


# # Text file format Fidas:
# header = ['Date',
# 'Time',
# 'Comment',
# 'PM1',
# 'PM2.5',
# 'PM4',
# 'PM10',
# 'PMtotal',
# 'Number Concentration',
# 'Humidity',
# 'Temperature',
# 'Pressure',
# 'Flow',
# 'Coincidence',
# 'Pumps',
# 'Weather station',
# 'IADS',
# 'Calibration',
# 'LED',
# 'Operating mode',
# 'Device status',
# 'PM1',
# 'PM2.5',
# 'PM4',
# 'PM10',
# 'PMtotal',
# 'PM1_classic',
# 'PM2.5_classic',
# 'PM4_classic',
# 'PM10_classic',
# 'PMtotal_classic',
# 'PMthoraic',
# 'PMalveo',
# 'PMrespirable',
# 'Flowrate',
# 'Velocity',
# 'Coincidence',
# 'Pump_output',
# 'IADS_temperature',
# 'Raw channel deviation',
# 'LED temperature',
# 'Temperature*',
# 'Humidity*',
# 'Pressure*',]

# device_status = {'Scope':0,
#                  'Auto':1,
#                  'Manual':2,
#                  'Idle':3,
#                  'Calib':4,
#                  'Offset':5,
#                  'PDControl':6,
#                  }


# # class ModbusTCPDriver:
# #     def __init__(self, ip: str, port: int = 11231, unit_id: int = 1):
# #         """
# #         Initialize a Modbus TCP connection.

# #         Args:
# #             ip (str): IP address of the Modbus instrument.
# #             port (int): TCP port number (default 502).
# #             unit_id (int): Modbus slave/unit ID.
# #         """
# #         self.ip = ip
# #         self.port = port
# #         self.unit_id = unit_id
# #         self.client = ModbusTcpClient(ip, port=port)
# #         self.connected = False

# #     def connect(self):
# #         """Establish the TCP connection."""
# #         self.connected = self.client.connect()
# #         if not self.connected:
# #             raise ConnectionError(f"Failed to connect to {self.ip}:{self.port}")

# #     def close(self):
# #         """Close the TCP connection."""
# #         self.client.close()
# #         self.connected = False

# #     def read_holding_registers(self, address: int, count: int):
# #         """Read holding registers starting at address."""
# #         try:
# #             response = self.client.read_holding_registers(address=address, count=count, slave=self.unit_id)
# #             if response.isError():
# #                 raise ModbusException(f"Error reading registers at {address}: {response}")
# #             return response.registers
# #         except ModbusException as e:
# #             print(f"Modbus error: {e}")
# #             return None

# #     def write_single_register(self, address: int, value: int):
# #         """Write a single value to one holding register."""
# #         try:
# #             response = self.client.write_register(address=address, value=value, slave=self.unit_id)
# #             if response.isError():
# #                 raise ModbusException(f"Error writing to register {address}: {response}")
# #             return True
# #         except ModbusException as e:
# #             print(f"Modbus error: {e}")
# #             return False

# #     def __enter__(self):
# #         self.connect()
# #         return self

# #     def __exit__(self, exc_type, exc_val, exc_tb):
# #         self.close()






# import argparse
# import logging
# import os
# import re
# import time
# from datetime import datetime
# from typing import Callable

# import polars as pl
# import schedule


# def setup_logging() -> None:
#     logging.basicConfig(
#         level=logging.INFO,
#         format="%(asctime)s [%(levelname)s] %(message)s",
#     )


# def read_from_instrument() -> str:
#     # Replace this with your actual instrument I/O
#         """
#         Establish a connection and retrieve a record.

#         :return: response of instrument, decoded
#         """
#         rcvd = str()
#         _socktout = 2
#         _sockaddr = ('192.168.2.129', 56790)

#         try:
#             # open socket connection as a client
#             with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, ) as s:
#                 # connect to the server
#                 s.settimeout(_socktout)
#                 s.bind(_sockaddr)

#                 while True:
#                     data, addr = s.recvfrom(1024)
#                     if '>' in data.decode():
#                         rcvd = f"{rcvd}{data.decode()}"
#                         break

#             print(f"{time.time()} {rcvd}")

#             return rcvd

#         except Exception as err:
#             print(err)
#             return str()
#     # return '6082<sendVal 0=0.0;1=1.0;2=2.0;8=4.8;14=42.4;74=0.0>3E'


# def collect_and_aggregate_polars(
#     read_func: Callable[[], str],
#     raw_record_interval: int,
#     output_dir: str
# ) -> None:
#     """
#     Collects instrument data for 1 minute, parses into a Polars DataFrame,
#     computes medians, and saves results to a timestamped CSV file.
#     """
#     logging.info("Collecting data...")
#     rows = []
#     end_time = time.time() + 60

#     while time.time() < end_time:
#         line = read_func()
#         match = re.search(r"<sendVal (.+?)>", line)
#         if match:
#             payload = match.group(1)
#             parsed = {}
#             for item in payload.split(";"):
#                 if "=" not in item:
#                     continue
#                 key_str, value_str = item.split("=")
#                 try:
#                     key = f"v{int(key_str)}"
#                     value = float(value_str)
#                     if not value_str.lower() == "nan":
#                         parsed[key] = value
#                 except ValueError:
#                     continue
#             if parsed:
#                 rows.append(parsed)
#         time.sleep(raw_record_interval)

#     if not rows:
#         logging.warning("No valid data collected in this interval.")
#         return

#     df = pl.DataFrame(rows).fill_nan(None)
#     median_row = df.select(pl.all().median()).to_dict(as_series=False)

#     now = dt.datetime.now(dt.timezone.utc)
#     timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
#     filename = os.path.join(output_dir, f"fidas-{now.strftime('%Y%m%d%H')}.csv")

#     sorted_keys = sorted(median_row.keys())
#     file_exists = os.path.exists(filename)

#     os.makedirs(output_dir, exist_ok=True)
#     with open(filename, "a") as f:
#         if not file_exists:
#             f.write("timestamp," + ",".join(sorted_keys) + "\n")
#         line = timestamp + "," + ",".join(
#             f"{median_row[k]:.4f}" if median_row[k] is not None else "NaN"
#             for k in sorted_keys
#         )
#         f.write(line + "\n")

#     logging.info("Wrote 1-minute aggregate to %s", filename)


# def main():
#     parser = argparse.ArgumentParser(description="Fidas Data Collector")
#     parser.add_argument("--interval", type=int, default=5,
#                         help="Raw data sampling interval in seconds (default: 5)")
#     parser.add_argument("--output", type=str, default=".",
#                         help="Output directory for CSV files")
#     args = parser.parse_args()

#     setup_logging()
#     logging.info("Starting Fidas data collector...")
#     schedule.every(1).minutes.do(
#         collect_and_aggregate_polars,
#         read_func=read_from_instrument,
#         raw_record_interval=args.interval,
#         output_dir=args.output
#     )

#     while True:
#         schedule.run_pending()
#         time.sleep(1)


# if __name__ == "__main__":
#     main()


# # if __name__ == "__main__":
#     # ip = "192.168.0.216"  # your instrument's IP
#     # port = 502            # default Modbus TCP port
#     # unit_id = 1           # check your instrument docs

#     # with ModbusTCPDriver(ip, port, unit_id) as driver:
#     #     registers = driver.read_holding_registers(address=0, count=10)
#     #     if registers is not None:
#     #         print("Register values:", registers)
#     #     else:
#     #         print("Failed to read registers")
