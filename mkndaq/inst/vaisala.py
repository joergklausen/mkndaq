
from __future__ import annotations

import functools
import logging
import os
import re
import threading
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import schedule
import serial
from pymodbus.client import ModbusSerialClient

# Compile once at module import
_HMP_READING_RE = re.compile(
    r"T=\s*([+-]?\d+(?:\.\d+)?)\s*'C\s+"
    r"RH=\s*([+-]?\d+(?:\.\d+)?)\s*%RH\s+"
    r"Td=\s*([+-]?\d+(?:\.\d+)?)\s*'C"
)

@dataclass
class HMP110Reading:
    rh: float              # Relative humidity [%]
    temperature: float     # Temperature [°C]
    dew_point: Optional[float] = None  # Dew/frost point [°C], if available


def with_serial(func):
    @functools.wraps(func)
    def wrapper(self, cmd: str, *args, retries: int = 3, **kwargs) -> str:
        # cooldown gate
        now = time.time()
        if getattr(self, "_cooldown_until", 0.0) > now:
            return ""

        # non-overlapping I/O
        if not self._io_lock.acquire(blocking=False):
            return ""

        try:
            last_err: Exception | None = None
            for i in range(retries):
                try:
                    if not self._serial.is_open:
                        self._serial.open()

                    resp = func(self, cmd, *args, **kwargs)

                    if resp:
                        # success
                        self._fail_count = 0
                        self._cooldown_until = 0.0
                        return resp

                    # Empty / incomplete response -> treat like a soft timeout
                    if i < retries - 1:
                        self.logger.debug(
                            f"[{self.name}] serial_comm attempt {i+1}/{retries} "
                            f"returned empty for {cmd!r}; retrying..."
                        )
                    else:
                        self.logger.error(
                            f"[{self.name}] serial_comm empty response after "
                            f"{retries} attempts for {cmd!r}"
                        )

                except (serial.SerialTimeoutException, serial.SerialException, OSError) as err:
                    last_err = err
                    # Only escalate to ERROR on the last attempt; warn before that
                    level = logging.WARNING if i < retries - 1 else logging.ERROR
                    self.logger.log(
                        level,
                        f"[{self.name}] serial_comm attempt {i+1}/{retries} failed for {cmd!r}: {err}",
                    )
                    try:
                        if self._serial.is_open:
                            self._serial.close()
                    except Exception:
                        pass

                # Backoff before next try
                time.sleep(min(0.5 * (2 ** i), 3.0))

            # All attempts failed or empty
            self._fail_count = getattr(self, "_fail_count", 0) + 1
            max_fail = getattr(self, "_max_fail_before_cooldown", 5)
            cooldown = getattr(self, "_cooldown_seconds", 120)
            if self._fail_count >= max_fail:
                self._cooldown_until = time.time() + cooldown
                self.logger.error(
                    f"[{self.name}] communication failing repeatedly; "
                    f"backing off for {cooldown}s."
                )
            return ""
        finally:
            self._io_lock.release()

    return wrapper


class HMP110ASCII:
    # One shared Serial + lock per OS port (e.g. "COM5")
    _serial_by_port: dict[str, serial.Serial] = {}
    _lock_by_port: dict[str, threading.Lock] = {}
    _refcount_by_port: dict[str, int] = {}

    def __init__(self, name: str, config: dict) -> None:
        try:
            self.name = name
            self.serial_number = config[name]['serial_number']

            # configure logging
            _logger = f"{os.path.basename(config['logging']['file'])}".split('.')[0]
            self.logger = logging.getLogger(f"{_logger}.{__name__}")
            self.logger.info(f"[{self.name}] Initializing HMP110 (S/N: {self.serial_number})")

            # read instrument control properties for later use
            self._id = config[name]['id']
            self._data_header = config[name]['data_header']

            # configure serial port and open it (shared per OS port)
            port = config[name]["port"]
            self._port = port

            baudrate = config[port]["baudrate"]
            bytesize = config[port]["bytesize"]
            parity = config[port]["parity"]
            stopbits = config[port]["stopbits"]
            timeout = config[port]["timeout"]
            write_timeout = config[port].get("write_timeout", 2.0)

            try:
                if port in HMP110ASCII._serial_by_port:
                    # Reuse existing shared serial + lock
                    self._serial = HMP110ASCII._serial_by_port[port]
                    self._io_lock = HMP110ASCII._lock_by_port[port]
                    HMP110ASCII._refcount_by_port[port] += 1
                    self.logger.debug(
                        "[%s] Reusing shared serial port %s", self.name, port
                    )
                else:
                    # Create new serial + lock for this port
                    self._io_lock = threading.Lock()
                    self._serial = serial.Serial(
                        port=port,
                        baudrate=baudrate,
                        bytesize=bytesize,
                        parity=parity,
                        stopbits=stopbits,
                        timeout=timeout,
                        write_timeout=write_timeout,
                    )
                    HMP110ASCII._serial_by_port[port] = self._serial
                    HMP110ASCII._lock_by_port[port] = self._io_lock
                    HMP110ASCII._refcount_by_port[port] = 1
                    self.logger.debug(
                        "[%s] Opened serial port %s (baud=%s, parity=%s, stopbits=%s)",
                        self.name,
                        port,
                        baudrate,
                        parity,
                        stopbits,
                    )

                # track repeated communication failures and back off if necessary
                self._fail_count = 0
                self._max_fail_before_cooldown = 5   # consecutive failing commands
                self._cooldown_seconds = 120         # pause 2 minutes after repeated failures
                self._cooldown_until = 0.0           # unix timestamp until which we stay quiet

            except serial.SerialException as err:
                self.logger.error(
                    "[%s] __init__ produced SerialException %s", self.name, err
                )
                raise

            # sampling, aggregation, reporting/storage
            self.sampling_interval = config[name]['sampling_interval']
            self.reporting_interval = config[name]['reporting_interval']
            if not (self.reporting_interval==10 or (self.reporting_interval % 60)==0) and self.reporting_interval<=1440:
                raise ValueError(f"[{self.name}] reporting_interval must be 10 or a multiple of 60 and less or equal to 1440 minutes.")

            # configure saving, staging and archiving
            root = os.path.expanduser(config['root'])
            self.data_path = os.path.join(root, config['data'], config[name]['data_path'])
            self.staging_path = os.path.join(root, config['staging'], config[name]['staging_path'])
            self._file_to_stage = ""
            self._zip = config[name]['staging_zip']

            # command to retrieve data
            self.cmd = f"SEND {self._id}\r\n"

            # configure remote transfer
            self.remote_path = config[name]['remote_path']

            # initialize data response
            self._data = ""

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")


    @staticmethod
    def _parse_reading(raw: str) -> str:
        """
        Parse a line like:
            "T=  19.30 'C RH=  27.26 %RH Td=  -0.02 'C \\r\\n"
        into:
            "19.30,27.26,-0.02"

        Raises ValueError if the pattern cannot be parsed.
        """
        m = _HMP_READING_RE.search(raw)
        if not m:
            raise ValueError(f"Could not parse HMP110 reading: {raw!r}")

        t, rh, td = m.groups()
        return f"{t},{rh},{td}"

    @with_serial
    def serial_comm(self, cmd: str) -> str:

        # Clear stale buffers once per call
        self._serial.reset_input_buffer()
        self._serial.reset_output_buffer()

        # Send command
        self._serial.rs485_mode
        self._serial.write(cmd.encode())
        self._serial.flush()
        time.sleep(0.1)

        timeout = self._serial.timeout or 1.0
        start = time.time()
        rcvd = bytearray()

        while time.time() - start < timeout:
            waiting = self._serial.in_waiting
            if waiting:
                chunk = self._serial.read(waiting)
                rcvd.extend(chunk)

        return rcvd.decode()
    
    def setup_schedules(self, delay_job: int=1):
        try:
            # configure folders needed
            os.makedirs(self.data_path, exist_ok=True)
            os.makedirs(self.staging_path, exist_ok=True)

            # configure data acquisition schedule
            schedule.every(self.sampling_interval).minutes.at(':00').do(self.accumulate_readings)

            # configure saving and staging schedules
            if self.reporting_interval == 10:
                self._file_timestamp_format = '%Y%m%d%H'
                for minute in (0, 10, 20, 30, 40, 50):
                    schedule.every(1).hours.at(f"{minute:02d}:{delay_job:02d}").do(self._save_and_stage_data)
            elif (self.reporting_interval % 60) == 0 and self.reporting_interval < 1440:
                self._file_timestamp_format = '%Y%m%d'
                hours = self.reporting_interval // 60
                schedule.every(hours).hours.at(f"00:{delay_job:02d}").do(self._save_and_stage_data)
            elif self.reporting_interval == 1440:
                schedule.every().day.at(f"00:00:{delay_job:02d}").do(self._save_and_stage_data)
            else:
                raise ValueError("'reporting_interval' must be 10 minutes, a multiple of 60 minutes (<1440), or 1440.")

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")

    def accumulate_readings(self) -> None:
        """Send lrec, append response to buffer, respecting cooldown.

        Locking, retries and cooldown are handled by @with_serial on serial_comm().
        """
        # If we recently saw repeated failures, stay quiet for a while.
        if getattr(self, "_cooldown_until", 0.0) and time.time() < self._cooldown_until:
            return

        try:
            dtm = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            response = self.serial_comm(self.cmd)  # @with_serial handles locking
            if not response:
                return  # no data, nothing to append
            csv_response = self._parse_reading(response)
            self._data += f"{dtm},{csv_response}\n"
            self.logger.debug(f"[{self.name}] {response}")
        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")

    def _save_data(self) -> None:
        try:
            if self._data:
                # create appropriate file name and write mode
                now = datetime.now()
                timestamp = now.strftime(self._file_timestamp_format)
                yyyy = now.strftime('%Y')
                mm = now.strftime('%m')
                dd = now.strftime('%d')
                self.data_file = os.path.join(self.data_path, yyyy, mm, dd, f"{self.name}-{timestamp}.csv")
                os.makedirs(os.path.dirname(self.data_file), exist_ok=True)

                # configure file mode, open file and write to it
                if os.path.exists(self.data_file):
                    mode = 'a'
                    header = ""
                else:
                    mode = 'w'
                    header = f"{self._data_header}\n"

                with open(file=self.data_file, mode=mode) as fh:
                    fh.write(header)
                    fh.write(self._data)
                    self.logger.info(f"[{self.name}] file saved: {self.data_file}")

                # reset self._data
                self._data = ""

            return

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")

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
            self.logger.error(f"[{self.name}] {err}")

    def _save_and_stage_data(self):
        self._save_data()
        self._stage_file()


"""
NB: This has not been tested!!

Simple utility to read Vaisala HMP110 over RS-485 / Modbus RTU.

Assumptions
----------
- HMP110 digital (RS-485) variant with Modbus enabled.
- Default Modbus config unless you changed it with Insight or serial commands:
    baudrate = 19200
    parity   = 'N'
    data bits = 8
    stop bits = 2
    slave / device address = 240
- We use the 32-bit float measurement registers:
    RH: register 1 (address 0x0000, occupies 0x0000–0x0001)
    T : register 3 (address 0x0002, occupies 0x0002–0x0003)
    Td: register 9 (address 0x0008, occupies 0x0008–0x0009, if enabled)
"""
# class HMP110Modbus:
#     """
#     Minimal Modbus RTU client for Vaisala HMP110.

#     Parameters
#     ----------
#     port:
#         Serial port device (e.g. '/dev/ttyUSB0' on Linux, 'COM3' on Windows).
#     slave:
#         Modbus address of the probe (default factory setting is 240).
#     baudrate:
#         Serial baud rate. Default Modbus factory setting is 19200.
#     timeout:
#         Read timeout in seconds.
#     """

#     def __init__(
#         self,
#         port: str = "/dev/ttyUSB0",
#         slave: int = 240,
#         baudrate: int = 19200,
#         timeout: float = 1.0,
#     ) -> None:
#         self.client = ModbusSerialClient(
#             port=port,
#             baudrate=baudrate,
#             parity="N",
#             stopbits=2,
#             bytesize=8,
#             timeout=timeout,
#         )
#         self.slave = slave

#     def connect(self) -> bool:
#         """Open the serial connection."""
#         return bool(self.client.connect())

#     def close(self) -> None:
#         """Close the serial connection."""
#         self.client.close()

#     @staticmethod
#     def _decode_float32(registers: list[int]) -> float:
#         """
#         Decode a 32-bit IEEE float from 2 Modbus registers.

#         HMP110 uses 2 registers per float, least significant word first.
#         """
#         decoder = BinaryPayloadDecoder.fromRegisters(
#             registers,
#             byteorder=Endian.BIG,    # bytes inside each 16-bit register
#             wordorder=Endian.LITTLE, # first register is low word
#         )
#         return float(decoder.decode_32bit_float())

#     def read_once(self) -> HMP110Reading:
#         """
#         Read RH, T, and dew point once from the probe.

#         Returns
#         -------
#         HMP110Reading
#         """
#         # Read RH + T in one go (4 registers: 0..3)
#         result = self.client.read_holding_registers(
#             address=0,
#             count=4,
#             slave=self.slave,
#         )
#         if result.isError():
#             raise IOError(f"Modbus error reading RH/T: {result}")

#         regs = result.registers
#         if len(regs) != 4:
#             raise IOError(f"Unexpected number of registers for RH/T: {len(regs)}")

#         rh = self._decode_float32(regs[0:2])
#         temperature = self._decode_float32(regs[2:4])

#         # Dew/frost point (optional parameter; may not be enabled in your order code)
#         dew_point: Optional[float] = None
#         dp_result = self.client.read_holding_registers(
#             address=8,  # dew/frost point: register 9 (0x0008–0x0009)
#             count=2,
#             slave=self.slave,
#         )
#         if not dp_result.isError() and len(dp_result.registers) == 2:
#             try:
#                 dew_point = self._decode_float32(dp_result.registers)
#             except Exception:
#                 dew_point = None

#         return HMP110Reading(rh=rh, temperature=temperature, dew_point=dew_point)


# def main_modbus() -> None:
#     parser = argparse.ArgumentParser(
#         description="Read Vaisala HMP110 via RS-485 / Modbus RTU"
#     )
#     parser.add_argument(
#         "--port",
#         default="/dev/ttyUSB0",
#         help="Serial port (e.g. /dev/ttyUSB0, /dev/ttyS0, COM3)",
#     )
#     parser.add_argument(
#         "--slave",
#         type=int,
#         default=240,
#         help="Modbus slave address (default: 240)",
#     )
#     parser.add_argument(
#         "--interval",
#         type=float,
#         default=5.0,
#         help="Seconds between reads (0 = read once and exit)",
#     )
#     args = parser.parse_args()

#     sensor = HMP110Modbus(port=args.port, slave=args.slave)

#     if not sensor.connect():
#         raise SystemExit(f"Could not open serial port {args.port}")

#     try:
#         if args.interval <= 0:
#             reading = sensor.read_once()
#             line = f"RH={reading.rh:.1f} %  T={reading.temperature:.2f} °C"
#             if reading.dew_point is not None:
#                 line += f"  Td={reading.dew_point:.2f} °C"
#             print(line)
#         else:
#             while True:
#                 reading = sensor.read_once()
#                 timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
#                 line = (
#                     f"{timestamp}  RH={reading.rh:.1f} %  "
#                     f"T={reading.temperature:.2f} °C"
#                 )
#                 if reading.dew_point is not None:
#                     line += f"  Td={reading.dew_point:.2f} °C"
#                 print(line)
#                 time.sleep(args.interval)
#     finally:
#         sensor.close()

def main_ascii() -> None:
    pass


if __name__ == "__main__":
    main_ascii()
