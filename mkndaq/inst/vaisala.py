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

import colorama
import schedule
import serial

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
    """Decorator that serializes access to a shared serial port.

    - Uses a per-port lock shared across HMP110ASCII instances.
    - Uses a small lock timeout to avoid piling up blocked threads.
    - Optionally retries a few times on transient serial errors.

    NOTE: This intentionally does *not* implement any cooldown/backoff state.
    """

    @functools.wraps(func)
    def wrapper(self, cmd: str, *args, retries: int = 2, lock_timeout: float = 2.0, **kwargs) -> str:
        acquired = self._io_lock.acquire(timeout=lock_timeout)
        if not acquired:
            # Keep this at DEBUG to avoid log spam during heavy scheduling.
            self.logger.debug(f"[{self.name}] serial busy; skipping this cycle for {cmd!r}")
            return ""

        try:
            last_err: Exception | None = None
            for i in range(retries):
                try:
                    if not self._serial.is_open:
                        self._serial.open()

                    resp = func(self, cmd, *args, **kwargs)
                    return resp or ""

                except (serial.SerialTimeoutException, serial.SerialException, OSError) as err:
                    last_err = err
                    level = logging.WARNING if i < retries - 1 else logging.ERROR
                    self.logger.log(level, f"[{self.name}] serial_comm failed for {cmd!r}: {err}")
                    try:
                        if self._serial.is_open:
                            self._serial.close()
                    except Exception:
                        pass
                    time.sleep(0.2)

            if last_err:
                self.logger.error(f"[{self.name}] serial_comm giving up for {cmd!r}: {last_err}")
            return ""
        finally:
            self._io_lock.release()

    return wrapper


class HMP110ASCII:
    # One shared Serial + lock per OS port (e.g. "COM1")
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
                    self.logger.debug("[%s] Reusing shared serial port %s", self.name, port)
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
                        self.name, port, baudrate, parity, stopbits,
                    )

            except serial.SerialException as err:
                self.logger.error("[%s] __init__ produced SerialException %s", self.name, err)
                raise

            # sampling, aggregation, reporting/storage
            self.sampling_interval = config[name]['sampling_interval']
            self.reporting_interval = config[name]['reporting_interval']
            if not (self.reporting_interval == 10 or (self.reporting_interval % 60) == 0) and self.reporting_interval <= 1440:
                raise ValueError(
                    f"[{self.name}] reporting_interval must be 10 or a multiple of 60 and <= 1440 minutes."
                )

            # configure saving, staging and archiving
            root = os.path.expanduser(config['root'])
            self.data_path = os.path.join(root, config['data'], config[name]['data_path'])
            self.staging_path = os.path.join(root, config['staging'], config[name]['staging_path'])
            self._file_to_stage = ""
            self._zip = config[name]['staging_zip']

            # command to retrieve data
            self.cmd = f"SEND {self._id}\r\n"
            self.logger.info(f"[{self.name}] command: {self.cmd!r}")

            # configure remote transfer
            self.remote_path = config[name]['remote_path']

            # initialize data response
            self._data = ""
            self.data_file = None

        except Exception as err:
            self.logger.exception(f"[{self.name}] __init__ failed: {err}")
            raise


    @staticmethod
    def _parse_reading(raw: str) -> str:
        """
        Parse a line like:
            "T=  19.30 'C RH=  27.26 %RH Td=  -0.02 'C \\r\\n"
        into:
            "19.30,27.26,-0.02"
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


    def setup_schedules(self, delay_job: int = 1) -> None:
        """Register acquisition + save/stage schedules.

        Two probes on one RS-485 bus:
        - share the underlying serial port handle (per OS port)
        - serialize I/O via a per-port lock
        - stagger schedules by passing delay_job=1/2/... (seconds within the minute)
        """
        try:
            os.makedirs(self.data_path, exist_ok=True)
            os.makedirs(self.staging_path, exist_ok=True)

            sec = max(0, min(int(delay_job), 59))

            # Acquisition
            try:
                schedule.every(self.sampling_interval).minutes.at(f":{sec:02d}").do(self.accumulate_readings)
            except Exception:
                # Fallback for schedule versions that don't support `.at()` on minute jobs
                schedule.every(self.sampling_interval).minutes.do(self.accumulate_readings)

            # Timestamp format for output file naming
            if self.reporting_interval == 10:
                self._file_timestamp_format = "%Y%m%d%H%M"
            elif self.reporting_interval < 1440:
                self._file_timestamp_format = "%Y%m%d%H"
            elif self.reporting_interval == 1440:
                self._file_timestamp_format = "%Y%m%d"
            else:
                raise ValueError(f"[{self.name}] reporting_interval must be 10, <1440 (minutes), or 1440.")

            # Save + stage (treat as minutes to avoid hourly `.at("MM:SS")` issues)
            try:
                schedule.every(self.reporting_interval).minutes.at(f":{sec:02d}").do(self._save_and_stage_data)
            except Exception:
                schedule.every(self.reporting_interval).minutes.do(self._save_and_stage_data)

        except Exception as err:
            self.logger.exception(f"[{self.name}] setup_schedules failed: {err}")
            raise


    def accumulate_readings(self) -> None:
        """Request one reading, parse it, and append it to the in-memory buffer."""
        try:
            dtm = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            response = self.serial_comm(self.cmd)  # serialized by @with_serial
            if not response:
                return
            csv_response = self._parse_reading(response)
            self._data += f"{dtm},{csv_response}\n"
        except Exception as err:
            self.logger.error(f"[{self.name}] accumulate_readings: {err}")


    def print_readings(self) -> None:
        """Log a one-shot HMP110 readout (for console monitoring)."""
        try:
            response = self.serial_comm(self.cmd)  # serialized by @with_serial
            if not response:
                return
            csv_response = self._parse_reading(response)
            self.logger.info(f"[{self.name}] T,RH,Td: {csv_response}")
        except Exception as err:
            self.logger.error(f"[{self.name}] print_readings: {err}")


    def _save_data(self) -> None:
        try:
            if self._data:
                now = datetime.now()
                timestamp = now.strftime(self._file_timestamp_format)
                yyyy = now.strftime('%Y')
                mm = now.strftime('%m')
                dd = now.strftime('%d')
                self.data_file = os.path.join(self.data_path, yyyy, mm, dd, f"{self.name}-{timestamp}.dat")
                os.makedirs(os.path.dirname(self.data_file), exist_ok=True)

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

                self._data = ""

            return

        except Exception as err:
            self.logger.error(f"[{self.name}] {err}")


    def _stage_file(self):
        """Create zip file from self.data_file and stage archive."""
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