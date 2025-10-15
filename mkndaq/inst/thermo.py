# -*- coding: utf-8 -*-
"""
Thermo Electron Ozone Analyzers: 49C (serial) and 49i (TCP/IP or serial)

This module provides two instrument drivers hardened against I/O stalls:
- Serial writes use a finite ``write_timeout``.
- Reads are bounded by a deadline (no infinite waits).
- Retries with exponential backoff; serial port is closed/reopened on failure.
- Non-overlapping I/O via a non-blocking lock per instrument.
- Per-instrument cool-down disables polling after repeated failures.

Both classes expose a similar surface:
- ``setup_schedules()`` to configure periodic acquisition/saving.
- ``get_config()``, ``set_config()``, ``set_datetime()``, ``get_o3()``, ``print_o3()``.
- Internal helpers to save to disk and stage a ZIP file.

All filesystem interactions use :mod:`pathlib`.
"""
from __future__ import annotations

import logging
import socket
import time
import zipfile
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional, List

import colorama
import schedule
import serial


# =====================================================================
# TEI 49C (serial)
# =====================================================================
class Thermo49C:
    """Thermo Electron 49C ozone analyzer (serial only).

    Parameters
    ----------
    name : str
        Short instrument identifier, e.g. ``"49C"``.
    config : dict
        Configuration dictionary. Expected keys (indicative):
        ``root``, ``data``, ``staging``, ``logging``;
        under ``config[name]``: ``serial_number``, ``id``, ``get_config``, ``set_config``,
        ``data_header``, ``port``, ``data_path``, ``staging_path``, ``staging_zip``,
        ``remote_path``, ``sampling_interval``, ``reporting_interval``;
        under ``config[port]``: ``baudrate``, ``bytesize``, ``parity``, ``stopbits``, ``timeout``,
        optional ``write_timeout``.
    """

    # --- static attribute annotations for type checkers (initialized in __init__) ---
    _io_lock: threading.Lock
    _fail_count: int
    _cooldown_until: float
    _max_fail_before_cooldown: int
    _cooldown_seconds: int
    _serial: serial.Serial

    # paths & file state
    data_path: Path
    staging_path: Path
    _file_to_stage: Optional[Path]

    def __init__(self, name: str, config: dict) -> None:
        """Construct the instrument and open the serial port."""
        # concurrency guard and failure management
        self._io_lock = threading.Lock()
        self._fail_count = 0
        self._cooldown_until = 0.0
        self._max_fail_before_cooldown = 5
        self._cooldown_seconds = 300

        colorama.init(autoreset=True)
        self.name = name
        self.serial_number = config[name]['serial_number']

        # logging
        _logger = Path(config['logging']['file']).stem
        self.logger = logging.getLogger(f"{_logger}.{__name__}")
        self.logger.info(f"[{self.name}] Initializing TEI49C (S/N: {self.serial_number})")

        # instrument control
        self._id = config[name]['id'] + 128
        self._get_config: List[str] = config[name]['get_config']
        self._set_config: List[str] = config[name]['set_config']
        self._data_header: str = config[name]['data_header']

        # serial port
        port = config[name]['port']
        prt = config[port]
        self._serial = serial.Serial(
            port=port,
            baudrate=prt['baudrate'],
            bytesize=prt['bytesize'],
            parity=prt['parity'],
            stopbits=prt['stopbits'],
            timeout=prt['timeout'],
            write_timeout=prt.get('write_timeout', 2.0),
        )

        # data & paths (pathlib)
        root = Path(config['root']).expanduser()
        self.data_path = root / config['data'] / config[name]['data_path']
        self.staging_path = root / config['staging'] / config[name]['staging_path']
        self._file_to_stage = None
        self._zip = config[name]['staging_zip']
        self.remote_path = config[name]['remote_path']
        self._data = ''
        self.sampling_interval = int(config[name]['sampling_interval'])
        self.reporting_interval = int(config[name]['reporting_interval'])

    # ---------- schedules ----------
    def setup_schedules(self) -> None:
        """Create directories and register periodic collection & save jobs."""
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.staging_path.mkdir(parents=True, exist_ok=True)

        # collect raw records (lrec) every X minutes, aligned to :00
        schedule.every(self.sampling_interval).minutes.at(':00').do(self.accumulate_lrec)

        # save/stage according to reporting_interval
        if self.reporting_interval == 10:
            self._file_timestamp_format = '%Y%m%d%H%M'
            minutes = [f"{self.reporting_interval*n:02}" for n in range(6) if self.reporting_interval*n < 60]
            for minute in minutes:
                schedule.every().hour.at(f"{minute}:01").do(self._save_and_stage_data)
        elif self.reporting_interval == 60:
            self._file_timestamp_format = '%Y%m%d%H'
            schedule.every().hour.at('00:01').do(self._save_and_stage_data)
        elif self.reporting_interval == 1440:
            self._file_timestamp_format = '%Y%m%d'
            schedule.every().day.at('00:01').do(self._save_and_stage_data)
        else:
            raise ValueError(f"[{self.name}] reporting_interval must be 10, 60 or 1440 minutes.")

    # ---------- low-level comm ----------
    def serial_comm(self, cmd: str) -> str:
        """Exchange a command/response over serial with retries and bounded read.

        Returns
        -------
        str
            Tidy response (command echo and trailing marker removed) or ``\"\"`` on failure.
        """
        _id = bytes([self._id])
        for i in range(3):
            try:
                if not self._serial.is_open:
                    self._serial.open()
                self._serial.reset_input_buffer()
                self._serial.reset_output_buffer()

                self._serial.write(_id + (f"{cmd}\\x0D").encode())
                self._serial.flush()

                rcvd = b""
                deadline = time.monotonic() + max(self._serial.timeout or 1.0, 1.0)
                while time.monotonic() < deadline:
                    if self._serial.in_waiting:
                        rcvd += self._serial.read(1024)
                        time.sleep(0.05)
                    else:
                        time.sleep(0.05)

                text = rcvd.decode(errors="ignore").split("*")[0].replace(cmd, "").strip()
                if not text:
                    raise serial.SerialTimeoutException("empty response")
                return text

            except (serial.SerialTimeoutException, serial.SerialException) as err:
                self.logger.error(f"serial_comm attempt {i+1}/3 failed: {err}")
                try:
                    self._serial.close()
                except Exception:
                    pass
                time.sleep(min(0.5 * (2 ** i), 3.0))

            except Exception as err:
                self.logger.error(f"serial_comm unexpected: {err}")
                try:
                    self._serial.close()
                except Exception:
                    pass
                break

        return ""

    # ---------- high-level ops ----------
    def get_config(self) -> List[str]:
        """Query and log current device configuration. Returns list of responses."""
        if not self._get_config:
            return []
        config_list: List[str] = []
        for cmd in self._get_config:
            rsp = self.serial_comm(cmd)
            if rsp:
                config_list.append(rsp)
        if config_list:
            self.logger.info(f"[{self.name}] current configuration: {'; '.join(config_list)}")
        return config_list

    def set_datetime(self) -> None:
        """Synchronize device clock to system time."""
        try:
            dte = self.serial_comm(f"set date {time.strftime('%m-%d-%y')}")
            tme = self.serial_comm(f"set time {time.strftime('%H:%M')}")
            self.logger.info(f"[{self.name}] Date and time set and reported as: {dte} {tme}")
        except Exception as err:
            self.logger.error(err)

    def set_config(self) -> List[str]:
        """Apply configuration commands defined in config; return successful responses."""
        self.logger.info(f"[{self.name}] .set_config")
        if not self._set_config:
            return []
        result: List[str] = []
        for cmd in self._set_config:
            rsp = self.serial_comm(cmd)
            if rsp:
                result.append(rsp)
        self.logger.info(f"[{self.name}] New configuration: {'; '.join(result)}")
        return result

    def get_o3(self) -> str:
        """Return a one-shot O3 readout or empty string on failure."""
        try:
            return self.serial_comm("o3")
        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"[{self.name}] get_o3: {err}")
            return ""

    def print_o3(self) -> None:
        """Log a one-shot O3 readout (guarded by non-blocking lock)."""
        try:
            if not self._io_lock.acquire(blocking=False):
                return
            try:
                o3 = self.get_o3()
                if o3:
                    self.logger.info(colorama.Fore.CYAN + f"[{self.name}] O3: {o3}")
                else:
                    self.logger.error(colorama.Fore.RED + f"[{self.name}] O3 read failed")
            finally:
                self._io_lock.release()
        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"[{self.name}] print_o3: {err}")

    def accumulate_lrec(self) -> None:
        """Collect a raw record (``lrec``) and append to the in-memory buffer.

        Employs a per-instrument cool-down after repeated failures.
        """
        try:
            now = time.time()
            if now < self._cooldown_until:
                return
            if not self._io_lock.acquire(blocking=False):
                return
            try:
                dtm = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                resp = self.serial_comm('lrec')
                if resp:
                    self._fail_count = 0
                    self._data += f"{dtm} {resp}\n"
                    self.logger.debug(f"[{self.name}] {resp[:60]}[...]")
                else:
                    self._fail_count += 1
                    if self._fail_count >= self._max_fail_before_cooldown:
                        self._cooldown_until = now + self._cooldown_seconds
                        self.logger.error(
                            f"[{self.name}] communication failing repeatedly; "
                            f"pausing for {self._cooldown_seconds}s."
                        )
            finally:
                self._io_lock.release()
        except Exception as err:
            self.logger.error(err)

    # ---------- saving / staging ----------
    def _save_data(self) -> None:
        """Write accumulated data to a .dat file and clear the buffer."""
        try:
            if self._data and self._zip:
                self._file_to_stage = self.staging_path / f"{self.name}-{self.serial_number}-{datetime.now().strftime(self._file_timestamp_format)}.dat"
                with self._file_to_stage.open("w", encoding="utf-8") as fh:
                    fh.write(self._data)
                self.logger.info(colorama.Fore.GREEN + f"{self._file_to_stage.name} written.")
                self._data = ""
        except Exception as err:
            self.logger.error(err)

    def _stage_file(self) -> None:
        """Zip the latest .dat file into the staging directory (if any)."""
        try:
            if self._file_to_stage:
                archive = self._file_to_stage.with_suffix('.zip')
                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.write(self._file_to_stage, self._file_to_stage.name)
                self.logger.info(f"file staged: {archive}")
        except Exception as err:
            self.logger.error(err)

    def _save_and_stage_data(self) -> None:
        """Save accumulated buffer and stage as a .zip file."""
        self._save_data()
        self._stage_file()

# =====================================================================
# TEI 49i (TCP/IP by default; serial optional)
# =====================================================================
class Thermo49i:
    """Thermo Electron 49i ozone analyzer (TCP/IP by default; optional serial).

    Parameters
    ----------
    name : str
        Short instrument identifier, e.g. ``"49i"``.
    config : dict
        Configuration dictionary. In addition to the 49C fields, you can set:
        - ``config[name]['serial_com'] = True`` to force serial mode.
        - For TCP/IP: ``config[name]['host']``, ``config[name]['port']``.
    """

    # --- static attribute annotations for type checkers (initialized in __init__) ---
    _io_lock: threading.Lock
    _fail_count: int
    _cooldown_until: float
    _max_fail_before_cooldown: int
    _cooldown_seconds: int
    _serial: Optional[serial.Serial]
    _sock: Optional[socket.socket]

    # paths & file state
    data_path: Path
    staging_path: Path
    _file_to_stage: Optional[Path]

    def __init__(self, name: str, config: dict) -> None:
        """Construct the instrument and open the requested transport (TCP or serial)."""
        # concurrency guard and failure management
        self._io_lock = threading.Lock()
        self._fail_count = 0
        self._cooldown_until = 0.0
        self._max_fail_before_cooldown = 5
        self._cooldown_seconds = 300

        colorama.init(autoreset=True)
        self.name = name
        self.serial_number = config[name]['serial_number']

        # logging
        _logger = Path(config['logging']['file']).stem
        self.logger = logging.getLogger(f"{_logger}.{__name__}")
        self.logger.info(f"[{self.name}] Initializing TEI49i (S/N: {self.serial_number})")

        # instrument control
        self._id = config[name]['id'] + 128
        self._get_config: List[str] = config[name]['get_config']
        self._set_config: List[str] = config[name]['set_config']
        self._data_header: str = config[name]['data_header']

        # IO selection
        self._serial_com = bool(config[name].get('serial_com', False))
        self._serial = None
        self._sock = None

        if self._serial_com:
            port = config[name]['port']
            prt = config[port]
            self._serial = serial.Serial(
                port=port,
                baudrate=prt['baudrate'],
                bytesize=prt['bytesize'],
                parity=prt['parity'],
                stopbits=prt['stopbits'],
                timeout=prt['timeout'],
                write_timeout=prt.get('write_timeout', 2.0),
            )
        else:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.settimeout(2.0)
            self._sock.connect((config[name]['host'], int(config[name]['port'])))

        # data & paths (pathlib)
        root = Path(config['root']).expanduser()
        self.data_path = root / config['data'] / config[name]['data_path']
        self.staging_path = root / config['staging'] / config[name]['staging_path']
        self._file_to_stage = None
        self._zip = config[name]['staging_zip']
        self.remote_path = config[name]['remote_path']
        self._data = ''
        self.sampling_interval = int(config[name]['sampling_interval'])
        self.reporting_interval = int(config[name]['reporting_interval'])

    # ---------- schedules ----------
    def setup_schedules(self) -> None:
        """Create directories and register periodic collection & save jobs."""
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.staging_path.mkdir(parents=True, exist_ok=True)

        schedule.every(int(self.sampling_interval)).minutes.at(':00').do(self.accumulate_lr00)

        if self.reporting_interval == 10:
            self._file_timestamp_format = '%Y%m%d%H%M'
            minutes = [f"{self.reporting_interval*n:02}" for n in range(6) if self.reporting_interval*n < 60]
            for minute in minutes:
                schedule.every().hour.at(f"{minute}:01").do(self._save_and_stage_data)
        elif self.reporting_interval == 60:
            self._file_timestamp_format = '%Y%m%d%H'
            schedule.every().hour.at('00:01').do(self._save_and_stage_data)
        elif self.reporting_interval == 1440:
            self._file_timestamp_format = '%Y%m%d'
            schedule.every().day.at('00:01').do(self._save_and_stage_data)
        else:
            raise ValueError(f"[{self.name}] reporting_interval must be 10, 60 or 1440 minutes.")

    # ---------- low-level comm ----------
    def tcpip_comm(self, cmd: str) -> str:
        """Send command via TCP/IP and read response (bounded by a short timeout)."""
        try:
            sock = self._sock
            if not sock:
                return ""
            sock.sendall((cmd + "\r").encode())
            sock.settimeout(2.0)
            chunks: List[bytes] = []
            deadline = time.monotonic() + 1.5
            while time.monotonic() < deadline:
                try:
                    data = sock.recv(4096)
                except socket.timeout:
                    break
                if not data:
                    break
                chunks.append(data)
                if b"*" in data:
                    break
            if not chunks:
                return ""
            text = b"".join(chunks).decode(errors="ignore")
            text = text.split("*")[0].replace(cmd, "").strip()
            return text
        except Exception as err:
            self.logger.error(f"tcpip_comm: {err}")
            return ""

    def serial_comm(self, cmd: str, tidy: bool = True) -> str:
        """Exchange a command/response over serial with retries and bounded read.

        If this instance is configured for TCP/IP (``_serial is None``), this function
        returns an empty string to satisfy type checkers and avoid attribute access on ``None``.
        """
        ser = self._serial
        if ser is None:
            # Serial not selected; caller should be using tcpip_comm()
            return ""

        _id = bytes([self._id])
        for i in range(3):
            try:
                if not ser.is_open:
                    ser.open()
                ser.reset_input_buffer()
                ser.reset_output_buffer()

                ser.write(_id + (f"{cmd}\\x0D").encode())
                ser.flush()

                rcvd = b""
                timeout = ser.timeout if ser.timeout is not None else 1.0
                deadline = time.monotonic() + max(timeout, 1.0)
                while time.monotonic() < deadline:
                    if ser.in_waiting:
                        rcvd += ser.read(1024)
                        time.sleep(0.05)
                    else:
                        time.sleep(0.05)

                text = rcvd.decode(errors="ignore")
                if tidy:
                    text = text.split("*")[0].replace(cmd, "").strip()
                if not text:
                    raise serial.SerialTimeoutException("empty response")
                return text

            except (serial.SerialTimeoutException, serial.SerialException) as err:
                self.logger.error(f"serial_comm attempt {i+1}/3 failed: {err}")
                try:
                    ser.close()
                except Exception:
                    pass
                time.sleep(min(0.5 * (2 ** i), 3.0))

            except Exception as err:
                self.logger.error(f"serial_comm unexpected: {err}")
                try:
                    ser.close()
                except Exception:
                    pass
                break

        return ""

    def send_command(self, cmd: str) -> str:
        """Send a command using the active transport (serial or TCP/IP)."""
        if self._serial_com:
            # some devices respond better after close/open cycles
            ser = self._serial
            if ser is None:
                return ""
            try:
                if ser.is_open:
                    ser.close()
            except Exception:
                pass
            return self.serial_comm(cmd)
        else:
            return self.tcpip_comm(cmd)

    # ---------- high-level ops ----------
    def get_config(self) -> List[str]:
        """Query and log current device configuration. Returns list of responses."""
        if not self._get_config:
            return []
        result: List[str] = []
        for cmd in self._get_config:
            rsp = self.send_command(cmd)
            if rsp:
                result.append(rsp)
        self.logger.info(f"[{self.name}] current configuration: {'; '.join(result)}")
        return result

    def set_datetime(self) -> None:
        """Synchronize device clock to system time using active transport."""
        try:
            dte = self.send_command(f"set date {time.strftime('%m-%d-%y')}")
            tme = self.send_command(f"set time {time.strftime('%H:%M')}")
            self.logger.info(f"[{self.name}] Date and time set and reported as: {dte} {tme}")
        except Exception as err:
            self.logger.error(err)

    def set_config(self) -> List[str]:
        """Apply configuration commands defined in config; return successful responses."""
        self.logger.info(f"[{self.name}] .set_config")
        if not self._set_config:
            return []
        result: List[str] = []
        for cmd in self._set_config:
            rsp = self.send_command(cmd)
            if rsp:
                result.append(rsp)
        self.logger.info(f"[{self.name}] New configuration: {'; '.join(result)}")
        return result

    def get_o3(self) -> str:
        """Return a one-shot O3 readout or empty string on failure using active transport."""
        try:
            return self.send_command('o3')
        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"[{self.name}] get_o3: {err}")
            return ""

    def print_o3(self) -> None:
        """Log a one-shot O3 readout (guarded by non-blocking lock)."""
        try:
            if not self._io_lock.acquire(blocking=False):
                return
            try:
                o3 = self.get_o3()
                if o3:
                    self.logger.info(colorama.Fore.CYAN + f"[{self.name}] O3: {o3}")
                else:
                    self.logger.error(colorama.Fore.RED + f"[{self.name}] O3 read failed")
            finally:
                self._io_lock.release()
        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"[{self.name}] print_o3: {err}")

    def accumulate_lr00(self) -> None:
        """Collect a raw record (``lr00``) and append to the in-memory buffer.

        Employs a per-instrument cool-down after repeated failures.
        Uses serial or TCP/IP depending on configuration.
        """
        try:
            now = time.time()
            if now < self._cooldown_until:
                return
            if not self._io_lock.acquire(blocking=False):
                return
            try:
                dtm = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                if self._serial_com:
                    resp = self.serial_comm('lr00')
                else:
                    resp = self.tcpip_comm('lr00')
                if resp:
                    self._fail_count = 0
                    self._data += f"{dtm} {resp}\n"
                    self.logger.debug(f"[{self.name}] {resp[:60]}[...]")
                else:
                    self._fail_count += 1
                    if self._fail_count >= self._max_fail_before_cooldown:
                        self._cooldown_until = now + self._cooldown_seconds
                        self.logger.error(
                            f"[{self.name}] communication failing repeatedly; "
                            f"pausing for {self._cooldown_seconds}s."
                        )
            finally:
                self._io_lock.release()
        except Exception as err:
            self.logger.error(err)

    # ---------- saving / staging ----------
    def _save_data(self) -> None:
        """Write accumulated data to a .dat file and clear the buffer."""
        try:
            if self._data and self._zip:
                self._file_to_stage = self.staging_path / f"{self.name}-{self.serial_number}-{datetime.now().strftime(self._file_timestamp_format)}.dat"
                with self._file_to_stage.open("w", encoding="utf-8") as fh:
                    fh.write(self._data)
                self.logger.info(colorama.Fore.GREEN + f"{self._file_to_stage.name} written.")
                self._data = ""
        except Exception as err:
            self.logger.error(err)

    def _stage_file(self) -> None:
        """Zip the latest .dat file into the staging directory (if any)."""
        try:
            if self._file_to_stage:
                archive = self._file_to_stage.with_suffix('.zip')
                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.write(self._file_to_stage, self._file_to_stage.name)
                self.logger.info(f"file staged: {archive}")
        except Exception as err:
            self.logger.error(err)

    def _save_and_stage_data(self) -> None:
        """Save accumulated buffer and stage as a .zip file."""
        self._save_data()
        self._stage_file()


if __name__ == "__main__":
    # This module is designed to be imported and used by a scheduler.
    pass
