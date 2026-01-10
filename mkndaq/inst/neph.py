"""
Define a class NE300 facilitating communication with a Acoem NE-300 nephelometer.

@author: joerg.klausen@meteoswiss.ch
"""

import logging
import os
import socket
import struct
import threading
import time
import warnings
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Any

import colorama
import schedule


class AcoemProtocolError(RuntimeError):
    """Raised when the instrument responds with an ACOEM protocol error packet."""

    def __init__(self, code: int, message: str, *, cmd: int | None = None) -> None:
        super().__init__(f"ACOEM error {code}: {message}" + (f" (cmd={cmd})" if cmd is not None else ""))
        self.code = code
        self.cmd = cmd
        self.message = message


def _default_run_threaded(job, *args, **kwargs):
    thread = threading.Thread(target=job, args=args, kwargs=kwargs, daemon=True)
    thread.start()
    return thread

class NEPH:
    """
    Instrument of type Acoem NE-300 or Ecotech Aurora 3000 nephelometer with methods, attributes for interaction.
    """
    def __init__(self, name: str, config: dict, verbosity: int=0, initialize: bool=True) -> None:
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
            - config[name]['get_data_interval']
            - config[name]['reporting_interval']
            - config[name]['zero_check_duration']
            - config[name]['span_check_duration']
            - config['staging']['path'])
            - config[name]['staging_zip']
            - config[name]['protocol']
            - config[name]['data_log']['parameters']
            - config[name]['data_log']['wavelengths']
            - config[name]['data_log']['angles']
            - config[name]['data_log']['interval']
        """
        colorama.init(autoreset=True)

        try:
            self.name = name
            self.type = config[name]['type']
            self.serial_number = config[name]['serial_number']

            # configure logging
            _logger = f"{os.path.basename(config['logging']['file'])}".split('.')[0]
            self.logger = logging.getLogger(f"{_logger}.{__name__}")
            self.logger.info(f"[{self.name}] Initializing {self.type} (S/N: {self.serial_number})", extra={"to_logfile": True})

            self.verbosity = config[name]['verbosity']

            # read instrument control properties for later use
            self.serial_id = config[name]['serial_id']

            # configure tcp/ip
            self.sockaddr = (config[name]['socket']['host'],
                             config[name]['socket']['port'])
            self.socktout = config[name]['socket']['timeout']
            # self.__socksleep = config[name]['socket']['sleep']
            
            # flag to maintain state to prevent concurrent tcpip communication in a threaded setup
            self._tcpip_line_is_busy = False

            # configure comms protocol
            if config[name]['protocol'] in ["acoem", "aurora"]:
                self._protocol = config[name]['protocol']
            else:
               raise ValueError("Communication protocol not recognized.")

            # data log
            # self.data_log_parameters = config[name]['data_log']['parameters']
            # self.data_log_wavelengths = config[name]['data_log']['wavelengths']
            # self.data_log_angles = config[name]['data_log']['angles']
            # self.data_log_interval = config[name]['data_log']['interval']

            # sampling, aggregation, reporting/storage
            self.sampling_interval = config[name]['sampling_interval']
            self.reporting_interval = config[name]['reporting_interval']

            # zero and span check interval and durations
            self.zero_span_check_interval = config[name]['zero_span_check_interval']
            self.zero_check_duration = config[name]['zero_check_duration']
            self.span_check_duration = config[name]['span_check_duration']
            if self.zero_span_check_interval % 60 != 0:
                raise ValueError(
                    f"zero_span_check_interval={self.zero_span_check_interval} must be a multiple of 60 minutes."
                )
            total_duration = self.zero_check_duration + self.span_check_duration
            if total_duration >= 60:
                raise ValueError(
                    f"zero_check_duration + span_check_duration = {total_duration} must be < 60 minutes."
                )
            self._zero_span_check_hours = self.zero_span_check_interval // 60
            self._span_offset_min = self.zero_check_duration                                # mm offset from :00
            self._ambient_offset_min = self.zero_check_duration + self.span_check_duration  # mm offset from :00
            if self.zero_span_check_interval > 0:
                self.logger.info(
                    "zero/span checks every %d hours, zero @ :00, span @ :%02d, return to ambient @ :%02d",
                    self._zero_span_check_hours, self._span_offset_min, self._ambient_offset_min,
                )
            else:
                self.logger.info("zero/span checks disabled.")

            # configure saving, staging and remote path
            root = os.path.expanduser(config['root'])
            self.data_path = os.path.join(root, config['data'], config[name]['data_path'])
            os.makedirs(self.data_path, exist_ok=True)
            self.staging_path = os.path.join(root, config['staging'], config[name]['staging_path'])
            os.makedirs(self.staging_path, exist_ok=True)
            self.staging_zip = config[name]['staging_zip']

            self.remote_path = config[name]['remote_path']

            # retrieve instrument id and other instrument info
            id = self.get_id(verbosity=verbosity)
            if id=={}:
                self.logger.error(colorama.Fore.RED + f"[{self.name}] Could not communicate with instrument. Protocol set to '{self._protocol}'. Please verify instrument settings." + colorama.Fore.GREEN)
            else:
                self.logger.info(f"[{self.name}] id reported: '{id}'.", extra={"to_logfile": True})

                # get version
                version = self.get_version()
                self.logger.info(f"[{self.name}] firmware version reported: {version}.", extra={"to_logfile": True})            

                # put instrument in ambient mode
                state = self.do_ambient(verbosity=verbosity)
                if state==0:
                    self.logger.info(f"[{self.name}] current operation reported: 'ambient'.", extra={"to_logfile": True})
                else:
                    self.logger.warning(colorama.Fore.YELLOW + f"[{self.name}] Could not verify measurement mode as 'ambient'." + colorama.Fore.GREEN)

                # get dtm from instrument, then set date and time
                # and configure logging interval (optional)
                if initialize:
                    dtm_found, dtm_set = self.get_set_datetime(dtm=datetime.now(timezone.utc))
                    self.logger.info(
                        f"[{self.name}] dtm found: {dtm_found} > dtm set: {dtm_set}.",
                        extra={"to_logfile": True},
                    )

                    datalog_interval = self.set_datalog_interval()
                    self.logger.info(
                        f"[{self.name}] Datalog interval set to {datalog_interval} seconds.",
                    )
                else:
                    self.logger.info(f"[{self.name}] initialize=False: leaving instrument time / logging interval unchanged.")

                # get logging config (=header ids)
                cfg = self.get_data_log_config()[1:]  # drop the leading "number of fields"

                # ensure 4035 and 2002 are present as the first two data columns after dtm
                self._header = [4035, 2002] + [pid for pid in cfg if pid not in (4035, 2002)]

                self.logger.info(f"[{self.name}] logging config reported: {self._header}.", extra={"to_logfile": True})

            # datetime to keep track of retrievals from datalog
            self._start_datalog = datetime.now(timezone.utc).replace(second=0, microsecond=0)

            # initialize data response
            self._data = str()

            # initialize other stuff
            self._file_timestamp_format = str()
            self.data_file = str()

        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)

    def setup_schedules(self, run_threaded=_default_run_threaded):
        try:
            # configure data acquisition schedule
            schedule.every(int(self.sampling_interval)).minutes.at(':02').do(run_threaded, self._accumulate_new_data)

            # configure zero and span check schedules
            if self.zero_span_check_interval > 0:
                self._setup_zero_span_check_schedules()

            # configure saving and staging schedules
            if self.reporting_interval==10:
                self._file_timestamp_format = '%Y%m%d%H%M'
                for minute in range(6):
                    schedule.every(1).hour.at(f"{minute}0:05").do(run_threaded, self._save_and_stage_data)
            elif self.reporting_interval==60:
                self._file_timestamp_format = '%Y%m%d%H'
                # schedule.every().hour.at('00:01').do(self._save_and_stage_data)
                schedule.every(1).hour.at('00:05').do(run_threaded, self._save_and_stage_data)
            elif self.reporting_interval==1440:
                self._file_timestamp_format = '%Y%m%d'
                # schedule.every().day.at('00:00:01').do(self._save_and_stage_data)
                schedule.every(1).day.at('00:00:05').do(run_threaded, self._save_and_stage_data)
            else:
                raise ValueError(f"A reporting interval of {self.reporting_interval} is not supported.")
            
        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)

    def _setup_zero_span_check_schedules(self, run_threaded=_default_run_threaded) -> None:
        """
        Zero/span/ambient checks aligned to the top of the hour:

        - Zero:     every N hours at :00
        - Span:     every N hours at :<zero_duration>
        - Ambient:  every N hours at :<zero_duration + span_duration>

        Assumes __init__ validated that:
        - zero_span_check_interval is a multiple of 60
        - zero_check_duration + span_check_duration < 60
        """
        try:
            # Zero at :00 every N hours
            schedule.every(self._zero_span_check_hours).hours.at(":00").do(run_threaded, self.do_zero)
            # Span offset within the hour
            schedule.every(self._zero_span_check_hours).hours.at(f":{self._span_offset_min:02d}").do(run_threaded, self.do_span)
            # Ambient offset within the hour
            schedule.every(self._zero_span_check_hours).hours.at(f":{self._ambient_offset_min:02d}").do(run_threaded, self.do_ambient)

            self.logger.info(
                "Scheduled zero/span/ambient: every %d hour(s) at :00/:%02d/:%02d",
                self._zero_span_check_hours, self._span_offset_min, self._ambient_offset_min
            )
        except Exception as err:  # pragma: no cover
            self.logger.error("setup_zero_span_check_schedules failed: %s", err)

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
            self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)
            return b''

    def _acoem_timestamp_to_datetime(self, timestamp: int) -> datetime:
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

            return datetime(yyyy, mm, dd, HH, MM, SS)

        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)
            return datetime(1111, 1, 1)

    def _acoem_datetime_to_timestamp(self, dtm: datetime=datetime.now(timezone.utc)) -> bytes:
        try:
            SS = bin(dtm.time().second)[2:].zfill(6)
            MM = bin(dtm.time().minute)[2:].zfill(6)
            HH = bin(dtm.time().hour)[2:].zfill(5)
            dd = bin(dtm.date().day)[2:].zfill(5)
            mm = bin(dtm.date().month)[2:].zfill(4)
            yyyy = bin(dtm.date().year - 2000).zfill(6)

            return (int(yyyy + mm + dd + HH + MM + SS, base=2)).to_bytes(4, byteorder='big')

        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)
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
            msg_data = (parameter_id).to_bytes(4, byteorder='big')
        if len(payload)>0:
            msg_data += payload
        msg_len = len(msg_data)
        msg = bytes([2, self.serial_id, command, 3]) + (msg_len).to_bytes(2, byteorder='big') + msg_data
        return msg + self._acoem_checksum(msg) + bytes([4])
    



    def _acoem_parse_packet(
        self,
        raw: bytes,
        *,
        expect_cmd: int | None = None,
        verbosity: int = 0,
    ) -> tuple[int, bytes]:
        """Parse and validate a single ACOEM packet.

        Returns:
            (cmd, payload)

        Notes:
            - Strips any leading bytes before STX (0x02) and uses the first EOT (0x04) found.
            - Validates checksum (XOR of all bytes up to and including message data).
            - Raises AcoemProtocolError on CMD==0 packets.
        """
        if not raw:
            raise ValueError("Empty response.")

        # Some devices may send Telnet negotiation bytes (0xFF...) or other noise; locate STX.
        stx = raw.find(b"\x02")
        if stx < 0:
            raise ValueError(f"Missing STX in response: {raw!r}")

        eot = raw.find(b"\x04", stx + 1)
        if eot < 0:
            raise ValueError("Missing EOT in response.")

        pkt = raw[stx : eot + 1]
        if len(pkt) < 8:
            raise ValueError(f"Response too short: {pkt!r}")

        if pkt[3] != 3:
            raise ValueError(f"Invalid ETX byte (expected 0x03) in packet: {pkt!r}")

        msg_len = int.from_bytes(pkt[4:6], byteorder="big")
        payload_start = 6
        payload_end = payload_start + msg_len
        checksum_pos = payload_end
        eot_pos = checksum_pos + 1

        if len(pkt) < eot_pos + 1:
            raise ValueError(
                f"Truncated packet: expected at least {eot_pos+1} bytes, got {len(pkt)}."
            )
        if pkt[eot_pos] != 4:
            # If our first-EOT cut was too early (noise), fall back to strict indexing.
            # Try to re-slice using declared message length.
            if stx + eot_pos < len(raw) and raw[stx + eot_pos] == 4:
                pkt = raw[stx : stx + eot_pos + 1]
            else:
                raise ValueError(f"EOT not at expected position in packet (msg_len={msg_len}).")

        payload = pkt[payload_start:payload_end]
        checksum = pkt[checksum_pos : checksum_pos + 1]

        calc = self._acoem_checksum(pkt[:payload_end])
        if checksum != calc:
            raise ValueError(
                f"Checksum mismatch: got {checksum.hex()}, expected {calc.hex()}."
            )

        cmd = pkt[2]

        if expect_cmd is not None and cmd not in (expect_cmd, 0):
            raise ValueError(f"Unexpected CMD in response: got {cmd}, expected {expect_cmd}.")

        if cmd == 0:
            # A.3.1 Error packet: msg_len=4, error code carried in last 2 bytes (upper 2 often zero)
            if len(payload) >= 2:
                code = int.from_bytes(payload[-2:], byteorder="big")
            else:
                code = int.from_bytes(payload, byteorder="big") if payload else -1
            msg = self._acoem_decode_error(code) if code >= 0 else "unknown"
            raise AcoemProtocolError(code, msg, cmd=expect_cmd)

        if verbosity > 2:
            self.logger.debug(f"ACOEM packet parsed: cmd={cmd}, msg_len={msg_len}, payload_len={len(payload)}")

        return cmd, payload

    def _acoem_bytes2int(self, response: bytes, verbosity: int = 0) -> list[int]:
        """Decode a response containing 4-byte integer words.

        Used for commands like Get Logging Config (cmd=6), where the payload is a sequence of
        big-endian 4-byte integers (count + IDs).

        Returns:
            list[int]: decoded integers (one per 4-byte word in payload)
        """
        try:
            _cmd, payload = self._acoem_parse_packet(response, verbosity=verbosity)
            if len(payload) % 4 != 0:
                # Keep best-effort decoding; this should not happen in a valid packet.
                if verbosity > 0:
                    self.logger.warning(
                        f"Payload length not divisible by 4 (len={len(payload)}); truncating to full words."
                    )
            n = len(payload) // 4
            return [int.from_bytes(payload[i * 4 : (i + 1) * 4], byteorder="big", signed=False) for i in range(n)]
        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)
            return []


    def _acoem_response2values(self, parameters: 'list[int]', response: bytes, verbosity: int = 0) -> dict:
        """Decode a Get Values (cmd=4) response into python values.

        The manual specifies that the response payload contains **only values** (one 4-byte word per requested
        parameter). Some firmware variants are observed to respond with **id/value pairs** (2 words per parameter).
        This decoder supports both forms.

        Args:
            parameters: parameter IDs as sent in the request, in request order.
            response: raw packet bytes.
            verbosity: debug verbosity.

        Returns:
            dict: {parameter_id: decoded_value}

        Raises:
            AcoemProtocolError: on CMD==0 error packets.
            ValueError: on malformed packets / mismatched payload sizes.
        """
        data: dict[int, Any] = {}
        cmd, payload = self._acoem_parse_packet(response, expect_cmd=4, verbosity=verbosity)

        # Split payload into 4-byte words
        if len(payload) % 4 != 0:
            raise ValueError(f"Invalid payload length for Get Values: {len(payload)} bytes")
        words = [payload[i : i + 4] for i in range(0, len(payload), 4)]

        n_req = len(parameters)
        n_words = len(words)

        if verbosity > 1:
            self.logger.debug(colorama.Fore.WHITE + f"Get Values response words={n_words}, requested={n_req}" + colorama.Fore.GREEN)

        # Determine whether this is values-only or id/value pairs
        values_by_param: dict[int, bytes] = {}

        if n_words == n_req:
            values_by_param = dict(zip(parameters, words))
        elif n_words == 2 * n_req:
            # Attempt id/value pairing: [id, value, id, value, ...]
            ids = [int.from_bytes(words[i], byteorder="big", signed=False) for i in range(0, n_words, 2)]
            vals = [words[i] for i in range(1, n_words, 2)]
            if ids == parameters:
                values_by_param = dict(zip(parameters, vals))
            else:
                # Some variants might return [all ids..., all vals...]
                first = ids  # actually first half interpreted as ids via stepping, might be wrong
                ids2 = [int.from_bytes(w, byteorder="big", signed=False) for w in words[:n_req]]
                vals2 = words[n_req:]
                if ids2 == parameters and len(vals2) == n_req:
                    values_by_param = dict(zip(parameters, vals2))
                else:
                    raise ValueError(
                        "Get Values response appears to contain id/value pairs, but returned IDs do not match request. "
                        f"requested={parameters}, got_ids_head={ids2[:min(10,len(ids2))]}"
                    )
        else:
            raise ValueError(
                "Number of parameters does not match number of items retrieved from response. "
                f"requested={n_req}, payload_words={n_words}, payload_len={len(payload)}"
            )

        # Decode each 4-byte word
        for parameter, item in values_by_param.items():
            if parameter in [1, 2201]:
                # Timestamp -> datetime
                data[parameter] = self._acoem_timestamp_to_datetime(int.from_bytes(item, byteorder="big", signed=False))
            elif (
                (1000 < parameter < 5000)
                or (12000000 < parameter < 13000000)
                or (14000000 < parameter < 15000000)
                or (16000000 < parameter < 17000000)
                or (27000000 < parameter < 2027000000)
            ):
                data[parameter] = struct.unpack(">i", item)[0]
            else:
                data[parameter] = struct.unpack(">f", item)[0]

        if verbosity > 1:
            self.logger.debug(f"Decoded Get Values: {data}")

        return data


    def _acoem_decode_logged_data(
        self,
        response: bytes,
        digits: int = 5,
        verbosity: int = 0,
    ) -> list[dict]:
        """Decode the binary response received from the instrument after sending cmd=7 (Get Logged Data).

        The response payload is a list of *records*; each record starts with:
          - record type (0=data, 1=header)
          - instrument operation (CURRENT_OPERATION)
          - 2 reserved bytes
          - timestamp (4 bytes)
          - logging period (4 bytes)
          - field count (4 bytes)
          - field_count 4-byte words (IDs for header records, values for data records)

        This function maintains the latest header-record keys and applies them to subsequent data records.

        Returns:
            list[dict]: Each dict contains:
              - "dtm" (UTC, '%Y-%m-%d %H:%M:%S')
              - 4035: CURRENT_OPERATION (from record header byte)
              - 2002: logging period (seconds)
              - plus logged parameter IDs from the header record (if available)

            If a data record is encountered before any header record, or if counts mismatch, the raw value words
            are returned under "values_raw".
        """
        result: list[dict] = []
        keys: list[int] = []

        def _safe_dtm(ts: int) -> str:
            try:
                return self._acoem_timestamp_to_datetime(ts).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return ""

        def _decode_value(pid: int, b4: bytes) -> Any:
            if len(b4) != 4:
                return None
            if pid in [1, 2201]:
                return self._acoem_timestamp_to_datetime(int.from_bytes(b4, byteorder="big", signed=False))
            if (
                (1000 < pid < 5000)
                or (12000000 < pid < 13000000)
                or (14000000 < pid < 15000000)
                or (16000000 < pid < 17000000)
                or (27000000 < pid < 2027000000)
            ):
                return struct.unpack(">i", b4)[0]
            return round(struct.unpack(">f", b4)[0], digits)

        try:
            _cmd, body = self._acoem_parse_packet(response, expect_cmd=7, verbosity=verbosity)

            offset = 0
            while offset + 16 <= len(body):
                record_type = body[offset]
                current_operation = body[offset + 1]
                # body[offset+2:offset+4] reserved
                ts = int.from_bytes(body[offset + 4 : offset + 8], byteorder="big", signed=False)
                logging_period = int.from_bytes(body[offset + 8 : offset + 12], byteorder="big", signed=False)
                n_fields = int.from_bytes(body[offset + 12 : offset + 16], byteorder="big", signed=False)

                record_len = 16 + n_fields * 4
                if offset + record_len > len(body):
                    if verbosity > 0:
                        self.logger.warning(
                            f"Truncated logged-data record at offset={offset}: need {record_len} bytes, have {len(body)-offset}."
                        )
                    break

                fields = [body[offset + 16 + i * 4 : offset + 20 + i * 4] for i in range(n_fields)]

                if record_type == 1:
                    keys = [int.from_bytes(f, byteorder="big", signed=False) for f in fields]
                    if verbosity > 1:
                        self.logger.debug(f"Header record: n_fields={n_fields}, keys={keys}")
                elif record_type == 0:
                    base: dict[Any, Any] = {
                        "dtm": _safe_dtm(ts),
                        4035: current_operation,
                        2002: logging_period,
                    }

                    if keys and len(keys) == len(fields):
                        rec: dict[Any, Any] = dict(base)
                        for k, v in zip(keys, fields):
                            if k == 0:
                                continue
                            rec[k] = _decode_value(k, v)
                        result.append(rec)
                    else:
                        # No header (or mismatch) — keep raw payload so we don’t silently lose data
                        result.append({**base, "values_raw": fields})
                else:
                    # Unknown record type — preserve raw
                    result.append(
                        {
                            "dtm": _safe_dtm(ts),
                            4035: current_operation,
                            2002: logging_period,
                            "record_type": record_type,
                            "fields_raw": fields,
                        }
                    )

                offset += record_len

            return result

        except Exception as err:
            self.logger.error(err)
            return result

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
    
    def _aurora_timestamp_to_date_time(self, fmt: str, dte: str, tme: str) -> datetime:
        """Convert a aurora timestamp to datetime

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
            return datetime.strptime(f"{dte} {tme}", f"{fmt} %H:%M:%S")

        except Exception as err:
            self.logger.error(err)
            return datetime(1111, 1, 1, 1, 1, 1)
        
    def _acoem_logged_data_to_string(self, data: 'list[dict]', sep: str=',') -> str:
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

    def _tcpip_comm_wait_for_line(self) -> None:                        
        t0 = time.perf_counter()
        while self._tcpip_line_is_busy:
            time.sleep(0.1)
            if time.perf_counter() > (t0 + 3 * self.socktout):
                self.logger.warning(colorama.Fore.YELLOW + "'_tcpip_comm_wait_for_line' timed out!" + colorama.Fore.GREEN)
                break
        return

    def _tcpip_comm(self, message: bytes, expect_response: bool=True, verbosity: int=0) -> bytes:
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
            self._tcpip_line_is_busy = True

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM, ) as s:
                # connect to the server
                s.settimeout(self.socktout)
                s.connect(self.sockaddr)

                # clear buffer (at least the first 1024 bytes, should be sufficient)
                # tmp = s.recv(128)
                start = time.perf_counter()

                # send message
                if verbosity>0:
                    self.logger.debug(f"message sent: {message}")
                s.sendall(message)

                # receive response
                rcvd = b''
                if expect_response:
                    if self._protocol=='acoem':
                        while not b'\x04' in rcvd:
                        # while not rcvd.endswith(b'\x04'):
                            data = s.recv(1024)
                            if not data:
                                break
                            rcvd += data
                    elif self._protocol=='aurora':
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
                        self.logger.debug(f"response (bytes): {rcvd}")
                        self.logger.debug(f"time elapsed (s): {end - start:0.4f}")

                # inform other callers tha line is free
                self._tcpip_line_is_busy = False
                return rcvd

        except Exception as err:
            self.logger.error(err)
            # inform other callers that line is free
            self._tcpip_line_is_busy = False
            return b''

    def get_instr_type(self, verbosity: int=0) -> list:
        """A.3.2 Requests details on the type of analyser being communicated with.

        Args:
            verbosity (int, optional): Verbosity for debugging purposes. Defaults to 0.

        Returns:
            list: 4 integers, namely, Model, Variant, Sub-Type, and Range.
        """
        try:
            if self._protocol=='acoem':
                message = self._acoem_construct_message(1)
                self._tcpip_comm_wait_for_line()       
                response = self._tcpip_comm(message, verbosity=verbosity)
                return self._acoem_bytes2int(response=response, verbosity=verbosity)
            else:
                self.logger.warning(colorama.Fore.YELLOW + "Not implemented." + colorama.Fore.GREEN)
                return []
        except Exception as err:
            self.logger.error(err)
            return []

    def get_version(self, verbosity: int=0) -> list:
        """A.3.3 Requests the current firmware version running on the analyser.

        Args:
            verbosity (int, optional): Verbosity for debugging purposes. Defaults to 0.

        Returns:
            list: 2 integers, namely, Build, and Branch.
        """
        try:
            if self._protocol=='acoem':
                message = self._acoem_construct_message(2)
                self._tcpip_comm_wait_for_line()
                response = self._tcpip_comm(message, verbosity=verbosity)        
                return self._acoem_bytes2int(response=response, verbosity=verbosity)
            else:
                warnings.warn("Not implemented.")
                return []
        except Exception as err:
            self.logger.error(err)
            return []

    def reset(self, verbosity: int=0) -> None:
        """
        A.3.4 Forces the analyser to do a full restart.
        The payload must contain the letters REALLY exactly, as 6 4-byte words.

        Args:
            verbosity (int, optional): Verbosity for debugging purposes. Defaults to 0.

        Returns:
            None.
        """
        try:
            if self._protocol=='acoem':
                payload = bytes([82, 69, 65, 76, 76, 89])
                message = self._acoem_construct_message(command=1, payload=payload)
            elif self._protocol=='aurora':
                message = f"**B\r".encode()
            else:
                raise ValueError('Protocol not recognized.')
            self._tcpip_comm(message, expect_response=False, verbosity=verbosity)
            return
        except Exception as err:
            self.logger.error(err)
  
    def get_values(self, parameters: 'list[int]', verbosity: int=0) -> dict:
        """
        Requests the value of one or more instrument parameters.
        If the ACOEM protocol is used, cf. A.3.5 Get Values, with A.4 List of Aurora parameters.
        If the aurora protocol s used, cf. B.7 Command VI, with Table 47 VI voltage input numbers.

        Args:
            parameters (list[int]): list of parameters to query
            verbosity (int, optional): _description_. Defaults to 0.

        Returns:
            dict: requested indexes are the keys, values are the responses from the instrument, decoded.
        """
        try:
            if self._protocol=='acoem':
                msg_data = b''
                for p in parameters:
                    msg_data += (p).to_bytes(4, byteorder='big')
                msg_len = len(msg_data)
                msg = bytes([2, self.serial_id, 4, 3]) + (msg_len).to_bytes(2, byteorder='big') + msg_data
                msg += self._acoem_checksum(msg) + bytes([4])
                self._tcpip_comm_wait_for_line()                
                response = self._tcpip_comm(message=msg, verbosity=verbosity)
                data = self._acoem_response2values(parameters=parameters, response=response, verbosity=verbosity)
                return data
            elif self._protocol=='aurora':
                items = []
                for p in parameters:
                    if p in range(100):
                        items.append(self._tcpip_comm(message=f"VI{self.serial_id}{p:02.0f}\r".encode(), verbosity=verbosity).decode())
                    else:
                        items.append('')
                data = dict(zip(parameters, items))
                return data
            else:
                self.logger.warning(colorama.Fore.YELLOW + "Not implemented." + colorama.Fore.GREEN)
                return dict()
        except Exception as err:
            self.logger.error(err)
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
            response = int()
            if self._protocol=='acoem':
                # payload = bytes([0,0,0,value])
                payload = (value).to_bytes(4, byteorder='big')
                message = self._acoem_construct_message(command=5, parameter_id=parameter_id, payload=payload)
                self._tcpip_comm_wait_for_line()                
                self._tcpip_comm(message=message, expect_response=False, verbosity=verbosity)
                if verify:
                    time.sleep(0.1)
                    i = 0
                    while response!=value:
                        response = self.get_values(parameters=[parameter_id], verbosity=verbosity)[parameter_id]
                        # print(f"{time.perf_counter()} {response}")
                        time.sleep(0.1)
                        i = i + 1
                        if i > 100:
                            break
                    return response
                else:
                    return response
            else:
                warnings.warn("Not implemented.")
                return int()
        except Exception as err:
            self.logger.error(err)
            return int()

    def get_data_log_config(self, verbosity: int=0, insert_4035_2002: bool=True) -> list:
        """
        A.3.7 Return the list of parameter IDs currently being logged. 
        It is sent with zero message data length.
        The first 4 byte word of the response data is the number of fields being logged (0..500).
        Each following 4 byte word is the ID of the parameter.
        
        NB: Values '4035' (CURRENT_OPERATION) and '2002' (LOGGING_PERIOD) are always logged, 
        even if not listed here.

        Args:
            verbosity (int, optional): Verbosity. Defaults to 0.
            insert_4035_2002 (bool, optional): If True, ensure that 4035 and 2002 are included in the returned list. Defaults to True.

        Returns:
            list: Parameter IDs (integers) currently being logged
        """
        try:
            if self._protocol=='acoem':
                message = self._acoem_construct_message(6)
                response = self._tcpip_comm(message, verbosity=verbosity)
                data_log_config = self._acoem_bytes2int(response=response, verbosity=verbosity)
                if insert_4035_2002:
                    if 4035 not in data_log_config:
                        data_log_config.insert(1, 4035)
                    if 2002 not in data_log_config:
                        data_log_config.insert(2, 2002)
                return data_log_config
            else:
                self.logger.warning(colorama.Fore.YELLOW + "Not implemented." + colorama.Fore.GREEN)
                return list()
        except Exception as err:
            self.logger.error(err)
            return list()

    # def set_datalog_param_index(
    #     self, 
    #     index_id: int, 
    #     parameter_id: int, 
    #     wavelength_config_id: int=0,
    #     ):
    #     param_base = 2003
    #     wavelength_base = 2026
    #     angle_base = 2069
    #     if index_id > 0 and index_id < 33:
    #         param_index_id = param_base + index_id
    #         wavelength_index_id = wavelength_base + index_id
    #         angle_index_id = angle_base + index_id

    # def set_data_log_config(self, verify: bool=False, verbosity: int=0) -> 'list[int]':
    #     """Pass datalog config to instrument. Verify configuration

    #     Args:
    #         verify (bool, optional): Should the datalog configuration be queried and returned after setting it? Defaults to False.
    #         verbosity (int, optional): _description_. Defaults to 0.

    #     Returns:
    #         list[int]: List of parameters logged.
    #     """
    #     try:
    #         data_log_parameter_indexes = range(2004, 2036)
    #         data_log_parameters = iter(self.data_log_parameters)
    #         for index in data_log_parameter_indexes:
    #             self.set_value(index, next(data_log_parameters), verify=False, verbosity=2)
    #         data_log_wavelength_indexes = range(2037, 2069)            
    #         data_log_wavelengths = iter(self.data_log_wavelengths)
    #         for index in data_log_wavelength_indexes:
    #             self.set_value(index, next(data_log_wavelengths), verify=False)
    #         data_log_angle_indexes = range(2070, 2102)
    #         data_log_angles = iter(self.data_log_angles)
    #         for index in data_log_angle_indexes:
    #             self.set_value(index, next(data_log_angles), verify=False)
    #         if verify:
    #             return self.get_data_log_config(verbosity=verbosity)
    #         else:
    #             return list()
    #     except Exception as err:
    #         self.logger.error(err)
    #         return list()
    
    def set_datalog_interval(self) -> int:
        try:
            datalog_interval = self.set_value(parameter_id=2002, value=self.sampling_interval)
            return datalog_interval

        except Exception as err:
            self.logger.error(err)
            return int()
        
    def get_logged_data(self, start: datetime, end: datetime, verbosity: int=0, raw: bool=False) -> 'list[dict]':
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
            if self._protocol=='acoem':
                if start:
                    payload = self._acoem_datetime_to_timestamp(start)
                    if end:
                        payload += self._acoem_datetime_to_timestamp(end)
                else:
                    raise ValueError("start and/or end date not valid.")
                message = self._acoem_construct_message(command=7, payload=payload)
                response = self._tcpip_comm(message, verbosity=verbosity)
                data =  self._acoem_decode_logged_data(response=response, verbosity=verbosity)
                if raw:
                    return [{'raw': response}, {'data': data}]
                return data
                # # get next record
                # message = self._acoem_construct_message(command=7, payload=bytes([4]))
                # while response:=self._tcpip_comm(message, verbosity=verbosity):
                #     data.append(self._acoem_decode_logged_data(response=response, verbosity=verbosity))
            else:
                self.logger.warning(colorama.Fore.YELLOW + "Not implemented. For the aurora protocol, try 'get_all_data' or 'accumulate_new_data'." + colorama.Fore.GREEN)
                return list(dict())
        except Exception as err:
            self.logger.error(err)
            return []

    def get_current_operation(self, verbosity: int=0) -> int:
        """
        Retrieves the operating state of the instrument. 
        If the ACOEM protocol is used, this requests Aurora parameter 4035 (cf.A.4 List of Aurora parameters).
        If the aurora protocol is used, this requests Voltage input number 71 (cf. Appendix B.7 Command: VI).

        Args:
            verbosity (int, optional): _description_. Defaults to 0.

        Returns:
            int: 0 (Normal monitoring), 1 (Zero calibration/check), 2 (Span calibration/check), 9 (Error)
        """
        try:
            if self._protocol=='acoem':
                parameter_id = 4035
                # message = self._acoem_construct_message(command=4, parameter_id=parameter_id)
                return self.get_values(parameters=[parameter_id], verbosity=verbosity)[parameter_id]
                # return self._acoem_bytes2int(response=response, verbosity=verbosity)[0]
            elif self._protocol=='aurora':
                response = self._tcpip_comm(message=f"VI{self.serial_id}71\r".encode(), verbosity=verbosity).decode()
                mapping = {'000': 0, '016': 2, '032': 1}
                return mapping[response]
                # warnings.warn("Not implemented.")
            else:
                raise ValueError("Protocol not recognized.")
        except Exception as err:
            self.logger.error(err)
            return 9

    def set_current_operation(self, state: int=0, verify: bool=True, verbosity: int=0) -> int:
        """Sets the instrument operating state by actuating the internal valve.

        Args:
            state (int, optional): 0: ambient, 1: zero, 2: span. Defaults to 0.
            verify (bool, optional): Should the value be queried and echoed after setting? Defaults to True.
            verbosity (int, optional): _description_. Defaults to 0.
        """
        try:
            if self._protocol=='acoem':
                return self.set_value(parameter_id=4035, value=state, verify=verify, verbosity=verbosity)
            elif self._protocol=='aurora':
                # if self.get_instr_type()[0]==158:
                warnings.warn("Not implemented for NE-300.")
                return int()
                # else:
                #     map_state = {
                #         0: "0000", 
                #         1: "0011", 
                #         2: "0001", 
                #         }
                #     message = f"DO{self.serial_number}{map_state[state]}\r".encode()
                #     response = self._tcpip_comm(message=message, verbosity=verbosity).decode()
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
            self.logger.error(err)
            return 9

    def get_id(self, verbosity: int=0) -> 'dict[str, str]':
        """Get instrument type, s/w, firmware versions

        Parameters:
            verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

        Returns:
            dict: response depends on protocol
        """
        try:
            if self._protocol=="acoem":
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

            elif self._protocol=="aurora":
                resp = {'id': self._tcpip_comm(message=f"ID{self.serial_id}\r".encode(), verbosity=verbosity).decode()}
            else:
                raise ValueError(f"[{self.name}] Communication protocol unknown")

            self.logger.info(f"[{self.name}] get_id: {resp}")
            return resp

        except Exception as err:
            self.logger.error(err)
            return dict()

    def get_datetime(self, verbosity: int=0) -> datetime:
        """Get date and time of instrument

        Parameters:
            verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

        Returns:
            datetime.datetime: Date and time of instrument
        """
        response = datetime(1,1,1)
        try:
            if self._protocol=="acoem":
                msg = self._acoem_construct_message(4, 1)
                response_bytes = self._tcpip_comm(message=msg, verbosity=verbosity)
                response_int = self._acoem_bytes2int(response=response_bytes, verbosity=verbosity)[0]
                response = self._acoem_timestamp_to_datetime(response_int)
            elif self._protocol=='aurora':
                fmt = self._tcpip_comm(message=f"VI{self.serial_id}64\r".encode(), verbosity=verbosity).decode()
                dte = self._tcpip_comm(message=f"VI{self.serial_id}80\r".encode(), verbosity=verbosity).decode()
                tme = self._tcpip_comm(message=f"VI{self.serial_id}81\r".encode(), verbosity=verbosity).decode()
                response = self._aurora_timestamp_to_date_time(fmt, dte, tme)
            else:
                raise ValueError("Protocol not recognized.")
            self.logger.info(f"get_datetime: {response}")
            return response
        except Exception as err:
            self.logger.error(err)
            return response

    def get_set_datetime(self, dtm: datetime=datetime.now(timezone.utc), verbosity: int=0) -> 'tuple[dict, dict]':
        """Get and then set date and time of instrument

        Parameters:
            dtm (datetime.datetime, optional): Date and time to be set. Defaults to time.gmtime().
            verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

        Returns:
            None
        """
        try:
            if self._protocol=="acoem":
                # get dtm from instrument
                dtm_found = self.get_values(parameters=[1])[1].strftime('%Y-%m-%d %H:%M:%S')
                
                # set dtm of instrument
                payload = self._acoem_datetime_to_timestamp(dtm=dtm)
                msg = self._acoem_construct_message(command=5, parameter_id=1, payload=payload)
                self._tcpip_comm(message=msg, expect_response=False, verbosity=verbosity)
                
                # get new dtm from instrument
                dtm_set = self.get_values(parameters=[1])[1].strftime('%Y-%m-%d %H:%M:%S')
                return (dtm_found, dtm_set)
            else:
                warnings.warn("Not implemented.")
                return (dict(), dict())
                # resp = self.tcpip_comm1(f"**{self.serial_id}S{dtm.strftime('%H%M%S%d%m%y')}")
                # msg = f"DateTime of instrument {self.name} set to {dtm} ... {resp}"
                # print(f"{dtm} {msg}")
                # self.logger.info(msg)

        except Exception as err:
            self.logger.error(err)
            return (dict(), dict())

    def do_span(self, verify: bool=True, verbosity: int=0) -> int:
        """
        Override digital IO control and DOSPAN. Wrapper for set_current_operation.

        Parameters:
            verbosity (int, optional): ...

        Returns:
            int: 2: span
        """
        self._tcpip_comm_wait_for_line()
        return self.set_current_operation(state=2, verify=verify, verbosity=verbosity)

    def do_zero(self, verify: bool=True, verbosity: int=0) -> int:
        """
        Override digital IO control and DOZERO. Wrapper for set_current_operation.

        Parameters:
            verbosity (int, optional): ...
        
        Returns:
            int: 1: zero
        """
        self._tcpip_comm_wait_for_line()
        return self.set_current_operation(state=1, verify=verify, verbosity=verbosity)
    
    def do_ambient(self, verify: bool=True, verbosity: int=0) -> int:
        """
        Override digital IO control and return to ambient measurement. Wrapper for set_current_operation.

        Parameters:
            verbosity (int, optional): ...

        Returns:
            int: 0: ambient
        """
        self._tcpip_comm_wait_for_line()
        return self.set_current_operation(state=0, verify=verify, verbosity=verbosity)

    # def get_status_word(self, verbosity: int=0) -> int:
    #     """
    #     Read the System status of the Aurora 3000 microprocessor board. The status word 
    #     is the status of the nephelometer in hexadecimal converted to decimal.

    #     Parameters:
    
    #     Returns:
    #         int: {<STATUS WORD>}
    #     """
    #     return int(self._tcpip_comm(f"VI{self.serial_id}88\r".encode()).decode())

    # def get_all_data(self, verbosity: int=0) -> str:
    #     """
    #     Rewind the pointer of the data logger to the first entry, then retrieve all data (cf. B.4 ***R, B.3 ***D). 
    #     This only works with the aurora protocol (and doesn't work very well with the NE-300).

    #     Parameters:
    #         verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

    #     Returns:
    #         str: response
    #     """
    #     try:
    #         if self._protocol=="acoem":
    #             warnings.warn("Not implemented. Use 'get_logged_data' with specified period instead.")
    #         elif self._protocol=='aurora':
    #             self._tcpip_comm_wait_for_line()
    #             response = self._tcpip_comm(message=f"***R\r".encode(), verbosity=verbosity).decode()
    #             response = self.accumulate_new_data(verbosity=verbosity)
    #             # response = self._tcpip_comm(message=f"***D\r".encode(), verbosity=verbosity).decode()
    #             return response
    #         else:
    #             raise ValueError("Protocol not implemented.")
    #         return str()
    #     except Exception as err:
    #         self.logger.error(err)
    #         return str()

    def get_current_data(self, add_params: list=[], strict: bool=False, sep: str=' ', verbosity: int=0) -> dict:
        """
        Retrieve latest near-real-time reading on one line.
        With the aurora protocol, this uses the command 99 (cf. B.7 VI: 99), returning parameters [80,81,30,2,31,3,32,17,18,16,19,00,90].
        These are mapped to the corresponding Acoem parameters (cf. A.4 List of Aurora parametes) [1,1635000,1525000,1450000,1635090,1525090,1450090,5001,5004,5003,5002,4036,4035].
        Optionally, several more parameters can be retrieved, depending on the protocol.

        Parameters:
            add_params (list, optional): read more values and append to dictionary. Defaults to [].
            strict (bool, optional): If True, the dictionary returned is {99: response}, where response is the <sep>-separated response of the VI<serial_id>99 aurora command. Defaults to False.
            sep (str, optional): Separator applied if strict=True. Defaults to ' '.
            verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

        Returns:
            dict: Dictionary of parameters and values obtained.
        """
        parameters = [1,1635000,1525000,1450000,1635090,1525090,1450090,5001,5004,5003,5002,4036,4035]
        if add_params:
            parameters += add_params
        try:
            if self._protocol=='acoem':
                # warnings.warn("Not implemented.")
                data = self.get_values(parameters=parameters, verbosity=verbosity)
                if strict:
                    if 1 in parameters:
                        data[1] = data[1].strftime(format=f"%d/%m/%Y{sep}%H:%M:%S")
                    response = sep.join([str(data[k]) for k, v in data.items()])
                    data = {99: response}
            elif self._protocol=='aurora':
                response = self._tcpip_comm(f"VI{self.serial_id}99\r".encode(), verbosity=verbosity).decode()
                response = response.replace(", ", ",")
                if strict:
                    response = response.replace(',', sep)
                    data = {99: response}
                else:
                    response = response.split(',')
                    response = [response[0]] + [float(v) for v in response[1:]]
                    data = dict(zip(parameters, response))
            else:
                raise ValueError("Protocol not recognized.")
            return data
        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)
            return dict()


    def _accumulate_new_data(self, sep: str = ",", verbosity: int = 0) -> None:
        """Retrieve recent records and append to the in-memory buffer (and optionally file).

        For ACOEM:
            - requests logged data for (now - sampling_interval) .. now, rounded to minutes
            - writes rows in the exact order: dtm, 4035, 2002, <rest of datalog config>
        For Aurora (legacy text protocol):
            - uses ***D cursor download

        Args:
            sep: output separator
            verbosity: debug verbosity
        """
        try:
            if self._protocol == "acoem":
                now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
                start = now - timedelta(seconds=int(self.sampling_interval))
                end = now

                data = self.get_logged_data(start=start, end=end, verbosity=verbosity)

                rows: list[str] = []
                for rec in data:
                    # Build row in header order (dtm + self._header)
                    dtm = rec.get("dtm", "")
                    vals = [dtm]
                    for pid in getattr(self, "_header", []):
                        v = rec.get(pid, None)
                        vals.append("" if v is None else str(v))
                    rows.append(sep.join(vals))

                chunk = "\n".join(rows) + ("\n" if rows else "")
                if verbosity > 0 and chunk:
                    self.logger.info(chunk)

                self._data += chunk
                return

            if self._protocol == "aurora":
                data = self._tcpip_comm(f"***D\r".encode()).decode()
                data = data.replace("\r\n\n", "\r\n").replace(", ", ",").replace(",", sep)
                if verbosity > 0:
                    self.logger.info(data)
                self._data += data
                return

            raise ValueError("Protocol not recognized.")

        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)
            return

    def _save_data(self) -> str | None:
        try:
            self.logger.debug(f"[{self.name}] _save_data")

            if not self._data or not self._data.strip():
                self.logger.info(f"[{self.name}] no data to save (skipping file creation).")
                self.data_file = str()
                return None

            now = datetime.now()
            timestamp = now.strftime(self._file_timestamp_format)
            yyyy = now.strftime('%Y')
            mm = now.strftime('%m')
            dd = now.strftime('%d')
          
            # create appropriate file name and write mode
            self.data_file = os.path.join(self.data_path, yyyy, mm, dd, f"{self.name}-{timestamp}.dat")
            os.makedirs(os.path.dirname(self.data_file), exist_ok=True)
            if os.path.exists(self.data_file):
                header = str()
            else:
                header_ids = ["dtm"] + [str(pid) for pid in self._header]
                header = ",".join(header_ids) + "\n"

            with open(file=self.data_file, mode='a') as fh:
                if header:
                    fh.write(header)
                fh.write(self._data)
                self.logger.info(f"[{self.name}] file saved: {self.data_file}")

                # reset self._data
                self._data = str()
            return self.data_file

        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)

    def _stage_file(self):
        """ Stage file, optionally as .zip archive.
        """
        try:
            self.logger.debug(f"[{self.name}]: ._stage_file")

            if not self.data_file or not os.path.isfile(self.data_file):
                self.logger.info(f"[{self.name}] no source file to stage (skipping).")
                return

            archive = os.path.join(self.staging_path, os.path.basename(self.data_file).replace('.dat', '.zip'))
            with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.write(self.data_file, os.path.basename(self.data_file))
                self.logger.info(f"[{self.name}] file staged: {archive}")            
                # reset
                self.data_file = str()

        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)

    def _save_and_stage_data(self):
        try:
            self.logger.debug(f"[{self.name}] ._save_and_stage_data")
        
            self._save_data()
            if self.data_file:
                self._stage_file()

        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)

    def print_ssp_bssp(self) -> None:
        """Retrieve current readings and print."""
        try:
            data = self.get_values(parameters=[2635000, 2635090, 2525000, 2525090, 2450000, 2450090])
            data = f"ssp|bssp (Mm-1) r: {data[2635000]:0.4f}|{data[2635090]:0.4f} g: {data[2525000]:0.4f}|{data[2525090]:0.4f} b: {data[2450000]:0.4f}|{data[2450090]:0.4f}"
            self.logger.info(colorama.Fore.GREEN + f"[{self.name}] {data}")

        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"[{self.name}] print_ssp_bssp: {err}" + colorama.Fore.GREEN)


if __name__ == "__main__":
    pass

# """
# Define a class NE300 facilitating communication with a Acoem NE-300 nephelometer.

# @author: joerg.klausen@meteoswiss.ch
# """

# import logging
# import os
# import socket
# import struct
# import threading
# import time
# import warnings
# import zipfile
# from datetime import datetime, timedelta, timezone

# import colorama
# import schedule


# def _default_run_threaded(job, *args, **kwargs):
#     thread = threading.Thread(target=job, args=args, kwargs=kwargs, daemon=True)
#     thread.start()
#     return thread

# class NEPH:
#     """
#     Instrument of type Acoem NE-300 or Ecotech Aurora 3000 nephelometer with methods, attributes for interaction.
#     """
#     def __init__(self, name: str, config: dict, verbosity: int=0) -> None:
#         """
#         Initialize instrument class.

#         :param name: name of instrument
#         :param config: dictionary of attributes defining instrument, serial port and more
#             - config[name]['type']
#             - config[name]['serial_number']
#             - config[name]['serial_id']
#             - config[name]['socket']['host']
#             - config[name]['socket']['port']
#             - config[name]['socket']['timeout']
#             - config[name]['socket']['sleep']
#             - config[name]['get_data_interval']
#             - config[name]['reporting_interval']
#             - config[name]['zero_check_duration']
#             - config[name]['span_check_duration']
#             - config['staging']['path'])
#             - config[name]['staging_zip']
#             - config[name]['protocol']
#             - config[name]['data_log']['parameters']
#             - config[name]['data_log']['wavelengths']
#             - config[name]['data_log']['angles']
#             - config[name]['data_log']['interval']
#         """
#         colorama.init(autoreset=True)

#         try:
#             self.name = name
#             self.type = config[name]['type']
#             self.serial_number = config[name]['serial_number']

#             # configure logging
#             _logger = f"{os.path.basename(config['logging']['file'])}".split('.')[0]
#             self.logger = logging.getLogger(f"{_logger}.{__name__}")
#             self.logger.info(f"[{self.name}] Initializing {self.type} (S/N: {self.serial_number})", extra={"to_logfile": True})

#             self.verbosity = config[name]['verbosity']

#             # read instrument control properties for later use
#             self.serial_id = config[name]['serial_id']

#             # configure tcp/ip
#             self.sockaddr = (config[name]['socket']['host'],
#                              config[name]['socket']['port'])
#             self.socktout = config[name]['socket']['timeout']
#             # self.__socksleep = config[name]['socket']['sleep']
            
#             # flag to maintain state to prevent concurrent tcpip communication in a threaded setup
#             self._tcpip_line_is_busy = False

#             # configure comms protocol
#             if config[name]['protocol'] in ["acoem", "aurora"]:
#                 self._protocol = config[name]['protocol']
#             else:
#                raise ValueError("Communication protocol not recognized.")

#             # data log
#             # self.data_log_parameters = config[name]['data_log']['parameters']
#             # self.data_log_wavelengths = config[name]['data_log']['wavelengths']
#             # self.data_log_angles = config[name]['data_log']['angles']
#             # self.data_log_interval = config[name]['data_log']['interval']

#             # sampling, aggregation, reporting/storage
#             self.sampling_interval = config[name]['sampling_interval']
#             self.reporting_interval = config[name]['reporting_interval']

#             # zero and span check interval and durations
#             self.zero_span_check_interval = config[name]['zero_span_check_interval']
#             self.zero_check_duration = config[name]['zero_check_duration']
#             self.span_check_duration = config[name]['span_check_duration']
#             if self.zero_span_check_interval % 60 != 0:
#                 raise ValueError(
#                     f"zero_span_check_interval={self.zero_span_check_interval} must be a multiple of 60 minutes."
#                 )
#             total_duration = self.zero_check_duration + self.span_check_duration
#             if total_duration >= 60:
#                 raise ValueError(
#                     f"zero_check_duration + span_check_duration = {total_duration} must be < 60 minutes."
#                 )
#             self._zero_span_check_hours = self.zero_span_check_interval // 60
#             self._span_offset_min = self.zero_check_duration                                # mm offset from :00
#             self._ambient_offset_min = self.zero_check_duration + self.span_check_duration  # mm offset from :00
#             if self.zero_span_check_interval > 0:
#                 self.logger.info(
#                     "zero/span checks every %d hours, zero @ :00, span @ :%02d, return to ambient @ :%02d",
#                     self._zero_span_check_hours, self._span_offset_min, self._ambient_offset_min,
#                 )
#             else:
#                 self.logger.info("zero/span checks disabled.")

#             # configure saving, staging and remote path
#             root = os.path.expanduser(config['root'])
#             self.data_path = os.path.join(root, config['data'], config[name]['data_path'])
#             os.makedirs(self.data_path, exist_ok=True)
#             self.staging_path = os.path.join(root, config['staging'], config[name]['staging_path'])
#             os.makedirs(self.staging_path, exist_ok=True)
#             self.staging_zip = config[name]['staging_zip']

#             self.remote_path = config[name]['remote_path']

#             # retrieve instrument id and other instrument info
#             id = self.get_id(verbosity=verbosity)
#             if id=={}:
#                 self.logger.error(colorama.Fore.RED + f"[{self.name}] Could not communicate with instrument. Protocol set to '{self._protocol}'. Please verify instrument settings." + colorama.Fore.GREEN)
#             else:
#                 self.logger.info(f"[{self.name}] id reported: '{id}'.", extra={"to_logfile": True})

#                 # get version
#                 version = self.get_version()
#                 self.logger.info(f"[{self.name}] firmware version reported: {version}.", extra={"to_logfile": True})            

#                 # put instrument in ambient mode
#                 state = self.do_ambient(verbosity=verbosity)
#                 if state==0:
#                     self.logger.info(f"[{self.name}] current operation reported: 'ambient'.", extra={"to_logfile": True})
#                 else:
#                     self.logger.warning(colorama.Fore.YELLOW + f"[{self.name}] Could not verify measurement mode as 'ambient'." + colorama.Fore.GREEN)

#                 # get dtm from instrument, then set date and time
#                 dtm_found, dtm_set = self.get_set_datetime(dtm=datetime.now(timezone.utc))            
#                 self.logger.info(f"[{self.name}] dtm found: {dtm_found} > dtm set: {dtm_set}.", extra={"to_logfile": True})            

#                 # set datalog interval
#                 datalog_interval = self.set_datalog_interval()
#                 self.logger.info(f"[{self.name}] Datalog interval set to {datalog_interval} seconds.")

#                 # get logging config (=header ids)
#                 cfg = self.get_data_log_config()[1:]  # drop the leading "number of fields"

#                 # ensure 4035 and 2002 are present as the first two data columns after dtm
#                 self._header = [4035, 2002] + [pid for pid in cfg if pid not in (4035, 2002)]

#                 self.logger.info(f"[{self.name}] logging config reported: {self._header}.", extra={"to_logfile": True})

#             # datetime to keep track of retrievals from datalog
#             self._start_datalog = datetime.now(timezone.utc).replace(second=0, microsecond=0)

#             # initialize data response
#             self._data = str()

#             # initialize other stuff
#             self._file_timestamp_format = str()
#             self.data_file = str()

#         except Exception as err:
#             self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)

#     def setup_schedules(self, run_threaded=_default_run_threaded):
#         try:
#             # configure data acquisition schedule
#             schedule.every(int(self.sampling_interval)).minutes.at(':02').do(run_threaded, self._accumulate_new_data)

#             # configure zero and span check schedules
#             if self.zero_span_check_interval > 0:
#                 self._setup_zero_span_check_schedules()

#             # configure saving and staging schedules
#             if self.reporting_interval==10:
#                 self._file_timestamp_format = '%Y%m%d%H%M'
#                 for minute in range(6):
#                     schedule.every(1).hour.at(f"{minute}0:05").do(run_threaded, self._save_and_stage_data)
#             elif self.reporting_interval==60:
#                 self._file_timestamp_format = '%Y%m%d%H'
#                 # schedule.every().hour.at('00:01').do(self._save_and_stage_data)
#                 schedule.every(1).hour.at('00:05').do(run_threaded, self._save_and_stage_data)
#             elif self.reporting_interval==1440:
#                 self._file_timestamp_format = '%Y%m%d'
#                 # schedule.every().day.at('00:00:01').do(self._save_and_stage_data)
#                 schedule.every(1).day.at('00:00:05').do(run_threaded, self._save_and_stage_data)
#             else:
#                 raise ValueError(f"A reporting interval of {self.reporting_interval} is not supported.")
            
#         except Exception as err:
#             self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)

#     def _setup_zero_span_check_schedules(self, run_threaded=_default_run_threaded) -> None:
#         """
#         Zero/span/ambient checks aligned to the top of the hour:

#         - Zero:     every N hours at :00
#         - Span:     every N hours at :<zero_duration>
#         - Ambient:  every N hours at :<zero_duration + span_duration>

#         Assumes __init__ validated that:
#         - zero_span_check_interval is a multiple of 60
#         - zero_check_duration + span_check_duration < 60
#         """
#         try:
#             # Zero at :00 every N hours
#             schedule.every(self._zero_span_check_hours).hours.at(":00").do(run_threaded, self.do_zero)
#             # Span offset within the hour
#             schedule.every(self._zero_span_check_hours).hours.at(f":{self._span_offset_min:02d}").do(run_threaded, self.do_span)
#             # Ambient offset within the hour
#             schedule.every(self._zero_span_check_hours).hours.at(f":{self._ambient_offset_min:02d}").do(run_threaded, self.do_ambient)

#             self.logger.info(
#                 "Scheduled zero/span/ambient: every %d hour(s) at :00/:%02d/:%02d",
#                 self._zero_span_check_hours, self._span_offset_min, self._ambient_offset_min
#             )
#         except Exception as err:  # pragma: no cover
#             self.logger.error("setup_zero_span_check_schedules failed: %s", err)

#     def _acoem_checksum(self, x: bytes) -> bytes:
#         """
#         Compute the checksum of all bytes except checksum and EOT by XORing bytes from left 
#         to right. (Reference: Aurora NE Series User Manual v1.2 Appendix A.1)

#         Args:
#             x (bytes): Bytes 1..(N-2)

#         Returns:
#             bytes: Checksum (1 Byte)
#         """
#         try:
#             cs = bytes([0])
#             for i in range(len(x)):
#                 cs = bytes([a^b for a,b in zip(cs, bytes([x[i]]))])
#             return cs

#         except Exception as err:
#             self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)
#             return b''

#     def _acoem_timestamp_to_datetime(self, timestamp: int) -> datetime:
#         try:
#             dtm = timestamp
#             SS = dtm % 64
#             dtm = dtm // 64
#             MM = dtm % 64
#             dtm = dtm // 64
#             HH = dtm % 32
#             dtm = dtm // 32
#             dd = dtm % 32
#             dtm = dtm // 32
#             mm = dtm % 16
#             yyyy = dtm // 16 + 2000

#             return datetime(yyyy, mm, dd, HH, MM, SS)

#         except Exception as err:
#             self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)
#             return datetime(1111, 1, 1)

#     def _acoem_datetime_to_timestamp(self, dtm: datetime=datetime.now(timezone.utc)) -> bytes:
#         try:
#             SS = bin(dtm.time().second)[2:].zfill(6)
#             MM = bin(dtm.time().minute)[2:].zfill(6)
#             HH = bin(dtm.time().hour)[2:].zfill(5)
#             dd = bin(dtm.date().day)[2:].zfill(5)
#             mm = bin(dtm.date().month)[2:].zfill(4)
#             yyyy = bin(dtm.date().year - 2000).zfill(6)

#             return (int(yyyy + mm + dd + HH + MM + SS, base=2)).to_bytes(4, byteorder='big')

#         except Exception as err:
#             self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)
#             return b''

#     def _acoem_construct_parameter_id(self, base_id: int, wavelength: int, angle: int) -> int:
#         """
#         Construct ACOEM measurement parameter id. Cf. ACOEM manual for details.
#         Parameter ID = Base ID * 1,000,000 + Wavelength * 1,000 + Angle

#         Args:
#             base_id (int): cf. ACOEM manual Table 45 - Aurora Base IDs for Constructed Parameters
#             wavelength (int): One of <450|525|635>. Defaults to None.
#             angle (int): 0 for Fullscatter, 90 for Backscatter

#         Returns:
#             int: _description_
#         """
#         return base_id * 1000000 + wavelength * 1000 + angle
    
#     def _acoem_construct_message(self, command: int, parameter_id: int=0, payload: bytes=b'') -> bytes:
#         """
#         Construct ACOEM packet to be sent to instrument. See the ACOEM manual for explanations.
        
#         Byte  |1  |2  |3  |4  |5..6     |7..10    |11       |12
#               |STX|SID|CMD|ETX|msg_len  |msg_data |checksum |EOT
#         STX = chr(2)
#         SID = serial_id
#         CMD = command
#         ETX = chr(3)
#         msg_len = message length
#         msg_data = message data
#         EOT = chr(4)

#         Args:
#             command (int): cf. ACOEM manual Table 19 - List of Commands
#             parameter (int, optional): cf. ACOEM manual Table 46 - Aurora Parameters. Defaults to 0 (Not a valid parameter).
#             payload (int, optional): _description_. Defaults to None.

#         Returns:
#             bytes: _description_
#         """
#         msg_data = bytes()
#         if parameter_id>0:
#             msg_data = (parameter_id).to_bytes(4, byteorder='big')
#         if len(payload)>0:
#             msg_data += payload
#         msg_len = len(msg_data)
#         msg = bytes([2, self.serial_id, command, 3]) + (msg_len).to_bytes(2, byteorder='big') + msg_data
#         return msg + self._acoem_checksum(msg) + bytes([4])
    
#     def _acoem_bytes2int(self, response: bytes, verbosity: int=0) -> 'list[int]':
#         """Convert byte response obtained from instrument into integers. 
    

#         Args:
#             response (bytes): Raw response obtained from instrument
#             verbosity (int, optional): _description_. Defaults to 0.

#         Returns:
#             list[int]: integers corresponding to the bytes returned. NB: The resulting integers may represent IEEE encoded floats, i.e., this conversion is only meaningful for certian responses.
#         """
#         response_length = int(int.from_bytes(response[4:6], byteorder='big') / 4)
#         if verbosity>1:
#             self.logger.debug(f"response length : {response_length}")
        
#         items = []
#         for i in range(6, (response_length + 1) * 4 + 2, 4):
#             item = int.from_bytes(response[i:(i+4)], byteorder='big')

#             if verbosity>1:
#                 self.logger.debug(f"response item{(i-2)/4:3.0f}: {item}")
#             items.append(item)

#         return items

#     def _acoem_response2values(self, parameters: 'list[int]', response: bytes, verbosity: int=0) -> dict:
#         """Convert byte response obtained from instrument into integers, floats or datetime, depending on parameter.     

#         Args:
#             parameters (list[int]): Parameters requested from instrument
#             response (bytes): Raw response obtained from instrument
#             verbosity (int, optional): _description_. Defaults to 0.

#         Returns:
#             dict: dictionary with parameters and corresponding values, decoded. Parameter 1 is decoded to datetime, the others to either int or float.
#         """
#         data = dict()
#         response_length = int(int.from_bytes(response[4:6], byteorder='big') / 4)
#         if verbosity>1:
#             self.logger.debug(colorama.Fore.WHITE + f"response length : {response_length}" + colorama.Fore.GREEN)

#         items_bytes = [response[i:(i+4)] for i in range(6, (response_length + 1) * 4 + 2, 4)]
#         if verbosity>1:
#             self.logger.debug(f"items: {len(items_bytes)}\nitems (bytes): {items_bytes}")

#         if len(parameters)==len(items_bytes):
#             data_bytes = dict(zip(parameters, items_bytes))
#         else:
#             raise ValueError("Number of parameters does not match number of items retrieved from response.")    

#         # decode values
#         for parameter, item in data_bytes.items():
#             if parameter in [1, 2201]:
#                 data[parameter] = self._acoem_timestamp_to_datetime(int.from_bytes(item, byteorder='big'))
#             elif ((parameter>1000 and parameter<5000) \
#                   or (parameter>12000000 and parameter<13000000) \
#                   or (parameter>14000000 and parameter<15000000) \
#                   or (parameter>14000000 and parameter<15000000) \
#                   or (parameter>16000000 and parameter<17000000) \
#                   or (parameter>27000000 and parameter<2027000000)):
#                 data[parameter] = struct.unpack('>i', item)[0]
#             else:
#                 data[parameter] = struct.unpack('>f', item)[0]

#         if verbosity==1:
#             self.logger.debug(f"response items:\n{data}")
#         if verbosity>1:
#             self.logger.debug(f"response items (bytes):\n{data_bytes}")
#             self.logger.debug(f"response items:\n{data}")

#         return data

#     def _acoem_decode_logged_data(
#         self,
#         response: bytes,
#         digits: int = 5,
#         verbosity: int = 0,
#     ) -> list[dict]:
#         """
#         Decode the binary response received from the instrument after sending command 7.

#         Returns:
#             list[dict]: List of dictionaries, where keys are parameter ids and values are decoded values.
#                         Adds:
#                         - data['dtm'] = '%Y-%m-%d %H:%M:%S'
#                         - data[4035] = current operation (0 ambient, 1 zero, 2 span, 9 error/unknown)
#                         - data[2002] = logging period (seconds)
#         """
#         from typing import Any

#         result: list[dict[Any, Any]] = []

#         # Defensive: empty / too short
#         if not response or len(response) < 8:
#             return result

#         cmd = response[2]

#         # Error response
#         if cmd == 0:
#             try:
#                 return [{"communication_error": self._acoem_decode_error(error_code=response[7])}]
#             except Exception:
#                 return [{"communication_error": "unknown"}]

#         # Not a logged-data response
#         if cmd != 7:
#             return result

#         try:
#             # Message layout:
#             # [0]=STX [1]=SID [2]=CMD [3]=ETX [4:6]=msg_len [6:6+msg_len]=body [..]=checksum+EOT
#             msg_len = int.from_bytes(response[4:6], byteorder="big")

#             # Use declared length (robust), but don’t crash if packet is shorter than declared
#             body_start = 6
#             body_end = min(len(response), body_start + msg_len)
#             body = response[body_start:body_end]

#             if verbosity > 1:
#                 self.logger.debug(f"message length (bytes): {msg_len}")
#                 self.logger.debug(f"body length (bytes): {len(body)}")
#                 self.logger.debug(f"body (bytes): {body!r}")

#             # Record format (per record):
#             # 0: record_type (1 byte)
#             # 1: instrument_operation (1 byte)
#             # 2..3: reserved (2 bytes)
#             # 4..7: timestamp (4 bytes)
#             # 8..11: logging_period (4 bytes)
#             # 12..15: n_fields (4 bytes)
#             # 16.. : fields (n_fields * 4 bytes)
#             offset = 0
#             keys: list[int] = []

#             def _safe_dtm(ts: int) -> str:
#                 # Only called for non-header records
#                 return self._acoem_timestamp_to_datetime(ts).strftime("%Y-%m-%d %H:%M:%S")

#             while offset + 16 <= len(body):
#                 record_type = body[offset]
#                 current_operation = body[offset + 1]
#                 ts = int.from_bytes(body[offset + 4 : offset + 8], byteorder="big")
#                 logging_period = int.from_bytes(body[offset + 8 : offset + 12], byteorder="big")
#                 n_fields = int.from_bytes(body[offset + 12 : offset + 16], byteorder="big")

#                 record_len = 16 + 4 * n_fields
#                 if offset + record_len > len(body):
#                     if verbosity > 0:
#                         self.logger.debug(
#                             f"Truncated record at offset={offset}, need {record_len} bytes, have {len(body) - offset}."
#                         )
#                     break

#                 fields = [body[offset + 16 + i * 4 : offset + 20 + i * 4] for i in range(n_fields)]

#                 if record_type == 1:
#                     # Header record: parameter IDs
#                     # NOTE: header timestamp / logging_period are often 0 / undefined -> do NOT decode dtm here.
#                     keys = [int.from_bytes(f, byteorder="big") for f in fields]
#                     if verbosity > 1:
#                         self.logger.debug(f"Header record: n_fields={n_fields}, keys={keys}")
#                 else:
#                     # Build shared metadata once per record
#                     base: dict[Any, Any] = {
#                         4035: current_operation,          # CURRENT_OPERATION (from record header byte)
#                         "logging_interval": logging_period,
#                         "dtm": _safe_dtm(ts),
#                     }

#                     if record_type == 0:
#                         # Data record
#                         if keys and len(keys) == len(fields):
#                             data: dict[Any, Any] = dict(base)
#                             for k, v in zip(keys, fields):
#                                 if k == 0:
#                                     continue  # invalid parameter id (often a symptom of old truncation)

#                                 if k > 1000 and len(v) == 4:
#                                     try:
#                                         data[k] = round(struct.unpack(">f", v)[0], digits)
#                                     except Exception:
#                                         data[k] = v
#                                 else:
#                                     data[k] = v

#                             result.append(data)
#                         else:
#                             # No header (or mismatch) — keep raw payload so we don’t silently lose data
#                             result.append({**base, "values_raw": fields})
#                     else:
#                         # Unknown record type — preserve raw
#                         result.append({**base, "record_type": record_type, "fields_raw": fields})

#                 offset += record_len

#             return result

#         except Exception as err:
#             self.logger.error(err)
#             return result

#     def _acoem_decode_error(self, error_code: int) -> str:
#         """A.3.1 Return description of error code

#         Args:
#             error_code (bytes): A.3.1. Table 21 error codes

#         Returns:
#             str: description of error code
#         """
#         error_map = {0: 'checksum failed',
#                      1: 'invalid command byte',
#                      2: 'invalid parameter',
#                      3: 'invalid message length',
#                      4: 'reserved',
#                      5: 'reserved',
#                      6: 'reserved',
#                      7: 'reserved',
#                      8: 'media not connected',
#                      9: 'media busy',}
#         return error_map[error_code]
    
#     def _aurora_timestamp_to_date_time(self, fmt: str, dte: str, tme: str) -> datetime:
#         """Convert a aurora timestamp to datetime

#         Args:
#             fmt (str): date reporting format as string: D/M/Y, M/D/Y or Y-M-D (where D=Day, M=Month, Y=Year)
#             dte (str): instrument date
#             tme (str): instrument time as %H:%M:%S

#         Returns:
#             datetime.datetime: instrument date and time
#         """
#         try:
#             fmt = fmt.replace(' ', '').replace('\r\n', '')
#             dte = dte.replace('\r\n', '')
#             tme = tme.replace('\r\n', '')
#             if fmt=="D/M/Y":
#                 fmt = "%d/%m/%Y"
#             elif fmt=="M/D/Y":
#                 fmt = "%m/%d/%Y"
#             elif fmt=="Y-M-D":
#                 fmt = "%Y-%m-%d"
#             else:
#                 raise ValueError("'fmt' not recognized.")
#             return datetime.strptime(f"{dte} {tme}", f"{fmt} %H:%M:%S")

#         except Exception as err:
#             self.logger.error(err)
#             return datetime(1111, 1, 1, 1, 1, 1)
        
#     def _acoem_logged_data_to_string(self, data: 'list[dict]', sep: str=',') -> str:
#         """Convert data retrieved using the get_logged_data to a string format ready for saving.

#         Args:
#             data (list[dict]): data retrieved from get_logged_data
#             sep (str, optional): Separator to use for export. Defaults to ','.

#         Returns:
#             str: string consisting of rows of <sep>-separated items
#         """
#         result = []
#         for d in data:
#             dtm_value = d.pop('dtm')
#             values = [str(dtm_value)] + [str(value) for key, value in d.items()]
#             result.append(sep.join(values))

#         return '\n'.join(result)

#     def _tcpip_comm_wait_for_line(self) -> None:                        
#         t0 = time.perf_counter()
#         while self._tcpip_line_is_busy:
#             time.sleep(0.1)
#             if time.perf_counter() > (t0 + 3 * self.socktout):
#                 self.logger.warning(colorama.Fore.YELLOW + "'_tcpip_comm_wait_for_line' timed out!" + colorama.Fore.GREEN)
#                 break
#         return

#     def _tcpip_comm(self, message: bytes, expect_response: bool=True, verbosity: int=0) -> bytes:
#         """
#         Send and receive data using ACOEM protocol
        
#         Args:
#             message (bytes): message data as required by the ACOEM protocol.
#             expect_repsonse (bool, optional): If True, read response after sending message. Defaults to True.
#             verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

#         Raises:
#             ValueError: if protocol is unknown.

#         Returns:
#             bytes: bytes returned.
#         """
#         try:
#             # prevent other callers from proceeding if they require to send and retrieve
#             self._tcpip_line_is_busy = True

#             with socket.socket(socket.AF_INET, socket.SOCK_STREAM, ) as s:
#                 # connect to the server
#                 s.settimeout(self.socktout)
#                 s.connect(self.sockaddr)

#                 # clear buffer (at least the first 1024 bytes, should be sufficient)
#                 # tmp = s.recv(128)
#                 start = time.perf_counter()

#                 # send message
#                 if verbosity>0:
#                     self.logger.debug(f"message sent: {message}")
#                 s.sendall(message)

#                 # receive response
#                 rcvd = b''
#                 if expect_response:
#                     if self._protocol=='acoem':
#                         while not b'\x04' in rcvd:
#                         # while not rcvd.endswith(b'\x04'):
#                             data = s.recv(1024)
#                             if not data:
#                                 break
#                             rcvd += data
#                     elif self._protocol=='aurora':
#                         while not (rcvd.endswith(b'\r\n') or rcvd.endswith(b'\r\n\n')):
#                             data = s.recv(1024)
#                             rcvd += data
#                     else:
#                         raise ValueError('Protocol not recognized.')
#                     rcvd = rcvd.strip()
#                     # remove pre-ambel
#                     rcvd = rcvd.replace(b'\xff\xfb\x01\xff\xfe\x01\xff\xfb\x03', b'')

#                     end = time.perf_counter()    
#                     if verbosity>1:
#                         self.logger.debug(f"response (bytes): {rcvd}")
#                         self.logger.debug(f"time elapsed (s): {end - start:0.4f}")

#                 # inform other callers tha line is free
#                 self._tcpip_line_is_busy = False
#                 return rcvd

#         except Exception as err:
#             self.logger.error(err)
#             # inform other callers that line is free
#             self._tcpip_line_is_busy = False
#             return b''

#     def get_instr_type(self, verbosity: int=0) -> list:
#         """A.3.2 Requests details on the type of analyser being communicated with.

#         Args:
#             verbosity (int, optional): Verbosity for debugging purposes. Defaults to 0.

#         Returns:
#             list: 4 integers, namely, Model, Variant, Sub-Type, and Range.
#         """
#         try:
#             if self._protocol=='acoem':
#                 message = self._acoem_construct_message(1)
#                 self._tcpip_comm_wait_for_line()       
#                 response = self._tcpip_comm(message, verbosity=verbosity)
#                 return self._acoem_bytes2int(response=response, verbosity=verbosity)
#             else:
#                 self.logger.warning(colorama.Fore.YELLOW + "Not implemented." + colorama.Fore.GREEN)
#                 return []
#         except Exception as err:
#             self.logger.error(err)
#             return []

#     def get_version(self, verbosity: int=0) -> list:
#         """A.3.3 Requests the current firmware version running on the analyser.

#         Args:
#             verbosity (int, optional): Verbosity for debugging purposes. Defaults to 0.

#         Returns:
#             list: 2 integers, namely, Build, and Branch.
#         """
#         try:
#             if self._protocol=='acoem':
#                 message = self._acoem_construct_message(2)
#                 self._tcpip_comm_wait_for_line()
#                 response = self._tcpip_comm(message, verbosity=verbosity)        
#                 return self._acoem_bytes2int(response=response, verbosity=verbosity)
#             else:
#                 warnings.warn("Not implemented.")
#                 return []
#         except Exception as err:
#             self.logger.error(err)
#             return []

#     def reset(self, verbosity: int=0) -> None:
#         """
#         A.3.4 Forces the analyser to do a full restart.
#         The payload must contain the letters REALLY exactly, as 6 4-byte words.

#         Args:
#             verbosity (int, optional): Verbosity for debugging purposes. Defaults to 0.

#         Returns:
#             None.
#         """
#         try:
#             if self._protocol=='acoem':
#                 payload = bytes([82, 69, 65, 76, 76, 89])
#                 message = self._acoem_construct_message(command=1, payload=payload)
#             elif self._protocol=='aurora':
#                 message = f"**B\r".encode()
#             else:
#                 raise ValueError('Protocol not recognized.')
#             self._tcpip_comm(message, expect_response=False, verbosity=verbosity)
#             return
#         except Exception as err:
#             self.logger.error(err)
  
#     def get_values(self, parameters: 'list[int]', verbosity: int=0) -> dict:
#         """
#         Requests the value of one or more instrument parameters.
#         If the ACOEM protocol is used, cf. A.3.5 Get Values, with A.4 List of Aurora parameters.
#         If the aurora protocol s used, cf. B.7 Command VI, with Table 47 VI voltage input numbers.

#         Args:
#             parameters (list[int]): list of parameters to query
#             verbosity (int, optional): _description_. Defaults to 0.

#         Returns:
#             dict: requested indexes are the keys, values are the responses from the instrument, decoded.
#         """
#         try:
#             if self._protocol=='acoem':
#                 msg_data = b''
#                 for p in parameters:
#                     msg_data += (p).to_bytes(4, byteorder='big')
#                 msg_len = len(msg_data)
#                 msg = bytes([2, self.serial_id, 4, 3]) + (msg_len).to_bytes(2, byteorder='big') + msg_data
#                 msg += self._acoem_checksum(msg) + bytes([4])
#                 self._tcpip_comm_wait_for_line()                
#                 response = self._tcpip_comm(message=msg, verbosity=verbosity)
#                 data = self._acoem_response2values(parameters=parameters, response=response, verbosity=verbosity)
#                 return data
#             elif self._protocol=='aurora':
#                 items = []
#                 for p in parameters:
#                     if p in range(100):
#                         items.append(self._tcpip_comm(message=f"VI{self.serial_id}{p:02.0f}\r".encode(), verbosity=verbosity).decode())
#                     else:
#                         items.append('')
#                 data = dict(zip(parameters, items))
#                 return data
#             else:
#                 self.logger.warning(colorama.Fore.YELLOW + "Not implemented." + colorama.Fore.GREEN)
#                 return dict()
#         except Exception as err:
#             self.logger.error(err)
#             return dict()

#     def set_value(self, parameter_id: int, value: int, verify: bool=True, verbosity: int=0) -> int:
#         """A.3.6 Sets the value of an instrument parameter.

#         Args:
#             parameter_id (int): Parameter to set.
#             value (int): Value to be set.
#             verify (bool, optional): Should the new value be queried and echoed after setting? Defaults to True.
#             verbosity (int, optional): _description_. Defaults to 0.
#         """
#         try:
#             response = int()
#             if self._protocol=='acoem':
#                 # payload = bytes([0,0,0,value])
#                 payload = (value).to_bytes(4, byteorder='big')
#                 message = self._acoem_construct_message(command=5, parameter_id=parameter_id, payload=payload)
#                 self._tcpip_comm_wait_for_line()                
#                 self._tcpip_comm(message=message, expect_response=False, verbosity=verbosity)
#                 if verify:
#                     time.sleep(0.1)
#                     i = 0
#                     while response!=value:
#                         response = self.get_values(parameters=[parameter_id], verbosity=verbosity)[parameter_id]
#                         # print(f"{time.perf_counter()} {response}")
#                         time.sleep(0.1)
#                         i = i + 1
#                         if i > 100:
#                             break
#                     return response
#                 else:
#                     return response
#             else:
#                 warnings.warn("Not implemented.")
#                 return int()
#         except Exception as err:
#             self.logger.error(err)
#             return int()

#     def get_data_log_config(self, verbosity: int=0, insert_4035_2002: bool=True) -> list:
#         """
#         A.3.7 Return the list of parameter IDs currently being logged. 
#         It is sent with zero message data length.
#         The first 4 byte word of the response data is the number of fields being logged (0..500).
#         Each following 4 byte word is the ID of the parameter.
        
#         NB: Values '4035' (CURRENT_OPERATION) and '2002' (LOGGING_PERIOD) are always logged, 
#         even if not listed here.

#         Args:
#             verbosity (int, optional): Verbosity. Defaults to 0.
#             insert_4035_2002 (bool, optional): If True, ensure that 4035 and 2002 are included in the returned list. Defaults to True.

#         Returns:
#             list: Parameter IDs (integers) currently being logged
#         """
#         try:
#             if self._protocol=='acoem':
#                 message = self._acoem_construct_message(6)
#                 response = self._tcpip_comm(message, verbosity=verbosity)
#                 data_log_config = self._acoem_bytes2int(response=response, verbosity=verbosity)
#                 if insert_4035_2002:
#                     if 4035 not in data_log_config:
#                         data_log_config.insert(1, 4035)
#                     if 2002 not in data_log_config:
#                         data_log_config.insert(2, 2002)
#                 return data_log_config
#             else:
#                 self.logger.warning(colorama.Fore.YELLOW + "Not implemented." + colorama.Fore.GREEN)
#                 return list()
#         except Exception as err:
#             self.logger.error(err)
#             return list()

#     # def set_datalog_param_index(
#     #     self, 
#     #     index_id: int, 
#     #     parameter_id: int, 
#     #     wavelength_config_id: int=0,
#     #     ):
#     #     param_base = 2003
#     #     wavelength_base = 2026
#     #     angle_base = 2069
#     #     if index_id > 0 and index_id < 33:
#     #         param_index_id = param_base + index_id
#     #         wavelength_index_id = wavelength_base + index_id
#     #         angle_index_id = angle_base + index_id

#     # def set_data_log_config(self, verify: bool=False, verbosity: int=0) -> 'list[int]':
#     #     """Pass datalog config to instrument. Verify configuration

#     #     Args:
#     #         verify (bool, optional): Should the datalog configuration be queried and returned after setting it? Defaults to False.
#     #         verbosity (int, optional): _description_. Defaults to 0.

#     #     Returns:
#     #         list[int]: List of parameters logged.
#     #     """
#     #     try:
#     #         data_log_parameter_indexes = range(2004, 2036)
#     #         data_log_parameters = iter(self.data_log_parameters)
#     #         for index in data_log_parameter_indexes:
#     #             self.set_value(index, next(data_log_parameters), verify=False, verbosity=2)
#     #         data_log_wavelength_indexes = range(2037, 2069)            
#     #         data_log_wavelengths = iter(self.data_log_wavelengths)
#     #         for index in data_log_wavelength_indexes:
#     #             self.set_value(index, next(data_log_wavelengths), verify=False)
#     #         data_log_angle_indexes = range(2070, 2102)
#     #         data_log_angles = iter(self.data_log_angles)
#     #         for index in data_log_angle_indexes:
#     #             self.set_value(index, next(data_log_angles), verify=False)
#     #         if verify:
#     #             return self.get_data_log_config(verbosity=verbosity)
#     #         else:
#     #             return list()
#     #     except Exception as err:
#     #         self.logger.error(err)
#     #         return list()
    
#     def set_datalog_interval(self) -> int:
#         try:
#             datalog_interval = self.set_value(parameter_id=2002, value=self.sampling_interval)
#             return datalog_interval

#         except Exception as err:
#             self.logger.error(err)
#             return int()
        
#     def get_logged_data(self, start: datetime, end: datetime, verbosity: int=0, raw: bool=False) -> 'list[dict]':
#         """
#         A.3.8 Requests all logged data over a specific date range.

#         Args:
#             start (datetime.datetime): Beginning of period.
#             end (datetime.datetime): End of period.
#             verbosity (int, optional): _description_. Defaults to 0.

#         Raises:
#             ValueError: _description_
#             Warning: _description_

#         Returns:
#             list[dict]: _description_
#         """
#         try:
#             if self._protocol=='acoem':
#                 if start:
#                     payload = self._acoem_datetime_to_timestamp(start)
#                     if end:
#                         payload += self._acoem_datetime_to_timestamp(end)
#                 else:
#                     raise ValueError("start and/or end date not valid.")
#                 message = self._acoem_construct_message(command=7, payload=payload)
#                 response = self._tcpip_comm(message, verbosity=verbosity)
#                 data =  self._acoem_decode_logged_data(response=response, verbosity=verbosity)
#                 if raw:
#                     return [{'raw': response}, {'data': data}]
#                 return data
#                 # # get next record
#                 # message = self._acoem_construct_message(command=7, payload=bytes([4]))
#                 # while response:=self._tcpip_comm(message, verbosity=verbosity):
#                 #     data.append(self._acoem_decode_logged_data(response=response, verbosity=verbosity))
#             else:
#                 self.logger.warning(colorama.Fore.YELLOW + "Not implemented. For the aurora protocol, try 'get_all_data' or 'accumulate_new_data'." + colorama.Fore.GREEN)
#                 return list(dict())
#         except Exception as err:
#             self.logger.error(err)
#             return []

#     def get_current_operation(self, verbosity: int=0) -> int:
#         """
#         Retrieves the operating state of the instrument. 
#         If the ACOEM protocol is used, this requests Aurora parameter 4035 (cf.A.4 List of Aurora parameters).
#         If the aurora protocol is used, this requests Voltage input number 71 (cf. Appendix B.7 Command: VI).

#         Args:
#             verbosity (int, optional): _description_. Defaults to 0.

#         Returns:
#             int: 0 (Normal monitoring), 1 (Zero calibration/check), 2 (Span calibration/check), 9 (Error)
#         """
#         try:
#             if self._protocol=='acoem':
#                 parameter_id = 4035
#                 # message = self._acoem_construct_message(command=4, parameter_id=parameter_id)
#                 return self.get_values(parameters=[parameter_id], verbosity=verbosity)[parameter_id]
#                 # return self._acoem_bytes2int(response=response, verbosity=verbosity)[0]
#             elif self._protocol=='aurora':
#                 response = self._tcpip_comm(message=f"VI{self.serial_id}71\r".encode(), verbosity=verbosity).decode()
#                 mapping = {'000': 0, '016': 2, '032': 1}
#                 return mapping[response]
#                 # warnings.warn("Not implemented.")
#             else:
#                 raise ValueError("Protocol not recognized.")
#         except Exception as err:
#             self.logger.error(err)
#             return 9

#     def set_current_operation(self, state: int=0, verify: bool=True, verbosity: int=0) -> int:
#         """Sets the instrument operating state by actuating the internal valve.

#         Args:
#             state (int, optional): 0: ambient, 1: zero, 2: span. Defaults to 0.
#             verify (bool, optional): Should the value be queried and echoed after setting? Defaults to True.
#             verbosity (int, optional): _description_. Defaults to 0.
#         """
#         try:
#             if self._protocol=='acoem':
#                 return self.set_value(parameter_id=4035, value=state, verify=verify, verbosity=verbosity)
#             elif self._protocol=='aurora':
#                 # if self.get_instr_type()[0]==158:
#                 warnings.warn("Not implemented for NE-300.")
#                 return int()
#                 # else:
#                 #     map_state = {
#                 #         0: "0000", 
#                 #         1: "0011", 
#                 #         2: "0001", 
#                 #         }
#                 #     message = f"DO{self.serial_number}{map_state[state]}\r".encode()
#                 #     response = self._tcpip_comm(message=message, verbosity=verbosity).decode()
#                 #     inverse_map_state = {0: 0, 1: 2, 11: 1}
#                 #     # while response!=[state]:
#                 #     #     response = self.get_values(parameters=[71], verbosity=verbosity)    # Could be 68 (major state) rather than 71 (DO span/zero measure mode)
#                 #     #     # [k for k, v in inverse_map_state.items()]
#                 #     #     time.sleep(1)
#                 #     #     warnings.warn(f"Incomplete implementation. Please expand and test. Call returned {response}.")
#                 #     warnings.warn(f"Incomplete implementation. Please expand and test. Call returned '{response}'.")
#                 #     return state
#             else:
#                 raise ValueError("Protocol not recognized.")
#         except Exception as err:
#             self.logger.error(err)
#             return 9

#     def get_id(self, verbosity: int=0) -> 'dict[str, str]':
#         """Get instrument type, s/w, firmware versions

#         Parameters:
#             verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

#         Returns:
#             dict: response depends on protocol
#         """
#         try:
#             if self._protocol=="acoem":
#                 instr_type = self.get_instr_type(verbosity=verbosity)
#                 version = self.get_version(verbosity=verbosity)
#                 map_instr_type = {
#                         'Model': {158: 'ACOEM Aurora',
#                                   },
#                         'Variant': {300: 'NE-300',
#                                     },
#                         }
#                 # resp = dict(zip(['Model', 'Variant', 'Sub-Type', 'Range', 'Build', 'Branch'], instr_type + version))
#                 id = f"{map_instr_type['Model'][instr_type[0]]} {map_instr_type['Variant'][instr_type[1]]} Sub-Type: {instr_type[2]} Range: {instr_type[3]} "
#                 id += f"Build: {version[0]} Branch: {version[1]}"
#                 resp = {'id': id}

#             elif self._protocol=="aurora":
#                 resp = {'id': self._tcpip_comm(message=f"ID{self.serial_id}\r".encode(), verbosity=verbosity).decode()}
#             else:
#                 raise ValueError(f"[{self.name}] Communication protocol unknown")

#             self.logger.info(f"[{self.name}] get_id: {resp}")
#             return resp

#         except Exception as err:
#             self.logger.error(err)
#             return dict()

#     def get_datetime(self, verbosity: int=0) -> datetime:
#         """Get date and time of instrument

#         Parameters:
#             verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

#         Returns:
#             datetime.datetime: Date and time of instrument
#         """
#         response = datetime(1,1,1)
#         try:
#             if self._protocol=="acoem":
#                 msg = self._acoem_construct_message(4, 1)
#                 response_bytes = self._tcpip_comm(message=msg, verbosity=verbosity)
#                 response_int = self._acoem_bytes2int(response=response_bytes, verbosity=verbosity)[0]
#                 response = self._acoem_timestamp_to_datetime(response_int)
#             elif self._protocol=='aurora':
#                 fmt = self._tcpip_comm(message=f"VI{self.serial_id}64\r".encode(), verbosity=verbosity).decode()
#                 dte = self._tcpip_comm(message=f"VI{self.serial_id}80\r".encode(), verbosity=verbosity).decode()
#                 tme = self._tcpip_comm(message=f"VI{self.serial_id}81\r".encode(), verbosity=verbosity).decode()
#                 response = self._aurora_timestamp_to_date_time(fmt, dte, tme)
#             else:
#                 raise ValueError("Protocol not recognized.")
#             self.logger.info(f"get_datetime: {response}")
#             return response
#         except Exception as err:
#             self.logger.error(err)
#             return response

#     def get_set_datetime(self, dtm: datetime=datetime.now(timezone.utc), verbosity: int=0) -> 'tuple[dict, dict]':
#         """Get and then set date and time of instrument

#         Parameters:
#             dtm (datetime.datetime, optional): Date and time to be set. Defaults to time.gmtime().
#             verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

#         Returns:
#             None
#         """
#         try:
#             if self._protocol=="acoem":
#                 # get dtm from instrument
#                 dtm_found = self.get_values(parameters=[1])[1].strftime('%Y-%m-%d %H:%M:%S')
                
#                 # set dtm of instrument
#                 payload = self._acoem_datetime_to_timestamp(dtm=dtm)
#                 msg = self._acoem_construct_message(command=5, parameter_id=1, payload=payload)
#                 self._tcpip_comm(message=msg, expect_response=False, verbosity=verbosity)
                
#                 # get new dtm from instrument
#                 dtm_set = self.get_values(parameters=[1])[1].strftime('%Y-%m-%d %H:%M:%S')
#                 return (dtm_found, dtm_set)
#             else:
#                 warnings.warn("Not implemented.")
#                 return (dict(), dict())
#                 # resp = self.tcpip_comm1(f"**{self.serial_id}S{dtm.strftime('%H%M%S%d%m%y')}")
#                 # msg = f"DateTime of instrument {self.name} set to {dtm} ... {resp}"
#                 # print(f"{dtm} {msg}")
#                 # self.logger.info(msg)

#         except Exception as err:
#             self.logger.error(err)
#             return (dict(), dict())

#     def do_span(self, verify: bool=True, verbosity: int=0) -> int:
#         """
#         Override digital IO control and DOSPAN. Wrapper for set_current_operation.

#         Parameters:
#             verbosity (int, optional): ...

#         Returns:
#             int: 2: span
#         """
#         self._tcpip_comm_wait_for_line()
#         return self.set_current_operation(state=2, verify=verify, verbosity=verbosity)

#     def do_zero(self, verify: bool=True, verbosity: int=0) -> int:
#         """
#         Override digital IO control and DOZERO. Wrapper for set_current_operation.

#         Parameters:
#             verbosity (int, optional): ...
        
#         Returns:
#             int: 1: zero
#         """
#         self._tcpip_comm_wait_for_line()
#         return self.set_current_operation(state=1, verify=verify, verbosity=verbosity)
    
#     def do_ambient(self, verify: bool=True, verbosity: int=0) -> int:
#         """
#         Override digital IO control and return to ambient measurement. Wrapper for set_current_operation.

#         Parameters:
#             verbosity (int, optional): ...

#         Returns:
#             int: 0: ambient
#         """
#         self._tcpip_comm_wait_for_line()
#         return self.set_current_operation(state=0, verify=verify, verbosity=verbosity)

#     # def get_status_word(self, verbosity: int=0) -> int:
#     #     """
#     #     Read the System status of the Aurora 3000 microprocessor board. The status word 
#     #     is the status of the nephelometer in hexadecimal converted to decimal.

#     #     Parameters:
    
#     #     Returns:
#     #         int: {<STATUS WORD>}
#     #     """
#     #     return int(self._tcpip_comm(f"VI{self.serial_id}88\r".encode()).decode())

#     # def get_all_data(self, verbosity: int=0) -> str:
#     #     """
#     #     Rewind the pointer of the data logger to the first entry, then retrieve all data (cf. B.4 ***R, B.3 ***D). 
#     #     This only works with the aurora protocol (and doesn't work very well with the NE-300).

#     #     Parameters:
#     #         verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

#     #     Returns:
#     #         str: response
#     #     """
#     #     try:
#     #         if self._protocol=="acoem":
#     #             warnings.warn("Not implemented. Use 'get_logged_data' with specified period instead.")
#     #         elif self._protocol=='aurora':
#     #             self._tcpip_comm_wait_for_line()
#     #             response = self._tcpip_comm(message=f"***R\r".encode(), verbosity=verbosity).decode()
#     #             response = self.accumulate_new_data(verbosity=verbosity)
#     #             # response = self._tcpip_comm(message=f"***D\r".encode(), verbosity=verbosity).decode()
#     #             return response
#     #         else:
#     #             raise ValueError("Protocol not implemented.")
#     #         return str()
#     #     except Exception as err:
#     #         self.logger.error(err)
#     #         return str()

#     def get_current_data(self, add_params: list=[], strict: bool=False, sep: str=' ', verbosity: int=0) -> dict:
#         """
#         Retrieve latest near-real-time reading on one line.
#         With the aurora protocol, this uses the command 99 (cf. B.7 VI: 99), returning parameters [80,81,30,2,31,3,32,17,18,16,19,00,90].
#         These are mapped to the corresponding Acoem parameters (cf. A.4 List of Aurora parametes) [1,1635000,1525000,1450000,1635090,1525090,1450090,5001,5004,5003,5002,4036,4035].
#         Optionally, several more parameters can be retrieved, depending on the protocol.

#         Parameters:
#             add_params (list, optional): read more values and append to dictionary. Defaults to [].
#             strict (bool, optional): If True, the dictionary returned is {99: response}, where response is the <sep>-separated response of the VI<serial_id>99 aurora command. Defaults to False.
#             sep (str, optional): Separator applied if strict=True. Defaults to ' '.
#             verbosity (int, optional): level of printed output, one of 0 (none), 1 (condensed), 2 (full). Defaults to 0.

#         Returns:
#             dict: Dictionary of parameters and values obtained.
#         """
#         parameters = [1,1635000,1525000,1450000,1635090,1525090,1450090,5001,5004,5003,5002,4036,4035]
#         if add_params:
#             parameters += add_params
#         try:
#             if self._protocol=='acoem':
#                 # warnings.warn("Not implemented.")
#                 data = self.get_values(parameters=parameters, verbosity=verbosity)
#                 if strict:
#                     if 1 in parameters:
#                         data[1] = data[1].strftime(format=f"%d/%m/%Y{sep}%H:%M:%S")
#                     response = sep.join([str(data[k]) for k, v in data.items()])
#                     data = {99: response}
#             elif self._protocol=='aurora':
#                 response = self._tcpip_comm(f"VI{self.serial_id}99\r".encode(), verbosity=verbosity).decode()
#                 response = response.replace(", ", ",")
#                 if strict:
#                     response = response.replace(',', sep)
#                     data = {99: response}
#                 else:
#                     response = response.split(',')
#                     response = [response[0]] + [float(v) for v in response[1:]]
#                     data = dict(zip(parameters, response))
#             else:
#                 raise ValueError("Protocol not recognized.")
#             return data
#         except Exception as err:
#             self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)
#             return dict()

#     def _accumulate_new_data(self, sep: str=",", verbosity: int=0) -> None:
#         """
#         For the acoem format: Retrieve all readings from (now - get_data_interval) until now.
#         For the aurora format: Retrieve all readings from current cursor.
        
#         Args:
#             sep (str, optional): Separator to use for output and file, respectively. Defaults to ",".
#             save (bool, optional): Should data be saved to file? Defaults to True.
#             verbosity (int, optional): _description_. Defaults to 0.

#         Raises:
#             Warning: _description_
#             ValueError: _description_

#         Returns:
#             nothing (but updates self._data)
#         """
#         try:
#             if self._protocol=='acoem':
#                 if self.sampling_interval is None:
#                     raise ValueError("'get_data_interval' cannot be None.")
#                 tmp = []

#                 # define period to retrieve and update state variable
#                 start = self._start_datalog
#                 end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
#                 self._start_datalog = end + timedelta(seconds=self.sampling_interval)

#                 # retrieve data
#                 self.logger.info(f"[{self.name}] .accumulate_new_data from {start} to {end}")
#                 self._tcpip_comm_wait_for_line()            
#                 data = self.get_logged_data(start=start, end=end, verbosity=verbosity)

#                 # prepare result
#                 for d in data:
#                     values = [str(d.pop('dtm'))] + [str(value) for key, value in d.items()]
#                     tmp.append(sep.join(values))
#                 data = '\n'.join(tmp) + '\n'

#             elif self._protocol=='aurora':
#                 data = self._tcpip_comm(f"***D\r".encode()).decode()
#                 data = data.replace('\r\n\n', '\r\n').replace(", ", ",").replace(",", sep)
#             else:
#                 raise ValueError("Protocol not recognized.")
            
#             if verbosity>0:
#                 self.logger.info(data)

#             self._data += data

#             return 
        
#         except Exception as err:
#             self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)

#     def _save_data(self) -> str | None:
#         try:
#             self.logger.debug(f"[{self.name}] _save_data")

#             if not self._data or not self._data.strip():
#                 self.logger.info(f"[{self.name}] no data to save (skipping file creation).")
#                 self.data_file = str()
#                 return None

#             now = datetime.now()
#             timestamp = now.strftime(self._file_timestamp_format)
#             yyyy = now.strftime('%Y')
#             mm = now.strftime('%m')
#             dd = now.strftime('%d')
          
#             # create appropriate file name and write mode
#             self.data_file = os.path.join(self.data_path, yyyy, mm, dd, f"{self.name}-{timestamp}.dat")
#             os.makedirs(os.path.dirname(self.data_file), exist_ok=True)
#             if os.path.exists(self.data_file):
#                 header = str()
#             else:
#                 header_ids = ["dtm"] + [str(pid) for pid in self._header]
#                 header = ",".join(header_ids) + "\n"

#             with open(file=self.data_file, mode='a') as fh:
#                 if header:
#                     fh.write(header)
#                 fh.write(self._data)
#                 self.logger.info(f"[{self.name}] file saved: {self.data_file}")

#                 # reset self._data
#                 self._data = str()
#             return self.data_file

#         except Exception as err:
#             self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)

#     def _stage_file(self):
#         """ Stage file, optionally as .zip archive.
#         """
#         try:
#             self.logger.debug(f"[{self.name}]: ._stage_file")

#             if not self.data_file or not os.path.isfile(self.data_file):
#                 self.logger.info(f"[{self.name}] no source file to stage (skipping).")
#                 return

#             archive = os.path.join(self.staging_path, os.path.basename(self.data_file).replace('.dat', '.zip'))
#             with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
#                 zf.write(self.data_file, os.path.basename(self.data_file))
#                 self.logger.info(f"[{self.name}] file staged: {archive}")            
#                 # reset
#                 self.data_file = str()

#         except Exception as err:
#             self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)

#     def _save_and_stage_data(self):
#         try:
#             self.logger.debug(f"[{self.name}] ._save_and_stage_data")
        
#             self._save_data()
#             if self.data_file:
#                 self._stage_file()

#         except Exception as err:
#             self.logger.error(colorama.Fore.RED + f"{err}" + colorama.Fore.GREEN)

#     def print_ssp_bssp(self) -> None:
#         """Retrieve current readings and print."""
#         try:
#             data = self.get_values(parameters=[2635000, 2635090, 2525000, 2525090, 2450000, 2450090])
#             data = f"ssp|bssp (Mm-1) r: {data[2635000]:0.4f}|{data[2635090]:0.4f} g: {data[2525000]:0.4f}|{data[2525090]:0.4f} b: {data[2450000]:0.4f}|{data[2450090]:0.4f}"
#             self.logger.info(colorama.Fore.GREEN + f"[{self.name}] {data}")

#         except Exception as err:
#             self.logger.error(colorama.Fore.RED + f"[{self.name}] print_ssp_bssp: {err}" + colorama.Fore.GREEN)


# if __name__ == "__main__":
#     pass