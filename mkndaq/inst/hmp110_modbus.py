# NB: Not tested, not reviewed!!

from __future__ import annotations

import logging
import struct
from dataclasses import dataclass
from typing import Any

from pymodbus.client import ModbusSerialClient

logger = logging.getLogger(__name__)


@dataclass
class HMP110Config:
    """
    Configuration for a Vaisala HMP60/HMP110 in Modbus RTU mode.

    Defaults follow the HMP60/HMP110 user guide:
    - Baud rate 19200
    - 8 data bits, no parity, 2 stop bits
    - Default Modbus address 240 (0xF0)
    """

    port: str = "COM9"        # e.g. "COM3" on Windows, "/dev/ttyUSB0" on Linux
    baudrate: int = 19200
    parity: str = "N"         # 'N', 'E', or 'O'
    stopbits: int = 2
    bytesize: int = 8
    timeout: float = 1.0      # seconds
    device_id: int = 240      # Modbus address (1–247)


@dataclass
class HMP110Reading:
    rh: float           # %RH
    temperature: float  # °C


class HMP110Modbus:
    """
    Minimal Modbus RTU client for Vaisala HMP60/HMP110.

    Uses 32-bit float registers (Table 41 in the manual):
      - RH:  registers 1–2 (address 0x0000–0x0001)
      - T:   registers 3–4 (address 0x0002–0x0003)

    The probe uses "Modicon" word order:
    least-significant 16 bits at the first register address, most-significant
    16 bits at address+1.
    """

    # 0-based Modbus addresses for 32-bit float values
    _RH_ADDR = 0      # register number 1
    _T_ADDR = 2       # register number 3

    def __init__(self, cfg: HMP110Config) -> None:
        self.cfg = cfg
        self._client = ModbusSerialClient(
            port=cfg.port,
            baudrate=cfg.baudrate,
            bytesize=cfg.bytesize,
            parity=cfg.parity,
            stopbits=cfg.stopbits,
            timeout=cfg.timeout,
            # framer defaults to RTU in recent pymodbus; no need to pass "method".
        )

    # ------------------------------------------------------------------ #
    # Connection handling
    # ------------------------------------------------------------------ #
    def connect(self) -> bool:
        """Open the serial line to the probe."""
        ok = self._client.connect()
        if ok:
            logger.info("Connected to HMP110 on %s", self.cfg.port)
        else:
            logger.error("Failed to connect to HMP110 on %s", self.cfg.port)
        return ok

    def close(self) -> None:
        """Close the serial connection."""
        if self._client:
            self._client.close()
            logger.info("Disconnected HMP110 on %s", self.cfg.port)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def read_measurement(self) -> HMP110Reading:
        """
        Read RH and T as 32-bit floats in a single Modbus transaction.

        Returns
        -------
        HMP110Reading
        """
        # Read 4 registers starting at address 0: RH (2 regs) + T (2 regs)
        rr = self._read_holding_registers(self._RH_ADDR, count=4)

        if hasattr(rr, "isError") and rr.isError():
            raise RuntimeError(f"Modbus error: {rr}")

        if not hasattr(rr, "registers") or len(rr.registers) < 4:
            raise RuntimeError(f"Unexpected Modbus response: {rr!r}")

        regs = rr.registers
        rh = self._decode_float32_modicon(regs[0:2])
        temp = self._decode_float32_modicon(regs[2:4])

        logger.debug("HMP110 reading: RH=%.2f %%RH, T=%.2f °C", rh, temp)
        return HMP110Reading(rh=rh, temperature=temp)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _read_holding_registers(self, address: int, count: int):
        """
        Compatibility wrapper around client.read_holding_registers.

        Newer pymodbus uses device_id=, older versions use slave=.
        We try device_id first (matches current stubs/Pylance),
        and fall back to slave= if necessary.
        """
        func = self._client.read_holding_registers

        # First try the modern API (device_id)
        kwargs: dict[str, Any] = {
            "address": address,
            "count": count,
            "device_id": self.cfg.device_id,
        }
        try:
            return func(**kwargs)
        except TypeError:
            # Fall back to older pymodbus that expects "slave="
            logger.debug("read_holding_registers: falling back to slave=")
            kwargs.pop("device_id", None)
            kwargs["slave"] = self.cfg.device_id
            return func(**kwargs)

    @staticmethod
    def _decode_float32_modicon(words: list[int]) -> float:
        """
        Decode a 32-bit IEEE-754 float stored in 2 Modbus registers
        using "Modicon" (little-endian word) order.

        words[0] = least-significant 16 bits
        words[1] = most-significant 16 bits
        """
        if len(words) != 2:
            raise ValueError(f"Need exactly 2 registers, got {len(words)}")

        low, high = words  # LS word first
        # Each register is transmitted big-endian within the word.
        # Construct bytes as MS-word first for big-endian float decode.
        b = high.to_bytes(2, byteorder="big", signed=False) + \
            low.to_bytes(2, byteorder="big", signed=False)
        return struct.unpack(">f", b)[0]


def main() -> None:
    """
    Very small CLI for ad-hoc testing:

        python -m mkndaq.inst.hmp110_modbus --port COM5 --device-id 240
    """
    import argparse

    parser = argparse.ArgumentParser(description="Read HMP110 via Modbus RTU")
    parser.add_argument("--port", required=True, help="Serial port (e.g. COM5 or /dev/ttyUSB0)")
    parser.add_argument(
        "--device-id",
        type=int,
        default=240,
        help="Modbus device address (default: 240)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = HMP110Config(port=args.port, device_id=args.device_id)
    sensor = HMP110Modbus(cfg)

    if not sensor.connect():
        raise SystemExit(1)

    try:
        reading = sensor.read_measurement()
        print(f"RH = {reading.rh:.2f} %RH, T = {reading.temperature:.2f} °C")
    finally:
        sensor.close()


if __name__ == "__main__":
    main()
