# NB: This has not been tested!!

from __future__ import annotations

import struct
from typing import Any

from pymodbus.client import ModbusSerialClient


DEVICE_ID = 240          # Modbus address of the HMP110
PORT = "COM5"            # change to your COM port

CANDIDATES = [
    # baudrate, parity, stopbits
    # (9600,  "N", 1),
    # (9600,  "N", 2),
    (19200, "N", 1),
    (19200, "N", 2),
    # (38400, "N", 1),
    # (38400, "N", 2),
    (19200, "E", 1),
    (19200, "O", 1),
]


def _read_holding_registers(client: ModbusSerialClient, address: int, count: int):
    """
    Compatibility wrapper around read_holding_registers for different pymodbus versions.
    """
    func = client.read_holding_registers

    # Try modern API (device_id)
    try:
        return func(address=address, count=count, device_id=DEVICE_ID)
    except TypeError:
        # Fall back to older API (slave)
        return func(address=address, count=count, slave=DEVICE_ID)


def _decode_float32_modicon(words: list[int]) -> float:
    """
    Decode a 32-bit float from 2 registers using 'Modicon' word order (LS word first).
    """
    if len(words) != 2:
        raise ValueError(f"Need exactly 2 registers, got {len(words)}")

    low, high = words
    b = high.to_bytes(2, "big") + low.to_bytes(2, "big")
    return struct.unpack(">f", b)[0]


def try_setting(baudrate: int, parity: str, stopbits: int) -> bool:
    print(f"Testing {baudrate} baud, parity={parity}, stopbits={stopbits} ... ", end="")
    client = ModbusSerialClient(
        port=PORT,
        baudrate=baudrate,
        parity=parity,
        stopbits=stopbits,
        bytesize=8,
        timeout=1.0,
    )

    if not client.connect():
        print("cannot open port")
        return False

    try:
        rr = _read_holding_registers(client, address=0, count=4)
        if getattr(rr, "isError", lambda: True)():
            print("Modbus error")
            return False

        regs = getattr(rr, "registers", [])
        if len(regs) != 4:
            print(f"unexpected register count: {len(regs)}")
            return False

        rh = _decode_float32_modicon(regs[0:2])
        t = _decode_float32_modicon(regs[2:4])

        # Simple sanity check: RH in [0, 100], T in [-50, 80] (approx)
        if not (0.0 <= rh <= 100.0):
            print(f"invalid RH={rh:.2f}")
            return False
        if not (-50.0 <= t <= 80.0):
            print(f"invalid T={t:.2f}")
            return False

        print(f"OK  -> RH={rh:.2f} %RH, T={t:.2f} °C")
        return True
    except Exception as exc:
        print(f"exception: {exc}")
        return False
    finally:
        client.close()


def main() -> None:
    success = False
    for baudrate, parity, stopbits in CANDIDATES:
        if try_setting(baudrate, parity, stopbits):
            success = True
            # Keep going in case you want to see multiple valid combos,
            # but normally the correct one will stand out clearly.
    if not success:
        print("No working serial settings found in candidate list.")


if __name__ == "__main__":
    main()
