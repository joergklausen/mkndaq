#!/usr/bin/env python3
"""
Scan serial settings for a Vaisala HMP60/HMP110 over RS-485
using the ASCII text protocol (STOP/RUN/POLL/VDIGI modes).

We try a set of common (baud, parity, stopbits, databits) combinations,
send typical Vaisala commands (?, ??, SEND, R), and look for responses
that resemble HMP output:

  - Lines containing "HMP", "T=", "RH=", or "Td=".
  - Mostly printable ASCII.

Usage (example on Windows):

    python scan_hmp110_serial_ascii.py --port COM5
"""

from __future__ import annotations

import argparse
import string
import time
from typing import Tuple

import serial


# You can override this via --port on the CLI
PORT_DEFAULT = "COM5"

# Candidate (baudrate, parity, stopbits, bytesize) combinations.
# Based on HMP60/110 manual defaults and allowed SERI settings.
CANDIDATES = [
    # baud, parity, stopbits, bytesize
    # (9600,  serial.PARITY_NONE, serial.STOPBITS_ONE, serial.EIGHTBITS),
    # (9600,  serial.PARITY_NONE, serial.STOPBITS_TWO, serial.EIGHTBITS),
    (19200, serial.PARITY_NONE, serial.STOPBITS_ONE, serial.EIGHTBITS),  # factory default
    # (19200, serial.PARITY_NONE, serial.STOPBITS_TWO, serial.EIGHTBITS),
    # (38400, serial.PARITY_NONE, serial.STOPBITS_ONE, serial.EIGHTBITS),
    # (38400, serial.PARITY_NONE, serial.STOPBITS_TWO, serial.EIGHTBITS),
    # Some even/odd parity + 7-bit combos for completeness
    # (19200, serial.PARITY_EVEN, serial.STOPBITS_ONE, serial.SEVENBITS),
    # (19200, serial.PARITY_ODD,  serial.STOPBITS_ONE, serial.SEVENBITS),
]


COMMANDS = [
    # b"? 4\r",      # device info
    # b"?? 4\r",     # POLL mode info
    # b"SEND\r",   # one-off reading
    b"SEND 3\r",   # one-off reading
    b"SEND 4\r",   # one-off reading
    # b"R\r",      # start continuous output
]


def score_response(data: bytes) -> Tuple[float, str]:
    """
    Return a (score, text_preview) for the received bytes.

    Score is based on:
      - how many printable characters we see
      - whether we find typical tokens: 'HMP', 'T=', 'RH=', 'Td='
    """
    if not data:
        return 0.0, ""

    try:
        text = data.decode("ascii", errors="replace")
    except Exception:
        return 0.0, ""

    printable_chars = set(string.printable)
    n_printable = sum(ch in printable_chars for ch in text)
    ratio = n_printable / max(1, len(text))

    bonus = 0.0
    key_tokens = ("HMP", "T=", "RH=", "Td=", "HMP60", "HMP110")
    if any(tok in text for tok in key_tokens):
        bonus += 0.5

    # Base score: printable ratio + length factor + any bonus
    base = ratio + min(len(text) / 100.0, 0.5) + bonus
    return base, text.replace("\r", "").replace("\x00", "")


def try_setting(
    port: str,
    baudrate: int,
    parity: str,
    stopbits: float,
    bytesize: int,
    read_window: float = 1.0,
) -> None:
    label = f"{baudrate} baud, parity={parity}, stopbits={stopbits}, bits={bytesize}"
    print(f"\nTesting {label} ...")

    try:
        ser = serial.Serial(
            port=port,
            baudrate=baudrate,
            parity=parity,
            stopbits=stopbits,
            bytesize=bytesize,
            timeout=0.1,
            write_timeout=0.5,
        )
    except serial.SerialException as exc:
        print(f"  Cannot open port with these settings: {exc}")
        return

    try:
        # Clear any old data
        ser.reset_input_buffer()
        ser.reset_output_buffer()

        start = time.time()
        buf = bytearray()

        # Send a few different commands that HMP60/110 understands in serial mode
        # according to the user guide: ?, ??, SEND, R.
        cmd_index = 0

        while time.time() - start < read_window:
            if cmd_index < len(COMMANDS):
                cmd = COMMANDS[cmd_index]
                ser.write(cmd)
                ser.flush()
                cmd_index += 1

            time.sleep(0.1)
            waiting = ser.in_waiting
            if waiting:
                chunk = ser.read(waiting)
                buf.extend(chunk)

        score, preview = score_response(bytes(buf))

        if score <= 0.0:
            print("  No data or non-printable garbage received.")
        else:
            print(f"  cmd: {cmd}; Received: {len(buf)} bytes, score={score:.2f}, content: {repr(buf.decode())}")
            # Show a short preview (first line or so)
            lines = preview.split("\n")
            show = "\n      ".join(lines[:3])
            print(f"  Preview:\n      {show}")

    finally:
        ser.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scan likely serial settings for a Vaisala HMP110 using ASCII commands."
    )
    parser.add_argument(
        "--port",
        default=PORT_DEFAULT,
        help="Serial port (e.g. COM5 on Windows, /dev/ttyUSB0 on Linux)",
    )
    parser.add_argument(
        "--window",
        type=float,
        default=1.0,
        help="Seconds to listen per candidate configuration (default: 1.0)",
    )
    args = parser.parse_args()

    print(f"Using port {args.port}")
    print("Make sure the MOXA / UPort is configured as RS-485 and wired correctly.\n")

    for baud, parity, stopbits, bits in CANDIDATES:
        try_setting(args.port, baud, parity, stopbits, bits, read_window=args.window)


if __name__ == "__main__":
    main()

# Example output
# Testing 19200 baud, parity=N, stopbits=1, bits=8 ...
#   cmd: b'SEND 4\r'; Received: 44 bytes, score=1.94, content: "T=  21.03 'C RH=  26.43 %RH Td=   1.04 'C \r\n"
#   Preview:
#       T=  21.03 'C RH=  26.43 %RH Td=   1.04 'C
