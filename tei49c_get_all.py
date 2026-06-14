#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Download all stored LREC/SREC records from a Thermo Scientific Model 49C.

Drop-in replacement for the original ``tei49c_get_all.py`` script.

Key differences from the old script:
- never requests more than 10 records per ``lrec/srec xxxx yy`` command;
- discovers the actual number of stored records using ``no of lrec`` and
  ``no of srec`` instead of using fixed capacities;
- writes LREC and SREC to separate files;
- creates the output directory before writing;
- exits with a non-zero status on communication or protocol errors.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
import zipfile
from pathlib import Path
from typing import Callable, Iterable

from mkndaq.inst.thermo import Thermo49C
from mkndaq.utils.utils import load_config, setup_logging

MAX_RECORDS_PER_QUERY = 10
ERROR_MARKERS = (
    "too high",
    "too low",
    "bad cmd",
    "bad command",
    "can't",
    "cannot",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Retrieve all stored LREC/SREC records from a Thermo 49C instrument.",
        usage="python3 tei49c_get_all.py [-c CONFIG] [options]",
    )
    parser.add_argument(
        "-c",
        "--configuration",
        type=str,
        default="dist/mkndaq.yml",
        help="Full path to configuration file. Default: dist/mkndaq.yml",
    )
    parser.add_argument(
        "--name",
        type=str,
        default="tei49c",
        help="Instrument section name in the configuration file. Default: tei49c",
    )
    parser.add_argument(
        "--record-types",
        nargs="+",
        choices=("lrec", "srec"),
        default=("lrec", "srec"),
        help="Record types to download. Default: lrec srec",
    )
    parser.add_argument(
        "--lrec-count",
        type=int,
        default=None,
        help="Maximum number of newest LREC records to download. Default: all records reported by the instrument.",
    )
    parser.add_argument(
        "--srec-count",
        type=int,
        default=None,
        help="Maximum number of newest SREC records to download. Default: all records reported by the instrument.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=MAX_RECORDS_PER_QUERY,
        help="Records requested per command. The instrument limit is 10; larger values are capped. Default: 10",
    )
    parser.add_argument(
        "--format",
        type=str,
        default="",
        help=(
            "Temporarily set the output format before download. "
            "For 49C this is often something like '00 02'. Empty string leaves the current format unchanged."
        ),
    )
    parser.add_argument(
        "--no-zip",
        action="store_true",
        help="Do not create zip archives next to the downloaded .dat files.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Optional sleep in seconds between record chunk requests. Default: 0",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-chunk progress messages. Final file messages are still printed.",
    )
    return parser.parse_args()


def configure(config_file: str) -> dict:
    cfg = load_config(config_file=config_file)
    root = Path(os.path.expanduser(str(cfg["root"])))
    logfile = root / str(cfg["logging"]["file"])
    logfile.parent.mkdir(parents=True, exist_ok=True)
    setup_logging(file=str(logfile))
    return cfg


def first_int(text: str) -> int | None:
    match = re.search(r"(?<![\d.:-])(\d+)(?![\d.:-])", text or "")
    return int(match.group(1)) if match else None


def has_error(text: str) -> bool:
    lower = (text or "").lower()
    return any(marker in lower for marker in ERROR_MARKERS)


def command(send: Callable[[str], str], cmd: str, attempts: int = 3, delay: float = 0.5) -> str:
    last = ""
    for attempt in range(1, attempts + 1):
        last = (send(cmd) or "").strip()
        if last:
            return last
        if attempt < attempts:
            time.sleep(delay)
    raise RuntimeError(f"No response for command {cmd!r}")


def get_format(send: Callable[[str], str], record_type: str) -> str | None:
    try:
        response = command(send, f"{record_type} format")
    except Exception:
        return None

    prefix = f"{record_type} format"
    value = response.strip()
    if value.lower().startswith(prefix):
        value = value[len(prefix):].strip()

    # Keep only the leading numeric format fields.  Some firmware returns
    # extra explanatory text after the format value.
    numeric: list[str] = []
    for token in value.split():
        if re.fullmatch(r"\d+", token):
            numeric.append(token)
        else:
            break
    if numeric:
        return " ".join(numeric)
    return value or None


def set_format(send: Callable[[str], str], record_type: str, value: str) -> None:
    response = command(send, f"set {record_type} format {value}")
    if "ok" not in response.lower():
        raise RuntimeError(f"Could not set {record_type} format to {value!r}: {response!r}")


def discover_count(send: Callable[[str], str], record_type: str) -> int:
    response = command(send, f"no of {record_type}")
    count = first_int(response)
    if count is None:
        raise RuntimeError(f"Could not parse {record_type.upper()} record count from {response!r}")
    return count


def clean_record_lines(response: str, cmd: str) -> list[str]:
    lines: list[str] = []
    for line in response.replace("\r", "\n").split("\n"):
        item = line.strip()
        if not item:
            continue
        if item.lower() == cmd.lower():
            continue
        lines.append(item)
    return lines


def download_records(
    send: Callable[[str], str],
    record_type: str,
    count: int,
    chunk_size: int,
    sleep: float = 0.0,
    quiet: bool = False,
) -> list[str]:
    chunk_size = max(1, min(int(chunk_size), MAX_RECORDS_PER_QUERY))
    records_back = int(count)
    requested_so_far = 0
    lines: list[str] = []

    while records_back > 0:
        n = min(chunk_size, records_back)
        cmd = f"{record_type} {records_back} {n}"
        if not quiet:
            print(
                f"{record_type.upper()}: requesting {n} record(s) starting at record {records_back} "
                f"({requested_so_far}/{count} requested so far)...",
                flush=True,
            )

        response = command(send, cmd)

        if has_error(response):
            raise RuntimeError(
                f"Instrument rejected {cmd!r} with {response!r}. "
                "The second argument must not exceed 10, and the first argument must not exceed the stored record count."
            )

        got = clean_record_lines(response, cmd)
        if not got:
            raise RuntimeError(f"Command {cmd!r} returned no records")

        lines.extend(got)
        records_back -= n
        requested_so_far += n

        if not quiet:
            print(
                f"{record_type.upper()}: received {len(got)} line(s) from this chunk; "
                f"{len(lines)} line(s) accumulated; {requested_so_far}/{count} record(s) requested.",
                flush=True,
            )

        if sleep > 0:
            time.sleep(sleep)

    return lines


def write_text(path: Path, lines: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        for line in lines:
            fh.write(f"{line.rstrip()}\n")


def zip_file(path: Path) -> Path:
    archive = path.with_suffix(".zip")
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(path, path.name)
    return archive


def requested_count(args: argparse.Namespace, record_type: str, available: int) -> int:
    limit = args.lrec_count if record_type == "lrec" else args.srec_count
    if limit is None:
        return available
    return min(max(int(limit), 0), available)


def main() -> int:
    args = parse_args()
    cfg = configure(args.configuration)

    if args.name not in cfg:
        raise KeyError(f"Instrument {args.name!r} not found in {args.configuration!r}")

    instrument = Thermo49C(name=args.name, config=cfg)
    send: Callable[[str], str] = instrument.serial_comm

    written: list[Path] = []
    previous_formats: dict[str, str] = {}

    try:
        for record_type in args.record_types:
            if args.format:
                old = get_format(send, record_type)
                if old:
                    previous_formats[record_type] = old
                set_format(send, record_type, args.format)

            available = discover_count(send, record_type)
            count = requested_count(args, record_type, available)
            print(
                f"{record_type.upper()}: instrument reports {available} stored record(s); "
                f"downloading {count} record(s) in chunks of up to {min(max(int(args.chunk_size), 1), MAX_RECORDS_PER_QUERY)}.",
                flush=True,
            )
            if count == 0:
                print(f"No {record_type.upper()} records available.")
                continue

            records = download_records(
                send=send,
                record_type=record_type,
                count=count,
                chunk_size=args.chunk_size,
                sleep=args.sleep,
                quiet=args.quiet,
            )

            outdir = Path(instrument.data_path)
            timestamp = time.strftime("%Y%m%d%H%M%S")
            outfile = outdir / f"{args.name}-all-{record_type}-{timestamp}.dat"
            write_text(outfile, records)
            written.append(outfile)
            print(f"{outfile} written ({len(records)} lines; {count}/{available} records requested).")

            if not args.no_zip:
                archive = zip_file(outfile)
                print(f"{archive} written.")

    finally:
        for record_type, value in previous_formats.items():
            try:
                set_format(send, record_type, value)
            except Exception as err:
                print(f"WARNING: could not restore {record_type} format to {value!r}: {err}", file=sys.stderr)

    if not written:
        print("No files written.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as err:
        print(f"ERROR: {err}", file=sys.stderr)
        raise SystemExit(1)
