"""
Download logged data from the internal logger of a NEPH instrument and save
them as a `.dat` file with the same column layout used by `neph.py`.

The utility reads the normal `mkndaq.yml` configuration, connects to the NEPH
instrument defined there, resolves the active logger header from the instrument,
downloads logged data for a user-specified time window, and writes the result
to a file named

    neph-<start>-<end>.dat

under the normal configured data path:

    <root>/<data>/<instrument.data_path>/

Input dates are interpreted as UTC. Accepted date/time formats are:

- YYYY-MM-DD
- YYYY-MM-DDTHH:MM
- YYYY-MM-DDTHH:MM:SS
- YYYY-MM-DD HH:MM
- YYYY-MM-DD HH:MM:SS

If the end argument is given as a date only, it is interpreted as the end of
that day (23:59:59 UTC).

The download is performed in chunks so progress can be shown while data are
retrieved. By default, each written data row is also printed to stdout.

Examples
--------
Download one full day using the default config file and default instrument
(`ne300`):

    python download_neph_logged_data.py 2026-03-01 2026-03-01

Download a multi-day period:

    python download_neph_logged_data.py 2026-03-01 2026-03-03 -c mkndaq.yml

Download a shorter time window with explicit times:

    python download_neph_logged_data.py 2026-03-01T06:00 2026-03-01T12:00

Use a different instrument key from the YAML config:

    python download_neph_logged_data.py 2026-03-01 2026-03-02 -n ne300

Download in 6-hour chunks:

    python download_neph_logged_data.py 2026-03-01 2026-03-02 --chunk-hours 6

Suppress printing of each written row:

    python download_neph_logged_data.py 2026-03-01 2026-03-02 --quiet-rows

Increase logging verbosity:

    python download_neph_logged_data.py 2026-03-01 2026-03-02 -v
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import logging
from pathlib import Path
import re
from typing import Iterable

import colorama
import yaml

from mkndaq.inst.neph import NEPH


DATETIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
)


def parse_datetime(value: str, *, is_end: bool = False) -> datetime:
    """Parse a user-provided date or datetime and return an aware UTC datetime.

    Accepted formats:
      - YYYY-MM-DD
      - YYYY-MM-DDTHH:MM
      - YYYY-MM-DDTHH:MM:SS
      - same with a space instead of 'T'

    Date-only end values are interpreted as the end of the day (23:59:59 UTC).
    All naive inputs are treated as UTC.
    """
    value = value.strip()
    for fmt in DATETIME_FORMATS:
        try:
            dt = datetime.strptime(value, fmt)
            if fmt == "%Y-%m-%d" and is_end:
                dt = dt + timedelta(days=1) - timedelta(seconds=1)
            return dt.replace(tzinfo=UTC)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f"Could not parse datetime '{value}'. Use YYYY-MM-DD or YYYY-MM-DDTHH:MM[:SS]."
    )


def stamp(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def output_directory(cfg: dict, instrument: str) -> Path:
    root = Path(str(cfg["root"])).expanduser()
    return root / str(cfg["data"]) / str(cfg[instrument]["data_path"])


def build_reader(name: str, cfg: dict, *, verbosity: int = 0) -> NEPH:
    """Create a minimal NEPH instance for read-only logger downloads.

    This intentionally avoids NEPH.__init__ so the utility does not change the
    instrument state (ambient mode, time sync, schedule setup, etc.). It only
    configures the attributes needed by read commands 6 and 7.
    """
    colorama.init(autoreset=True)

    inst = NEPH.__new__(NEPH)
    inst.name = name
    inst.type = cfg[name]["type"]
    inst.serial_number = cfg[name]["serial_number"]
    inst.verbosity = verbosity
    inst.serial_id = int(cfg[name]["serial_id"])
    inst.sockaddr = (
        str(cfg[name]["socket"]["host"]),
        int(cfg[name]["socket"]["port"]),
    )
    inst.socktout = float(cfg[name]["socket"]["timeout"])
    inst._protocol = str(cfg[name]["protocol"])
    inst._tcpip_line_is_busy = False
    inst._header = [4035, 2002]

    log_name = Path(str(cfg["logging"]["file"])).stem
    logger = logging.getLogger(f"{log_name}.{name}.download")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO if verbosity > 0 else logging.WARNING)
    logger.propagate = False
    inst.logger = logger

    return inst


def resolve_header(inst: NEPH) -> list[int]:
    cfg = inst.get_data_log_config()[1:]
    header = [4035, 2002] + [pid for pid in cfg if pid not in (4035, 2002)]
    inst._header = header
    return header


def format_record(record: dict, header_ids: Iterable[int]) -> str | None:
    if "communication_error" in record:
        return None
    if "dtm" not in record:
        return None
    values = [str(record["dtm"])]
    for pid in header_ids:
        values.append(str(record.get(pid, "")))
    return ",".join(values)


def iter_periods(start: datetime, end: datetime, step: timedelta) -> Iterable[tuple[datetime, datetime]]:
    current = start
    while current < end:
        nxt = min(current + step, end)
        yield current, nxt
        current = nxt


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download NEPH logged data from the internal logger and save it as a .dat file."
    )
    parser.add_argument("start", help="Start date/datetime (UTC). Example: 2026-03-01 or 2026-03-01T12:00")
    parser.add_argument("end", help="End date/datetime (UTC). Example: 2026-03-02 or 2026-03-02T12:00")
    parser.add_argument(
        "-c",
        "--config",
        default="mkndaq.yml",
        help="Path to the mkndaq YAML config file. Default: mkndaq.yml",
    )
    parser.add_argument(
        "-n",
        "--instrument",
        default="ne300",
        help="Instrument key in the YAML config. Default: ne300",
    )
    parser.add_argument(
        "--chunk-hours",
        type=float,
        default=1.0,
        help="Download in chunks of this many hours so progress can be shown. Default: 1",
    )
    parser.add_argument(
        "--quiet-rows",
        action="store_true",
        help="Do not print each written row to stdout.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase logging verbosity.",
    )

    args = parser.parse_args()

    start = parse_datetime(args.start, is_end=False)
    end = parse_datetime(args.end, is_end=True)
    if end <= start:
        raise SystemExit("end must be later than start")

    if args.chunk_hours <= 0:
        raise SystemExit("--chunk-hours must be > 0")

    cfg_path = Path(args.config).expanduser()
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8-sig"))
    if args.instrument not in cfg:
        raise SystemExit(f"Instrument '{args.instrument}' not found in {cfg_path}")

    inst = build_reader(args.instrument, cfg, verbosity=args.verbose)
    header_ids = resolve_header(inst)

    out_dir = output_directory(cfg, args.instrument)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"neph-{stamp(start)}-{stamp(end)}.dat"

    chunk = timedelta(hours=args.chunk_hours)
    periods = list(iter_periods(start, end, chunk))

    print(f"Reading {args.instrument} logger from {start.isoformat()} to {end.isoformat()}")
    print(f"Output file: {out_file}")
    print(f"Resolved header ({len(header_ids)} parameters): {header_ids}")

    rows_written = 0
    with out_file.open("w", encoding="utf-8", newline="") as fh:
        fh.write(",".join(["dtm"] + [str(pid) for pid in header_ids]) + "\n")

        for index, (chunk_start, chunk_end) in enumerate(periods, start=1):
            print(f"[{index}/{len(periods)}] requesting {chunk_start.isoformat()} -> {chunk_end.isoformat()}")
            inst._tcpip_comm_wait_for_line()
            data = inst.get_logged_data(start=chunk_start, end=chunk_end, verbosity=args.verbose)

            if not data:
                print("  no records returned")
                continue

            for record in data:
                line = format_record(record, header_ids)
                if line is None:
                    if "communication_error" in record:
                        print(f"  communication error: {record['communication_error']}")
                    continue
                fh.write(line + "\n")
                rows_written += 1
                if not args.quiet_rows:
                    print(line)

    print(f"Done. Wrote {rows_written} rows to {out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
