#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utility: Download logged data from an Acoem NE-300 (NEPH) for a specified time window,
reading connection & instrument settings from a mkndaq-style YAML config, and writing
the output under the YAML's root/data/<instrument data_path> folder.

This version is more defensive than a single-shot fetch:
- prints progress while running
- fetches adaptively in smaller windows when the backend appears to truncate results
- de-duplicates rows by timestamp before writing

Examples:
  python ne300_fetch.py --start "2026-04-02 14:00" --end "2026-04-07 06:00"
  python ne300_fetch.py --start "2026-04-02 14:00" --end "2026-04-07 06:00" --chunk-hours 24
  python ne300_fetch.py --start "2026-04-02 14:00" --end "2026-04-07 06:00" --single-shot
"""
from __future__ import annotations

import argparse
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List
import sys

try:
    import yaml  # PyYAML
except Exception:
    print("ERROR: This utility requires PyYAML. Please install with 'pip install pyyaml'.", file=sys.stderr)
    raise

try:
    from mkndaq.inst.neph import NEPH
except Exception:
    print(
        "ERROR: Could not import NEPH from neph.py. Make sure mkndaq.inst.neph is importable.",
        file=sys.stderr,
    )
    raise


DEFAULT_SUSPECT_CAP = 5
DEFAULT_MIN_WINDOW_MINUTES = 15
DEFAULT_CHUNK_HOURS = 24


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch logged NE-300 data for a given time range (using mkndaq.yml).")
    p.add_argument("--config", default="mkndaq.yml", help="Path to mkndaq.yml (default: ./mkndaq.yml)")
    p.add_argument("--name", default="ne300", help='Instrument key in YAML (default: "ne300")')
    p.add_argument("--start", required=True, help='Start time "yyyy-mm-dd HH:MM"')
    p.add_argument("--end", required=True, help='End time "yyyy-mm-dd HH:MM"')
    p.add_argument("--sep", default=",", help="Field separator (default: ,)")
    p.add_argument(
        "--chunk-hours",
        type=float,
        default=DEFAULT_CHUNK_HOURS,
        help=(
            "Initial fetch window size in hours for multi-shot mode. "
            f"Default: {DEFAULT_CHUNK_HOURS}"
        ),
    )
    p.add_argument(
        "--min-window-minutes",
        type=int,
        default=DEFAULT_MIN_WINDOW_MINUTES,
        help=(
            "Smallest window size used when auto-splitting on apparent truncation. "
            f"Default: {DEFAULT_MIN_WINDOW_MINUTES}"
        ),
    )
    p.add_argument(
        "--suspect-cap",
        type=int,
        default=DEFAULT_SUSPECT_CAP,
        help=(
            "If a fetch returns exactly this many rows, the script assumes the backend may have truncated the response "
            "and retries with smaller windows. Set to 0 to disable. "
            f"Default: {DEFAULT_SUSPECT_CAP}"
        ),
    )
    p.add_argument(
        "--single-shot",
        action="store_true",
        help="Do one single NEPH.get_logged_data(start, end) call without chunking or auto-splitting.",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Print more progress information during execution.",
    )
    return p.parse_args()


def load_config(cfg_path: Path) -> dict:
    if not cfg_path.exists():
        print(f"ERROR: config not found: {cfg_path}", file=sys.stderr)
        raise SystemExit(2)
    with cfg_path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _fmt_dt(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _stringify(v: Any) -> str:
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return "" if v is None else str(v)


def _sort_key(value: Any) -> tuple[int, str]:
    if isinstance(value, int):
        return (0, f"{value:010d}")
    return (1, str(value))


def _to_csv_with_header(rows: List[Dict[str, Any]], sep: str = ",") -> str:
    """Return text with a header row. 'dtm' is placed first, remaining columns are sorted stably."""
    if not rows:
        return ""

    cols = set()
    for r in rows:
        cols.update(r.keys())

    ordered: list[Any] = []
    if "dtm" in cols:
        ordered.append("dtm")
        cols.discard("dtm")

    ordered.extend(sorted(cols, key=_sort_key))

    out_lines = [sep.join(str(k) for k in ordered)]
    for r in rows:
        out_lines.append(sep.join(_stringify(r.get(c, "")) for c in ordered))
    return "\n".join(out_lines)


def _resolve_output_dir(cfg: dict, name: str) -> Path:
    """Resolve output directory as <root>/<data>/<instrument data_path> with fallbacks.
    If the instrument's data_path is absolute, it is used as-is.
    """
    root = Path(cfg.get("root", ".")).expanduser()
    data_root = root / cfg.get("data", "")
    instr_cfg = cfg.get(name, {}) if isinstance(cfg.get(name), dict) else {}
    data_path = Path(instr_cfg.get("data_path", f"{name}/data")).expanduser()
    if data_path.is_absolute():
        return data_path
    return data_root / data_path


def _row_time(row: Dict[str, Any]) -> Any:
    return row.get("dtm")


def _sorted_rows(rows: Iterable[Dict[str, Any]]) -> list[Dict[str, Any]]:
    return sorted(rows, key=lambda r: (_fmt_dt(_row_time(r)), repr(sorted(r.items(), key=lambda item: str(item[0])))))


def _dedupe_rows(rows: Iterable[Dict[str, Any]]) -> list[Dict[str, Any]]:
    """Dedupe primarily by dtm, retaining first-seen row for that timestamp.

    If dtm is absent, fall back to the full row signature.
    """
    seen: set[Any] = set()
    out: list[Dict[str, Any]] = []
    for row in _sorted_rows(rows):
        if "dtm" in row:
            key = ("dtm", _fmt_dt(row.get("dtm")))
        else:
            key = tuple(sorted((str(k), _stringify(v)) for k, v in row.items()))
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def _describe_rows(rows: list[Dict[str, Any]]) -> str:
    if not rows:
        return "0 rows"
    first = rows[0].get("dtm", "?")
    last = rows[-1].get("dtm", "?")
    return f"{len(rows)} rows, first={_fmt_dt(first)}, last={_fmt_dt(last)}"


def _fetch_once(ne: NEPH, window_start: datetime, window_end: datetime, verbosity: int) -> list[Dict[str, Any]]:
    rows = ne.get_logged_data(start=window_start, end=window_end, verbosity=verbosity)
    if rows is None:
        return []
    if not isinstance(rows, list):
        raise TypeError(f"Expected list from get_logged_data(), got {type(rows).__name__}")
    return rows


def _fetch_chunked(
    ne: NEPH,
    start: datetime,
    end: datetime,
    *,
    chunk_hours: float,
    min_window_minutes: int,
    suspect_cap: int,
    verbosity: int,
    verbose: bool,
) -> list[Dict[str, Any]]:
    min_window = timedelta(minutes=min_window_minutes)
    base_chunk = timedelta(hours=chunk_hours)
    if base_chunk <= timedelta(0):
        raise ValueError("chunk_hours must be > 0")

    queue: deque[tuple[datetime, datetime, int]] = deque()
    cursor = start
    depth0 = 0
    while cursor < end:
        next_cursor = min(cursor + base_chunk, end)
        queue.append((cursor, next_cursor, depth0))
        cursor = next_cursor

    all_rows: list[Dict[str, Any]] = []
    request_no = 0
    accepted_no = 0

    while queue:
        window_start, window_end, depth = queue.popleft()
        request_no += 1
        span = window_end - window_start
        indent = "  " * depth
        print(
            f"{indent}Request {request_no}: {window_start:%Y-%m-%d %H:%M} -> {window_end:%Y-%m-%d %H:%M} "
            f"(span {span})"
        )

        try:
            rows = _fetch_once(ne, window_start, window_end, verbosity=verbosity)
        except Exception as exc:
            print(f"{indent}  ERROR during fetch: {exc!r}", file=sys.stderr)
            raise

        rows = _dedupe_rows(rows)
        print(f"{indent}  Returned {_describe_rows(rows)}")

        should_split = (
            suspect_cap > 0
            and len(rows) == suspect_cap
            and span > min_window
        )

        if should_split:
            mid = window_start + (span / 2)
            mid = datetime.fromtimestamp(mid.timestamp())
            if mid <= window_start or mid >= window_end:
                print(f"{indent}  WARNING: cannot split window further safely; accepting rows.")
            else:
                print(
                    f"{indent}  Returned exactly {suspect_cap} rows; possible truncation. "
                    f"Retrying as two smaller windows."
                )
                queue.appendleft((mid, window_end, depth + 1))
                queue.appendleft((window_start, mid, depth + 1))
                continue

        accepted_no += 1
        all_rows.extend(rows)
        if verbose:
            print(f"{indent}  Accepted window #{accepted_no}; cumulative raw rows={len(all_rows)}")

    final_rows = _dedupe_rows(all_rows)
    print(f"Finished fetching. Raw rows={len(all_rows)}, unique rows={len(final_rows)}")
    print(f"Coverage after merge: {_describe_rows(final_rows)}")
    return final_rows


def main() -> int:
    args = parse_args()

    try:
        start = datetime.strptime(args.start, "%Y-%m-%d %H:%M")
        end = datetime.strptime(args.end, "%Y-%m-%d %H:%M")
    except ValueError as ve:
        print(f"ERROR: {ve}. Expected format is 'yyyy-mm-dd HH:MM'.", file=sys.stderr)
        return 2

    if end <= start:
        print("ERROR: end must be after start.", file=sys.stderr)
        return 2

    cfg_path = Path(args.config)
    cfg = load_config(cfg_path)
    name = args.name
    if name not in cfg:
        print(f"ERROR: instrument '{name}' not found in {cfg_path}", file=sys.stderr)
        return 2

    instr_cfg = cfg.get(name, {}) if isinstance(cfg.get(name), dict) else {}
    verbosity = int(instr_cfg.get("verbosity", 0))
    effective_verbosity = max(verbosity, 1 if args.verbose else 0)

    print(f"Config     : {cfg_path.resolve()}")
    print(f"Instrument : {name}")
    print(f"Requested  : {start:%Y-%m-%d %H:%M} -> {end:%Y-%m-%d %H:%M} (span {end - start})")
    print(f"Mode       : {'single-shot' if args.single_shot else 'chunked'}")
    if not args.single_shot:
        print(
            f"Chunking   : initial={args.chunk_hours} h, min={args.min_window_minutes} min, "
            f"suspect_cap={args.suspect_cap}"
        )

    ne = NEPH(name, cfg, verbosity=effective_verbosity)

    if args.single_shot:
        print("Fetching data in a single call ...")
        data = _dedupe_rows(_fetch_once(ne, start, end, verbosity=effective_verbosity))
        print(f"Single-shot result: {_describe_rows(data)}")
    else:
        data = _fetch_chunked(
            ne,
            start,
            end,
            chunk_hours=args.chunk_hours,
            min_window_minutes=args.min_window_minutes,
            suspect_cap=args.suspect_cap,
            verbosity=effective_verbosity,
            verbose=args.verbose,
        )

    if not data:
        print("No data returned for the requested period.", file=sys.stderr)
        return 1

    body = _to_csv_with_header(data, sep=args.sep)

    out_dir = _resolve_output_dir(cfg, name)
    out_dir.mkdir(parents=True, exist_ok=True)
    fn = f'{name}_{start.strftime("%Y%m%d%H%M")}_{end.strftime("%Y%m%d%H%M")}.dat'
    out_path = out_dir / fn
    out_path.write_text(body, encoding="utf-8")

    print(f"Wrote      : {out_path}")
    print(f"Rows       : {len(data)}")
    if data:
        print(f"First/last : {_fmt_dt(data[0].get('dtm', '?'))} -> {_fmt_dt(data[-1].get('dtm', '?'))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
