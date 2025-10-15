#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Utility: Download logged data from an Acoem NE-300 (NEPH) for a specified time window,
reading connection & instrument settings from a mkndaq-style YAML config, and writing
the output under the YAML's root/data/<instrument data_path> folder.

Minimal CLI:
  python ne300_fetch.py --start "2025-10-12 00:00" --end "2025-10-12 06:00"

Optional overrides:
  --config PATH_TO_mkndaq.yml    (default: ./mkndaq.yml)
  --name   ne300                 (instrument key in the YAML; default: ne300)
  --sep    ","                   (output delimiter; default comma)

The output filename is:
  ne300_yyyymmddHHMM_yyyymmddHHMM.dat
and it is placed in:
  <root>/<data>/<instrument data_path>/
as defined by mkndaq.yml.
"""
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
import sys

try:
    import yaml  # PyYAML
except Exception as e:
    print("ERROR: This utility requires PyYAML. Please install with 'pip install pyyaml'.", file=sys.stderr)
    raise

# Import the NEPH class from the provided module
try:
    from mkndaq.inst.neph import NEPH
except Exception as e:
    print("ERROR: Could not import NEPH from neph.py. Make sure neph.py is on PYTHONPATH or in the same folder.", file=sys.stderr)
    raise

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch logged NE-300 data for a given time range (using mkndaq.yml).")
    p.add_argument("--config", default="mkndaq.yml", help="Path to mkndaq.yml (default: ./mkndaq.yml)")
    p.add_argument("--name", default="ne300", help='Instrument key in YAML (default: "ne300")')
    p.add_argument("--start", required=True, help='Start time "yyyy-mm-dd HH:MM"')
    p.add_argument("--end", required=True, help='End time "yyyy-mm-dd HH:MM"')
    p.add_argument("--sep", default=",", help="Field separator (default: ,)")
    return p.parse_args()

def load_config(cfg_path: Path) -> dict:
    if not cfg_path.exists():
        print(f"ERROR: config not found: {cfg_path}", file=sys.stderr)
        raise SystemExit(2)
    with cfg_path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)

def _stringify(v: Any) -> str:
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return "" if v is None else str(v)

def _to_csv_with_header(rows: List[Dict[str, Any]], sep: str = ",") -> str:
    """Return text with a header row. 'dtm' is placed first, other columns sorted."""
    if not rows:
        return ""
    cols = {"dtm"}
    for r in rows:
        cols.update(r.keys())
    cols.discard("dtm")
    ordered = ["dtm"] + sorted(cols)
    out_lines = [sep.join(ordered)]
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
        out_dir = data_path
    else:
        out_dir = data_root / data_path
    return out_dir

def main() -> int:
    args = parse_args()

    # Parse requested time range (YYYY-mm-dd HH:MM), leave naive; NEPH will encode as required
    try:
        start = datetime.strptime(args.start, "%Y-%m-%d %H:%M")
        end = datetime.strptime(args.end, "%Y-%m-%d %H:%M")
    except ValueError as ve:
        print(f"ERROR: {ve}. Expected format is 'yyyy-mm-dd HH:MM'.", file=sys.stderr)
        return 2
    if end <= start:
        print("ERROR: end must be after start.", file=sys.stderr)
        return 2

    # Load YAML and assemble config dict expected by NEPH
    cfg_path = Path(args.config)
    cfg = load_config(cfg_path)
    name = args.name
    if name not in cfg:
        print(f"ERROR: instrument '{name}' not found in {cfg_path}", file=sys.stderr)
        return 2

    # Instantiate NEPH with the full mkndaq config
    verbosity = int(cfg.get(name, {}).get("verbosity", 0))
    ne = NEPH(name, cfg, verbosity=verbosity)

    # Fetch data
    data = ne.get_logged_data(start=start, end=end, verbosity=verbosity)
    if not data:
        print("No data returned for the requested period.", file=sys.stderr)
        return 1

    # Convert to delimited text WITH header
    body = _to_csv_with_header(data, sep=args.sep)

    # Compose output path based on YAML root/data/... and instrument data_path
    out_dir = _resolve_output_dir(cfg, name)
    out_dir.mkdir(parents=True, exist_ok=True)
    fn = f'{name}_{start.strftime("%Y%m%d%H%M")}_{end.strftime("%Y%m%d%H%M")}.dat'
    out_path = out_dir / fn

    out_path.write_text(body, encoding="utf-8")
    print(f"Wrote {out_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

