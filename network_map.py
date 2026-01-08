#!/usr/bin/env python3
"""
Print instrument name, MAC and host address from an mkndaq.yml-style config.

Usage:
  python print_instruments.py /path/to/mkndaq.yml
  python print_instruments.py              # defaults to ./mkndaq.yml
"""

from __future__ import annotations

from pathlib import Path
import argparse

try:
    import yaml  # PyYAML
except ImportError as e:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: PyYAML. Install with: pip install pyyaml"
    ) from e


MAC_KEYS = ("mac", "mac_number", "mac_address", "macAddress")
HOST_KEYS = ("host", "ip", "address", "hostname")
NESTED_HOST_CONTAINERS = ("socket", "tcp", "net", "network")


def load_yaml(path: Path) -> dict:
    # utf-8-sig tolerates a BOM if present
    txt = path.read_text(encoding="utf-8-sig")
    data = yaml.safe_load(txt) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Top-level YAML must be a mapping/dict, got {type(data).__name__}")
    return data


def is_instrument_section(value: object) -> bool:
    # In mkndaq.yml, instruments are usually dicts with a "type" key.
    return isinstance(value, dict) and "type" in value


def pick_first(cfg: dict, keys: tuple[str, ...]) -> str | None:
    for k in keys:
        v = cfg.get(k)
        if v not in (None, "", []):
            return str(v)
    return None


def extract_host(cfg: dict) -> str | None:
    # Prefer nested "socket.host" (or similar) if present
    for container in NESTED_HOST_CONTAINERS:
        nested = cfg.get(container)
        if isinstance(nested, dict):
            v = nested.get("host")
            if v not in (None, "", []):
                return str(v)

    # Fallback to direct keys like host/ip/hostname
    return pick_first(cfg, HOST_KEYS)


def print_table(rows: list[tuple[str, str, str]]) -> None:
    headers = ("instrument", "mac", "host")
    cols = list(zip(*([headers] + rows))) if rows else [headers, (), ()]
    widths = [max(len(str(x)) for x in col) for col in cols]

    def fmt(r: tuple[str, str, str]) -> str:
        return " | ".join(str(v).ljust(w) for v, w in zip(r, widths))

    sep = "-+-".join("-" * w for w in widths)

    print(fmt(headers))
    print(sep)
    for r in rows:
        print(fmt(r))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("config", nargs="?", default="dist/mkndaq.yml", help="Path to YAML config (default: ./dist/mkndaq.yml)")
    args = ap.parse_args()

    path = Path(args.config)
    if not path.exists():
        raise SystemExit(f"Config not found: {path}")

    cfg_all = load_yaml(path)

    rows: list[tuple[str, str, str]] = []
    for inst_name, inst_cfg in cfg_all.items():
        if not is_instrument_section(inst_cfg):
            continue
        mac = pick_first(inst_cfg, MAC_KEYS) or "-"
        host = extract_host(inst_cfg) or "-"
        rows.append((str(inst_name), mac, host))

    rows.sort(key=lambda r: r[0].lower())
    print_table(rows)


if __name__ == "__main__":
    main()
