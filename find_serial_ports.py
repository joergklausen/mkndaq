#!/usr/bin/env python
"""
Find which serial port your instruments are connected to by probing all available ports.

- Thermo TEI49C: sends "o3" using the TEI serial framing (ID byte + "cmd\\r").
- Vaisala HMP110 (ASCII/RS-485): sends "SEND <id>\\r\\n" and looks for a parseable response.

This script intentionally reuses your existing drivers (Thermo49C + HMP110ASCII) so the probing
matches your production protocol details.

Usage (Windows):
  py -3 tools\find_serial_ports.py --config dist\mkndaq.yml

If you don’t pass --config, it will try "mkndaq.yml" and "dist/mkndaq.yml".
"""

from __future__ import annotations

import argparse
import copy
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import yaml  # type: ignore
except Exception as e:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: pyyaml\n"
        "Install with: python -m pip install pyyaml"
    ) from e

try:
    import serial  # type: ignore
    from serial.tools import list_ports  # type: ignore
except Exception as e:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: pyserial\n"
        "Install with: python -m pip install pyserial"
    ) from e


# ---- Import your existing drivers (package or local fallback) ----
def _import_drivers():
    thermo_mod = None
    vaisala_mod = None

    # 1) package layout
    try:
        from mkndaq.inst.thermo import Thermo49C  # type: ignore
        thermo_mod = ("mkndaq.inst.thermo", Thermo49C)
    except Exception:
        pass

    try:
        from mkndaq.inst.vaisala import HMP110ASCII  # type: ignore
        vaisala_mod = ("mkndaq.inst.vaisala", HMP110ASCII)
    except Exception:
        pass

    # 2) local files next to this script / in cwd (thermo.py, vaisala.py)
    if thermo_mod is None:
        try:
            from thermo import Thermo49C  # type: ignore
            thermo_mod = ("thermo", Thermo49C)
        except Exception as e:
            raise SystemExit(
                "Could not import Thermo49C. Expected one of:\n"
                "  - mkndaq.inst.thermo.Thermo49C\n"
                "  - thermo.Thermo49C (thermo.py on PYTHONPATH)\n"
                f"Import error: {e}"
            ) from e

    if vaisala_mod is None:
        try:
            from vaisala import HMP110ASCII  # type: ignore
            vaisala_mod = ("vaisala", HMP110ASCII)
        except Exception as e:
            raise SystemExit(
                "Could not import HMP110ASCII. Expected one of:\n"
                "  - mkndaq.inst.vaisala.HMP110ASCII\n"
                "  - vaisala.HMP110ASCII (vaisala.py on PYTHONPATH)\n"
                f"Import error: {e}"
            ) from e

    return thermo_mod[1], vaisala_mod[1], thermo_mod[0], vaisala_mod[0]


Thermo49C, HMP110ASCII, THERMO_MODNAME, VAISALA_MODNAME = _import_drivers()


# ---- Helpers ----
def load_config(path: Path) -> Dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    cfg = yaml.safe_load(raw) or {}
    if not isinstance(cfg, dict):
        raise ValueError(f"Config root must be a mapping, got: {type(cfg).__name__}")
    return cfg


def find_config_path(cli: Optional[str]) -> Path:
    if cli:
        p = Path(cli).expanduser()
        if not p.exists():
            raise SystemExit(f"Config not found: {p}")
        return p

    candidates = [
        Path("mkndaq.yml"),
        Path("dist/mkndaq.yml"),
        Path("dist/mkndaq-fallback.yml"),
    ]
    for p in candidates:
        if p.expanduser().exists():
            return p.expanduser()
    raise SystemExit(
        "Could not find a config file. Pass one explicitly:\n"
        "  --config path/to/mkndaq.yml"
    )


def _cfg_port_sections(cfg: Dict[str, Any]) -> List[str]:
    ports: List[str] = []
    for k, v in cfg.items():
        if isinstance(k, str) and isinstance(v, dict) and "baudrate" in v and "timeout" in v:
            # Typical Windows: COM1/COM2..., Linux: /dev/ttyUSB0 etc.
            ports.append(k)
    return ports


def list_candidate_ports(cfg: Dict[str, Any]) -> List[str]:
    ports = set(_cfg_port_sections(cfg))
    try:
        for p in list_ports.comports():
            ports.add(str(p.device))
    except Exception:
        pass

    def _sort_key(s: str):
        # Natural-ish for COM numbers: COM10 after COM2
        import re
        m = re.match(r"^(COM)(\d+)$", s.upper())
        if m:
            return (m.group(1), int(m.group(2)))
        return ("ZZZ", s)

    return sorted(ports, key=_sort_key)


def find_instruments(cfg: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """Return (thermo_names, hmp_names) found in the YAML."""
    thermo: List[str] = []
    hmp: List[str] = []

    for name, section in cfg.items():
        if not isinstance(name, str) or not isinstance(section, dict):
            continue
        t = str(section.get("type", "")).lower()
        if ("49c" in t or "tei49c" in name.lower() or "thermo 49c" in t) and "id" in section:
            thermo.append(name)
        if ("hmp110" in t or "vaisala hmp110" in t or name.lower().startswith("hmp110")) and "id" in section:
            hmp.append(name)

    return thermo, hmp


def _ensure_port_section(cfg: Dict[str, Any], port: str, template_port: Optional[str], timeout_override: Optional[float]) -> None:
    """Ensure cfg has cfg[port] for pyserial settings; copy from template_port if needed."""
    if port not in cfg or not isinstance(cfg.get(port), dict):
        if template_port and isinstance(cfg.get(template_port), dict):
            cfg[port] = copy.deepcopy(cfg[template_port])
        else:
            # Conservative defaults (match your mkndaq-fallback.yml patterns)
            cfg[port] = {"baudrate": 9600, "bytesize": 8, "stopbits": 1, "parity": "N", "timeout": 1.0}

    if timeout_override is not None:
        cfg[port]["timeout"] = float(timeout_override)
        # Keep write_timeout short too
        cfg[port]["write_timeout"] = min(float(timeout_override), 2.0)


def _close_serial(ser: Optional[serial.Serial]) -> None:
    try:
        if ser is not None and getattr(ser, "is_open", False):
            ser.close()
    except Exception:
        pass


def probe_thermo49c_on_port(
    cfg: Dict[str, Any],
    instrument_name: str,
    port: str,
    *,
    timeout_override: Optional[float],
) -> Tuple[bool, str]:
    """Return (ok, message)."""
    inst_cfg = cfg.get(instrument_name, {})
    if not isinstance(inst_cfg, dict):
        return (False, "invalid instrument section")

    template_port = inst_cfg.get("port")
    cfg2 = copy.deepcopy(cfg)
    cfg2[instrument_name]["port"] = port

    _ensure_port_section(cfg2, port, template_port if isinstance(template_port, str) else None, timeout_override)

    try:
        inst = Thermo49C(instrument_name, cfg2)  # uses id+128 and serial framing in serial_comm
        ser = getattr(inst, "_serial", None)
        if ser is None:
            return (False, "serial init failed")
        # quick probe; avoid multiple retries while scanning
        resp = inst.serial_comm("o3", retries=1)  # "o3" command as you described
        _close_serial(ser)

        if not resp:
            return (False, "no response")
        # TEI "o3" should be numeric-ish
        try:
            float(resp.strip().split()[0])
            return (True, resp.strip())
        except Exception:
            return (True, resp.strip())  # still something meaningful came back
    except Exception as e:
        return (False, f"{type(e).__name__}: {e}")


def _cleanup_hmp_shared_port(port: str) -> None:
    """HMP110ASCII caches serial instances; ensure we release it between probes."""
    try:
        ser = HMP110ASCII._serial_by_port.get(port)  # type: ignore[attr-defined]
        _close_serial(ser)
        # remove from caches so next probe is clean
        HMP110ASCII._serial_by_port.pop(port, None)  # type: ignore[attr-defined]
        HMP110ASCII._lock_by_port.pop(port, None)    # type: ignore[attr-defined]
        HMP110ASCII._refcount_by_port.pop(port, None)  # type: ignore[attr-defined]
    except Exception:
        pass


def probe_hmp110_on_port(
    cfg: Dict[str, Any],
    instrument_name: str,
    port: str,
    *,
    timeout_override: Optional[float],
) -> Tuple[bool, str]:
    """Return (ok, message)."""
    inst_cfg = cfg.get(instrument_name, {})
    if not isinstance(inst_cfg, dict):
        return (False, "invalid instrument section")

    template_port = inst_cfg.get("port")
    cfg2 = copy.deepcopy(cfg)
    cfg2[instrument_name]["port"] = port

    _ensure_port_section(cfg2, port, template_port if isinstance(template_port, str) else None, timeout_override)

    try:
        inst = HMP110ASCII(instrument_name, cfg2)
        ser = getattr(inst, "_serial", None)
        if ser is None:
            return (False, "serial init failed")

        cmd = getattr(inst, "cmd", None) or f"SEND {cfg2[instrument_name].get('id')}\r\n"
        resp = inst.serial_comm(cmd, retries=1)
        _cleanup_hmp_shared_port(port)

        if not resp:
            return (False, "no response")

        # Validate parseability using driver's parser (if available)
        try:
            parsed = inst._parse_reading(resp)  # type: ignore[attr-defined]
            return (True, f"{resp.strip()}  ->  {parsed}")
        except Exception:
            return (True, resp.strip())
    except Exception as e:
        _cleanup_hmp_shared_port(port)
        return (False, f"{type(e).__name__}: {e}")


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Probe serial ports for TEI49C and HMP110 sensors.")
    ap.add_argument("--config", default=None, help="Path to mkndaq.yml (default: auto-detect).")
    ap.add_argument("--timeout", type=float, default=None, help="Override per-port read timeout (seconds).")
    ap.add_argument("--ports", nargs="*", default=None, help="Optional explicit port list (e.g. COM4 COM5).")
    ap.add_argument("--thermo", nargs="*", default=None, help="Optional thermo instrument names to probe.")
    ap.add_argument("--hmp", nargs="*", default=None, help="Optional HMP instrument names to probe.")
    args = ap.parse_args(argv)

    cfg_path = find_config_path(args.config)
    cfg = load_config(cfg_path)

    ports = args.ports or list_candidate_ports(cfg)
    if not ports:
        print("No serial ports found.")
        return 2

    thermo_names, hmp_names = find_instruments(cfg)
    if args.thermo is not None:
        thermo_names = args.thermo
    if args.hmp is not None:
        hmp_names = args.hmp

    print(f"Config: {cfg_path}")
    print(f"Drivers: Thermo49C from {THERMO_MODNAME}; HMP110ASCII from {VAISALA_MODNAME}")
    print("Ports to probe:", ", ".join(ports))
    print()

    if not thermo_names and not hmp_names:
        print("No TEI49C or HMP110 instrument sections found in config.")
        print("Tip: enable sections like 'tei49c:' and 'hmp110-...:' in your YAML.")
        return 3

    found_any = False

    if thermo_names:
        print("=== Thermo TEI49C probes (command: o3) ===")
        for inst_name in thermo_names:
            print(f"\n[{inst_name}]")
            ok_ports: List[str] = []
            for port in ports:
                ok, msg = probe_thermo49c_on_port(cfg, inst_name, port, timeout_override=args.timeout)
                mark = "✅" if ok else "❌"
                print(f"  {mark} {port}: {msg}")
                if ok:
                    ok_ports.append(port)
                    found_any = True
            if ok_ports:
                print(f"  -> likely port(s) for {inst_name}: {', '.join(ok_ports)}")

        print()

    if hmp_names:
        print("=== Vaisala HMP110 probes (command: SEND <id>) ===")
        for inst_name in hmp_names:
            print(f"\n[{inst_name}]")
            ok_ports = []
            for port in ports:
                ok, msg = probe_hmp110_on_port(cfg, inst_name, port, timeout_override=args.timeout)
                mark = "✅" if ok else "❌"
                print(f"  {mark} {port}: {msg}")
                if ok:
                    ok_ports.append(port)
                    found_any = True
            if ok_ports:
                print(f"  -> likely port(s) for {inst_name}: {', '.join(ok_ports)}")

        print()

    if not found_any:
        print("No instruments responded.")
        print("If this is unexpected:")
        print("  - make sure no other program has the COM port open")
        print("  - try a larger timeout: --timeout 2")
        print("  - verify your cabling/USB-RS232/RS485 adapter and instrument power")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
