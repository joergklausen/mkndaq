from __future__ import annotations

import json
import socket
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest


def _imports():
    # tolerate both layouts
    try:
        from mkndaq.inst.meteo import METEO  # type: ignore
    except Exception:
        from mkndaq.mkndaq.inst.meteo import METEO  # type: ignore
    return METEO


def _load_yaml(path: Path) -> dict[str, Any]:
    import yaml  # type: ignore
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _find_mkndaq_yml() -> Path:
    # repo-root relative
    repo_root = Path(__file__).resolve().parents[1]
    for p in (
        repo_root / "mkndaq.yml",
        repo_root / "mkndaq.yaml",
        repo_root / "dist" / "mkndaq.yml",
        repo_root / "configs" / "mkndaq.yml",
    ):
        if p.exists():
            return p
    raise FileNotFoundError(
        "Could not find mkndaq.yml in repo. Place it in repo root or dist/ or configs/."
    )


def _tcp_probe(host: str, port: int, timeout: float = 2.0) -> tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, "ok"
    except Exception as e:
        return False, repr(e)


def _local_ip_for(host: str) -> str:
    """
    Determine which local IP would be used to reach `host` (no packets sent).
    Helps diagnose "not on same LAN" situations.
    """
    with suppress(Exception):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect((host, 1))
            return s.getsockname()[0]
        finally:
            s.close()
    return "unknown"


def _require_str(d: dict[str, Any], key: str, ctx: str) -> str:
    v = d.get(key)
    if not isinstance(v, str) or not v.strip():
        raise AssertionError(f"Missing/invalid {ctx}.{key}")
    return v.strip()


def _require_int(d: dict[str, Any], key: str, default: int, ctx: str) -> int:
    v = d.get(key, default)
    try:
        return int(v)
    except Exception as e:
        raise AssertionError(f"Invalid {ctx}.{key}={v!r}: {e}") from e


def test_meteo_fetches_from_linuxbox_using_mkndaq_yml(tmp_path: Path):
    """
    Integration test (no env vars):
    - reads mkndaq.yml
    - overrides cfg['root'] to tmp_path
    - downloads ONE latest VRXA00* from linuxbox into staging
    - archives it under data/YYYY/MM/DD
    - confirms state prevents re-download
    """
    METEO = _imports()

    cfg_path = _find_mkndaq_yml()
    cfg = _load_yaml(cfg_path)

    # sandbox root so we never write into your real mkndaq root
    cfg["root"] = str((tmp_path / "mkndaq_root").resolve())
    cfg.setdefault("data", "data")
    cfg.setdefault("staging", "staging")
    cfg.setdefault("logging", {}).setdefault("file", "mkndaq.log")

    if "meteo" not in cfg:
        pytest.fail(f"'meteo' section missing in {cfg_path}")

    meteo_cfg: dict[str, Any] = cfg["meteo"]
    socket_cfg: dict[str, Any] = meteo_cfg.get("socket", {}) or {}

    host = _require_str(socket_cfg, "host", "meteo.socket")
    port = _require_int(socket_cfg, "port", 22, "meteo.socket")

    # ensure key exists on THIS machine (path comes from mkndaq.yml)
    key_path = Path(str(meteo_cfg.get("key", ""))).expanduser()
    if not key_path.exists():
        pytest.skip(f"SSH key from mkndaq.yml not found on this machine: {key_path}")

    # fast reachability check; if not on that LAN, this is where you'll skip
    ok, err = _tcp_probe(host, port, timeout=2.0)
    if not ok:
        local_ip = _local_ip_for(host)
        pytest.skip(
            f"Linuxbox not reachable on {host}:{port}. "
            f"Local route would use {local_ip}. Probe error: {err}"
        )

    # Instantiate METEO (uses mkndaq.yml for linuxbox access)
    m = METEO("meteo", cfg)

    remote_dir = str(m.source)
    prefix = str(m.pattern)  # typically 'VRXA00'

    # List remote files and pick latest one
    with m.lan_sftp.open_sftp() as sftp:
        attrs = sftp.listdir_attr(remote_dir)

    candidates = [a for a in attrs if isinstance(a.filename, str) and prefix in a.filename]
    if not candidates:
        pytest.fail(f"No remote files matching '{prefix}' in {remote_dir} on {host}")

    latest = max(candidates, key=lambda a: ((a.st_mtime or 0), a.filename))
    fname = latest.filename
    mtime_i = int(latest.st_mtime or 0)

    # Force download of exactly that single file
    m.pattern = fname

    staged = m.fetch_new_bulletins()
    assert len(staged) == 1, f"Expected 1 staged file, got: {staged}"
    staged_path = staged[0]
    assert staged_path.exists()

    # Verify staging location
    staging_dir = (Path(cfg["root"]) / cfg["staging"] / meteo_cfg.get("staging_path", "meteo")).resolve()
    assert staging_dir in staged_path.parents, f"Expected staging under {staging_dir}, got {staged_path}"

    # Verify archive location
    if mtime_i == 0:
        mtime_i = int(staged_path.stat().st_mtime)
    dt = datetime.fromtimestamp(mtime_i)
    archive_path = (
        Path(cfg["root"])
        / cfg["data"]
        / meteo_cfg.get("data_path", "meteo")
        / dt.strftime("%Y")
        / dt.strftime("%m")
        / dt.strftime("%d")
        / fname
    )
    assert archive_path.exists(), f"Archive copy missing: {archive_path}"

    # Verify state file recorded it
    state_file = Path(cfg["root"]) / cfg["data"] / meteo_cfg.get("data_path", "meteo") / ".meteo_fetch_state.json"
    assert state_file.exists()
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert fname in set(state.get("seen", []))

    # Second run should skip due to state
    staged2 = m.fetch_new_bulletins()
    assert staged2 == []
