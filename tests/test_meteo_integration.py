from __future__ import annotations

"""Live integration tests for the METEO/linuxbox fetch path.

These tests intentionally exercise the same path that ``mkndaq.py`` uses::

    mkndaq.py -> METEO.store_and_stage_files() ->
    METEO.fetch_new_bulletins() -> SFTPClient.download_files()

The tests are split into two steps:
  1. verify that the linuxbox can be reached with the configured SSH key
  2. verify that the fetch/archive/stage path transfers the most recent 3 VRXA00 files

This file does **not** rely on environment variables. It expects the repository
layout to provide ``mkndaq.yml`` and either the importable ``mkndaq`` package or
flat copies of ``meteo.py`` and ``sftp.py`` next to the test or under ``/mnt/data``.

Recommended explicit run:

    pytest -q tests/test_meteo_integration.py -m integration -s

If your ``pytest.ini`` excludes integration tests by default (for example via
``addopts = -m \"not integration\"``), running the file without ``-m integration``
will deselect the tests. That is a pytest selection issue, not a test skip.
"""

import copy
import importlib.util
import sys
import types
from datetime import datetime
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Import helpers
# ---------------------------------------------------------------------------

def _ensure_package(name: str) -> None:
    if name in sys.modules:
        return
    module = types.ModuleType(name)
    module.__path__ = []  # type: ignore[attr-defined]
    sys.modules[name] = module



def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module {module_name!r} from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module



def _resolve_candidate_file(filename: str) -> Path:
    here = Path(__file__).resolve()
    candidates = [
        Path.cwd() / filename,
        here.parent / filename,
        here.parents[1] / filename if len(here.parents) > 1 else None,
        Path("/mnt/data") / filename,
    ]
    for candidate in candidates:
        if candidate and candidate.exists():
            return candidate.resolve()
    raise FileNotFoundError(f"Could not find {filename!r} in any expected location")



def _import_meteo_class():
    try:
        from mkndaq.inst.meteo import METEO
        return METEO
    except ModuleNotFoundError as err:
        if not (err.name or "").startswith("mkndaq"):
            raise

    _ensure_package("mkndaq")
    _ensure_package("mkndaq.utils")
    _ensure_package("mkndaq.inst")

    _load_module("mkndaq.utils.sftp", _resolve_candidate_file("sftp.py"))
    meteo_module = _load_module("mkndaq.inst.meteo", _resolve_candidate_file("meteo.py"))
    return meteo_module.METEO


# ---------------------------------------------------------------------------
# Config / fixtures
# ---------------------------------------------------------------------------

def _resolve_config_path() -> Path:
    here = Path(__file__).resolve()
    candidates = [
        Path.cwd() / "./dist/mkndaq.yml",
        here.parents[1] / "./dist/mkndaq.yml" if len(here.parents) > 1 else None,
        Path("/mnt/data") / "mkndaq.yml",
    ]
    for candidate in candidates:
        if candidate and candidate.exists():
            return candidate.resolve()
    raise FileNotFoundError("Could not locate mkndaq.yml in the repository or test upload locations")


@pytest.fixture(scope="module")
def live_config(tmp_path_factory):
    cfg_path = _resolve_config_path()
    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    cfg = copy.deepcopy(cfg)

    tmp_root = tmp_path_factory.mktemp("mkndaq-meteo-it")
    cfg["root"] = str(tmp_root)
    cfg.setdefault("logging", {})["file"] = str(tmp_root / "mkndaq-integration.log")

    return cfg


@pytest.fixture(scope="module")
def meteo_live(live_config):
    METEO = _import_meteo_class()
    return METEO("meteo", live_config)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _list_remote_vrxa00(meteo_live):
    with meteo_live.lan_sftp.open_sftp() as sftp:
        attrs = [
            attr
            for attr in sftp.listdir_attr(str(meteo_live.source))
            if meteo_live.pattern in attr.filename
        ]
    attrs.sort(key=lambda attr: (int(attr.st_mtime or 0), attr.filename), reverse=True)
    return attrs


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_01_can_connect_to_linuxbox_and_list_remote_source(meteo_live):
    """Smoke-test the exact SSH/SFTP connection that METEO uses."""
    assert meteo_live.host
    assert meteo_live.usr
    assert meteo_live.key_path.exists(), f"SSH key does not exist: {meteo_live.key_path}"

    with meteo_live.lan_sftp.open_sftp() as sftp:
        remote_names = sftp.listdir(str(meteo_live.source))

    assert isinstance(remote_names, list)
    assert remote_names, f"Remote source directory is empty: {meteo_live.source}"



def test_02_fetches_the_most_recent_three_vrxa00_files_via_meteo_pipeline(meteo_live):
    """Prime METEO state so only the latest 3 VRXA00 files are considered new, then run the real fetch/archive/stage path."""
    remote_attrs = _list_remote_vrxa00(meteo_live)
    assert len(remote_attrs) >= 3, (
        f"Need at least 3 remote {meteo_live.pattern} files in {meteo_live.source}, "
        f"found {len(remote_attrs)}"
    )

    latest_three = remote_attrs[:3]
    latest_names = {attr.filename for attr in latest_three}

    older_names = {attr.filename for attr in remote_attrs if attr.filename not in latest_names}
    meteo_live._write_state(older_names)

    meteo_live.store_and_stage_files()

    if meteo_live._zip:
        staged_paths = {meteo_live.staging_path / f"{Path(name).stem}.zip" for name in latest_names}
    else:
        staged_paths = {meteo_live.staging_path / name for name in latest_names}

    missing_staged = [str(path) for path in sorted(staged_paths) if not path.exists()]
    assert not missing_staged, f"Expected staged files are missing: {missing_staged}"

    for attr in latest_three:
        mtime = int(attr.st_mtime or 0)
        dt = datetime.fromtimestamp(mtime)
        archived = (
            meteo_live.data_path
            / dt.strftime("%Y")
            / dt.strftime("%m")
            / dt.strftime("%d")
            / attr.filename
        )
        assert archived.exists(), f"Archive copy missing: {archived}"
        assert archived.stat().st_size >= 0

    seen_after = meteo_live._read_state()
    assert latest_names.issubset(seen_after), "Latest three files were not recorded in METEO state"
