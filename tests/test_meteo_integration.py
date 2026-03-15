from __future__ import annotations

"""Live integration tests for the METEO/linuxbox fetch path.

These tests intentionally exercise the same path that `mkndaq.py` uses:

    mkndaq.py -> METEO.store_and_stage_files() ->
    METEO.fetch_new_bulletins() -> SFTPClient.download_files()

The tests are split into two steps:
  1. verify that the linuxbox can be reached with the configured SSH key
  2. verify that the fetch/archive/stage path transfers the most recent 3 VRXA00 files

Run explicitly, for example:

    MKNDAQ_RUN_LIVE_INTEGRATION=1 pytest -q tests/test_meteo_integration.py -s

Optional environment variables:
  * MKNDAQ_CONFIG     path to mkndaq.yml
  * MKNDAQ_UPLOAD_DIR fallback directory containing flat meteo.py/sftp.py uploads

Note:
  * The tests override only `root` and `logging.file` so that all downloaded data goes
    into a temporary test directory, while the real linuxbox host/user/key/source from
    mkndaq.yml remain unchanged.
"""

import copy
import importlib
import importlib.util
import os
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
    upload_dir = Path(os.environ.get("MKNDAQ_UPLOAD_DIR", "/mnt/data")).expanduser()
    candidates = [
        Path.cwd() / filename,
        Path(__file__).resolve().parent / filename,
        Path(__file__).resolve().parents[1] / filename if len(Path(__file__).resolve().parents) > 1 else None,
        upload_dir / filename,
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
        # Fall back to the flat uploaded files if the real package is not importable
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
    env_path = os.environ.get("MKNDAQ_CONFIG")
    candidates = []
    if env_path:
        candidates.append(Path(env_path).expanduser())
    candidates.extend(
        [
            Path.cwd() / "mkndaq.yml",
            Path(__file__).resolve().parents[1] / "mkndaq.yml" if len(Path(__file__).resolve().parents) > 1 else None,
            _resolve_candidate_file("mkndaq.yml"),
        ]
    )
    for candidate in candidates:
        if candidate and candidate.exists():
            return candidate.resolve()
    raise FileNotFoundError("Could not locate mkndaq.yml; set MKNDAQ_CONFIG explicitly")


@pytest.fixture(scope="module")
def live_enabled() -> None:
    if os.environ.get("MKNDAQ_RUN_LIVE_INTEGRATION") != "1":
        pytest.skip("Set MKNDAQ_RUN_LIVE_INTEGRATION=1 to run live linuxbox integration tests")


@pytest.fixture(scope="module")
def live_config(live_enabled, tmp_path_factory):
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

    # Use the same METEO state mechanism to mark every older file as already seen.
    older_names = {attr.filename for attr in remote_attrs if attr.filename not in latest_names}
    meteo_live._write_state(older_names)

    # This is the exact entry point used by mkndaq.py.
    meteo_live.store_and_stage_files()

    # Staging is raw for meteo (staging_zip: False in mkndaq.yml), but keep the check generic.
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
