from __future__ import annotations

"""Integration tests for mkndaq SFTP connectivity and upload path handling.

These tests intentionally use the real mkndaq configuration and the real
``mkndaq.utils.sftp.SFTPClient`` implementation. They are meant to diagnose
connection-layer problems such as Paramiko's::

    SSHException: Error reading SSH protocol banner

Typical calls from the mkndaq repository root::

    pytest -vv -rs -s tests/test_sftp_integration.py
    pytest -vv -rs -s tests/test_sftp_integration.py --log-cli-level=INFO

The configuration file is resolved in this order:

1. ``./mkndaq.yml``
2. ``./dist/mkndaq.yml``

The test creates one small local file below ``root/data/<test.staging_path>``
and uploads it to the configured SFTP target under the configured test path.
The remote test file is removed again after verification.
"""

import logging
import os
import socket
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

import pytest

from mkndaq.utils.sftp import SFTPClient
from mkndaq.utils.utils import load_config

pytestmark = pytest.mark.integration

LOGGER = logging.getLogger(__name__)

CONNECT_TIMEOUT_SECONDS = 20
BANNER_TIMEOUT_SECONDS = 60


@pytest.fixture(scope="module")
def config_file() -> Path:
    """Resolve the mkndaq configuration file for the integration test."""
    candidates = [Path("mkndaq.yml"), Path("dist/mkndaq.yml")]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()

    searched = ", ".join(str(p) for p in candidates)
    pytest.skip(f"Could not find mkndaq configuration file. Searched: {searched}")


@pytest.fixture(scope="module")
def cfg(config_file: Path) -> dict[str, Any]:
    """Load the mkndaq configuration used by the main application."""
    loaded = load_config(config_file=str(config_file))

    if not loaded.get("sftp"):
        pytest.skip("Missing 'sftp' section in mkndaq configuration.")
    if not loaded.get("test"):
        pytest.skip("Missing 'test' section in mkndaq configuration.")

    return loaded


def _sftp_host(cfg: dict[str, Any]) -> str:
    host = str(cfg["sftp"].get("host", "")).strip()
    assert host, "Missing sftp.host in mkndaq configuration."
    return host


def _sftp_port(cfg: dict[str, Any]) -> int:
    return int(cfg["sftp"].get("port", 22))


def _test_name(cfg: dict[str, Any]) -> str:
    """Return the test transfer name used by SFTPClient.

    In the provided mkndaq.yml this resolves to ``test``. The fallback order
    keeps the test useful if only one of the two fields is present.
    """
    value = (
        cfg.get("test", {}).get("remote_path")
        or cfg.get("test", {}).get("staging_path")
        or "test"
    )
    return str(value).strip("/") or "test"


def _open_configured_tcp_socket(cfg: dict[str, Any]) -> socket.socket:
    """Open a TCP socket to the configured SFTP endpoint.

    If ``sftp.proxy.socks5`` is set in mkndaq.yml, PySocks is used. Otherwise
    a normal direct socket connection is used.
    """
    host = _sftp_host(cfg)
    port = _sftp_port(cfg)
    proxy = cfg.get("sftp", {}).get("proxy") or {}
    socks5_host = str(proxy.get("socks5") or "").strip()

    if socks5_host:
        try:
            import socks  # type: ignore[import-not-found]
        except ImportError:
            pytest.skip(
                "mkndaq.yml configures sftp.proxy.socks5, but PySocks is not installed. "
                "Install it with: pip install PySocks"
            )

        socks5_port = int(proxy.get("port", 1080))
        LOGGER.info(
            "Opening SFTP TCP connection through SOCKS5 proxy %s:%s to %s:%s",
            socks5_host,
            socks5_port,
            host,
            port,
        )
        sock = socks.socksocket()
        sock.set_proxy(socks.SOCKS5, socks5_host, socks5_port)
        sock.settimeout(CONNECT_TIMEOUT_SECONDS)
        sock.connect((host, port))
        return sock

    LOGGER.info("Opening direct SFTP TCP connection to %s:%s", host, port)
    return socket.create_connection((host, port), timeout=CONNECT_TIMEOUT_SECONDS)


@pytest.fixture(scope="module")
def local_test_file(cfg: dict[str, Any]) -> Path:
    """Create one timestamped SFTP test file below root/data/test."""
    root = Path(os.path.expanduser(str(cfg["root"]))).resolve()
    staging_path = str(cfg["test"].get("staging_path") or _test_name(cfg))
    test_dir = root / str(cfg["data"]) / staging_path
    test_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    filename = f"mkn-sftp-test-{stamp}.txt"
    local_file = test_dir / filename

    local_file.write_text(
        "\n".join(
            [
                "mkndaq SFTP integration test",
                f"created_utc={datetime.now(timezone.utc).isoformat()}",
                f"host={cfg['sftp']['host']}",
                f"remote={cfg['sftp'].get('remote', '.')}",
                f"test_name={_test_name(cfg)}",
                f"local_file={local_file}",
                "",
            ]
        ),
        encoding="utf-8",
    )

    yield local_file

    with suppress(FileNotFoundError):
        local_file.unlink()


@pytest.fixture(scope="module")
def sftp(cfg: dict[str, Any]) -> SFTPClient:
    """Instantiate SFTPClient using the mkndaq configuration."""
    name = _test_name(cfg)
    return SFTPClient(config=cfg, name=name)


def test_sftp_config_private_key_exists_when_configured(cfg: dict[str, Any]) -> None:
    """Catch a common local configuration problem before opening SSH."""
    key = str(cfg["sftp"].get("key") or "").strip()
    if not key:
        pytest.skip("No sftp.key configured; skipping private-key file check.")

    key_path = Path(os.path.expanduser(key))
    assert key_path.exists(), f"Configured SFTP private key does not exist: {key_path}"
    assert key_path.is_file(), f"Configured SFTP private key is not a file: {key_path}"


def test_sftp_server_sends_ssh_banner_from_mkndaq_config(cfg: dict[str, Any]) -> None:
    """Verify that the configured endpoint responds with an SSH banner.

    This directly targets Paramiko failures such as::

        SSHException: Error reading SSH protocol banner

    It does not authenticate. It only verifies that the configured host/port
    behaves like an SSH/SFTP server and returns an ``SSH-...`` banner within
    ``BANNER_TIMEOUT_SECONDS``.
    """
    host = _sftp_host(cfg)
    port = _sftp_port(cfg)

    with _open_configured_tcp_socket(cfg) as sock:
        sock.settimeout(BANNER_TIMEOUT_SECONDS)
        banner = sock.recv(256)

    assert banner.startswith(b"SSH-"), (
        f"Expected SSH banner from {host}:{port}, got {banner!r}. "
        "Check sftp.host, sftp.port, firewall/proxy/VPN routing, and whether "
        "the endpoint is really an SSH/SFTP service."
    )


def _expected_remote_file(sftp: SFTPClient, local_file: Path) -> PurePosixPath:
    """Build the expected remote path using the real SFTPClient attributes."""
    remote_root = PurePosixPath(str(getattr(sftp, "remote_path", ".")))
    name = str(getattr(sftp, "name", "test")).strip("/") or "test"
    return remote_root / name / local_file.name


def test_sftp_upload_uses_same_client_and_test_path_as_mkndaq(
    sftp: SFTPClient,
    local_test_file: Path,
) -> None:
    """Upload one small file through the real mkndaq SFTPClient and clean up.

    The test uses ``SFTPClient.transfer_files(...)`` rather than Paramiko
    directly, so it verifies the same wrapper used by mkndaq. It also checks
    that the remote file exists, then removes the uploaded test file.
    """
    assert sftp.is_alive() is True

    expected_remote = _expected_remote_file(sftp, local_test_file)
    remote_dir = expected_remote.parent

    LOGGER.info("Preparing remote test directory: %s", remote_dir)
    sftp.setup_remote_path(remote_dir)

    LOGGER.info("Uploading test file: %s", local_test_file)
    sftp.transfer_files(local_path=local_test_file, remove_on_success=False)

    transferred_remote = [PurePosixPath(str(p)) for p in getattr(sftp, "transfered_remote", [])]
    matching_remote_files = [p for p in transferred_remote if p.name == local_test_file.name]

    assert matching_remote_files or sftp.remote_item_exists(expected_remote), (
        f"Uploaded file was not recorded/found remotely. expected={expected_remote}, "
        f"transfered_remote={transferred_remote}"
    )

    remote_file = matching_remote_files[-1] if matching_remote_files else expected_remote
    assert sftp.remote_item_exists(remote_file) is True

    LOGGER.info("Removing remote test file: %s", remote_file)
    sftp.remove_remote_item(remote_file, recursive=False)
    assert sftp.remote_item_exists(remote_file) is False
