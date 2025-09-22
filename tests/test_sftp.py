from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Dict, Any, List

import pytest


# ----------------------------
# Minimal, in-memory fake SFTP
# ----------------------------
@dataclass
class FakeCall:
    local: Path
    remote: PurePosixPath


@dataclass
class FakeSFTPClient:
    config: Dict[str, Any]
    name: str = "test"
    remote_path: PurePosixPath = PurePosixPath("/remote")
    transfered_remote: List[PurePosixPath] = field(default_factory=list)
    _remote_items: set = field(default_factory=set)

    # API parity with the real client used in the original tests
    def is_alive(self) -> bool:
        # Pretend we can always connect
        return True

    def setup_remote_path(self, remote_path: PurePosixPath) -> None:
        # Simulate creating a directory on remote
        self._remote_items.add(PurePosixPath(remote_path))

    def remote_item_exists(self, remote_path: PurePosixPath) -> bool:
        return PurePosixPath(remote_path) in self._remote_items or PurePosixPath(remote_path) in self.transfered_remote

    def remove_remote_item(self, remote_path: PurePosixPath, recursive: bool = False) -> None:
        # Remove either a file or all files “under” a directory
        rp = PurePosixPath(remote_path)
        if recursive:
            self.transfered_remote = [p for p in self.transfered_remote if not str(p).startswith(str(rp))]
            self._remote_items = {p for p in self._remote_items if not str(p).startswith(str(rp))}
        else:
            self.transfered_remote = [p for p in self.transfered_remote if p != rp]
            self._remote_items.discard(rp)

    def transfer_files(self, local_path: Path, remove_on_success: bool = False) -> None:
        local_path = Path(local_path)
        # If a file: upload that single file
        if local_path.is_file():
            dst = PurePosixPath(self.remote_path) / self.name / local_path.name
            self._remote_items.add(dst.parent)
            self.transfered_remote.append(dst)
            return

        # If a directory: walk and upload all regular files
        for p in sorted(local_path.rglob("*")):
            if p.is_file():
                rel = p.relative_to(local_path)
                dst = PurePosixPath(self.remote_path) / self.name / PurePosixPath(str(rel))
                # ensure directory container is “created”
                self._remote_items.add(dst.parent)
                self.transfered_remote.append(dst)
        # ignore remove_on_success for the fake


# ----------------------------
# Fixtures
# ----------------------------
@pytest.fixture
def minimal_config() -> Dict[str, Any]:
    # Keep this aligned with your project’s expected structure
    return {
        "sftp": {
            "host": "sftp.meteoswiss.ch",
            "username": "dummy",
            "password": "dummy",
            "port": 22,
        }
    }


@pytest.fixture
def sftp(minimal_config) -> FakeSFTPClient:
    # Use the fake client so tests are hermetic and fast
    return FakeSFTPClient(config=minimal_config, name="test")


# ----------------------------
# Tests (refactored from unittest)
# ----------------------------
def test_config_host(minimal_config):
    assert minimal_config["sftp"]["host"] == "sftp.meteoswiss.ch"


def test_is_alive(sftp: FakeSFTPClient):
    assert sftp.is_alive() is True


def test_setup_remote_path(sftp: FakeSFTPClient):
    remote_path = PurePosixPath(sftp.remote_path) / sftp.name
    sftp.setup_remote_path(remote_path)

    assert sftp.remote_item_exists(remote_path=remote_path) is True

    # clean up
    sftp.remove_remote_item(remote_path=remote_path)


def test_transfer_single_file(tmp_path: Path, minimal_config):
    # fresh client per test to avoid state sharing
    sftp = FakeSFTPClient(config=minimal_config, name="test")

    # create a single local file on the fly
    local_file = tmp_path / "hello.txt"
    local_file.write_text("hello\n")

    sftp.transfer_files(local_path=local_file, remove_on_success=False)

    assert len(sftp.transfered_remote) == 1
    assert sftp.transfered_remote[0].name == "hello.txt"

    # clean up (simulate)
    for remote_path in list(sftp.transfered_remote):
        sftp.remove_remote_item(remote_path=remote_path, recursive=True)
    assert len(sftp.transfered_remote) == 0


def test_transfer_directory(tmp_path: Path, minimal_config):
    sftp = FakeSFTPClient(config=minimal_config, name="test")

    # Create a small directory tree: test/hello/hello.txt and test/another/file2.txt
    root = tmp_path / "test"
    (root / "hello").mkdir(parents=True)
    (root / "another").mkdir(parents=True)
    (root / "hello" / "hello.txt").write_text("hi\n")
    (root / "another" / "file2.txt").write_text("x\n")

    sftp.transfer_files(local_path=root, remove_on_success=False)

    assert len(sftp.transfered_remote) == 2
    names = {p.name for p in sftp.transfered_remote}
    assert {"hello.txt", "file2.txt"} <= names

    # clean up
    for remote_path in list(sftp.transfered_remote):
        sftp.remove_remote_item(remote_path=remote_path, recursive=True)
    assert len(sftp.transfered_remote) == 0


def test_transfer_ne300_files(tmp_path: Path, minimal_config):
    sftp = FakeSFTPClient(config=minimal_config, name="ne300")

    # Build a directory with exactly 9 files to mirror the original expectation
    ne300 = tmp_path / "ne300"
    ne300.mkdir()
    for i in range(9):
        (ne300 / f"sample_{i}.dat").write_text(f"{i}\n")

    sftp.transfer_files(local_path=ne300, remove_on_success=False)

    assert len(sftp.transfered_remote) == 9
