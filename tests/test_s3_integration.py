from __future__ import annotations

import copy
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import pytest

from mkndaq.utils.s3fsc import S3FSC
from mkndaq.utils.utils import load_config

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def config_file() -> Path:
    """Resolve the mkndaq configuration file for the integration test.

    Search order:
      1. ./mkndaq.yml
      2. ./dist/mkndaq.yml

    The test is intended to be launched from within the mkndaq project,
    typically from VS Code on the remote machine.
    """
    candidates = [Path("mkndaq.yml"), Path("dist/mkndaq.yml")]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()

    searched = ", ".join(str(p) for p in candidates)
    pytest.skip(f"Could not find mkndaq configuration file. Searched: {searched}")


@pytest.fixture(scope="module")
def cfg(config_file: Path) -> dict[str, Any]:
    """Load the mkndaq configuration used by the main application."""
    cfg = load_config(config_file=str(config_file))

    if not cfg.get("s3"):
        pytest.skip("Missing 's3' section in mkndaq configuration.")
    if not cfg.get("test"):
        pytest.skip("Missing 'test' section in mkndaq configuration.")

    return cfg


@pytest.fixture(scope="module")
def s3fsc(cfg: dict[str, Any]) -> S3FSC:
    """Instantiate S3FSC exactly the way mkndaq.py does."""
    return S3FSC(
        config=cfg,
        use_proxies=bool(cfg["s3"].get("use_proxies", True)),
        addressing_style=cfg["s3"].get("addressing_style", "path"),
        verify=cfg["s3"].get("verify", True),
        default_prefix=cfg["s3"].get("default_prefix", ""),
    )


def _build_expected_key(cfg: dict[str, Any], key_prefix: str, filename: str) -> str:
    """Build the expected S3 object key for the generated test file."""
    parts = [
        str(cfg["s3"].get("default_prefix", "")).strip("/"),
        str(key_prefix).strip("/"),
        filename,
    ]
    return "/".join(part for part in parts if part)


@pytest.fixture(scope="module")
def local_test_file(cfg: dict[str, Any]) -> Path:
    """Create a timestamped S3 test file under root/data/test.

    This intentionally behaves like another instrument source directory.
    The file is kept locally after the test run.
    """
    root = Path(os.path.expanduser(str(cfg["root"]))).resolve()
    test_dir = root / str(cfg["data"]) / str(cfg["test"]["staging_path"])
    test_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H")
    filename = f"mkn-s3-test-{stamp}.txt"
    local_file = test_dir / filename

    local_file.write_text(
        "\n".join(
            [
                "mkndaq S3 integration test",
                f"created_utc={datetime.now(timezone.utc).isoformat()}",
                f"bucket={cfg['s3']['bucket_name']}",
                f"default_prefix={cfg['s3'].get('default_prefix', '')}",
                f"key_prefix={cfg['test']['remote_path']}",
                f"local_file={local_file}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return local_file


def test_s3_upload_uses_same_path_as_mkndaq(
    cfg: dict[str, Any],
    s3fsc: S3FSC,
    local_test_file: Path,
) -> None:
    """Upload a test file through the same S3 path logic as mkndaq.py.

    The test creates a file below root/data/test and then uploads it via
    S3FSC.transfer_files(..., key_prefix=cfg['test']['remote_path']).
    With the provided mkndaq.yml, the expected object key is:

        mkn/incoming/test/mkn-s3-test-YYYYMMDDHH.txt
    """
    key_prefix = str(cfg["test"]["remote_path"])
    local_dir = local_test_file.parent
    expected_key = _build_expected_key(cfg, key_prefix=key_prefix, filename=local_test_file.name)

    s3fsc.transfer_files(
        remove_on_success=False,
        local_path=local_dir,
        key_prefix=key_prefix,
    )

    assert str(local_test_file) in s3fsc.transfered_local
    assert expected_key in s3fsc.transfered_remote

    head = s3fsc.head(expected_key)
    assert head.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
    assert int(head.get("ContentLength", -1)) == local_test_file.stat().st_size


def test_s3_upload_direct(cfg, local_test_file):
    s3 = S3FSC(cfg)
    key = s3.upload(local_test_file, key_prefix=cfg["test"]["remote_path"])
    head = s3.head(key)
    assert head["ResponseMetadata"]["HTTPStatusCode"] == 200


boto3.set_stream_logger("botocore", logging.DEBUG)

def test_s3_put_object_probe(cfg, local_test_file):
    cfg2 = copy.deepcopy(cfg)
    cfg2["s3"]["addressing_style"] = "auto"   # or try "virtual"
    # optional one-off TLS probe:
    # cfg2["s3"]["verify"] = False

    s3 = S3FSC(cfg2)

    key = "/".join(
        p.strip("/")
        for p in [cfg2["s3"]["default_prefix"], cfg2["test"]["remote_path"], local_test_file.name]
        if p
    )

    body = local_test_file.read_bytes()

    resp = s3.s3_client.put_object(
        Bucket=s3.settings.bucket,
        Key=key,
        Body=body,
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    head = s3.head(key)
    assert head["ContentLength"] == len(body)
