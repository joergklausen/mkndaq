from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import os

import pytest

from mkndaq.utils.s3fsc import S3FSC
from mkndaq.utils.utils import load_config

@pytest.mark.integration
def test_real_upload_roundtrip_with_mkndaq_yml(tmp_path: Path):
    """
    Real S3/MinIO integration using your mkndaq.yml.
    No proxy monkeypatching needed; S3FSC can disable proxies via ctor arg.
    """
    yml_path = 'dist/mkndaq.yml'
    if not Path(yml_path).expanduser().exists():
        pytest.skip(f"Config file not found: {yml_path}.")

    cfg = load_config(yml_path)

    # Optional env overrides
    verify_env = os.getenv("S3_TLS_VERIFY")
    verify = (verify_env.lower() not in ("0", "false", "no")) if verify_env else True
    default_prefix = os.getenv("S3_DEFAULT_PREFIX", "")

    s3 = S3FSC(
        cfg,
        use_proxies=False,            # bypass corporate proxy
        addressing_style="path",      # robust default
        verify=verify,                # bool or CA bundle path accepted in config too
        default_prefix=default_prefix,
    )

    date_suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    f = tmp_path / f"hello.world-{date_suffix}"
    msg = "Hello from S3 integration test\n"
    msg += f"Generated at {datetime.now(timezone.utc).isoformat()}\n"
    msg += "This is a test file for upload and head operations.\n"
    msg += f"Endpoint: {cfg['s3']['endpoint_url']}\n"
    msg += f"Bucket: {cfg['s3']['aws_s3_bucket_name']}\n"
    msg += f"Region: {cfg['s3'].get('aws_region', '')}\n"
    msg += f"Access Key ID: {cfg['s3']['aws_access_key_id']}\n"
    msg += f"Key Prefix: {cfg['s3']['default_prefix']}\n"
    msg += "\nYou can safely delete it.\n"
    f.write_text(msg, encoding="utf-8")

    key = s3.upload(f, key_prefix=cfg['s3']['default_prefix'])

    head = s3.head(key)
    assert head["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert head["ContentLength"] == f.stat().st_size

    # s3.delete(key)
