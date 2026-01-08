from __future__ import annotations

import copy
import os
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from mkndaq.utils.s3fsc import S3FSC
from mkndaq.utils.utils import load_config


def _bool_from_env(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _int_from_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


@pytest.mark.integration
def test_real_upload_roundtrip_with_mkndaq_yml(tmp_path: Path) -> None:
    """S3 integration test (upload + head) against 2 endpoints.

    This version stays *simple*:
      - timeout/retry behavior is configured via S3FSC (botocore Config)
      - the test only enforces an overall 30s budget via monotonic checks

    Notes:
      - The overall 30s budget relies on botocore connect/read timeouts to avoid hangs.
        A truly "hard kill" timeout requires running the attempt in a separate process.
    """

    yml_path = "dist/mkndaq-fallback.yml"
    if not Path(yml_path).expanduser().exists():
        pytest.skip(f"Config file not found: {yml_path}.")

    cfg = load_config(yml_path)

    endpoint_urls = [
        "https://servicedevt.meteoswiss.ch/",
        "https://service.meteoswiss.ch/",
    ]

    # Optional env overrides
    verify = _bool_from_env("S3_TLS_VERIFY", default=True)
    default_prefix = os.getenv("S3_DEFAULT_PREFIX", "")

    # Keep per-request timeouts low so the test predictably finishes < 30s.
    # (These are passed into S3FSC -> botocore Config.)
    connect_timeout = _int_from_env("S3_CONNECT_TIMEOUT", 3)
    read_timeout = _int_from_env("S3_READ_TIMEOUT", 4)
    max_attempts = _int_from_env("S3_MAX_ATTEMPTS", 1)
    retry_mode = os.getenv("S3_RETRY_MODE", "standard")

    deadline = time.monotonic() + 30.0

    successes: list[dict[str, Any]] = []
    failures: list[tuple[str, str]] = []

    for endpoint_url in endpoint_urls:
        if time.monotonic() > deadline:
            failures.append((endpoint_url, "overall 30s time budget exhausted before starting this endpoint"))
            continue

        cfg_local = copy.deepcopy(cfg)
        cfg_local.setdefault("s3", {})
        cfg_local["s3"]["endpoint_url"] = endpoint_url
        cfg_local["s3"]["connect_timeout"] = connect_timeout
        cfg_local["s3"]["read_timeout"] = read_timeout
        cfg_local["s3"]["max_attempts"] = max_attempts
        cfg_local["s3"]["retry_mode"] = retry_mode

        # Build a tiny local file (kept for debugging in tmp_path)
        date_suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        endpoint_tag = (
            endpoint_url.replace("https://", "")
            .replace("http://", "")
            .replace("/", "_")
            .replace(":", "_")
        )
        local_path = tmp_path / f"hello.world-{endpoint_tag}-{date_suffix}.txt"

        msg = (
            "Hello from S3 integration test\n"
            f"Generated at {datetime.now(timezone.utc).isoformat()}\n"
            f"Endpoint: {endpoint_url}\n"
            f"Key Prefix: {default_prefix or cfg_local['s3'].get('default_prefix', '')}\n"
            "\nYou can safely delete it.\n"
        )
        local_path.write_text(msg, encoding="utf-8")

        try:
            s3 = S3FSC(
                cfg_local,
                use_proxies=False,  # bypass corporate proxies for this test
                addressing_style="path",
                verify=verify,
                default_prefix=default_prefix if default_prefix else None,
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                max_attempts=max_attempts,
                retry_mode=retry_mode,
            )

            key = s3.upload(local_path)
            head = s3.head(key)

            http_status = head.get("ResponseMetadata", {}).get("HTTPStatusCode")
            content_length = head.get("ContentLength")
            file_size = local_path.stat().st_size

            assert http_status == 200, f"Unexpected HTTP status for {endpoint_url}: {http_status}"
            assert content_length == file_size, (
                f"Content length mismatch for {endpoint_url}: {content_length} != {file_size}"
            )

            successes.append(
                {
                    "endpoint_url": endpoint_url,
                    "key": key,
                    "http_status": http_status,
                    "content_length": content_length,
                    "file_size": file_size,
                }
            )

            # Optional best-effort cleanup
            if _bool_from_env("S3_CLEANUP", default=False):
                try:
                    s3.delete(key)
                except Exception:
                    pass

        except Exception as e:
            failures.append((endpoint_url, f"{type(e).__name__}: {e}"))

        if time.monotonic() > deadline:
            break

    if successes and failures:
        details = "\n".join([f"- {ep}: {reason}" for ep, reason in failures])
        warnings.warn(
            "One or more endpoint_url targets failed in this integration test:\n" + details,
            pytest.PytestWarning,
        )

    if not successes:
        details = "\n".join([f"- {ep}: {reason}" for ep, reason in failures]) or "(no attempts made)"
        pytest.fail(
            "S3 integration test failed for all endpoint_url targets (30s overall budget).\n" + details,
            pytrace=False,
        )
