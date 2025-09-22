from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Optional, Union, List

import boto3
from botocore.config import Config as BotoConfig
import schedule

logger = logging.getLogger(__name__)


def _expand_secret(secret_or_path: str) -> str:
    """If it looks like a file path and exists, read it; else return as-is."""
    p = Path(str(secret_or_path)).expanduser()
    if p.exists() and p.is_file():
        return p.read_text(encoding="utf-8").strip()
    return secret_or_path


def _coerce_verify(value: Any) -> Union[bool, str]:
    """Allow bool, 'true'/'false', or path to CA bundle."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ("true", "1", "yes"):
            return True
        if v in ("false", "0", "no"):
            return False
        return value  # path
    return True


@dataclass(frozen=True)
class S3Settings:
    endpoint_url: str
    bucket: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"
    addressing_style: str = "path"   # 'path' or 'virtual'
    verify: Union[bool, str] = True  # bool or path to CA bundle
    use_proxies: bool = True         # False disables proxies for this client only
    default_prefix: str = ""         # base key prefix in the bucket


class S3FSC:
    """
    S3 workhorse for mkndaq: single-file upload + directory transfers + scheduling.

    Expected config:
      config['s3'] = {
        'endpoint_url': 'https://your-s3-endpoint',
        'aws_s3_bucket_name': 'your-bucket',
        'aws_region': 'eu-central-1',
        'aws_access_key_id': 'AKIA...',
        'aws_secret_access_key': '/path/to/secret_or_literal',
        # optional:
        'addressing_style': 'path',    # or 'virtual'
        'verify': true,                # bool or '/path/to/ca.pem'
        'use_proxies': false,          # set false to bypass corp proxy
        'default_prefix': 'staging'    # base key prefix inside bucket
      }
    """

    def __init__(
        self,
        config: Dict[str, Any],
        *,
        s3_client: Optional[Any] = None,
        default_prefix: Optional[str] = None,
        addressing_style: Optional[str] = None,
        verify: Optional[Union[bool, str]] = None,
        use_proxies: Optional[bool] = None,
    ) -> None:
        s3 = (config or {}).get("s3", {})
        if not s3:
            raise ValueError("Missing 's3' section in config")

        endpoint = s3.get("endpoint_url", "")
        bucket = s3.get("aws_s3_bucket_name", "")
        region = s3.get("aws_region", "us-east-1")
        ak = s3.get("aws_access_key_id", "")
        sk_raw = s3.get("aws_secret_access_key", "")

        if not all([endpoint, bucket, ak, sk_raw]):
            raise ValueError(
                "Config.s3 must define endpoint_url, aws_s3_bucket_name, "
                "aws_access_key_id, aws_secret_access_key"
            )

        settings = S3Settings(
            endpoint_url=endpoint,
            bucket=bucket,
            access_key=ak,
            secret_key=_expand_secret(sk_raw),
            region=region,
            addressing_style=(addressing_style or s3.get("addressing_style") or "path"),
            verify=_coerce_verify(verify if verify is not None else s3.get("verify", True)),
            use_proxies=use_proxies if use_proxies is not None else bool(s3.get("use_proxies", True)),
            default_prefix=(default_prefix if default_prefix is not None else s3.get("default_prefix", "")),
        )
        self.settings = settings

        if s3_client is None:
            proxies = None if settings.use_proxies else {}
            cfg = BotoConfig(s3={"addressing_style": settings.addressing_style}, proxies=proxies)
            self.s3_client = boto3.client(
                "s3",
                endpoint_url=settings.endpoint_url,
                region_name=settings.region,
                aws_access_key_id=settings.access_key,
                aws_secret_access_key=settings.secret_key,
                verify=settings.verify,
                config=cfg,
            )
        else:
            self.s3_client = s3_client

        self._default_prefix = settings.default_prefix.strip("/")
        # book-keeping like SFTP client does
        self.schedule_logger = logging.getLogger(f"{__name__}.schedule")
        self.schedule_logger.setLevel(logging.DEBUG)
        self.transfered_local: List[str] = []
        self.transfered_remote: List[str] = []

    # ---------- internal ----------

    def _join_key(self, *parts: Optional[Union[str, Path, PurePosixPath]]) -> str:
        clean: list[str] = []
        for p in parts:
            if p is None:
                continue
            s = str(p).strip("/")
            if s:
                clean.append(s)
        return "/".join(clean)
    
    def _make_final_key(self, local_file: Path, local_base: Path, key_prefix: Optional[str]) -> str:
        # Preserve directory structure relative to local_base
        rel = local_file.relative_to(local_base).as_posix()  # e.g. "sub/dir/file.txt"
        return self._join_key(self._default_prefix, key_prefix, rel)

    # ---------- single-object ops ----------

    def upload(
        self,
        local_path: Union[str, Path],
        *,
        key_prefix: Optional[str] = None,
        extra_args: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Upload a single file; returns the object key."""
        p = Path(local_path)
        if not p.is_file():
            raise FileNotFoundError(f"Local file not found: {p}")
        key = self._join_key(self._default_prefix, key_prefix, p.name)
        logger.info("Uploading %s to s3://%s/%s", p, self.settings.bucket, key)
        kwargs: Dict[str, Any] = {}
        if extra_args:
            kwargs["ExtraArgs"] = extra_args
        self.s3_client.upload_file(str(p), self.settings.bucket, key, **kwargs)
        return key

    def head(self, key: str) -> Dict[str, Any]:
        return self.s3_client.head_object(Bucket=self.settings.bucket, Key=key)

    def delete(self, key: str) -> None:
        self.s3_client.delete_object(Bucket=self.settings.bucket, Key=key)

    def object_exists(self, key: str) -> bool:
        try:
            self.head(key)
            return True
        except Exception:
            return False

    # ---------- directory transfer (mkndaq replacement for SFTP) ----------

    def transfer_files(
        self,
        *,
        remove_on_success: bool = True,
        local_path: Optional[Union[str, Path]] = None,
        key_prefix: Optional[Union[str, PurePosixPath]] = None,
    ) -> None:
        """
        Upload all files under local_path to s3://bucket/(default_prefix)/(key_prefix)/<relative path>.
        If remove_on_success=True, delete local files after verifying size by head_object.
        """
        try:
            self.transfered_local = []
            self.transfered_remote = []

            local_base = Path(local_path or ".").resolve()
            if local_base.is_file():
                # treat as a single-file upload but keep parent as base for pathing
                files = [local_base]
                base = local_base.parent
            else:
                if not local_base.exists() or not local_base.is_dir():
                    raise ValueError(f"Local path '{local_base}' is not a valid directory.")
                # Walk
                files = [p for p in local_base.rglob("*") if p.is_file()]
                base = local_base

            if not files:
                logger.debug("No files to transfer from '%s'", local_base)
                return

            key_prefix_str = str(key_prefix).strip("/") if key_prefix else None

            for f in files:
                key = self._make_final_key(f, base, key_prefix_str)
                logger.debug("Uploading %s -> s3://%s/%s", f, self.settings.bucket, key)
                self.s3_client.upload_file(str(f), self.settings.bucket, key)

                # Verify and optionally remove
                try:
                    head = self.head(key)
                    remote_size = int(head.get("ContentLength", -1))
                    local_size = f.stat().st_size
                    if remove_on_success and remote_size == local_size:
                        f.unlink()
                        logger.debug("Removed local file after successful upload: %s", f)
                except Exception as verify_err:
                    logger.warning("Could not verify uploaded object for %s: %s", f, verify_err)

                self.transfered_local.append(str(f))
                self.transfered_remote.append(key)

        except Exception as err:
            logger.error("transfer_files failed: %s", err)

    def setup_transfer_schedules(
        self,
        *,
        remove_on_success: bool = True,
        interval: int = 60,
        local_path: Optional[Union[str, Path]] = None,
        key_prefix: Optional[Union[str, PurePosixPath]] = None,
    ) -> None:
        """
        Schedule directory uploads at fixed intervals (minutes).
        Supported:
          - 10  -> every 10 minutes
          - n*60 (e.g., 60, 120, ...) -> every n hours
          - 1440 -> daily
        """
        try:
            if interval == 10:
                schedule.every(10).minutes.do(
                    self.transfer_files,
                    remove_on_success=remove_on_success,
                    local_path=local_path,
                    key_prefix=key_prefix,
                )
            elif (interval % 60) == 0 and interval < 1440:
                hours = interval // 60
                schedule.every(hours).hours.do(
                    self.transfer_files,
                    remove_on_success=remove_on_success,
                    local_path=local_path,
                    key_prefix=key_prefix,
                )
            elif interval == 1440:
                schedule.every().day.at("00:00:10").do(
                    self.transfer_files,
                    remove_on_success=remove_on_success,
                    local_path=local_path,
                    key_prefix=key_prefix,
                )
            else:
                raise ValueError(
                    "'interval' must be 10 minutes, a multiple of 60 minutes (<1440), or 1440."
                )
            self.schedule_logger.debug(
                "Scheduled S3 transfer: interval=%s, local=%s, key_prefix=%s",
                interval, local_path, key_prefix,
            )
        except Exception as err:
            self.schedule_logger.error(err)
