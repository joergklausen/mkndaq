#!/usr/bin/env python3
"""
meteo.py

CLI to pull meteo files from a remote Linux box via SSH/SFTP (password entered interactively).

Defaults
--------
host:              192.168.2.185
user:              admin
remote_path:        /home/moxa/data
file_pattern:       VRXA00.*          (shell-style glob on remote filenames)
local_path_data:    ~/Documents/mkndaq/data/meteo
local_path_outbox:  ~/Documents/mkndaq/staging/meteo

Behavior
--------
- Prompts for the SSH password (does not echo).
- Lists files in remote_path matching file_pattern.
- Downloads ONLY files that do not already exist under local_path_data.
- For each downloaded file, also copies it to local_path_outbox.

Requirements
------------
    pip install paramiko

Usage:
--------
    python meteo_get_from_linuxbox.py [--host HOST] [--user USER] [--port PORT]
                    [--remote-path REMOTE_PATH] [--file-pattern FILE_PATTERN]
                    [--local-path-data LOCAL_PATH_DATA] [--local-path-outbox LOCAL_PATH_OUTBOX]
                    [--timeout TIMEOUT] [--min-remote-age MIN_REMOTE_AGE]
                    [-v ...]    
"""

from __future__ import annotations

import argparse
import fnmatch
import getpass
import logging
import posixpath
import shutil
import sys
import time
from pathlib import Path
from typing import Optional

import paramiko


def _expand(p: str | Path) -> Path:
    return Path(p).expanduser().resolve()


def pull_meteo_files(
    *,
    host: str,
    user: str,
    password: str,
    remote_path: str,
    file_pattern: str,
    local_path_data: Path,
    local_path_outbox: Path,
    port: int = 22,
    timeout_s: int = 15,
    min_remote_age_s: int = 0,
    logger: Optional[logging.Logger] = None,
) -> list[Path]:
    """
    Pull meteo files via SFTP.

    Only downloads files that do not yet exist in local_path_data.

    Returns:
        List of local data files that were newly downloaded.
    """
    log = logger or logging.getLogger(__name__)

    local_path_data.mkdir(parents=True, exist_ok=True)
    local_path_outbox.mkdir(parents=True, exist_ok=True)

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    log.info("Connecting to %s:%d as %s", host, port, user)
    ssh.connect(
        hostname=host,
        port=port,
        username=user,
        password=password,
        timeout=timeout_s,
        banner_timeout=timeout_s,
        auth_timeout=timeout_s,
    )

    downloaded: list[Path] = []
    try:
        sftp = ssh.open_sftp()
        try:
            log.info("Remote directory: %s", remote_path)
            sftp.chdir(remote_path)

            now = time.time()
            entries = sftp.listdir_attr(".")
            candidates = []
            for e in entries:
                name = e.filename
                if not fnmatch.fnmatch(name, file_pattern):
                    continue

                # Optionally skip too-new files (if remote is still writing)
                if min_remote_age_s > 0:
                    age = now - float(getattr(e, "st_mtime", 0))
                    if age < min_remote_age_s:
                        log.debug("Skipping %s (age %.1fs < %ss)", name, age, min_remote_age_s)
                        continue

                candidates.append(e)

            if not candidates:
                log.info("No matching files found for pattern '%s'.", file_pattern)
                return []

            # deterministic order
            candidates.sort(key=lambda x: (getattr(x, "st_mtime", 0), x.filename))
            log.info("Found %d matching file(s).", len(candidates))

            for e in candidates:
                name = e.filename
                remote_full = posixpath.join(remote_path, name)

                local_data = local_path_data / name
                local_outbox = local_path_outbox / name

                if local_data.exists():
                    log.info("Skip (already exists): %s", local_data)
                    continue

                log.info("Downloading %s -> %s", remote_full, local_data)
                tmp = local_data.with_suffix(local_data.suffix + ".part")
                if tmp.exists():
                    tmp.unlink()

                sftp.get(remote_full, str(tmp))
                tmp.replace(local_data)

                # Copy into outbox (only for newly downloaded files)
                log.info("Copying to outbox: %s -> %s", local_data, local_outbox)
                shutil.copy2(local_data, local_outbox)

                downloaded.append(local_data)

        finally:
            sftp.close()
    finally:
        ssh.close()

    return downloaded


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="meteo.py",
        description="Pull meteo files from a remote host via SSH/SFTP (password prompted).",
    )

    p.add_argument("--host", default="192.168.2.185", help="Remote host (default: %(default)s)")
    p.add_argument("--user", default="admin", help="SSH username (default: %(default)s)")
    p.add_argument("--port", type=int, default=22, help="SSH port (default: %(default)s)")

    p.add_argument("--remote-path", default="/home/moxa/data", help="Remote directory (default: %(default)s)")
    p.add_argument(
        "--file-pattern",
        default="VRXA00.*",
        help="Remote filename glob pattern (default: %(default)s)",
    )

    p.add_argument(
        "--local-path-data",
        default="~/Documents/mkndaq/data/meteo",
        help="Local data directory (default: %(default)s)",
    )
    p.add_argument(
        "--local-path-outbox",
        default="~/Documents/mkndaq/staging/meteo",
        help="Local outbox directory (default: %(default)s)",
    )

    p.add_argument("--timeout", type=int, default=15, help="SSH timeout seconds (default: %(default)s)")
    p.add_argument(
        "--min-remote-age",
        type=int,
        default=0,
        help="Skip remote files newer than N seconds (default: %(default)s)",
    )

    p.add_argument("-v", "--verbose", action="count", default=0, help="Increase logging (-v, -vv)")
    return p


def main(argv: list[str]) -> int:
    args = build_parser().parse_args(argv)

    level = logging.WARNING
    if args.verbose == 1:
        level = logging.INFO
    elif args.verbose >= 2:
        level = logging.DEBUG
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    log = logging.getLogger("meteo")

    password = getpass.getpass(f"Password for {args.user}@{args.host}: ")

    local_data = _expand(args.local_path_data)
    local_outbox = _expand(args.local_path_outbox)

    try:
        downloaded = pull_meteo_files(
            host=args.host,
            user=args.user,
            password=password,
            remote_path=args.remote_path,
            file_pattern=args.file_pattern,
            local_path_data=local_data,
            local_path_outbox=local_outbox,
            port=args.port,
            timeout_s=args.timeout,
            min_remote_age_s=args.min_remote_age,
            logger=log,
        )
    except Exception as e:
        log.error("Failed: %s", e, exc_info=(level <= logging.DEBUG))
        return 2

    if downloaded:
        log.warning("Downloaded %d file(s).", len(downloaded))
        for p in downloaded:
            print(str(p))
    else:
        log.warning("No new files downloaded.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

