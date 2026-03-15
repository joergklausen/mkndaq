#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Manage file transfer. Currently, SFTP transfer to MeteoSwiss is supported.

This module is also used for LAN-side SFTP/SSH interactions (e.g., fetching VRXA0*
bulletins from a linuxbox) so that SSH key loading and connection handling stay
centralized.

@author: joerg.klausen@meteoswiss.ch
"""
from __future__ import annotations

import logging
import os
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterator, List, Optional, Set, Union

import paramiko
import schedule


@dataclass(frozen=True)
class DownloadedFile:
    """Metadata for a downloaded remote file."""
    name: str
    local_path: Path
    mtime: int
    size: int
    remote_path: PurePosixPath


class SFTPClient:
    """
    SFTP based file handling, optionally using SOCKS5 proxy.

    Available methods include:
      - is_alive()
      - list_local_files()
      - remote_item_exists()
      - list_remote_items()
      - list_remote_items_attr()
      - setup_remote_folders()
      - remove_remote_item()
      - transfer_files()          upload local -> remote
      - setup_transfer_schedules()
      - download_files()          download remote -> local (LAN use-case)
    """

    # one lock per instance prevents overlapping transfers
    _sched_lock = threading.Lock()

    def __init__(
        self,
        config: dict,
        name: str = str(),
        *,
        host: Optional[str] = None,
        usr: Optional[str] = None,
        key_path: Optional[Union[str, Path]] = None,
        port: Optional[int] = None,
        remote_root: Optional[Union[str, PurePosixPath]] = None,
        local_root: Optional[Union[str, Path]] = None,
    ):
        """
        Initialize the SFTPClient class with parameters from a configuration file.

        Defaults are taken from:
          config['sftp']['host']
          config['sftp']['usr']
          config['sftp']['key']
          config['sftp']['remote']   (remote destination root; often '.')

        Overrides can be passed (host/usr/key_path/port) to use the same client code
        for other SFTP endpoints (e.g. a linuxbox on the LAN).

        Args:
            config: parsed mkndaq.yml dict
            name: instrument name to expand local/remote subfolders for *upload* use-cases
            host, usr, key_path, port: optional connection overrides
            remote_root: optional remote base path override (PurePosixPath)
            local_root: optional local base path override (Path)
        """
        self.name = name

        # configure logging
        _logger = f"{os.path.basename(config['logging']['file'])}".split('.')[0]
        self.logger = logging.getLogger(f"{_logger}.{__name__}")
        self.schedule_logger = logging.getLogger(f"{_logger}.schedule")
        self.schedule_logger.setLevel(level=logging.DEBUG)
        self.logger.info("Initialize SFTPClient")

        # connection settings
        self.host = host or config["sftp"]["host"]
        self.usr = usr or config["sftp"]["usr"]
        self.port = int(port or config.get("sftp", {}).get("port", 22))

        raw_key = key_path or config["sftp"]["key"]
        self.key_path = Path(str(raw_key)).expanduser()
        self.key = self._load_private_key(self.key_path)

        # configure client proxy if needed
        if config["sftp"]["proxy"]["socks5"]:
            import sockslib
            with sockslib.SocksSocket() as sock:
                sock.set_proxy(
                    (config["sftp"]["proxy"]["socks5"], config["sftp"]["proxy"]["port"]),
                    sockslib.Socks.SOCKS5,
                )

        # configure local source root (staging by default)
        if local_root is not None:
            self.local_path = Path(local_root).expanduser().resolve()
        else:
            self.local_path = Path(config["root"]).expanduser() / config["staging"]

        # configure remote destination root
        if remote_root is not None:
            self.remote_path = PurePosixPath(remote_root)
        else:
            self.remote_path = PurePosixPath(config["sftp"]["remote"])

        # upload use-case: expand local+remote by instrument name (if provided)
        if name:
            self.local_path = self.local_path / config[name]["staging_path"]
            self.remote_path = self.remote_path / config[name]["remote_path"]

    # -------------------------------------------------------------------------
    # Key loading / connections
    # -------------------------------------------------------------------------

    @staticmethod
    def _load_private_key(path: Path) -> paramiko.PKey:
        """
        Load an SSH private key.

        Supports modern OpenSSH key formats and multiple key types (RSA, ECDSA, ED25519).
        If the key is passphrase-protected, set:
            MKNDAQ_SSH_PASSPHRASE

        Compatibility note:
            Some deployments keep OpenSSH keys with a .ppk extension. That is fine.
            True PuTTY-only PPK keys are not supported by Paramiko without conversion.
        """
        passphrase_str = os.environ.get("MKNDAQ_SSH_PASSPHRASE")

        # 1) Preferred: auto-detect via Paramiko 4+
        try:
            passphrase_bytes: bytes | None = passphrase_str.encode("utf-8") if passphrase_str else None
            return paramiko.PKey.from_path(path, passphrase=passphrase_bytes)
        except Exception:
            pass

        # 2) Fallbacks (older PEM formats / explicit types)
        loaders = [
            getattr(paramiko, "RSAKey", None),
            getattr(paramiko, "Ed25519Key", None),
            getattr(paramiko, "ECDSAKey", None),
            getattr(paramiko, "DSSKey", None),
        ]
        last_err: Exception | None = None
        for cls in loaders:
            if cls is None:
                continue
            try:
                return cls.from_private_key_file(str(path), password=passphrase_str)
            except Exception as err:
                last_err = err

        raise ValueError(
            f"Could not load SSH private key '{path}'. Ensure it is an OpenSSH private key "
            f"(PuTTY-only .ppk keys must be converted). Last error: {last_err}"
        ) from last_err

    @contextmanager
    def open_sftp(
        self,
        *,
        host: Optional[str] = None,
        usr: Optional[str] = None,
        key: Optional[paramiko.PKey] = None,
        port: Optional[int] = None,
        timeout: int = 10,
    ) -> Iterator[paramiko.SFTPClient]:
        """Open an SSH connection and yield an SFTP client."""
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect(
            hostname=host or self.host,
            port=int(port or self.port),
            username=usr or self.usr,
            pkey=key or self.key,
            look_for_keys=False,
            allow_agent=True,
            timeout=timeout,
        )
        try:
            sftp = ssh.open_sftp()
            try:
                yield sftp
            finally:
                sftp.close()
        finally:
            ssh.close()

    # -------------------------------------------------------------------------
    # Existing / upload oriented functionality
    # -------------------------------------------------------------------------

    def is_alive(self) -> bool:
        """Test ssh connection to sftp server."""
        try:
            with self.open_sftp():
                pass
            return True
        except Exception as err:
            self.logger.error(err)
            return False

    def list_local_files(self, local_path: Path = Path()) -> list:
        """Establish list of local files."""
        if local_path is None:
            local_path = Path(self.local_path)

        try:
            files = []
            for root, _, filenames in os.walk(local_path):
                for file in filenames:
                    files.append(Path(root) / file)
            return files
        except Exception as err:
            self.logger.error(err)
            return []

    def remote_item_exists(self, remote_path: Union[str, PurePosixPath]) -> bool:
        """Check on remote server if an item exists."""
        try:
            path = PurePosixPath(remote_path)
            with self.open_sftp() as sftp:
                try:
                    sftp.stat(str(path))
                    return True
                except FileNotFoundError:
                    return False
        except Exception as err:
            self.logger.error(err)
            return False

    def list_remote_items(self, remote_path: Optional[Union[str, PurePosixPath]] = None) -> List[str]:
        """List item names in a remote SFTP directory."""
        path = PurePosixPath(remote_path or ".")
        try:
            with self.open_sftp() as sftp:
                return sftp.listdir(str(path))
        except Exception as err:
            self.logger.error(f"Error listing remote items in '{path}': {err}")
            return []

    def list_remote_items_attr(self, remote_path: Optional[Union[str, PurePosixPath]] = None) -> List[paramiko.SFTPAttributes]:
        """List remote items including attributes (mtime/size)."""
        path = PurePosixPath(remote_path or ".")
        try:
            with self.open_sftp() as sftp:
                return sftp.listdir_attr(str(path))
        except Exception as err:
            self.logger.error(f"Error listing remote item attrs in '{path}': {err}")
            return []

    def setup_remote_folders(
        self,
        local_path: Optional[Union[str, Path]] = None,
        remote_path: Optional[Union[str, PurePosixPath]] = None,
    ) -> None:
        """Replicate the local directory structure on the remote server."""
        try:
            local_base = Path(local_path or self.local_path).resolve()
            remote_base = PurePosixPath(remote_path or self.remote_path)

            self.logger.info(f"Setting up remote folders from '{local_base}' to '{remote_base}'")

            with self.open_sftp() as sftp:
                for root, dirs, files in os.walk(local_base):
                    if not dirs and not files:
                        continue

                    rel_path = Path(root).relative_to(local_base)
                    remote_dir = remote_base / PurePosixPath(rel_path.as_posix())

                    try:
                        sftp.stat(str(remote_dir))
                    except FileNotFoundError:
                        try:
                            sftp.mkdir(str(remote_dir), mode=0o755)
                        except Exception as mkdir_err:
                            self.logger.error(f"Could not create '{remote_dir}': {mkdir_err}")
        except Exception as err:
            self.logger.error(f"setup_remote_folders failed: {err}")

    def remove_remote_item(self, remote_path: Union[str, PurePosixPath], recursive: bool = True) -> None:
        """Remove a file or (empty) directory from remote host. Optionally prune empty parents."""
        try:
            remote_path = PurePosixPath(remote_path)
            if not self.remote_item_exists(remote_path):
                raise ValueError("remove_remote_item: remote_path does not exist.")

            with self.open_sftp() as sftp:
                try:
                    if sftp.listdir(remote_path.as_posix()):
                        self.logger.warning(f"Cannot remove non-empty directory: {remote_path}")
                        return
                    sftp.rmdir(remote_path.as_posix())
                except IOError:
                    sftp.remove(remote_path.as_posix())

                if recursive:
                    parent = remote_path.parent
                    while str(parent) not in ("", "/"):
                        try:
                            if not sftp.listdir(parent.as_posix()):
                                sftp.rmdir(parent.as_posix())
                                parent = parent.parent
                            else:
                                break
                        except Exception:
                            break
        except Exception as err:
            self.logger.error(f"remove_remote_item: {err}")

    def setup_remote_path(self, remote_path: Union[str, PurePosixPath]) -> None:
        """Create (and navigate to the leaf of) a remote path (directories)."""
        try:
            remote_path = PurePosixPath(remote_path)
            with self.open_sftp() as sftp:
                try:
                    sftp.chdir(remote_path.as_posix())
                except IOError:
                    parts = remote_path.parts
                    current_path = "."
                    for part in parts:
                        if part:
                            current_path = f"{current_path}/{part}"
                        try:
                            sftp.chdir(current_path)
                        except IOError:
                            sftp.mkdir(part)
                            sftp.chdir(part)
                self.cwd = sftp.getcwd() or "."
        except Exception as err:
            self.logger.error(f"setup_remote_path: {err}")

    def transfer_files(
        self,
        remove_on_success: bool = True,
        local_path: Optional[Path] = None,
        remote_path: Optional[PurePosixPath] = None,
    ) -> None:
        """Transfer all files from local_path and its subfolders to remote_path."""
        try:
            self.transfered_local = []
            self.transfered_remote = []

            local_base = Path(local_path or self.local_path).resolve()
            if local_base.is_file():
                local_base = local_base.parent
            elif not local_base.is_dir():
                raise ValueError(f"Local path '{local_base}' is not a valid directory or file.")
            if not local_base.exists():
                raise FileNotFoundError(f"Local path '{local_base}' does not exist.")
            remote_base = PurePosixPath(remote_path or self.remote_path)

            self.logger.info(f"Starting SFTP file transfer: {local_base} -> {remote_base}")

            with self.open_sftp() as sftp:
                for root, _, files in os.walk(local_base):
                    if not files:
                        continue

                    rel_path = Path(root).relative_to(local_base).as_posix()
                    remote_dir = remote_base / rel_path

                    self.setup_remote_path(remote_dir)

                    for file in files:
                        local_file = Path(root) / file
                        remote_file = remote_dir / file

                        try:
                            attr = sftp.put(
                                localpath=local_file.as_posix(),
                                remotepath=remote_file.as_posix(),
                                confirm=True,
                            )
                            self.transfered_local.append(local_file.as_posix())
                            self.transfered_remote.append(remote_file.as_posix())

                            if remove_on_success:
                                if attr.st_size == local_file.stat().st_size:
                                    local_file.unlink()
                        except Exception as file_err:
                            self.logger.error(f"Failed to transfer {local_file} -> {remote_file}: {file_err}")
        except Exception as err:
            self.logger.error(f"transfer_files failed: {err}")

    def setup_transfer_schedules(
        self,
        remove_on_success: bool = True,
        interval: int = 60,
        local_path: Optional[str] = None,
        remote_path: Optional[str] = None,
        delay_transfer: int = 5,
    ) -> None:
        """Schedule directory uploads at fixed intervals (minutes), aligned to boundaries."""
        try:
            if not (0 <= delay_transfer <= 59):
                raise ValueError("delay_transfer must be between 0 and 59 seconds")

            def _sftp_transfer_files():
                if not self._sched_lock.acquire(blocking=False):
                    self.schedule_logger.warning("Skipping SFTP transfer: previous run still active")
                    return
                try:
                    self.transfer_files(
                        remove_on_success=bool(remove_on_success),
                        local_path=Path(local_path).resolve() if local_path is not None else None,
                        remote_path=PurePosixPath(remote_path) if remote_path is not None else None,
                    )
                finally:
                    self._sched_lock.release()

            if interval == 10:
                for minute in (0, 10, 20, 30, 40, 50):
                    schedule.every(1).hours.at(f"{minute:02d}:{delay_transfer:02d}").do(_sftp_transfer_files)
            elif (interval % 60) == 0 and interval < 1440:
                hours = interval // 60
                schedule.every(hours).hours.at(f"00:{delay_transfer:02d}").do(_sftp_transfer_files)
            elif interval == 1440:
                schedule.every().day.at(f"00:00:{delay_transfer:02d}").do(_sftp_transfer_files)
            else:
                raise ValueError("'interval' must be 10, a multiple of 60 (<1440), or 1440.")

            self.schedule_logger.debug(
                "Scheduled SFTP transfer: interval=%s, local=%s, remote=%s, delay=%ss",
                interval,
                local_path,
                remote_path,
                delay_transfer,
            )
        except Exception as err:
            self.schedule_logger.error(err)

    # -------------------------------------------------------------------------
    # NEW: download support (remote -> local), used by mkndaq.inst.meteo
    # -------------------------------------------------------------------------

    def download_files(
        self,
        *,
        remote_dir: Union[str, PurePosixPath],
        local_dir: Union[str, Path],
        pattern: Optional[str] = None,
        skip_names: Optional[Set[str]] = None,
        tmp_prefix: str = ".",
        tmp_suffix: str = ".part",
        keep_mtime: bool = True,
    ) -> List[DownloadedFile]:
        """
        Download files from a remote directory into a local directory.

        Args:
            remote_dir: remote directory to scan
            local_dir: local destination directory (created if missing)
            pattern: if provided, only download files whose name contains this string
            skip_names: optional set of remote filenames that should be skipped (already seen)
            tmp_prefix/tmp_suffix: used for atomic downloads (download to temp then rename)
            keep_mtime: if True, set local mtime to remote mtime

        Returns:
            List[DownloadedFile]: downloaded files with remote metadata (mtime/size)
        """
        remote_dir = PurePosixPath(remote_dir)
        local_dir = Path(local_dir).expanduser().resolve()
        local_dir.mkdir(parents=True, exist_ok=True)
        skip_names = skip_names or set()

        downloaded: List[DownloadedFile] = []

        with self.open_sftp() as sftp:
            for attr in sftp.listdir_attr(remote_dir.as_posix()):
                name = attr.filename
                if pattern and pattern not in name:
                    continue
                if name in skip_names:
                    continue

                remote_file = remote_dir / name
                final_local = local_dir / name
                tmp_local = local_dir / f"{tmp_prefix}{name}{tmp_suffix}"

                try:
                    sftp.get(remote_file.as_posix(), tmp_local.as_posix())
                    if final_local.exists():
                        try:
                            tmp_local.unlink()
                        except FileNotFoundError:
                            pass
                        continue
                    tmp_local.replace(final_local)
                except Exception:
                    try:
                        if tmp_local.exists():
                            tmp_local.unlink()
                    except Exception:
                        pass
                    raise

                # Normalize stub-typed attributes (Paramiko stubs allow None)
                raw_mtime = attr.st_mtime
                raw_size = attr.st_size

                mtime_i: int = int(raw_mtime) if raw_mtime is not None else 0
                size_i: int = int(raw_size) if raw_size is not None else 0

                if keep_mtime and mtime_i:
                    try:
                        os.utime(final_local, (mtime_i, mtime_i))
                    except Exception:
                        pass

                downloaded.append(
                    DownloadedFile(
                        name=name,
                        local_path=final_local,
                        mtime=mtime_i,
                        size=size_i,
                        remote_path=remote_file,
                    )
                )

        return downloaded


if __name__ == "__main__":
    pass
