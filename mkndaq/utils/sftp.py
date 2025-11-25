#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Manage file transfer. Currently, sftp transfer to MeteoSwiss is supported.

@author: joerg.klausen@meteoswiss.ch
"""
import logging
import os
import threading
from pathlib import Path, PurePosixPath
from typing import Union, Optional, List

import paramiko
import schedule


class SFTPClient:
    """
    SFTP based file handling, optionally using SOCKS5 proxy.

    Available methods include
    - is_alive():
    - list_local_files():
    - remote_item_exists():
    - list_remote_items():
    - setup_remote_folders():
    - put_file():
    - remove_remote_item():
    - transfer_files(): transfer files,  optionally removing files from source
    """

    def __init__(self, config: dict, name: str=str()):
        """
        Initialize the SFTPClient class with parameters from a configuration file.

        :param config_file: Path to the configuration file.
                    config['sftp']['host']:
                    config['sftp']['usr']:
                    config['sftp']['key']:
                    config['sftp']['local_path']: relative path to local source (= staging)
                    config['sftp']['remote_path']: (absolute?) root of remote destination
        :param name: name of instrument to use. Used to expand the name of the local and remote folders
        """
        try:
            self.name = name


            # configure logging
            _logger = f"{os.path.basename(config['logging']['file'])}".split('.')[0]
            self.logger = logging.getLogger(f"{_logger}.{__name__}")
            self.schedule_logger = logging.getLogger(f"{_logger}.schedule")
            self.schedule_logger.setLevel(level=logging.DEBUG)
            self.logger.info("Initialize SFTPClient")

            # sftp connection settings
            self.host = config['sftp']['host']
            self.usr = config['sftp']['usr']
            self.key = paramiko.RSAKey.from_private_key_file(\
                str(Path(config['sftp']['key']).expanduser()))

            # configure client proxy if needed
            if config['sftp']['proxy']['socks5']:
                import sockslib
                with sockslib.SocksSocket() as sock:
                    sock.set_proxy((config['sftp']['proxy']['socks5'],
                                    config['sftp']['proxy']['port']), sockslib.Socks.SOCKS5)

            # configure local source
            self.local_path = Path(config['root']).expanduser() / config['staging']

            # configure remote destination
            self.remote_path = PurePosixPath(config['sftp']['remote'])

            if name:
                self.local_path = self.local_path / config[name]['staging_path']
                self.remote_path = self.remote_path / config[name]['remote_path']

        except Exception as err:
            self.logger.error(err)


    # one lock per instance prevents overlapping transfers
    _sched_lock = threading.Lock()


    def is_alive(self) -> bool:
        """Test ssh connection to sftp server.

        Returns:
            bool: [description]
        """
        try:
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)

                with ssh.open_sftp() as sftp:
                    sftp.close()
            return True
        except Exception as err:
            self.logger.error(err)
            return False


    def list_local_files(self, local_path: Path=Path()) -> list:
        """Establish list of local files.

        Args:
            localpath (Path, optional): Absolute path to directory containing folders and files. Defaults to str().

        Returns:
            list: absolute paths of local files
        """
        files = list()

        if local_path is None:
            local_path = Path(self.local_path)

        try:
            files = []
            for root, dirs, filenames in os.walk(local_path):
                for file in filenames:
                    files.append(Path(root) / file)
            return files

        except Exception as err:
            self.logger.error(err)
            return list()


    def remote_item_exists(self, remote_path: Union[str, PurePosixPath]) -> bool:
        """Check on remote server if an item exists.

        Args:
            remote_path (PurePosixPath): Full path to remote item

        Returns:
            Boolean: True if item exists, False otherwise.
        """
        try:
            path = PurePosixPath(remote_path)
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)
                with ssh.open_sftp() as sftp:
                    try:
                        sftp.stat(str(path))
                        return True
                    except FileNotFoundError:
                        return False
        except Exception as err:
            self.logger.error(err)
            return False


    def list_remote_items(self, remote_path: Optional[Union[str, PurePosixPath]] = None) -> List[str]:
        """
        List items in a remote SFTP directory.

        Args:
            remote_path (str | PurePosixPath | None): Remote directory path. Defaults to user's SFTP root.

        Returns:
            List[str]: List of item names in the specified remote directory.
        """
        path = PurePosixPath(remote_path or ".")

        try:
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)

                with ssh.open_sftp() as sftp:
                    return sftp.listdir(str(path))

        except Exception as err:
            self.logger.error(f"Error listing remote items in '{path}': {err}")
            return []


    def setup_remote_folders(self,
                             local_path: Optional[Union[str, Path]] = None,
                             remote_path: Optional[Union[str, PurePosixPath]] = None
                             ) -> None:
        """
        Replicate the local directory structure under `local_path` to the remote SFTP server under `remote_path`.

        Args:
            local_path (str | None): Base local path to scan. Defaults to `self.local_path`.
            remote_path (str | None): Base remote path to create folders. Defaults to `self.remote_path`.
        """
        try:
            local_base = Path(local_path or self.local_path).resolve()
            remote_base = PurePosixPath(remote_path or self.remote_path)

            self.logger.info(f"Setting up remote folders from '{local_base}' to '{remote_base}'")

            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)

                with ssh.open_sftp() as sftp:
                    for root, dirs, files in os.walk(local_base):
                        if not dirs and not files:
                            continue  # Skip empty directories

                        rel_path = Path(root).relative_to(local_base)
                        remote_dir = remote_base / PurePosixPath(rel_path.as_posix())

                        self.logger.debug(f"Ensuring remote directory: {remote_dir}")
                        try:
                            sftp.stat(str(remote_dir))  # Check if directory exists
                        except FileNotFoundError:
                            try:
                                sftp.mkdir(str(remote_dir), mode=0o755)
                                self.logger.debug(f"Created remote directory: {remote_dir}")
                            except Exception as mkdir_err:
                                self.logger.error(f"Could not create '{remote_dir}': {mkdir_err}")
                        except Exception as stat_err:
                            self.logger.error(f"Error checking existence of '{remote_dir}': {stat_err}")

        except Exception as err:
            self.logger.error(f"setup_remote_folders failed: {err}")

        # """
        # Determine directory structure under local_path and replicate on remote host.

        # :param str local_path:
        # :param str remote_path:
        # :return: Nothing
        # """
        # try:
        #     if local_path is None:
        #         local_path = Path(self.local_path)

        #     # sanitize local_path
        #     # local_path = re.sub(r'(/?\.?\\){1,2}', '/', local_path)

        #     if remote_path is None:
        #         remote_path = PurePosixPath(self.remote_path)

        #     # sanitize remote_path
        #     # remote_path = re.sub(r'(\\){1,2}', '/', remote_path)

        #     self.logger.info(f"setup_remote_folders (local_path: {local_path}, remote_path: {remote_path})")

        #     with paramiko.SSHClient() as ssh:
        #         ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        #         ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)
        #         with ssh.open_sftp() as sftp:
        #             # determine local directory structure, establish same structure on remote host
        #             for root, dirs, files in os.walk(local_path):
        #                 root = re.sub(r'(/?\.?\\){1,2}', '/', root).replace(local_path, remote_path)
        #                 self.logger.debug(f"root: {root}")
        #                 try:
        #                     sftp.mkdir(root, mode=16877)
        #                 except OSError as err:
        #                     # [todo] check if remote items exists, adapt error message accordingly ...
        #                     self.logger.error(f"Could not create '{root}', error: {err}. Maybe path exists already?")
        #                     pass
        #             sftp.close()

        # except Exception as err:
        #     self.logger.error(err)


    def remove_remote_item(self, remote_path: Union[str, PurePosixPath], recursive: bool = True) -> None:
        """
        Remove a file or (empty) directory from a remote host using SFTP and SSH.
        If `recursive=True`, empty parent directories will also be pruned.

        Args:
            remote_path (Union[str, PurePosixPath]): Remote path to file or directory.
            recursive (bool): If True, recursively prune empty parent directories.
        """
        try:
            remote_path = PurePosixPath(remote_path)
            if not self.remote_item_exists(remote_path):
                raise ValueError("remove_remote_item: remote_path does not exist.")

            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)
                with ssh.open_sftp() as sftp:
                    try:
                        # Check if it's a directory
                        if sftp.listdir(remote_path.as_posix()):
                            self.logger.warning(
                                f"Cannot remove non-empty directory: {remote_path}. "
                                f"Provide full path to file to remove it, or empty the directory first."
                            )
                            return
                        sftp.rmdir(remote_path.as_posix())
                        self.logger.info(f"Removed directory: {remote_path}")
                    except IOError:
                        # Not a directory → try removing as a file
                        try:
                            sftp.remove(remote_path.as_posix())
                            self.logger.info(f"Removed file: {remote_path}")
                        except Exception as err:
                            self.logger.error(f"Failed to remove file: {remote_path}: {err}")
                            return

                    # Optionally prune empty parent directories
                    if recursive:
                        parent = remote_path.parent
                        while str(parent) not in ('', '/'):
                            try:
                                if not sftp.listdir(parent.as_posix()):
                                    sftp.rmdir(parent.as_posix())
                                    self.logger.info(f"Pruned empty parent directory: {parent}")
                                    parent = parent.parent
                                else:
                                    break
                            except Exception as err:
                                self.logger.warning(f"Could not check or prune parent {parent}: {err}")
                                break
        except Exception as err:
            self.logger.error(f"remove_remote_item: {err}")


    def setup_remote_path(self, remote_path: Union[str, PurePosixPath]) -> None:
        """Create (and navigate to the leaf of) a remote path.

        Args:
            remote_path (str, PurePosixPath): Remote path to create. NB: The last bit of the path is always interpreted as a directory

        """
        try:
            remote_path = PurePosixPath(remote_path)
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)
                with ssh.open_sftp() as sftp:
                    # create remote path if it doesn't exist and enter it
                    try:
                        sftp.chdir(remote_path.as_posix())
                    except IOError:
                        parts = remote_path.parts
                        current_path = '.'
                        for part in parts:
                            if part:
                                current_path = f"{current_path}/{part}"
                            try:
                                sftp.chdir(current_path)
                            except IOError:
                                sftp.mkdir(part)
                                sftp.chdir(part)
                                self.logger.debug(f"setup_remote_path: created {part}")
                    self.cwd = sftp.getcwd()
                    self.logger.debug(f"setup_remote_path: switched to {self.cwd}")
                    if self.cwd is None:
                        self.cwd = PurePosixPath()
            return
        except Exception as err:
            self.logger.error(f"setup_remote_path: {err}")


    def transfer_files(
        self,
        remove_on_success: bool = True,
        local_path: Optional[Path] = None,
        remote_path: Optional[PurePosixPath] = None,
    ) -> None:
        """
        Transfer all files from local_path and its subfolders to remote_path.

        Args:
            remove_on_success (bool): If True, delete local files after successful transfer.
            local_path (str | None): Full path to local directory. Defaults to self.local_path.
            remote_path (str | None): Base path on remote host. Defaults to self.remote_path.
                                    The last element must be a directory.
        """
        try:
            self.transfered_local = []
            self.transfered_remote = []

            local_base = Path(local_path or self.local_path).resolve()
            if local_base.is_file():
                # If local_base is a file, convert it to a directory containing that file
                local_base = local_base.parent
            elif not local_base.is_dir():
                raise ValueError(f"Local path '{local_base}' is not a valid directory or file.")
            if not local_base.exists():
                raise FileNotFoundError(f"Local path '{local_base}' does not exist.")
            remote_base = PurePosixPath(remote_path or self.remote_path)

            self.logger.info(f"Starting SFTP file transfer: {local_base} -> {remote_base}")

            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)

                with ssh.open_sftp() as sftp:
                    for root, _, files in os.walk(local_base):
                        if not files:
                            continue

                        rel_path = Path(root).relative_to(local_base).as_posix()
                        remote_dir = remote_base / rel_path

                        # Ensure remote subdirectory exists
                        self.setup_remote_path(remote_dir)

                        for file in files:
                            local_file = Path(root) / file
                            remote_file = remote_dir / file

                            try:
                                attr = sftp.put(
                                    localpath=local_file.as_posix(),
                                    remotepath=remote_file.as_posix(),
                                    confirm=True
                                )
                                self.logger.debug(f"transfered {local_file.as_posix()} -> {remote_file.as_posix()}")
                                self.transfered_local.append(local_file.as_posix())
                                self.transfered_remote.append(remote_file.as_posix())

                                if remove_on_success:
                                    local_size = local_file.stat().st_size
                                    remote_size = attr.st_size
                                    if remote_size == local_size:
                                        local_file.unlink()
                                        self.logger.debug(f"Removed local file: {local_file}")
                                    else:
                                        self.logger.warning(
                                            f"Size mismatch: {local_file} ({local_size}) != {remote_file} ({remote_size}). File not removed."
                                        )
                            except Exception as file_err:
                                self.logger.error(f"Failed to transfer {local_file} -> {remote_file}: {file_err}")
            return

        except Exception as err:
            self.logger.error(f"transfer_files failed: {err}")


    def setup_transfer_schedules(
        self,
        remove_on_success: bool=True,
        interval: int=60,
        local_path: Optional[str] = None,
        remote_path: Optional[str] = None,
        delay_transfer: int = 5,            # seconds (offset after boundary)
        ) -> None:
        """
        Schedule directory uploads at fixed intervals (minutes), aligned to boundaries:
          - 10    -> every 10 minutes at 00,10,20,30,40,50 + delay_transfer seconds
          - n*60  -> every n hours at :00 + delay_transfer seconds
          - 1440  -> daily at 00:00 + delay_transfer seconds
        """
        try:
            if not (0 <= delay_transfer <= 59):
                raise ValueError("delay_transfer must be between 0 and 59 seconds")

            def _sftp_transfer_files():
                # prevent overlap if a previous upload is still running
                if not self._sched_lock.acquire(blocking=False):
                    self.schedule_logger.warning("Skipping SFTP transfer: previous run still active")
                    return
                try:
                    # if min_age_seconds is not None:
                    #     # only if your transfer_files() supports this parameter
                    #     kwargs["min_age_seconds"] = int(min_age_seconds)
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
                raise ValueError("'interval' must be 10 minutes, a multiple of 60 minutes (<1440), or 1440.")

            self.schedule_logger.debug(
                "Scheduled SFTP transfer: interval=%s, local=%s, remote=%s, delay=%ss", # min_age=%s",
                interval, local_path, remote_path, delay_transfer, # min_age_seconds
            )
        except Exception as err:
            self.schedule_logger.error(err)        
        

if __name__ == "__main__":
    pass
