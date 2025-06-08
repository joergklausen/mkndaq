#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Manage file transfer. Currently, sftp transfer to MeteoSwiss is supported.

@author: joerg.klausen@meteoswiss.ch
"""
import logging
import os
import re
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
                Path(config['sftp']['key']).expanduser())

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
                self.local_path = self.local_path / config[name]['staging']
                self.remote_path = self.remote_path / config[name]['remote']

        except Exception as err:
            self.logger.error(err)


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


    def list_local_files(self, local_path: str=str()) -> list:
        """Establish list of local files.

        Args:
            localpath (str, optional): Absolute path to directory containing folders and files. Defaults to str().

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
        """Check on remote server if an item exists. Assume this indicates successful transfer.

        Args:
            remote_path (str): path to remote item

        Returns:
            Boolean: True if item exists, False otherwise.
        """
        try:
            remote_path = PurePosixPath(remote_path)
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)
                with ssh.open_sftp() as sftp:
                    try:
                        sftp.stat(str(remote_path))
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


    def setup_remote_folders(self, local_path: Optional[str] = None, remote_path: Optional[str] = None) -> None:
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

                        rel_root = Path(root).relative_to(local_base)
                        remote_dir = remote_base / PurePosixPath(rel_root.as_posix())

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


    def put_file(self, local_path: str, remote_path: Union[str, PurePosixPath]) -> Optional[paramiko.SFTPAttributes]:
        """
        Send a file to a remote host using SFTP.

        Args:
            local_path (str): Full path to local file.
            remote_path (str | PurePosixPath): Remote directory or full remote path.

        Returns:
            paramiko.SFTPAttributes | None: Attributes of the transferred file if successful.
        """
        try:
            local_file = Path(local_path).resolve()
            if not local_file.exists():
                raise FileNotFoundError(f"Local file does not exist: {local_file}")

            # Normalize remote path and ensure it's a full remote file path
            if isinstance(remote_path, str):
                remote_path = PurePosixPath(remote_path)
            if remote_path.name != local_file.name:
                remote_path = remote_path / local_file.name

            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)

                with ssh.open_sftp() as sftp:
                    attr = sftp.put(
                        localpath=str(local_file),
                        remotepath=str(remote_path),
                        confirm=True
                    )
                    self.logger.info(f"put_file {local_file} -> {remote_path}")
                    return attr

        except Exception as err:
            self.logger.error(f"put_file failed: {local_path} -> {remote_path}: {err}")
            return None


    def remove_remote_item(self, remote_path: str) -> None:
        """
        Remove a file or prune (the last part of remote_path, not iterative) an (empty) directory from a remote host using SFTP and SSH.

        Args:
            remote_path (str): relative path to remote item
        """
        try:
            remote_path = remote_path.replace('\\', '/')
            if self.remote_item_exists(remote_path):
                with paramiko.SSHClient() as ssh:
                    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)
                    with ssh.open_sftp() as sftp:
                        try:
                            if sftp.listdir(remote_path):
                                # neither an empty directory, nor a file: do nothing
                                self.logger.warning('Cannot remove non-empty directory. Provide full path to file to remove it, or empty the directory first.')
                                return
                            else:
                                # remote path is an empty directory
                                sftp.rmdir(remote_path)
                        except:
                            # remote_path is a file
                            try:
                                sftp.remove(remote_path)
                            except Exception as err:
                                self.logger.error(err)
                        self.logger.info(f"remove_remote_item {remote_path}")
                        sftp.close()
            else:
                raise ValueError("remove_remote_item: remote_path does not exist.")
        except Exception as err:
            self.logger.error(f"remove_remote_item: {err}")


    def setup_remote_path(self, remote_path: str) -> str:
        """Create (and navigate to the leaf of) a remote path.

        Args:
            remote_path (str): Remote path to create. NB: The last bit of the path is always interpreted as a directory

        Returns:
            str: full path of current remote directory
        """
        try:
            remote_path = remote_path.replace('\\', '/').replace('./', '')
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)
                with ssh.open_sftp() as sftp:
                    # create remote path if it doesn't exist and enter it
                    try:
                        sftp.chdir(remote_path)
                    except IOError:
                        parts = remote_path.split("/")
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
                    cwd = sftp.getcwd()
                    self.logger.debug(f"setup_remote_path: switched to {cwd}")
                    if cwd is None:
                        cwd = str()
            return cwd
        except Exception as err:
            self.logger.error(f"setup_remote_path: {err}")
            return str()


    def transfer_files(
        self,
        remove_on_success: bool = True,
        local_path: Optional[str] = None,
        remote_path: Optional[str] = None,
    ) -> Union[int, None]:
        """
        Transfer all files from local_path and its subfolders to remote_path.

        Args:
            remove_on_success (bool): If True, delete local files after successful transfer.
            local_path (str | None): Full path to local directory. Defaults to self.local_path.
            remote_path (str | None): Base path on remote host. Defaults to self.remote_path.
                                    The last element must be a directory.
        """
        try:
            self.transferred = []

            local_base = Path(local_path or self.local_path).resolve()
            remote_base = PurePosixPath(remote_path or self.remote_path)

            self.logger.info(f"Starting file transfer: {local_base} -> {remote_base}")

            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self.host, username=self.usr, pkey=self.key)

                with ssh.open_sftp() as sftp:
                    for root, _, files in os.walk(local_base):
                        if not files:
                            continue

                        rel_root = Path(root).relative_to(local_base)
                        remote_dir = remote_base / PurePosixPath(rel_root.as_posix())

                        # Ensure remote subdirectory exists
                        self.setup_remote_path(str(remote_dir))

                        for file in files:
                            local_file = Path(root) / file
                            remote_file = remote_dir / file

                            try:
                                attr = sftp.put(
                                    localpath=str(local_file),
                                    remotepath=str(remote_file),
                                    confirm=True
                                )
                                self.logger.debug(f"Transferred {local_file} -> {remote_file}")
                                self.transferred.append(str(local_file))

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
            return len(self.transferred)

        except Exception as err:
            self.logger.error(f"transfer_files failed: {err}")


    def setup_transfer_schedules(self,
                                 remove_on_success: bool=True,
                                 interval: int=60,
                                 local_path: Optional[str] = None,
                                 remote_path: Optional[str] = None,
                                 ):
        try:
            if interval==10:
                minutes = [f"{interval*n:02}" for n in range(6) if interval*n < 6]
                for minute in minutes:
                    schedule.every(1).hour.at(f"{minute}:10").do(self.transfer_files, remove_on_success, local_path, remote_path)
            elif (interval % 60) == 0:
                hrs = [f"{n:02}:00:10" for n in range(0, 24, interval // 60)]
                for hr in hrs:
                    schedule.every(1).day.at(hr).do(self.transfer_files, remove_on_success, local_path, remote_path)
            elif interval==1440:
                schedule.every(1).day.at('00:00:10').do(self.transfer_files, remove_on_success, local_path, remote_path)
            else:
                raise ValueError("'interval' must be 10 minutes or a multiple of 60 minutes and a maximum of 1440 minutes.")

        except Exception as err:
            self.schedule_logger.error(err)


if __name__ == "__main__":
    pass
