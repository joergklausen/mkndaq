#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Manage files. Currently, sftp transfer to MeteoSwiss is supported.

@author: joerg.klausen@meteoswiss.ch
"""
import logging
import os
import re
# import pysftp
import shutil
import time
import zipfile
from xmlrpc.client import Boolean

import colorama
# import sockslib
import paramiko


class SFTPClient:
    """
    SFTP based file handling, optionally using SOCKS5 proxy.

    Available methods include
    - is_alive():
    - localfiles():
    - stage_current_log_file():
    - stage_current_config_file():
    - setup_remote_folders():
    - put_r(): recursively put files
    - xfer_r(): recursively move files
    """
    def __init__(self, config: dict):
        """
        Initialize class.

        :param config: configuration
                    config['sftp']['host']:
                    config['sftp']['usr']:
                    config['sftp']['key']:
                    config['sftp']['proxy']['socks5']:
                    config['sftp']['proxy']['port']:
                    config['sftp']['logs']: relative path of log file, or empty
                    config['staging']['path']: relative path of staging area
        """
        colorama.init(autoreset=True)
        try:
            # configure logging
            _logger = f"{os.path.basename(config['logging']['file'])}".split('.')[0]
            self.logger = logging.getLogger(f"{_logger}.{__name__}")
            self.schedule_logger = logging.getLogger(f"{_logger}.schedule")
            self.schedule_logger.setLevel(level=logging.DEBUG)
            
            self.logger.info("Initialize SFTPClient")

            # sftp settings
            self._sftphost = config['sftp']['host']
            self._sftpusr = config['sftp']['usr']
            self._sftpkey = paramiko.RSAKey.from_private_key_file(\
                os.path.expanduser(config['sftp']['key']))

            # # configure client proxy if needed
            # if config['sftp']['proxy']['socks5']:
            #     with sockslib.SocksSocket() as sock:
            #         sock.set_proxy((config['sftp']['proxy']['socks5'],
            #                         config['sftp']['proxy']['port']), sockslib.Socks.SOCKS5)

            # configure staging
            self._staging = os.path.expanduser(config['staging']['path'])
            self._staging = re.sub(r'(/?\.?\\){1,2}', '/', self._staging)
            self._zip = config['staging']['zip']

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
                ssh.connect(hostname=self._sftphost, username=self._sftpusr, pkey=self._sftpkey)

                with ssh.open_sftp() as sftp:
                    sftp.close()
            return True
        except Exception as err:
            print(err)
            return False


    def localfiles(self, localpath: str=str()) -> list:
        """Establish list of local files.

        Args:
            localpath ([type], optional): [description]. Defaults to None.

        Returns:
            list: [description]
        """
        fnames = []
        dnames = []
        onames = []

        if localpath is None:
            localpath = self._staging

        try:
            root, dnames, fnames = os.walk(localpath)
            # pysftp.walktree(localpath, store_files_name, store_dir_name, store_other_file_types)
            # tidy up names
            # dnames = [re.sub(r'(/?\.?\\){1,2}', '/', s) for s in dnames]
            # fnames = [re.sub(r'(/?\.?\\){1,2}', '/', s) for s in fnames]
            fnames = [s.replace(old='\\', new='/') for s in fnames]
            # onames = [re.sub(r'(/?\.?\\){1,2}', '/', s) for s in onames]

            return fnames

        except Exception as err:
            self.logger.error(err)
            return list()


    # def stage_current_log_file(self) -> None:
    #     """
    #     Stage the most recent file.

    #     :return:
    #     """
    #     try:
    #         root = os.path.join(self._staging, os.path.basename(self._logs))
    #         os.makedirs(root, exist_ok=True)
    #         if self._zip:
    #             # create zip file
    #             archive = os.path.join(root, "".join([os.path.basename(self._logfile[:-4]), ".zip"]))
    #             with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
    #                 fh.write(self._logfile, os.path.basename(self._logfile))
    #         else:
    #             shutil.copyfile(self._logfile, os.path.join(root, os.path.basename(self._logfile)))

    #     except Exception as err:
    #         self.logger.error(err)


    def stage_current_config_file(self, config_file: str) -> None:
        """
        Stage the most recent file.

        :param: str config_file: path to config file
        :return:
        """
        try:
            os.makedirs(self._staging, exist_ok=True)
            if self._zip:
                # create zip file
                archive = os.path.join(self._staging, os.path.basename(config_file).replace(".cfg", ".zip"))
                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                    fh.write(config_file, os.path.basename(config_file))
            else:
                shutil.copyfile(config_file, os.path.join(self._staging, os.path.basename(config_file)))

        except Exception as err:
            self.logger.error(err)


    def put(self, localpath, remotepath) -> None:
        """Send a file to a remotehost using SFTP and SSH.

        Args:
            localpath (str): full path to local file
            remotepath (str): relative path to remotefile
        """
        try:
            remotepath = re.sub(r'(/?\.?\\){1,2}', '/', remotepath)
            msg = f"{time.strftime('%Y-%m-%d %H:%M:%S')} .put {localpath} > {remotepath}"
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self._sftphost, username=self._sftpusr, pkey=self._sftpkey)
                with ssh.open_sftp() as sftp:
                    sftp.put(localpath=localpath, remotepath=remotepath, confirm=True)
                    sftp.close()
                print(msg)
                self.logger.info(msg)

        except Exception as err:
            self.logger.error(err)


    def remote_item_exists(self, remoteitem: str) -> Boolean:
        """Check on remote server if an item exists. Assume this indicates successful transfer.

        Args:
            remoteitem (str): path to remote item

        Returns:
            Boolean: True if item exists, False otherwise.
        """
        try:
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self._sftphost, username=self._sftpusr, pkey=self._sftpkey)
                with ssh.open_sftp() as sftp:
                    if sftp.stat(remoteitem).st_size > 0:
                        return True
                    else:
                        return False
        except Exception as err:
            self.logger.error(err)


    def setup_remote_folders(self, localpath=None, remotepath=None) -> None:
        """
        Determine directory structure under localpath and replicate on remote host.

        :param str localpath:
        :param str remotepath:
        :return: Nothing
        """
        try:
            if localpath is None:
                localpath = self._staging

            # sanitize localpath
            localpath = re.sub(r'(/?\.?\\){1,2}', '/', localpath)

            if remotepath is None:
                remotepath = '.'

            # sanitize remotepath
            remotepath = re.sub(r'(/?\.?\\){1,2}', '/', remotepath)

            self.logger.info(f".setup_remote_folders (source: {localpath}, target: {remotepath})")

            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self._sftphost, username=self._sftpusr, pkey=self._sftpkey)
                with ssh.open_sftp() as sftp:
                    # determine local directory structure, establish same structure on remote host
                    for dirpath, dirnames, filenames in os.walk(top=localpath):
                        dirpath = re.sub(r'(/?\.?\\){1,2}', '/', dirpath).replace(localpath, remotepath)
                        try:
                            sftp.mkdir(dirpath, mode=16877)
                        except OSError:
                            pass
                    sftp.close()

        except Exception as err:
            self.logger.error(err)


    def xfer_r(self, localpath: str=str(), remotepath: str=str()) -> None:
        """
        Recursively transfer (move) all files from localpath to remotepath. Note: At present, parent elements of remote path must already exist.

        :param str localpath:
        :param str remotepath:
        :param bln preserve_mtime: see pysftp documentation
        :return: Nothing
        """
        try:
            if localpath is None:
                localpath = self._staging

            # sanitize localpath
            # localpath = re.sub(r'(/?\.?\\){1,2}', '/', localpath)

            if remotepath is None:
                remotepath = '.mkn'

            self.logger.info(f" .xfer_r (source: {localpath}, target: {self._sftphost}/{self._sftpusr}/{remotepath})")

            localitem = None
            remoteitem = None
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=self._sftphost, username=self._sftpusr, pkey=self._sftpkey)
                with ssh.open_sftp() as sftp:
                    # walk local directory structure, put file to remote location
                    for dirpath, dirnames, filenames in os.walk(top=localpath):
                        for filename in filenames:
                            localitem = os.path.join(dirpath, filename)
                            remoteitem = os.path.join(dirpath.replace(localpath, remotepath), filename)
                            remoteitem = re.sub(r'(\\){1,2}', '/', remoteitem)
                            msg = "%s .put %s > %s" % (time.strftime('%Y-%m-%d %H:%M:%S'),
                                                       localitem.replace(localpath, ''), remoteitem)
                            res = sftp.put(localpath=localitem, remotepath=remoteitem, confirm=True)
                            self.logger.info(msg)

                            # remove local file if it exists on remote host.
                            try:
                                localsize = os.stat(localitem).st_size
                                remotesize = res.st_size
                                self.logger.debug("localitem size: %s, remoteitem size: %s" % (localsize, remotesize))
                                if remotesize == localsize:
                                    os.remove(localitem)
                            except Exception as err:
                                msg = "%s not found on remote host, will try again later." % remoteitem
                                self.logger.error(colorama.Fore.RED + msg)

        except Exception as err:
            msg = "%s %s > %s failed." % (time.strftime('%Y-%m-%d %H:%M:%S'), localitem, remoteitem)
            self.logger.error(colorama.Fore.RED + msg)


if __name__ == "__main__":
    pass
