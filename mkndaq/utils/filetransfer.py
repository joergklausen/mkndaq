#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Manage files. Currently, sftp transfer to MeteoSwiss is supported.

@author: joerg.klausen@meteoswiss.ch
"""

import os
import logging
import re
import zipfile

import pysftp
import shutil
import sockslib
import time

import colorama

class SFTPClient:
    """
    SFTP based file handling, optionally using SOCKS5 proxy.

    Available methods include
    - put_r(): recursively put files
    - move_r(): recursively move files
    """

    _zip = None
    _logs = None
    _staging = None
    _logfile = None
    _log = False
    _logger = None
    _sftpkey = None
    _sftpusr = None
    _sftphost = None

    @classmethod
    def __init__(cls, config: dict):
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
        print("# Initialize SFTPClient")
        try:

            # setup logging
            if config['logs']:
                cls._log = True
                cls._logs = os.path.expanduser(config['logs'])
                os.makedirs(cls._logs, exist_ok=True)
                cls._logfile = '%s.log' % time.strftime('%Y%m%d')
                cls._logfile = os.path.join(cls._logs, cls._logfile)
                cls._logger = logging.getLogger(__name__)
                logging.basicConfig(level=logging.DEBUG,
                                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                                    datefmt='%y-%m-%d %H:%M:%S',
                                    filename=str(cls._logfile),
                                    filemode='a')
                logging.getLogger('paramiko.transport').setLevel(level=logging.ERROR)

            # sftp settings
            cls._sftphost = config['sftp']['host']
            cls._sftpusr = config['sftp']['usr']
            cls._sftpkey = config['sftp']['key']

            # configure client proxy if needed
            if config['sftp']['proxy']['socks5']:
                with sockslib.SocksSocket() as sock:
                    sock.set_proxy((config['sftp']['proxy']['socks5'],
                                    config['sftp']['proxy']['port']), sockslib.Socks.SOCKS5)

            # configure staging
            cls._staging = os.path.expanduser(config['staging']['path'])
            cls._staging = re.sub(r'(/?\.?\\){1,2}', '/', cls._staging)
            cls._zip = config['staging']['zip']

        except Exception as err:
            if cls._log:
                cls._logger.error(err)
            print(err)

    @classmethod
    def test_connection(cls) -> bool:
        """Test connection to sftp server.

        Returns:
            bool: [description]
        """
        try:
            with pysftp.Connection(cls._sftphost, username=cls._sftpusr, password=cls._sftpkey) as conn:
                conn.put(None)
                # sftp.close()
                return True
        except Exception as err:
            print(err)
            return False

    @classmethod
    def localfiles(cls, localpath=None) -> list:
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
            localpath = cls._staging

        def store_files_name(name):
            fnames.append(name)

        def store_dir_name(name):
            dnames.append(name)

        def store_other_file_types(name):
            onames.append(name)

        try:
            pysftp.walktree(localpath, store_files_name, store_dir_name, store_other_file_types)
            # tidy up names
            # dnames = [re.sub(r'(/?\.?\\){1,2}', '/', s) for s in dnames]
            fnames = [re.sub(r'(/?\.?\\){1,2}', '/', s) for s in fnames]
            # onames = [re.sub(r'(/?\.?\\){1,2}', '/', s) for s in onames]

            return fnames

        except Exception as err:
            if cls._log:
                cls._logger.error(err)
            print(err)

    @classmethod
    def stage_current_log_file(cls) -> None:
        """
        Stage the most recent file.

        :return:
        """
        try:
            root = os.path.join(cls._staging, os.path.basename(cls._logs))
            os.makedirs(root, exist_ok=True)
            if cls._zip:
                # create zip file
                archive = os.path.join(root, "".join([os.path.basename(cls._logfile[:-4]), ".zip"]))
                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                    fh.write(cls._logfile, os.path.basename(cls._logfile))
            else:
                shutil.copyfile(cls._logfile, os.path.join(root, os.path.basename(cls._logfile)))

        except Exception as err:
            if cls._log:
                cls._logger.error(err)
            print(err)

    @classmethod
    def stage_current_config_file(cls, config_file: str) -> None:
        """
        Stage the most recent file.

        :param: str config_file: path to config file
        :return:
        """
        try:
            os.makedirs(cls._staging, exist_ok=True)
            if cls._zip:
                # create zip file
                archive = os.path.join(cls._staging, "".join([os.path.basename(config_file[:-4]), ".zip"]))
                with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as fh:
                    fh.write(config_file, os.path.basename(config_file))
            else:
                shutil.copyfile(config_file, os.path.join(cls._staging, os.path.basename(config_file)))

        except Exception as err:
            if cls._log:
                cls._logger.error(err)
            print(err)

    @classmethod
    def put(cls, localpath, remotepath, preserve_mtime=True) -> None:
        try:
            msg = "%s .put %s > %s" % (time.strftime('%Y-%m-%d %H:%M:%S'), localpath, remotepath)
            with pysftp.Connection(host=cls._sftphost, username=cls._sftpusr, private_key=cls._sftpkey) as conn:
                res = conn.put(localpath=localpath, remotepath=remotepath, confirm=True, preserve_mtime=preserve_mtime)
                print(msg)
                print(res)
                cls._logger.info(msg)

        except Exception as err:
            if cls._log:
                cls._logger.error(err)
            print(err)

    @classmethod
    def file_exists(cls, remotepath) -> bool:
        try:
#            msg = "%s .put %s > %s" % (time.strftime('%Y-%m-%d %H:%M:%S'), localpath, remotepath)
            with pysftp.Connection(host=cls._sftphost, username=cls._sftpusr, private_key=cls._sftpkey) as conn:
                res = conn.isfile(remotepath=remotepath)
            return res

        except Exception as err:
            print(err)

    @classmethod
    def put_r(cls, localpath, remotepath, preserve_mtime=True) -> None:
        """
        Recursively transfer (copy) all files from localpath to remotepath. Note: At present, parent elements of remote path must already exist.

        :param str localpath:
        :param str remotepath:
        :param bln preserve_mtime: see pysftp documentation
        :return: Nothing
        """
        try:
            conn = pysftp.Connection(host=cls._sftphost, username=cls._sftpusr, private_key=cls._sftpkey)

            # sanitize localpath
            localpath = re.sub(r'(/?\.?\\){1,2}', '/', localpath)

            # make sure remote directory structure is complete
            for root, dirs, files in os.walk(localpath):
                root = re.sub(r'(/?\.?\\){1,2}', '/', root)
                try:
                    conn.mkdir(remotepath)
                except OSError:
                    pass
                for _dir in dirs:
                    _dir = re.sub(r'(/?\.?\\){1,2}', '/', _dir)
                    _dir = re.sub("".join([localpath, "/"]), "", "/".join([root, _dir]))
                    if remotepath:
                        remoteitem = "/".join([remotepath, _dir])
                    else:
                        remoteitem = _dir
                    try:
                        conn.mkdir(remoteitem)
                    except OSError:
                        pass

            # copy all local files to remote host
            for root, dirs, files in os.walk(localpath):
                root = re.sub(r'(/?\.?\\){1,2}', '/', root)
                for localitem in files:
                    localitem = re.sub(r'(/?\.?\\){1,2}', '/', os.path.join(root, localitem))
                    remoteitem = "/".join([remotepath, re.sub("".join([localpath, "/"]), "", localitem)])
                    msg = "%s .put_r %s > %s" % (time.strftime('%Y-%m-%d %H:%M:%S'), localitem, remoteitem)
                    print(msg)
                    conn.put(localpath=localitem, remotepath=remoteitem, confirm=True, preserve_mtime=preserve_mtime)
                    cls._logger.info(msg)
            conn.close()

        except Exception as err:
            if cls._log:
                cls._logger.error(err)
            print(err)

    @classmethod
    def xfer_r(cls, localpath=None, remotepath=None, preserve_mtime=True) -> None:
        """
        Recursively transfer (move) all files from localpath to remotepath. Note: At present, parent elements of remote path must already exist.

        :param str localpath:
        :param str remotepath:
        :param bln preserve_mtime: see pysftp documentation
        :return: Nothing
        """
        try:
            if localpath is None:
                localpath = cls._staging

            if remotepath is None:
                remotepath = '.'

            print(".xfer_r (source: %s, target: %s/%s/%s)" % (localpath, cls._sftphost, cls._sftpusr, remotepath))
            conn = pysftp.Connection(host=cls._sftphost, username=cls._sftpusr, private_key=cls._sftpkey)

            # sanitize localpath
            localpath = re.sub(r'(/?\.?\\){1,2}', '/', localpath)

            # make sure remote directory structure is complete
            for root, dirs, files in os.walk(localpath):
                root = re.sub(r'(/?\.?\\){1,2}', '/', root)
                try:
                    conn.mkdir(remotepath)
                except OSError:
                    pass
                for _dir in dirs:
                    _dir = re.sub(r'(/?\.?\\){1,2}', '/', _dir)
                    _dir = re.sub("".join([localpath, "/"]), "", "/".join([root, _dir]))
                    if remotepath:
                        remoteitem = "/".join([remotepath, _dir])
                    else:
                        remoteitem = _dir
                    try:
                        conn.mkdir(remoteitem)
                    except OSError:
                        pass

            # copy all local files to remote host
            for root, dirs, files in os.walk(localpath):
                root = re.sub(r'(/?\.?\\){1,2}', '/', root)
                for localitem in files:
                    localitem = re.sub(r'(/?\.?\\){1,2}', '/', os.path.join(root, localitem))
                    remoteitem = "/".join([remotepath, re.sub("".join([localpath, "/"]), "", localitem)])
                    conn.put(localpath=localitem, remotepath=remoteitem, confirm=True, preserve_mtime=preserve_mtime)
                    if conn.isfile(remoteitem):
                        msg = "%s %s > %s okay." % (time.strftime('%Y-%m-%d %H:%M:%S'), localitem, remoteitem)
                        os.remove(localitem)
                        print(msg)
                    else:
                        msg = "%s %s > %s failed." % (time.strftime('%Y-%m-%d %H:%M:%S'), localitem, remoteitem)
                        print(colorama.Fore.RED + msg)
                        cls._logger.info(msg)
            conn.close()

        except Exception as err:
            if cls._log:
                cls._logger.error(err)
            print(err)


if __name__ == "__main__":
    pass
