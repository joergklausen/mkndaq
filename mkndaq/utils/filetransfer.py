#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Manage files. Currently, sftp transfer to MeteoSwiss is supported.

@author: joerg.klausen@meteoswiss.ch
"""

import os
import logging
import re
import pysftp
import sockslib
import time
from mkndaq.utils import configparser


class SFTPClient:
    """
    SFTP based file handling, optionally using SOCKS5 proxy.

    Available methods include
    - put_r(): recursively put files
    - move_r(): recursively move files
    """

    _log = False
    _logger = None
    _localpath = None
    _remotepath = None
    _sftpkey = None
    _sftpusr = None
    _sftphost = None

    @classmethod
    def __init__(cls, config=None):
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
        print("# Initialize SFTPClient")
        try:
            if config is None:
                config = configparser.config()

            # setup logging
            if config['sftp']['logs']:
                cls._log = True
                logs = os.path.expanduser(config['sftp']['logs'])
                os.makedirs(logs, exist_ok=True)
                logfile = '%s.log' % time.strftime('%Y%m%d')
                logfile = os.path.join(logs, logfile)
                cls._logger = logging.getLogger(__name__)
                logging.basicConfig(level=logging.DEBUG,
                                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                                    datefmt='%y-%m-%d %H:%M:%S',
                                    filename=str(logfile),
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

            # configure localpath, remotepath
            cls._localpath = os.path.expanduser(config['staging']['path'])
            cls._localpath = re.sub(r'(/?\.?\\){1,2}', '/', cls._localpath)
            cls._remotepath = os.path.basename(config['data'])

        except Exception as err:
            if cls._log:
                cls._logger.error(err)
            else:
                print(err)

    @classmethod
    def localfiles(cls, localpath=None) -> list:
        fnames = []
        dnames = []
        onames = []

        if localpath is None:
            localpath = cls._localpath

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
            else:
                print(err)

    @classmethod
    def remotefiles(cls, remotepath=None) -> list:
        fnames = []
        dnames = []
        onames = []

        if remotepath is None:
            remotepath = '.'

        def store_files_name(name):
            fnames.append(name)

        def store_dir_name(name):
            dnames.append(name)

        def store_other_file_types(name):
            onames.append(name)

        try:
            with pysftp.Connection(host=cls._sftphost, username=cls._sftpusr, private_key=cls._sftpkey) as conn:
                conn.walktree(remotepath, store_files_name, store_dir_name, store_other_file_types)
                # tidy up names
                # dnames = [re.sub(r'(/?\.?\\){1,2}', '/', s) for s in dnames]
                fnames = [re.sub(r'(/?\.?\\){1,2}', '/', s) for s in fnames]
                # onames = [re.sub(r'(/?\.?\\){1,2}', '/', s) for s in onames]

            return fnames

        except Exception as err:
            if cls._log:
                cls._logger.error(err)
            else:
                print(err)

    @classmethod
    def put(cls, localpath, remotepath, preserve_mtime=True) -> None:
        try:
            with pysftp.Connection(host=cls._sftphost, username=cls._sftpusr, private_key=cls._sftpkey) as conn:
                msg = "sftp %s > %s" % (localpath, remotepath)
                print(msg)
                conn.put(localpath=localpath, remotepath=remotepath, confirm=True, preserve_mtime=preserve_mtime)
                cls._logger.info(msg)

        except Exception as err:
            if cls._log:
                cls._logger.error(err)
            else:
                print(err)

    @classmethod
    def put_r(cls, localpath, remotepath, preserve_mtime=True) -> None:
        """
        Recursively transfer all files from localpath to remotepath.
        Note: At present, parent elements of remote path must already exist.

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
                for dir in dirs:
                    dir = re.sub(r'(/?\.?\\){1,2}', '/', dir)
                    dir = re.sub("".join([localpath, "/"]), "", "/".join([root, dir]))
                    if remotepath:
                        remoteitem = "/".join([remotepath, dir])
                    else:
                        remoteitem = dir
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
                    msg = "sftp %s > %s" % (localitem, remoteitem)
                    print(msg)
                    conn.put(localpath=localitem, remotepath=remoteitem, confirm=True, preserve_mtime=preserve_mtime)
                    cls._logger.info(msg)
            conn.close()

        except Exception as err:
            if cls._log:
                cls._logger.error(err)
            else:
                print(err)

    @classmethod
    def remove_uploaded_files(cls, localpath=None, remotepath=None) -> None:
        """
        Compare files in localpath with remote path, remove duplicates from localpath

        :param localpath: local path from which to transfer directories and files
        :param remotepath: remote path to transfer files to
        :return:
        """
        if localpath is None:
            localpath = cls._localpath

        try:
            staged = cls.localfiles(localpath)
            staged = [re.sub(cls._localpath, '.', s) for s in staged]
            remote = cls.remotefiles(remotepath)

            # compare lists, find duplicates, then remove from staging
            xfered = set(remote).intersection(staged)
            for ele in xfered:
                os.remove(re.sub('\\./', "".join([cls._localpath, '/']), ele))
            if cls._log:
                cls._logger.info("Finished transfering %s" % xfered)

        except Exception as err:
            if cls._log:
                cls._logger.error(err)
            else:
                print(err)

    @classmethod
    def move_r(cls, localpath=None, remotepath=None) -> None:
        """
        Recursively put files using sftp, then verify existence of remote files and remove local copies.

        :param localpath: local path from which to transfer directories and files
        :param remotepath: remote path to transfer files to
        :return:
        """
        if localpath is None:
            localpath = cls._localpath

        if remotepath is None:
            remotepath = cls._remotepath

        print(".move_r (source: %s, target: %s/%s/%s)" % (localpath, cls._sftphost, cls._sftpusr, remotepath))
        cls.put_r(localpath, remotepath)
        cls.remove_uploaded_files(localpath, remotepath)


if __name__ == "__main__":
    pass
