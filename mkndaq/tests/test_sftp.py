#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Created on Tue Feb 11 18:37:35 2020.

@author: jkl
"""

import os
from mkndaq.utils import configparser
from mkndaq.utils.filetransfer import SFTPClient

def main():
    """Test suite for sftp."""
    config_file = input("Enter full path to config file or <Enter> for default: ")
    if config_file == "":
        config_file = "dist/mkndaq.cfg"
    config = configparser.config(config_file)

    sftp = SFTPClient(config)

    menu = "[1] Test connection to sftp server\n"
    menu += "[2] Transfer files in some other folder\n"
    menu += "[4] Verify remote file exists (.file_exists)\n"
    menu += "[5] Transfer test file and verify transfer\n"

    choice = input(menu)
    if choice == '1':
        # test connection to sftp server
        remotepath = '.'
        if sftp.is_alive():
            print("SFTP server is alive.")
        else:
            print("SFTP not reachable.")

    if choice == '2':
        # transfer folder with subfolder(s)
        localpath = os.path.expanduser(config['staging']['path'])
        remotepath = './test'
        print("Transfering folder(s) %s > %s" % (localpath, remotepath))
        sftp.setup_remote_folders(localpath=localpath)
        sftp.xfer_r(localpath=localpath, remotepath=remotepath, preserve_mtime=True)

    if choice == '3':
        # transfer files in folder(s)
        localpath = os.path.expanduser("~/Public/git/mkndaq/mkndaq/tests/data")
        remotepath = './test'
        print("Transfering folder %s > %s" % (localpath, remotepath))
        sftp.xfer_r(localpath=localpath, remotepath=remotepath, preserve_mtime=True)

    if choice == '4':
        # verify existence of file on remote server
        remotepath = "./logs/20210908.zip"
        print(sftp.file_exists(remotepath))

    if choice == '5':
        # Transfer test file and verify transfer
        localpath = os.path.expanduser("~/Desktop/tmp/testfile")
        remotepath = "./test"
        sftp.put(localpath=localpath, remotepath=remotepath)
if __name__ == "__main__":
    main()
