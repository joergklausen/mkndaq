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
        config_file = os.path.expanduser("~/Public/git/mkndaq/dist/mkndaq.cfg")
    config = configparser.config(config_file)

    sftp = SFTPClient(config)

    menu = "[1] Test connection to sftp server\n"
    menu += "[2] Transfer folder with subfolder(s) and files\n"

    choice = input(menu)
    if choice == '1':
        # test connection to sftp server
        remotepath = '.'
        if sftp.is_alive():
            print("SFTP server is alive.")
        else:
            print("SFTP not reachable.")

    if choice == '2':
        # transfer files in folder(s)
        localpath = os.path.expanduser("~/Public/git/mkndaq/mkndaq/tests/data")
        remotepath = '.'
        sftp.setup_remote_folders(localpath=localpath)
        print("Transfering folder %s > %s" % (localpath, remotepath))
        sftp.xfer_r(localpath=localpath, remotepath=remotepath)


if __name__ == "__main__":
    main()
