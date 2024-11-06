#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Created on Tue Feb 11 18:37:35 2020.

@author: jkl
"""
# %%
import os
from mkndaq.utils.utils import load_config
from mkndaq.utils.sftp import SFTPClient

def main():
    """Test suite for sftp."""
    config_file = input("Enter full path to config file or <Enter> for default: ")
    if config_file == "":
        config_file = os.path.expanduser("dist/mkndaq.yml")
    config = load_config(config_file)

    sftp = SFTPClient(config)

    menu = "[1] Test connection to sftp server\n"
    menu += "[2] Transfer folder with subfolder(s) and files\n"

    choice = input(menu)
    if choice == '1':
        # test connection to sftp server
        # remote_path = '.'
        if sftp.is_alive():
            print("SFTP server is alive.")
        else:
            print("SFTP not reachable.")

    if choice == '2':
        # transfer files in folder(s)
        local_path = os.path.expanduser("~/Public/git/mkndaq/mkndaq/tests/data")
        remote_path = '.'
        sftp.setup_remote_folders(local_path=local_path)
        print("Transfering folder %s > %s" % (local_path, remote_path))
        sftp.transfer_files(local_path=local_path, remote_path=remote_path)


if __name__ == "__main__":
    main()
