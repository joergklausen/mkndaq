#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 18:37:35 2020

@author: jkl
"""

import os
from mkndaq.utils import configparser
from mkndaq.utils.filetransfer import SFTPClient

def main():
    config_file = input("Enter full path to config file or <Enter> for default: ")
    if config_file == "":
        config_file = "C:/Users/jkl/Public/git/gaw-mkn-daq/dist/mkndaq.cfg"
    config = configparser.config(config_file)

    sftp = SFTPClient(config)

    menu = "[1] Transfer files in staging folder with subfolder(s)\n"
    menu += "[2] Transfer files in some other folder\n"
    menu += "[4] Verify remote file exists\n"

    choice = input(menu)
    if choice == '1':
        # transfer folder with subfolder(s)
        localpath = os.path.expanduser(config['staging']['path'])
        remotepath = '.'
        print("Transfering folder(s) %s > %s" % (localpath, remotepath))
        sftp.xfer_r(localpath=localpath, remotepath=remotepath, preserve_mtime=True)
        # sftp.put_r(localpath=localpath, remotepath=remotepath, preserve_mtime=True)
        # sftp.move_r()

    if choice == '2':
        # transfer folder with subfolder(s)
        localpath = os.path.expanduser("~/Desktop/tmp")
        remotepath = './test'
        print("Transfering folder(s) %s > %s" % (localpath, remotepath))
        sftp.xfer_r(localpath=localpath, remotepath=remotepath, preserve_mtime=True)

    if choice == '3':
        # transfer logs folder
        localpath = os.path.expanduser(config['logs'])
        remotepath = 'logs'
        print("Transfering folder %s > %s" % (localpath, remotepath))
        sftp.put_r(localpath=localpath, remotepath=remotepath, preserve_mtime=True)

    if choice == '4':
        # verify existence of file on remote server
        remotepath = "./logs/20210908.zip"
        print(sftp.file_exists(remotepath))
if __name__ == "__main__":
    main()
