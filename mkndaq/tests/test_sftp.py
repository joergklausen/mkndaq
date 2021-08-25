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
    config_file = input("Enter full path to config file: ")
    config = configparser.config(config_file)

    sftp = SFTPClient(config)

    # find most recent log file
    # path = sftp.most_recent_logfile(os.path.expanduser(config['logs']))

    # transfer folder with subfolder(s)
    localpath = os.path.expanduser(config['staging']['path'])
    remotepath = '.'
    print("Transfering folder(s) %s > %s" % (localpath, remotepath))
    sftp.put_r(localpath=localpath, remotepath=remotepath, preserve_mtime=True)
    sftp.move_r()

    # transfer single file
    localpath = input("Enter full path to file to transfer: ")
    remotepath = None
    print("Transfering file %s > %s" % (localpath, remotepath))
    sftp.put(localpath=localpath, remotepath=remotepath, preserve_mtime=True)

    # transfer folder
    localpath = os.path.expanduser(config['logs'])
    remotepath = 'logs'
    print("Transfering folder %s > %s" % (localpath, remotepath))
    sftp.put_r(localpath=localpath, remotepath=remotepath, preserve_mtime=True)


if __name__ == "__main__":
    main()
