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
    config = configparser.config()

    sftp = SFTPClient(config)

    # transfer single file
    localpath = os.path.abspath(os.path.join(os.pardir, "mkndaq.cfg"))
    remotepath = None
    print("Transfering file %s > %s" % (localpath, remotepath))
    sftp.put(localpath=localpath, remotepath=remotepath, preserve_mtime=True)

    # transfer folder
    localpath = os.path.expanduser(config['logs'])
    remotepath = 'logs'
    print("Transfering folder %s > %s" % (localpath, remotepath))
    sftp.put_r(localpath=localpath, remotepath=remotepath, preserve_mtime=True)

    # transfer folder with subfolder(s)
    localpath = os.path.expanduser(config['staging']['path'])
    remotepath = 'data'
    print("Transfering folder %s > %s" % (localpath, remotepath))
    sftp.put_r(localpath=localpath, remotepath=remotepath, preserve_mtime=True)

if __name__ == "__main__":
    main()
