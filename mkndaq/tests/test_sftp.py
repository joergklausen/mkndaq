# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 18:37:35 2020

@author: jkl
"""

import os
from ftplib import FTP
import io
import pandas as pd


host = '127.0.0.1'
port = '21'
usr = 'gast'
pwd = 'gast'
path = 'gast'
localdata = '~/git/gawkenya/daqman/data'
filename = 'CFKADS2329-20200131-083057Z-DataLog_User_Sync.dat'

# create local directory if it doesn't exists
localdata = os.path.join(os.path.expanduser(localdata),
                        path)
os.makedirs(localdata, exist_ok=True)
localfile = os.path.join(localdata, filename)

# create ftp context manager and transfer file
# with FTP(host) as ftp:
#     ftp.login(user=usr, passwd=pwd)
#     files = ftp.nlst()
    
#     with open(localfile, 'wb') as f:
#         response = ftp.retrbinary('RETR ' + filename,
#                        f.write)
#         f.close()

# read file into a pandas data.frame
virtual_file = io.BytesIO()
with FTP(host) as ftp:
    ftp.login(user=usr, passwd=pwd)
    ftp.retrbinary('RETR ' + filename,
                       virtual_file.write)
    virtual_file.seek(0)
    df = pd.read_fwf(virtual_file)
    virtual_file.close()