#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Identify the current (being written to) and 2nd recent (completed) Picarro hourly files and transmit to the MKN Minix through local ftp.

@author joerg.klausen@meteoswiss.ch

@version v0.1-20210907
"""
import os
import glob
import ftplib
import time

# configuration
FTP_HOST = "192.168.0.10"
FTP_DIR = "g2401"
FTP_USER = "mkn"
FTP_PASS = "gaw"
LOCAL_PATH = "C:/UserData/DataLog_User_Sync/" + time.strftime("%Y/%m/%d")

try:
    # connect to the FTP server
    ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
    res = ftp.cwd("./g2401")
    
    # force UTF-8 encoding
    ftp.encoding = "utf-8"

    # local files to upload
    files = sorted(glob.glob(LOCAL_PATH + "/*"), key=os.path.getmtime, reverse=True)[0:2]
    print(files)
    for filename in files:
        # upload
        with open(filename, "rb") as fh:
            res = ftp.storbinary("STOR %s" % os.path.basename(filename), fh)
            print(res)
    time.sleep(1)
    
except Exception as err:
    print(err)
    time.sleep(15)
