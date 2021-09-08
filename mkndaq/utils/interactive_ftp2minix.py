#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Let user select a date and transfer all Picarro hourly files to the MKN Minix through local ftp.

@author joerg.klausen@meteoswiss.ch

@version v0.1-20210908
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
LOCAL_PATH_ROOT = "C:/UserData/DataLog_User_Sync/"

try:
    dte = raw_input("Type a date (YYYY/MM/DD) to transfer files or <Enter> to quit: ")
    while dte:
        # Let user choose a date
        LOCAL_PATH = LOCAL_PATH_ROOT + dte
        
        # connect to the FTP server
        ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
        res = ftp.cwd("./g2401")
        
        # force UTF-8 encoding
        ftp.encoding = "utf-8"

        # local files to upload
        files = glob.glob(LOCAL_PATH + "/*")
        print(files)
        for filename in files:
            # upload
            with open(filename, "rb") as fh:
                print("Uploading file %s" % filename)
                res = ftp.storbinary("STOR %s" % os.path.basename(filename), fh)
                print(res)
        time.sleep(1)
        dte = raw_input("Type a date (YYYY/MM/DD) to transfer files or <Enter> to quit: ")
    
except Exception as err:
    print(err)
    time.sleep(15)
