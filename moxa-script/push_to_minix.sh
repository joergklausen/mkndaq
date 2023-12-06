#!/bin/bash
HOST='192.168.2.129'
USER='mkn'
PASS='gaw'

TARGETFOLDER='meteo'
SOURCEFOLDER='/bulletin/'

lftp -e "
open $HOST
user $USER $PASS
cd $TARGETFOLDER
#mput $SOURCEFOLDER/*
mput -E $SOURCEFOLDER/*
bye
"
