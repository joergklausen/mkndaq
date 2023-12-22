#!/bin/bash
HOST='192.168.2.129'
USER='mkn'
PASS='gaw'

TARGETFOLDER='meteo'
#SOURCEFOLDER='/home/moxa/bulletin/'
SOURCEFOLDER='/home/moxa/data/'

lftp -e "
open $HOST
user $USER $PASS
cd $TARGETFOLDER
#mput $SOURCEFOLDER/*
mput -E $SOURCEFOLDER/*
bye
"

