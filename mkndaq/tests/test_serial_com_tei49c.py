# -*- coding: utf-8 -*-
"""
Tests for serial communication with TEI49C
"""

import serial
import time

def serial_com(cmd='lrec', port='COM1', id=49, cfg=[9800, 8, 'N', 1, 1], sleep=0.5):
    err = None
    resp = None
    try:
        # configure serial port
        ser = serial.Serial()
        ser.port = port
        ser.baudrate = cfg[0]
        ser.bytesize = cfg[1]
        ser.parity = cfg[2]
        ser.stopbits = cfg[3]
        ser.timeout = cfg[4]
        ser.open()
        if ser.is_open == True:
            print('%s successfully opened.' % port)
            msg = bytes([128+id]) + ('%s\x0D' % cmd).encode()
            print('sent (encoded): ', msg)
            ser.write(msg)
            time.sleep(sleep)

            while ser.in_waiting > 0:
                rcvd = rcvd + ser.read(1024)

            rcvd = rcvd.decode()
            print('received (decoded): ', resp)
            ser.close()

            if not ser.is_open:
                print("%s correctly closed." % port)

        return rcvd

    except Exception as err:
        print(err)


if __name__ == '__main__':

    port = 'COM2'
    print ("Testing on port %s ..." % port)
    cmds = ['o3', 'lrec', 'srec', 'set save params', 'lrec 100 100']
    
    for cmd in cmds:
        serial_com(cmd, port, sleep=0)

    print('done')
    