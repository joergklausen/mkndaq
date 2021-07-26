# -*- coding: utf-8 -*-
"""
Tests for serial communication with TEI49C
"""

import sys
import glob
import serial
import time


def serial_ports():
    """ Lists serial port names

        :raises EnvironmentError:
            On unsupported or unknown platforms
        :returns:
            A list of the serial ports available on the system
    """
    if sys.platform.startswith('win'):
        ports = ['COM%s' % (i + 1) for i in range(256)]
    elif sys.platform.startswith('linux') or sys.platform.startswith('cygwin'):
        # this excludes your current terminal "/dev/tty"
        ports = glob.glob('/dev/tty[A-Za-z]*')
    elif sys.platform.startswith('darwin'):
        ports = glob.glob('/dev/tty.*')
    else:
        raise EnvironmentError('Unsupported platform')

    result = []
    for port in ports:
        try:
            s = serial.Serial(port)
            s.close()
            result.append(port)
        except (OSError, serial.SerialException):
            pass
    return result

def test_tei49c(cmd='lrec', port='COM1', id=49, timeout=1):
    try:
        # configure serial port
        ser = serial.Serial()
        ser.port = port
        ser.baudrate = 9600
        ser.bytesize = 8
        ser.parity = 'N'
        ser.stopbits = 1
        ser.timeout = timeout
        ser.open()
        if ser.is_open == True:
            print('%s successfully opened.' % port)
        else:
            raise
            
        id += 128
        msg = bytes([id]) + ('%s\x0D' % cmd).encode()
        print('message: ', msg)
        ser.write(msg)
        time.sleep(0.5)
        print('response: ', ser.read(256).decode())
        ser.close()
    except Exception as err:
        print(err)
        ser.close()
        
def test_tei49c_v1(cmd='lrec', port='COM1', id=49, timeout=1):
    try:
        # configure serial port
        ser = serial.Serial()
        ser.port = port
        ser.baudrate = 9600
        ser.bytesize = 8
        ser.parity = 'N'
        ser.stopbits = 1
        ser.timeout = timeout
        ser.open()
        if ser.is_open == True:
            print('%s successfully opened.' % port)
        else:
            raise
            
        id += 128
        ser.write(("%s%s\x0D" % (hex(id), cmd)).encode())
        time.sleep(0.5)
        print('1) using hex() and readline:')
        print(ser.readline().decode())
    except Exception as err:
        print(err)
        
def test_tei49c_v2(cmd='lrec', port='COM1', id=49, timeout=1):
    try:
        # configure serial port
        ser = serial.Serial()
        ser.port = port
        ser.baudrate = 9600
        ser.bytesize = 8
        ser.parity = 'N'
        ser.stopbits = 1
        ser.timeout = timeout
        ser.open()
        if ser.is_open == True:
            print('%s successfully opened.' % port)
        else:
            raise
            
        id += 128
        ser.write(("%s%s\x0D" % (bytes([id]), cmd)).encode())
        time.sleep(0.5)
        print('2) using bytes() and read():')
        print(ser.read(100).decode())
    except Exception as err:
        print(err)
    
def test_tei49c_v3(cmd='lrec', port='COM1', id=49, timeout=1):
    try:
        # configure serial port
        ser = serial.Serial()
        ser.port = port
        ser.baudrate = 9600
        ser.bytesize = 8
        ser.parity = 'N'
        ser.stopbits = 1
        ser.timeout = timeout
        ser.open()
        if ser.is_open == True:
            print('%s successfully opened.' % port)
        else:
            raise
            
        id += 128
        ser.write(("\\x%s%s\x0D" % ('{0:X}'.format(id), cmd)).encode())
        time.sleep(0.5)
        print('3) using {0:X}.format() and readline():')
        print(ser.readline().decode())
    except Exception as err:
        print(err)

def test_tei49c_v4(cmd='lrec', port='COM1', id=49, timeout=1):
    try:
        # configure serial port
        ser = serial.Serial()
        ser.port = port
        ser.baudrate = 9600
        ser.bytesize = 8
        ser.parity = 'N'
        ser.stopbits = 1
        ser.timeout = timeout
        ser.open()
        if ser.is_open == True:
            print('%s successfully opened.' % port)
        else:
            raise
            
        id += 128
        ser.write(("%s%s\x0D" % ('{0:X}'.format(id), cmd)).encode())
        time.sleep(0.5)
        print('4) using {0:X}.format() and read():')
        print(ser.read(100))
    except Exception as err:
        print(err)
    
def test_tei49c_v5(cmd='lrec', port='COM1', id=49, timeout=1):
    try:
        # configure serial port
        ser = serial.Serial()
        ser.port = port
        ser.baudrate = 9600
        ser.bytesize = 8
        ser.parity = 'N'
        ser.stopbits = 1
        ser.timeout = timeout
        ser.open()
        if ser.is_open == True:
            print('%s successfully opened.' % port)
        else:
            raise
            
        id += 128
        ser.write(("%s%s\x0D" % (bytes([id]), cmd)).encode())
        time.sleep(0.5)
        print('5) using bytes() and readline():')
        print(ser.readline().decode())
    except Exception as err:
        print(err)
    


if __name__ == '__main__':
    print(serial_ports())
    
    port = 'COM2'
    cmd = 'o3'
    
    test_tei49c(cmd, port)
    
    # test_tei49c_v1(cmd, port)
    
    # test_tei49c_v2(cmd, port)
    
    # test_tei49c_v3(cmd, port)
    
    # test_tei49c_v4(cmd, port)
    
    # test_tei49c_v5(cmd, port)
    
    print('done')
    