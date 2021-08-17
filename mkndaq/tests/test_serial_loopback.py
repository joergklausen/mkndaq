# -*- coding: utf-8 -*-
"""
Tests for serial communication with TEI49C
"""

import sys
import glob
import serial
import time


def find_serial_ports():
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

    found = []
    for port in ports:
        try:
            s = serial.Serial(port)
            print("Found %s: %s" % (port, s.getSettingsDict()))
            s.close()
            found.append(port)
        except (OSError, serial.SerialException):
            pass
    return found


def test_serial_loopback(port='COM1', cfg=None, sleep=0.5, cmd="Hello, World"):
    if cfg is None:
        cfg = [9800, 8, 'N', 1, 1]
    err = None

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
        rcvd = b''
        if ser.is_open:
            print('%s successfully opened.' % port)
            msg = ('%s\x0D' % cmd).encode()
            print('sent (encoded): ', msg)
            ser.write(msg)
            time.sleep(sleep)

            while ser.in_waiting > 0:
                rcvd = rcvd + ser.read(1024)
                time.sleep(0.1)

            rcvd = rcvd.decode()

            print('response (decoded): ', rcvd)
            ser.close()
            if not ser.is_open:
                print("%s correctly closed." % port)
        else:
            raise

        return rcvd

    except Exception as err:
        print(err)


if __name__ == '__main__':
    serial_ports = find_serial_ports()
    print("Serial ports found: %s" % serial_ports)

    for port in serial_ports:
        print(port, test_serial_loopback(port))
