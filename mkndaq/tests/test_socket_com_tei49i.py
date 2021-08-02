# -*- coding: utf-8 -*-
"""
Tests for TCP/IP communication with TEI49i.
Uses examples from
- https://pymotw.com/2/socket/tcp.html#easy-client-connections
-
"""

import socket
import time

def socket_com(address, cmd='lrec', id=49):
    rcvd = None
    try:
        # open socket connection as a client
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # connect to the server
            s.connect(address)
            print("Connection opened to", address)

            # send data
            msg = bytes([128 + id]) + ('%s\x0D' % cmd).encode()
            print('sent (encoded): ', msg)
            s.sendall(msg)

            # receive rcvdonse
            try:
                rcvd = b''
                while True:
                    data = s.recv(1024)
                    rcvd = rcvd + data
                    if b'\x0D' in data:
                        break
            except Exception as err:
                print(err)
            rcvd = rcvd.decode()
            print('received (decoded): ', rcvd)

        return rcvd

    except Exception as err:
        print(err)

    finally:
        # close socket connection
        s.close()
        print('Connection closed.')


if __name__ == '__main__':

    # TEI 49i communication uses port 9880
    addr_ip = '192.168.1.200'
    port = 9880
    address = (addr_ip, port)

    cmds = ["set format 01", # this doesn't seem to work
            "temp comp",
            "pres comp",
            "o3 bkg",
            "o3 coef",
            "high o3 coef",
            "low o3 coef",
            "lrec",
            "erec",
            "srec",
            "lr00", # this is really what we will use operationally.
            "instr name",
            "flags",
            "lrec layout",
            "erec layout",
            "srec layout",
            "set gas unit ppb",
            "date",
            "set lrec format 0",
            "lrec 100 5",
            "list lrec",
            "time",
            "set save params",
            ]
    # send command and receive response
    for cmd in cmds:
        socket_com(address, cmd=cmd, id=49)

    print('done')