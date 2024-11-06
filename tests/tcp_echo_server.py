#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test server for TCP/IP communication.
Uses examples from
- https://pymotw.com/2/socket/tcp.html#easy-client-connections
- https://realpython.com/python-sockets/
"""

import socket

def echo_server(address):
    """
    Mimick a server that listens and responds.

    :param address:
    :param id:
    :return:
    """
    err = None
    try:
        # open socket connection as server
        with socket.socketpair(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(address)
            s.listen()
            print("Server %s listening ..." % address)

            conn, client_addr = s.accept()
            while True:
                try:
                    print("Client connected at %s:%s" % client_addr)
                    with conn:
                        while True:
                            data = conn.recv(1024)
                            if not data:
                                print("No more data received.")
                                break
                            else:
                                print("- received: %s" % data)
                                print("- returned: %s" % data)
                                conn.sendall(data)

                except Exception as err:
                    print(err)
                finally:
                    conn.close()
                    print('Client was disconnected, and server shut down. Goodbye.')
                    break

    except Exception as err:
        print("Server error:", err)


if __name__ == '__main__':
    addr_ip = '127.0.0.1'
    port = 9880
    address = (addr_ip, port)

    echo_server(address)