#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Source: https://www.tutorialspoint.com/python_penetration_testing/python_penetration_testing_network_scanner.htm
"""

from socket import *
import time

startTime = time.time()

if __name__ == '__main__':
    target = input('Enter the host to be scanned: ')
    t_IP = gethostbyname(target)
    print('Starting scan on host: ', t_IP)

    for i in range(50, 500):
        s = socket(AF_INET, SOCK_STREAM)

        conn = s.connect_ex((t_IP, i))
        if (conn == 0):
            print('Port %d: OPEN' % (i,))
        s.close()
print('Time taken:', time.time() - startTime)