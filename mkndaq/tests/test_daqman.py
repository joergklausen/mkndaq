# -*- coding: utf-8 -*-

""" unit tests for filehandler package """

import logging
import io
# import os
# import sys
import unittest
import serial

from daqman.tei49c import TEI49C

class Tests(unittest.TestCase):
    """ unittests for pysolar package """

    @classmethod
    def setUpClass(cls):
        """ setup test cases and logging """
        # config_file = os.path.join(os.path.dirname(
                # os.path.dirname(__file__)), "config.yaml")

        
        cls.logger = logging.getLogger(__name__)
        cls.logger.info('Setup logfile and start testing')

    def test_serial_loopback(self, PORT='COM1'):
        """\
        Some tests for the serial module.
        Part of pyserial (http://pyserial.sf.net)  (C)2001-2009 cliechti@gmx.net
        Intended to be run on different platforms, to ensure portability of
        the code.
        This modules contains test for the interaction between Serial and the io
        library. This only works on Python 2.6+ that introduced the io library.
        For all these tests a simple hardware is required.
        Loopback HW adapter:
        Shortcut these pin pairs:
         TX  <-> RX
         RTS <-> CTS
         DTR <-> DSR
        On a 9 pole DSUB these are the pins (2-3) (4-6) (7-8)
        """
        self.s = serial.serial_for_url(PORT, timeout=0.1)
        self.io = io.TextIOWrapper(io.BufferedRWPair(self.s, self.s))

        self.io.write(b"hello\n".decode('utf-8'))
        self.io.flush()  # it is buffering. required to get the data out
        hello = self.io.readline()
        self.assertEqual(hello, b"hello\n".decode('utf-8'))

    def test_serial_loopback_2(self, PORT='COM1'):
        """\
        Some tests for the serial module.
        Part of pyserial (http://pyserial.sf.net)  (C)2001-2009 cliechti@gmx.net
        Intended to be run on different platforms, to ensure portability of
        the code.
        This modules contains test for the interaction between Serial and the io
        library. This only works on Python 2.6+ that introduced the io library.
        For all these tests a simple hardware is required.
        Loopback HW adapter:
        Shortcut these pin pairs:
         TX  <-> RX
         RTS <-> CTS
         DTR <-> DSR
        On a 9 pole DSUB these are the pins (2-3) (4-6) (7-8)
        """
        self.s = serial.Serial()
        self.s.port = PORT
        self.s.timeout = 0.1
        self.s.write(b"hello\n".decode('utf-8'))
        hello = self.s.readline()
        self.assertEqual(hello, b"hello\n".decode('utf-8'))

    def test_serial_com(self, PORT='COM1'):
        """\
        Some tests for the serial module.
        Part of pyserial (http://pyserial.sf.net)  (C)2001-2009 cliechti@gmx.net
        Intended to be run on different platforms, to ensure portability of
        the code.
        This modules contains test for the interaction between Serial and the io
        library. This only works on Python 2.6+ that introduced the io library.
        For all these tests a simple hardware is required.
        Loopback HW adapter:
        Shortcut these pin pairs:
         TX  <-> RX
         RTS <-> CTS
         DTR <-> DSR
        On a 9 pole DSUB these are the pins (2-3) (4-6) (7-8)
        """
        self.s = serial.serial_for_url(PORT, timeout=0.1)
        self.io = io.TextIOWrapper(io.BufferedRWPair(self.s, self.s))

        self.io.write(b"hello\n".decode('utf-8'))
        self.io.flush()  # it is buffering. required to get the data out
        hello = self.io.readline()
        self.assertEqual(hello, b"o3\n".decode('utf-8'))


#    def test_serial_interface_com1(self):
#        """ test access to serial interface COM1 """
#        self.logger.info('Testing access to serial interface COM1:')
#
#        # given
#        ftp = FTP(self.fh.host)
#
#        # when
#        ftp.login(user=self.fh.usr, passwd=self.fh.pwd)
#        res = ftp.voidcmd('NOOP')
#
#        # then
#        self.assertEqual(res, '200 Zzz...')
#
#    def test_from_wertematrix_file(self):
#        """ test from_wertematrix_file method """
#        self.logger.info('Testing from_wertematrix_file method')
#
#        # given
#        evg = 'A'
#        filename = 'A_201803_minute_values.csv'
#
#        # when
#        res = self.fh.from_wertematrix_file(evg, filename) 
#
#        # then
#        self.assertEqual(res.columns.values[0], 'dtm')
#
#
#    def test_from_solarlog_file(self):
#        """ test from_solarlog_file method """
#        self.logger.info('Testing from_solarlog_file method')
#
#        evg = 'A'
#        filename = 'min190524.csv'
#
#        # when
#        res = self.fh.from_solarlog_file(evg, filename)
#        
#        # then
#        self.assertEqual(res.columns.values[0], 'dtm')
        
#    def test_aggregate_data(self):
#        """ test data aggregation """
    

if __name__ == '__main__':
    unittest.main()

