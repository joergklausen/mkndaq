# -*- coding: utf-8 -*-
"""
Tests for TCP/IP communication with TEI49i.
Uses examples from
- https://pymotw.com/2/socket/tcp.html#easy-client-connections
-
"""

from mkndaq.inst.tei49i import TEI49I


if __name__ == '__main__':
    tei49i = TEI49I('tei49i', simulate=True)
    tei49i.get_config()
    tei49i.set_config()
    tei49i.get_data('lrec')

    print('done')

