# -*- coding: utf-8 -*-
"""
Tests for serial communication with TEI49C
"""


from mkndaq.inst.tei49c import TEI49C


if __name__ == '__main__':
    tei49c = TEI49C('tei49c', simulate=True)
    tei49c.get_config()
    tei49c.set_config()
    tei49c.get_data('lrec')

    print('done')
