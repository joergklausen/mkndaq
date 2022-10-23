# -*- coding: utf-8 -*-
"""
Tests for TCP/IP communication with TEI49i.
Uses examples from
- https://pymotw.com/2/socket/tcp.html#easy-client-connections
-
"""

from mkndaq.inst.tei49i import TEI49I

if __name__ == '__main__':
    cfg = {'file': 'mkndaq.cfg', 'version': '1.0.0-20210802', 'home': 'c:/users/jkl', 'reporting_interval': 10,
           'sftp': {'host': 'sftp.meteoswiss.ch', 'usr': 'gaw_mkn',
                    'key': 'C:\\Users\\jkl/.ssh/private-open-ssh-4096-mkn.ppk', 'proxy': {'socks5': None, 'port': 1080},
                    'logs': 'C:\\Users\\jkl/Documents/mkndaq/logs'}, 'logs': 'C:\\Users\\jkl/Documents/mkndaq/logs',
           'data': 'C:\\Users\\jkl/Documents/mkndaq/data',
           'staging': {'path': 'C:\\Users\\jkl/Documents/mkndaq/staging', 'zip': True},
           'tei49i': {'type': 'TEI49I', 'id': 49, 'serial_number': 'unknown',
                      'socket': {'host': '192.168.3.190', 'port': 9880, 'timeout': 5, 'sleep': 0.5},
                      'get_config': ['mode', 'gas unit', 'range', 'avg time', 'temp comp', 'pres comp', 'format',
                                     'lrec format', 'o3 coef', 'o3 bkg'],
                      'set_config': ['set mode remote', 'set gas unit ppb', 'set range 1', 'set avg time 3',
                                     'set temp comp on', 'set pres comp on', 'set format 00', 'set lrec format 0',
                                     'set save params'], 'get_data': 'lr00',
                      'data_header': 'time date  flags o3 hio3 cellai cellbi bncht lmpt o3lt flowa flowb pres',
                      'staging_zip': True,
                      'sampling_interval': 1, 'logs': 'C:\\Users\\jkl/Documents/mkndaq/logs'},
           'tei49i_2': {'type': 'TEI49I', 'id': 50, 'serial_number': 'tei49i_2_sn',
                      'socket': {'host': '192.168.3.173', 'port': 9880, 'timeout': 5, 'sleep': 0.5},
                      'get_config': ['mode', 'gas unit', 'range', 'avg time', 'temp comp', 'pres comp', 'format',
                                     'lrec format', 'o3 coef', 'o3 bkg'],
                      'set_config': ['set mode remote', 'set gas unit ppb', 'set range 1', 'set avg time 3',
                                     'set temp comp on', 'set pres comp on', 'set format 00', 'set lrec format 0',
                                     'set save params'], 'get_data': 'lr00',
                      'data_header': 'time date  flags o3 hio3 cellai cellbi bncht lmpt o3lt flowa flowb pres',
                      'staging_zip': True,
                      'sampling_interval': 1, 'logs': 'C:\\Users\\jkl/Documents/mkndaq/logs'},
           }

    print("# Setup tei49i")
    tei49i = TEI49I('tei49i', config=cfg, simulate=False)
    print(tei49i)

    print("# Setup tei49i_2")
    tei49i_2 = TEI49I('tei49i_2', config=cfg, simulate=False)
    print(tei49i_2)

    print("# Get tei49i config")
    print(tei49i.get_config())

    print("# Get tei49i_2 config")
    print(tei49i_2.get_config())

    run = True
    while run:
        instrument = input("Enter 1 for TEI49i, 2 for TEI49i_2, or Q to quit:")
        if instrument != "Q":
            cmd = input("Enter command or Q to quit: ")
            if instrument=="1" and cmd != "Q":
                print(f"Addressing instrument {instrument}")
#                tei49i = TEI49I('tei49i', config=cfg, simulate=False)
                print(tei49i.get_data(cmd, save=False))
            elif instrument=="2" and cmd != "Q":
                print(f"Addressing instrument {instrument}")
#                tei49i_2 = TEI49I('tei49i_2', config=cfg, simulate=False)
                print(tei49i_2.get_data(cmd, save=False))
            else:
                run = False
        else:
            run = False
    print('done')

    cmd = input("Enter any key to download all lrecs from both instruments or Q to quit: ")
    if cmd != "Q":
        print("# Setup tei49i and download all data")
        tei49i = TEI49I('tei49i', config=cfg, simulate=False)
        tei49i.get_all_lrec()

        print("# Setup tei49i_2 and download all data")
        tei49i = TEI49I('tei49i_2', config=cfg, simulate=False)
        tei49i.get_all_lrec()
    print('done')

    # print("# Setup tei49i, get config")
    # tei49i = TEI49I('tei49i', config=cfg, simulate=False)
    # print(tei49i.get_config())

    # print("# Setup tei49i_2, get config")
    # tei49i_2 = TEI49I('tei49i_2', config=cfg, simulate=False)
    # print(tei49i_2.get_config())

    # run = True
    # while run:
    #     instrument = input("Enter 1 for TEI49i, 2 for TEI49i_2, or Q to quit:")
    #     if instrument != "Q":
    #         cmd = input("Enter command or Q to quit: ")
    #         if instrument=="1" and cmd != "Q":
    #             print(f"Addressing instrument {instrument}")
    #             tei49i = TEI49I('tei49i', config=cfg, simulate=False)
    #             print(tei49i.get_data(cmd, save=False))
    #         elif instrument=="2" and cmd != "Q":
    #             print(f"Addressing instrument {instrument}")
    #             tei49i = TEI49I('tei49i_2', config=cfg, simulate=False)
    #             print(tei49i.get_data(cmd, save=False))
    #         else:
    #             run = False
    #     else:
    #         run = False
    # print('done')

    # cmd = input("Enter any key to download all lrecs from both instruments or Q to quit: ")
    # if cmd != "Q":
    #     print("# Setup tei49i and download all data")
    #     tei49i = TEI49I('tei49i', config=cfg, simulate=False)
    #     tei49i.get_all_lrec()

    #     print("# Setup tei49i_2 and download all data")
    #     tei49i = TEI49I('tei49i_2', config=cfg, simulate=False)
    #     tei49i.get_all_lrec()
    # print('done')

