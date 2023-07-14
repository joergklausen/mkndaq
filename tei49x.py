# %%
import os
import argparse
from mkndaq.inst.tei49c import TEI49C
from mkndaq.inst.tei49i import TEI49I


def main():
    try:
        parser = argparse.ArgumentParser(
                            prog='tei49x',
                            description='Download lrec from 49c or 49i instrument. Results will be saved to a text file of name "tei49<x>_<s|l>rec-<yyyymmdd>.dat" or similar.',
                            epilog='author: joerg.klausen@meteoswiss.ch')
        parser.add_argument('-t', '--type', type=str, default='tei49i', help='Instrument type. One of <tei49c> or <tei49i>. Defaults to "tei49i".')
        parser.add_argument('-i', '--ID', type=str, default='49', help='Instrument ID. Defaults to 49.')
        parser.add_argument('-b', '--begin', type=str, help='Begin date. format: yyyy-mm-dd. NB: Not implemented, all data will be downloaded!')
        parser.add_argument('-e', '--end', type=str, help='End date. format: yyyy-mm-dd. NB: Not implemented, all data will be downloaded!')
        parser.add_argument('-c', '--COM', type=str, default=None, help='COM port. format: Number from 1 to 9. Defaults to 0, indicating no serial connection.')
        parser.add_argument('-n', '--host', type=str, default='0.0.0.0', help='host IP address. Defaults to 0.0.0.0, indicating no TCP/IP connection.')

        # parse arguments
        args = parser.parse_args()
        print(vars(args))
        if args.COM:
            serial_com = True
        else:
            serial_com = False

        # convert to cfg
        cfg = { 'data': '~/data',
                'logs': '~/logs',
                'staging': {'path': '~', 'zip': True},
                'reporting_interval': 1,
                'COM1': {'protocol': 'RS232', 'baudrate': 9600, 'bytesize': 8, 'stopbits': 1, 'parity': 'N', 'timeout': 0.1},
                'COM2': {'protocol': 'RS232', 'baudrate': 9600, 'bytesize': 8, 'stopbits': 1, 'parity': 'N', 'timeout': 0.1},
                'COM3': {'protocol': 'RS232', 'baudrate': 9600, 'bytesize': 8, 'stopbits': 1, 'parity': 'N', 'timeout': 0.1},
                'COM4': {'protocol': 'RS232', 'baudrate': 9600, 'bytesize': 8, 'stopbits': 1, 'parity': 'N', 'timeout': 0.1},
                'COM5': {'protocol': 'RS232', 'baudrate': 9600, 'bytesize': 8, 'stopbits': 1, 'parity': 'N', 'timeout': 0.1},
                'COM6': {'protocol': 'RS232', 'baudrate': 9600, 'bytesize': 8, 'stopbits': 1, 'parity': 'N', 'timeout': 0.1},
                'COM7': {'protocol': 'RS232', 'baudrate': 9600, 'bytesize': 8, 'stopbits': 1, 'parity': 'N', 'timeout': 0.1},
                'COM8': {'protocol': 'RS232', 'baudrate': 9600, 'bytesize': 8, 'stopbits': 1, 'parity': 'N', 'timeout': 0.1},
                'COM9': {'protocol': 'RS232', 'baudrate': 9600, 'bytesize': 8, 'stopbits': 1, 'parity': 'N', 'timeout': 0.1},
                'tei49c': { 'type': 'TEI49C', 
                            'id': int(f"{args.ID}"), 
                            'serial_number': 'unknown', 
                            'port': f"COM{args.COM}",
                            'get_config': ['mode', 'gas unit', 'range', 'avg time', 'temp comp', 'pres comp', 'format',
                                            'lrec format', 'o3 coef', 'o3 bkg'],
                            'set_config': ['set mode remote', 'set gas unit ppb', 'set range 1', 'set avg time 3',
                                            'set temp comp on', 'set pres comp on', 'set format 00', 'set lrec format 01 02',
                                            'set srec format 01 02', 'set save params'],
                            'get_data': 'lrec',
                            'data_header': 'time date flags o3 cellai cellbi bncht lmpt o3lt flowa flowb pres',
                            'sampling_interval': 1,
                            'staging_zip': True,
                            },
                'tei49i': { 'type': 'TEI49I', 
                            'id': int(f"{args.ID}"), 
                            'serial_number': 'unknown', 
                            'socket': {'host': f"{args.host}", 'port': 9880, 'timeout': 5, 'sleep': 0.5},
                            'port': f"COM{args.COM}",
                            'get_config': ['mode', 'gas unit', 'range', 'avg time', 'temp comp', 'pres comp', 'format',
                                            'lrec format', 'o3 coef', 'o3 bkg'],
                            'set_config': ['set mode remote', 'set gas unit ppb', 'set range 1', 'set avg time 3',
                                            'set temp comp on', 'set pres comp on', 'set format 00', 'set lrec format 01 02',
                                            'set srec format 01 02', 'set save params'],
                            'get_data': 'lrec',
                            'data_header': 'time date flags o3 cellai cellbi bncht lmpt o3lt flowa flowb pres',
                            'sampling_interval': 1,
                            'staging_zip': True,
                            }}
        # print(cfg)
        if args.type=='tei49i':
            tei49i = TEI49I(name='tei49i', config=cfg, serial_com=serial_com)
            tei49i.get_all_lrec()
        elif args.type=='tei49c':
            tei49c = TEI49C(name='tei49c', config=cfg)
            tei49c.get_all_rec()
        else:
            print("Argument '-t' must be one of tei49c|tei49i.")
        print('done.')

    except Exception as err:
        print(err)

# %%
if __name__ == "__main__":
    main()      