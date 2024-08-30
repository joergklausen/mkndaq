import logging
import time
from mkndaq.inst.neph import NEPH

cfg = {'ne300': {
                'type': 'NE300',
                'serial_number': '23-0690',
                'serial_id': 0,
                'protocol': 'acoem',
                'socket': {
                    'host': '192.168.3.149',
                    'port': 32783,
                    'timeout': 10,
                },
                'data_log': {
                    'parameters': [1000000, 2000000, 3000000, 6000000, 7000000, 8000000, 
                                   11000000, 12000000, 13000000, 14000000, 15000000, 16000000, 
                                   17000000, 18000000, 19000000, 20000000, 21000000, 26000000, 
                                   5001, 5002, 5003, 5004, 5005, 5006, 
                                   5010, 6007, 6008, 6001, 6002, 6003, 
                                   4035, 4036],
                    'wavelengths': [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,],
                    'angles': [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,],
                    'interval': 60, 
                },
                'get_data_interval': 2, # minutes. logger retrieval interval
                'zero_span_check_interval': 1500,
                'zero_check_duration': 30,
                'span_check_duration': 0,
                'staging_zip': True,  
                'verbosity': 2,  # 0: silent, 1: medium, 2: full          
            },
            'reporting_interval': 10,
            'logs': 'mkndaq/logs',
            'data': 'mkndaq/data',
            'staging': {
                'path': 'mkndaq/staging',
            },
}

ne300 = NEPH('ne300', cfg, verbosity=1)
# # Initialize NEPH (name: ne300  S/N: 23-0690)
#   Instrument identified itself as '{'Model': 158, 'Variant': 300, 'Sub-Type': 0, 'Range': 0, 'Build': 158, 'Branch': 300}'.

ne300.print_ssp_bssp()
